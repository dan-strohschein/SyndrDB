package bundle

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"

	indexutils "syndrdb/src/internal/domain/index"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	hashindex "syndrdb/src/internal/domain/index/hashindexV3"

	"syndrdb/src/internal/registry"
)

func (s *BundleService) AddDocumentToBundle(database *models.Database, bundle *models.Bundle, docCommand *models.DocumentCommand) (string, error) {
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add document")
		return "", fmt.Errorf("bundle '%s' is nil, cannot add document ", docCommand.BundleName)
	}

	// bundle, err := s.GetBundleByName(database, docCommand.BundleName)
	// if err != nil {
	// 	return "", fmt.Errorf("bundle '%s' not found", docCommand.BundleName)
	// }

	// CRITICAL: Process NULL values and defaults FIRST, before validation
	// This allows default value substitution for required fields that are missing or NULL
	// Must happen before validation so that required fields with defaults can be satisfied
	nullStart := time.Now()
	err := s.processNullValues(bundle, docCommand)
	nullDuration := time.Since(nullStart)
	if nullDuration > 1*time.Millisecond {
		s.logger.Warnf("  ⚠️  processNullValues took %v", nullDuration)
	} else {
		s.logger.Debugf("  ✓ processNullValues took %v", nullDuration)
	}
	if err != nil {
		return "", fmt.Errorf("failed to process NULL values: %w", err)
	}

	// Validate document fields against bundle field definitions
	// This runs AFTER processNullValues so that default values are already substituted
	validateStart := time.Now()
	err = s.validateDocumentFields(bundle, docCommand)
	validateDuration := time.Since(validateStart)
	if validateDuration > 1*time.Millisecond {
		s.logger.Warnf("  ⚠️  validateDocumentFields took %v", validateDuration)
	} else {
		s.logger.Debugf("  ✓ validateDocumentFields took %v", validateDuration)
	}
	if err != nil {
		return "", fmt.Errorf("document field validation failed: %w", err)
	}

	// Validate unique constraints for all IsUnique fields
	uniqueStart := time.Now()
	uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	err = uniqueValidator.ValidateUniqueConstraints(bundle, docCommand)
	uniqueDuration := time.Since(uniqueStart)
	if uniqueDuration > 1*time.Millisecond {
		s.logger.Warnf("  ⚠️  ValidateUniqueConstraints took %v", uniqueDuration)
	} else {
		s.logger.Debugf("  ✓ ValidateUniqueConstraints took %v", uniqueDuration)
	}
	if err != nil {
		// Return the error directly - it's already a properly typed SyndrDBError
		// with ERR_VALIDATION_CONSTRAINT code that won't log stack traces
		return "", err
	}

	// FK validation: verify referenced parent documents exist
	// Only check outgoing relationships (this bundle's FK fields pointing to parent bundles).
	// Incoming relationships (other bundles referencing this one) are irrelevant for INSERT —
	// we're adding a new document, not changing an existing PK that others depend on.
	if len(bundle.Relationships) > 0 {
		validator := NewReferentialIntegrityValidator(s, s.logger)
		bundleCache := make(map[string]*models.Bundle)

		// Build field map from document command
		insertFields := make(map[string]string)
		for _, kv := range docCommand.Fields {
			if kv.Value == nil {
				continue
			}
			insertFields[kv.Key] = fmt.Sprintf("%v", kv.Value)
		}

		// Identify which fields are foreign keys (nil database = skip incoming relationship scan)
		foreignKeyUpdates := validator.IdentifyForeignKeyFields(nil, bundle, insertFields, bundleCache)

		if len(foreignKeyUpdates) > 0 {
			// Validate each FK — uses O(1) hash index lookups
			validationCache := make(map[string]*ForeignKeyViolation)
			violation := validator.batchValidateForeignKeys(bundle, []string{"new-document"}, foreignKeyUpdates, validationCache)
			if violation != nil {
				return "", fmt.Errorf("referential integrity violation: %s", violation.Error())
			}
		}
	}

	schema := bundle.DocumentStructure.FieldSchema()
	newDocument := s.documentFactory.NewDocument(*docCommand, schema)

	// DIAGNOSTIC: Log bundle index status (only if verbose logging enabled)
	if s.verboseLogging {
		s.logger.Debugf("DIAGNOSTIC: Bundle '%s' has Indexes map: %v, count: %d", bundle.Name, bundle.Indexes != nil, len(bundle.Indexes))
		if len(bundle.Indexes) > 0 {
			for idxName := range bundle.Indexes {
				s.logger.Debugf("DIAGNOSTIC: Found index: %s", idxName)
			}
		}
	}

	// Schedule deferred index updates for optimal performance instead of immediate updates
	// Schedule deferred metadata update instead of immediate calculation
	s.scheduleMetadataUpdate(docCommand.BundleName, "increment_docs", 1)

	// Add document to bundle file (storage layer handles page allocation and returns pageID)
	pageID, err := s.store.AddDocumentToBundleFile(bundle, newDocument)
	if err != nil {
		// Note: Metadata updates are deferred, so no rollback needed here
		// Failed operations won't have their metadata updates applied
		return "", fmt.Errorf("failed to add document to bundle: %w", err)
	}

	// WRITE-THROUGH CACHE: Immediately update page cache so reads see this document
	// This is the core write-through mechanism - after WriteBuffer commits, we update
	// the in-memory cache so subsequent reads don't need a disk round-trip
	s.updatePageCacheWithDocument(bundle.Name, pageID, newDocument)

	if s.statsUpdater != nil {
		if schema != nil && len(newDocument.Values) > 0 {
			for i, name := range schema.Names {
				if i < len(newDocument.Values) {
					s.statsUpdater.IncrementalUpdate(bundle.Name, name, nil, newDocument.Values[i].AsInterface(), bundle.TotalDocuments)
				}
			}
		} else if newDocument.Data != nil {
			for fieldName, v := range newDocument.Data {
				s.statsUpdater.IncrementalUpdate(bundle.Name, fieldName, nil, v, bundle.TotalDocuments)
			}
		}
	}

	// Now schedule index updates with the actual pageID from storage
	//indexStart := time.Now()
	indexCount := 0
	if bundle.Indexes != nil {
		// Look for indexes and schedule updates
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Scheduling deferred update for index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" {
				if indexRef.WherePredicate != "" {
					pred, predErr := compilePartialIndexPredicate(indexRef.WherePredicate, schema)
					if predErr != nil || !pred(newDocument) {
						continue
					}
				}

				fieldName := indexRef.HashIndexField.FieldName
				var fieldValue interface{}
				if fieldName == "DocumentID" {
					fieldValue = newDocument.DocumentID
				} else {
					extractedValue, err := extractFieldValueForIndex(*newDocument, fieldName, schema)
					if err != nil {
						s.logger.Warnf("[HASH-IDX] Skipped indexing document %s for index %s: field %q not in document (%v); check field name case (e.g. ID vs id)",
							newDocument.DocumentID, indexName, fieldName, err)
						continue
					}
					fieldValue = extractedValue
				}

				// Schedule hash index update with actual pageID; pass commitSeq/versionSeq to avoid GetDocument.
				// deferred=false for ADD operations (read-your-own-writes consistency)
				s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", newDocument.DocumentID, fieldValue, pageID, nil, false, newDocument.CommitSequence, newDocument.VersionSequence)
				s.logger.Debugf("Scheduled hash index '%s' update for document '%s' on field '%s' (page %d)",
					indexName, newDocument.DocumentID, fieldName, pageID)
				indexCount++

			} else if indexRef.IndexType == "btree" {
				if indexRef.WherePredicate != "" {
					pred, predErr := compilePartialIndexPredicate(indexRef.WherePredicate, schema)
					if predErr != nil || !pred(newDocument) {
						continue
					}
				}

				var fieldValue interface{}
				if indexRef.Expression != "" {
					// Use cached compiled expression, falling back to recompilation
					if err := indexRef.EnsureExpressionCompiled(indexutils.CompileIndexExpressionAST); err != nil {
						s.logger.Warnf("Failed to compile expression for index '%s': %v", indexName, err)
						continue
					}
					if indexRef.CompiledExpr != nil && len(indexRef.CompiledExpr.FieldNames) > 1 {
						// Multi-field expression: build full field map
						fieldMap := make(map[string]interface{}, len(indexRef.CompiledExpr.FieldNames))
						for _, fn := range indexRef.CompiledExpr.FieldNames {
							v, fErr := extractFieldValueForIndex(*newDocument, fn, schema)
							if fErr == nil {
								fieldMap[fn] = v
							}
						}
						fieldValue = indexRef.CompiledExpr.EvalFn(fieldMap)
					} else {
						rawValue, fErr := extractFieldValueForIndex(*newDocument, indexRef.BTreeIndexField.FieldName, schema)
						if fErr != nil {
							continue
						}
						if indexRef.CompiledExpr != nil {
							fieldMap := map[string]interface{}{indexRef.CompiledExpr.PrimaryField: rawValue}
							fieldValue = indexRef.CompiledExpr.EvalFn(fieldMap)
						} else {
							fieldValue = rawValue
						}
					}
				} else {
					var fErr error
					fieldValue, fErr = extractFieldValueForIndex(*newDocument, indexRef.BTreeIndexField.FieldName, schema)
					if fErr != nil {
						s.logger.Warnf("Failed to extract field value for document '%s': %v", newDocument.DocumentID, fErr)
						continue
					}
				}

				// Schedule BTree index update with actual pageID
				// deferred=false for ADD operations (read-your-own-writes consistency)
				s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", newDocument.DocumentID, fieldValue, pageID, nil, false)
				s.logger.Debugf("Scheduled BTree index update for document '%s' on field '%s' (page %d)",
					newDocument.DocumentID, indexRef.BTreeIndexField.FieldName, pageID)
				indexCount++
			}
		}
	}
	//indexDuration := time.Since(indexStart)
	// if indexDuration > 1*time.Millisecond {
	// 	s.logger.Warnf("  ⚠️  Index scheduling (%d indexes) took %v", indexCount, indexDuration)
	// } else {
	// 	s.logger.Debugf("  ✓ Index scheduling (%d indexes) took %v", indexCount, indexDuration)
	// }
	if bundle.Indexes == nil {
		s.logger.Warnf("No indexes found for bundle '%s'", bundle.Name)
	}

	// SNAPSHOT ISOLATION: No invalidation needed - scanners filter documents by MVCC visibility
	// Documents inserted after scanner creation are filtered out during iteration
	// This avoids cache churn and enables consistent reads without destroying scanners
	s.logger.Debugf("INSERT completed for bundle '%s' page %d - scanners use snapshot isolation", docCommand.BundleName, pageID)

	// PERFORMANCE: Invalidate JOIN hash table cache for this bundle
	// This ensures subsequent JOINs rebuild hash tables with the new document
	s.invalidateBundleCaches(bundle.Name)

	return newDocument.DocumentID, nil
}

// AddDocumentToBundleWithTxID is a transaction-aware wrapper for AddDocumentToBundle.
// When preallocDocID is non-empty, the document is created with that ID (Phase 3: document-level lock).
func (s *BundleService) AddDocumentToBundleWithTxID(database *models.Database, bundle *models.Bundle, docCommand *models.DocumentCommand, txID string, preallocDocID ...string) (string, error) {
	docID := ""
	if len(preallocDocID) > 0 {
		docID = preallocDocID[0]
	}
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add document")
		return "", fmt.Errorf("bundle '%s' is nil, cannot add document ", docCommand.BundleName)
	}

	// CRITICAL: Process NULL values and defaults FIRST, before validation
	err := s.processNullValues(bundle, docCommand)
	if err != nil {
		return "", fmt.Errorf("failed to process NULL values: %w", err)
	}

	// Validate document fields against bundle field definitions
	err = s.validateDocumentFields(bundle, docCommand)
	if err != nil {
		return "", fmt.Errorf("document field validation failed: %w", err)
	}

	// Validate unique constraints for all IsUnique fields
	uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	err = uniqueValidator.ValidateUniqueConstraints(bundle, docCommand)
	if err != nil {
		// Return the error directly - it's already a properly typed SyndrDBError
		// with ERR_VALIDATION_CONSTRAINT code that won't log stack traces
		return "", err
	}

	schema := bundle.DocumentStructure.FieldSchema()
	var newDocument *models.Document
	if docID != "" {
		newDocument = s.documentFactory.NewDocumentWithID(*docCommand, docID, schema)
	} else {
		newDocument = s.documentFactory.NewDocument(*docCommand, schema)
	}

	// Set MVCC version metadata
	s.setDocumentVersionFields(newDocument, txID, 1)

	s.scheduleMetadataUpdate(docCommand.BundleName, "increment_docs", 1)

	pageID, err := s.store.AppendDocumentToBundleFileWithTxID(bundle, newDocument, txID)
	if err != nil {
		return "", fmt.Errorf("failed to add document to bundle: %w", err)
	}

	// WRITE-THROUGH CACHE: Immediately update page cache so reads see this document
	s.updatePageCacheWithDocument(bundle.Name, pageID, newDocument)

	// Schedule index updates with the actual pageID from storage
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Scheduling deferred update for index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" {
				fieldName := indexRef.HashIndexField.FieldName
				var fieldValue interface{}
				if fieldName == "DocumentID" {
					fieldValue = newDocument.DocumentID
				} else {
					extractedValue, err := extractFieldValueForIndex(*newDocument, fieldName, schema)
					if err != nil {
						s.logger.Warnf("[HASH-IDX] Skipped indexing document %s for index %s: field %q not in document (%v)",
							newDocument.DocumentID, indexName, fieldName, err)
						continue
					}
					fieldValue = extractedValue
				}

				// deferred=false for ADD operations; pass commitSeq/versionSeq to avoid GetDocument.
				s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", newDocument.DocumentID, fieldValue, pageID, nil, false, newDocument.CommitSequence, newDocument.VersionSequence)
				s.logger.Debugf("Scheduled hash index '%s' update for document '%s' on field '%s' (page %d)",
					indexName, newDocument.DocumentID, fieldName, pageID)

			} else if indexRef.IndexType == "btree" {
				fieldValue, err := extractFieldValueForIndex(*newDocument, indexRef.BTreeIndexField.FieldName, schema)
				if err != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", newDocument.DocumentID, err)
					continue
				}

				// deferred=false for ADD operations (read-your-own-writes consistency)
				s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", newDocument.DocumentID, fieldValue, pageID, nil, false)
				s.logger.Debugf("Scheduled BTree index update for document '%s' on field '%s' (page %d)",
					newDocument.DocumentID, indexRef.BTreeIndexField.FieldName, pageID)
			}
		}
	}

	// SNAPSHOT ISOLATION: No invalidation needed - scanners filter documents by MVCC visibility
	// Documents inserted after scanner creation are filtered out during iteration
	// This avoids cache churn and enables consistent reads without destroying scanners
	s.logger.Debugf("INSERT completed for bundle '%s' page %d - scanners use snapshot isolation", docCommand.BundleName, pageID)

	return newDocument.DocumentID, nil
}

// AddDocumentsToBundle inserts multiple documents in a single batch operation.
// All documents are validated upfront (all-or-nothing semantics).
func (s *BundleService) AddDocumentsToBundle(database *models.Database, bundle *models.Bundle, bulkCmd *models.BulkDocumentCommand) ([]string, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle '%s' is nil, cannot add documents", bulkCmd.BundleName)
	}

	docCount := len(bulkCmd.Documents)
	schema := bundle.DocumentStructure.FieldSchema()

	// Phase 1: Validate ALL documents upfront (fail-fast, all-or-nothing)
	uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	for i, docKVs := range bulkCmd.Documents {
		tmpCmd := &models.DocumentCommand{
			CommandType: "ADD",
			BundleName:  bulkCmd.BundleName,
			Fields:      docKVs,
		}
		if err := s.processNullValues(bundle, tmpCmd); err != nil {
			return nil, fmt.Errorf("document %d: failed to process NULL values: %w", i, err)
		}
		// Update the fields back after processNullValues may have modified them
		bulkCmd.Documents[i] = tmpCmd.Fields
		if err := s.validateDocumentFields(bundle, tmpCmd); err != nil {
			return nil, fmt.Errorf("document %d: field validation failed: %w", i, err)
		}
		if err := uniqueValidator.ValidateUniqueConstraints(bundle, tmpCmd); err != nil {
			return nil, fmt.Errorf("document %d: unique constraint violation: %w", i, err)
		}
	}

	// Phase 1.5: FK validation for all documents (batched, deduplicated)
	// Only check outgoing relationships — incoming are irrelevant for INSERT.
	if len(bundle.Relationships) > 0 {
		validator := NewReferentialIntegrityValidator(s, s.logger)
		bundleCache := make(map[string]*models.Bundle)
		validationCache := make(map[string]*ForeignKeyViolation)

		for i, docKVs := range bulkCmd.Documents {
			insertFields := make(map[string]string, len(docKVs))
			for _, kv := range docKVs {
				if kv.Value == nil {
					continue
				}
				insertFields[kv.Key] = fmt.Sprintf("%v", kv.Value)
			}

			foreignKeyUpdates := validator.IdentifyForeignKeyFields(nil, bundle, insertFields, bundleCache)

			if len(foreignKeyUpdates) > 0 {
				violation := validator.batchValidateForeignKeys(bundle, []string{fmt.Sprintf("doc-%d", i)}, foreignKeyUpdates, validationCache)
				if violation != nil {
					return nil, fmt.Errorf("document %d: referential integrity violation: %s", i, violation.Error())
				}
			}
		}
	}

	// Phase 2: Create all documents
	documents := make([]*models.Document, 0, docCount)
	docIDs := make([]string, 0, docCount)
	for _, docKVs := range bulkCmd.Documents {
		tmpCmd := models.DocumentCommand{
			CommandType: "ADD",
			BundleName:  bulkCmd.BundleName,
			Fields:      docKVs,
		}
		newDoc := s.documentFactory.NewDocument(tmpCmd, schema)
		documents = append(documents, newDoc)
		docIDs = append(docIDs, newDoc.DocumentID)
	}

	// Phase 3: Batch storage writes
	pageIDs := make([]uint32, 0, docCount)
	for _, doc := range documents {
		pageID, err := s.store.AddDocumentToBundleFile(bundle, doc)
		if err != nil {
			return nil, fmt.Errorf("failed to add document %s to bundle: %w", doc.DocumentID, err)
		}
		pageIDs = append(pageIDs, pageID)
	}

	// Phase 4: Batch page cache updates
	for i, doc := range documents {
		s.updatePageCacheWithDocument(bundle.Name, pageIDs[i], doc)
	}

	// Phase 5: Single metadata update
	s.scheduleMetadataUpdate(bulkCmd.BundleName, "increment_docs", int64(docCount))

	// Phase 6: Batch statistics updates
	if s.statsUpdater != nil {
		for _, doc := range documents {
			if schema != nil && len(doc.Values) > 0 {
				for j, name := range schema.Names {
					if j < len(doc.Values) {
						s.statsUpdater.IncrementalUpdate(bundle.Name, name, nil, doc.Values[j].AsInterface(), bundle.TotalDocuments)
					}
				}
			} else if doc.Data != nil {
				for fieldName, v := range doc.Data {
					s.statsUpdater.IncrementalUpdate(bundle.Name, fieldName, nil, v, bundle.TotalDocuments)
				}
			}
		}
	}

	// Phase 7: Batch index scheduling
	if bundle.Indexes != nil {
		for i, doc := range documents {
			s.scheduleIndexUpdatesForDocument(bundle, doc, schema, pageIDs[i])
		}
	}

	// Phase 8: Single cache invalidation
	s.invalidateBundleCaches(bundle.Name)

	return docIDs, nil
}

// AddDocumentsToBundleWithTxID is the transaction-aware variant of AddDocumentsToBundle.
// Sets MVCC version fields and uses the transaction-aware storage path.
func (s *BundleService) AddDocumentsToBundleWithTxID(database *models.Database, bundle *models.Bundle, bulkCmd *models.BulkDocumentCommand, txID string, preallocDocIDs []string) ([]string, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle '%s' is nil, cannot add documents", bulkCmd.BundleName)
	}

	docCount := len(bulkCmd.Documents)
	schema := bundle.DocumentStructure.FieldSchema()

	// Phase 1: Validate ALL documents upfront
	uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	for i, docKVs := range bulkCmd.Documents {
		tmpCmd := &models.DocumentCommand{
			CommandType: "ADD",
			BundleName:  bulkCmd.BundleName,
			Fields:      docKVs,
		}
		if err := s.processNullValues(bundle, tmpCmd); err != nil {
			return nil, fmt.Errorf("document %d: failed to process NULL values: %w", i, err)
		}
		bulkCmd.Documents[i] = tmpCmd.Fields
		if err := s.validateDocumentFields(bundle, tmpCmd); err != nil {
			return nil, fmt.Errorf("document %d: field validation failed: %w", i, err)
		}
		if err := uniqueValidator.ValidateUniqueConstraints(bundle, tmpCmd); err != nil {
			return nil, fmt.Errorf("document %d: unique constraint violation: %w", i, err)
		}
	}

	// Phase 1.5: FK validation for all documents (batched, deduplicated)
	// Only check outgoing relationships — incoming are irrelevant for INSERT.
	if len(bundle.Relationships) > 0 {
		validator := NewReferentialIntegrityValidator(s, s.logger)
		bundleCache := make(map[string]*models.Bundle)
		validationCache := make(map[string]*ForeignKeyViolation)

		for i, docKVs := range bulkCmd.Documents {
			insertFields := make(map[string]string, len(docKVs))
			for _, kv := range docKVs {
				if kv.Value == nil {
					continue
				}
				insertFields[kv.Key] = fmt.Sprintf("%v", kv.Value)
			}

			foreignKeyUpdates := validator.IdentifyForeignKeyFields(nil, bundle, insertFields, bundleCache)

			if len(foreignKeyUpdates) > 0 {
				violation := validator.batchValidateForeignKeys(bundle, []string{fmt.Sprintf("doc-%d", i)}, foreignKeyUpdates, validationCache)
				if violation != nil {
					return nil, fmt.Errorf("document %d: referential integrity violation: %s", i, violation.Error())
				}
			}
		}
	}

	// Phase 2: Create all documents with pre-allocated IDs
	documents := make([]*models.Document, 0, docCount)
	docIDs := make([]string, 0, docCount)
	for i, docKVs := range bulkCmd.Documents {
		tmpCmd := models.DocumentCommand{
			CommandType: "ADD",
			BundleName:  bulkCmd.BundleName,
			Fields:      docKVs,
		}
		var newDoc *models.Document
		if i < len(preallocDocIDs) && preallocDocIDs[i] != "" {
			newDoc = s.documentFactory.NewDocumentWithID(tmpCmd, preallocDocIDs[i], schema)
		} else {
			newDoc = s.documentFactory.NewDocument(tmpCmd, schema)
		}
		s.setDocumentVersionFields(newDoc, txID, 1)
		documents = append(documents, newDoc)
		docIDs = append(docIDs, newDoc.DocumentID)
	}

	// Phase 3: Batch storage writes (transaction-aware)
	pageIDs := make([]uint32, 0, docCount)
	for _, doc := range documents {
		pageID, err := s.store.AppendDocumentToBundleFileWithTxID(bundle, doc, txID)
		if err != nil {
			return nil, fmt.Errorf("failed to add document %s to bundle: %w", doc.DocumentID, err)
		}
		pageIDs = append(pageIDs, pageID)
	}

	// Phase 4: Batch page cache updates
	for i, doc := range documents {
		s.updatePageCacheWithDocument(bundle.Name, pageIDs[i], doc)
	}

	// Phase 5: Single metadata update
	s.scheduleMetadataUpdate(bulkCmd.BundleName, "increment_docs", int64(docCount))

	// Phase 6: Batch statistics updates
	if s.statsUpdater != nil {
		for _, doc := range documents {
			if schema != nil && len(doc.Values) > 0 {
				for j, name := range schema.Names {
					if j < len(doc.Values) {
						s.statsUpdater.IncrementalUpdate(bundle.Name, name, nil, doc.Values[j].AsInterface(), bundle.TotalDocuments)
					}
				}
			} else if doc.Data != nil {
				for fieldName, v := range doc.Data {
					s.statsUpdater.IncrementalUpdate(bundle.Name, fieldName, nil, v, bundle.TotalDocuments)
				}
			}
		}
	}

	// Phase 7: Batch index scheduling
	if bundle.Indexes != nil {
		for i, doc := range documents {
			s.scheduleIndexUpdatesForDocument(bundle, doc, schema, pageIDs[i])
		}
	}

	// Phase 8: Single cache invalidation
	s.invalidateBundleCaches(bundle.Name)

	return docIDs, nil
}

// scheduleIndexUpdatesForDocument schedules index updates for a single document across all bundle indexes.
func (s *BundleService) scheduleIndexUpdatesForDocument(bundle *models.Bundle, doc *models.Document, schema *models.BundleFieldSchema, pageID uint32) {
	for indexName, indexRef := range bundle.Indexes {
		if indexRef.IndexType == "hash" {
			if indexRef.WherePredicate != "" {
				pred, predErr := compilePartialIndexPredicate(indexRef.WherePredicate, schema)
				if predErr != nil || !pred(doc) {
					continue
				}
			}
			fieldName := indexRef.HashIndexField.FieldName
			var fieldValue interface{}
			if fieldName == "DocumentID" {
				fieldValue = doc.DocumentID
			} else {
				extractedValue, err := extractFieldValueForIndex(*doc, fieldName, schema)
				if err != nil {
					continue
				}
				fieldValue = extractedValue
			}
			s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", doc.DocumentID, fieldValue, pageID, nil, false, doc.CommitSequence, doc.VersionSequence)
		} else if indexRef.IndexType == "btree" {
			if indexRef.WherePredicate != "" {
				pred, predErr := compilePartialIndexPredicate(indexRef.WherePredicate, schema)
				if predErr != nil || !pred(doc) {
					continue
				}
			}
			var fieldValue interface{}
			if indexRef.Expression != "" {
				// Use cached compiled expression, falling back to recompilation
				if err := indexRef.EnsureExpressionCompiled(indexutils.CompileIndexExpressionAST); err != nil {
					continue
				}
				if indexRef.CompiledExpr != nil && len(indexRef.CompiledExpr.FieldNames) > 1 {
					fieldMap := make(map[string]interface{}, len(indexRef.CompiledExpr.FieldNames))
					for _, fn := range indexRef.CompiledExpr.FieldNames {
						v, fErr := extractFieldValueForIndex(*doc, fn, schema)
						if fErr == nil {
							fieldMap[fn] = v
						}
					}
					fieldValue = indexRef.CompiledExpr.EvalFn(fieldMap)
				} else {
					rawValue, fErr := extractFieldValueForIndex(*doc, indexRef.BTreeIndexField.FieldName, schema)
					if fErr != nil {
						continue
					}
					if indexRef.CompiledExpr != nil {
						fieldMap := map[string]interface{}{indexRef.CompiledExpr.PrimaryField: rawValue}
						fieldValue = indexRef.CompiledExpr.EvalFn(fieldMap)
					} else {
						fieldValue = rawValue
					}
				}
			} else {
				var fErr error
				fieldValue, fErr = extractFieldValueForIndex(*doc, indexRef.BTreeIndexField.FieldName, schema)
				if fErr != nil {
					continue
				}
			}
			s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", doc.DocumentID, fieldValue, pageID, nil, false)
		}
	}
}

func (s *BundleService) AddDocumentToBundleByStruct(database *models.Database, bundle *models.Bundle, document *models.Document) error {
	return s.AddDocumentToBundleByStructWithTxID(database, bundle, document, "")
}

// AddDocumentToBundleByStructWithTxID adds a document with transaction tracking.
// PHASE 3: No bundle write lock; append path is concurrent-safe (WriteBuffer, rotationLock).
func (s *BundleService) AddDocumentToBundleByStructWithTxID(database *models.Database, bundle *models.Bundle, document *models.Document, txID string) error {
	schema := bundle.DocumentStructure.FieldSchema()
	// TODO: Unique constraint validation disabled for AddDocumentToBundleByStruct
	// This method is primarily used for primary catalog initialization where we trust
	// the developer to create bundles correctly. Enabling validation would require
	// creating unique indexes for all catalog bundles, which adds unnecessary overhead.
	// If needed in the future, add validation selectively based on bundle/database context.
	//
	// Validate unique constraints for all IsUnique fields
	// Convert Document struct to DocumentCommand for validation
	// docCommand := &models.DocumentCommand{
	// 	BundleName: bundle.Name,
	// 	Fields:     make([]models.KeyValue, 0, len(document.Fields)),
	// }
	// for fieldName, field := range document.Fields {
	// 	docCommand.Fields = append(docCommand.Fields, models.KeyValue{
	// 		Key:   fieldName,
	// 		Value: field.Value,
	// 	})
	// }
	//
	// uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	// err := uniqueValidator.ValidateUniqueConstraints(bundle, docCommand)
	// if err != nil {
	// 	return fmt.Errorf("unique constraint validation failed: %w", err)
	// }

	// Schedule deferred metadata update instead of immediate calculation
	s.scheduleMetadataUpdate(bundle.Name, "increment_docs", 1)

	// Add document to bundle file (storage layer handles page allocation and returns pageID)
	// Use transaction-aware method to track txID in buffer
	pageID, err := s.store.AppendDocumentToBundleFileWithTxID(bundle, document, txID)
	if err != nil {
		return fmt.Errorf("failed to add document to bundle: %w", err)
	}

	// Now schedule index updates with the actual pageID from storage
	if bundle.Indexes != nil {
		// Schedule deferred index updates instead of immediate updates
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Scheduling deferred update for index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" {
				// Handle ALL hash indexes (DocumentID and foreign keys)
				fieldName := indexRef.HashIndexField.FieldName

				// Extract the field value for hash indexing
				var fieldValue interface{}
				if fieldName == "DocumentID" {
					fieldValue = document.DocumentID
				} else {
					// Extract the foreign key or other field value
					extractedValue, err := extractFieldValueForIndex(*document, fieldName, schema)
					if err != nil {
						s.logger.Warnf("[HASH-IDX] Skipped indexing document %s for index %s: field %q not in document (%v)",
							document.DocumentID, indexName, fieldName, err)
						continue
					}
					fieldValue = extractedValue
				}

				// Schedule hash index update with actual pageID; pass commitSeq/versionSeq to avoid GetDocument.
				// deferred=false for ADD operations (read-your-own-writes consistency)
				s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", document.DocumentID, fieldValue, pageID, nil, false, document.CommitSequence, document.VersionSequence)
				s.logger.Debugf("Scheduled hash index '%s' update for document '%s' on field '%s' (page %d)",
					indexName, document.DocumentID, fieldName, pageID)

			} else if indexRef.IndexType == "btree" {
				// Extract the field value for BTree indexing
				fieldValue, err := extractFieldValueForIndex(*document, indexRef.BTreeIndexField.FieldName, schema)
				if err != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", document.DocumentID, err)
					continue
				}

				// Schedule BTree index update with actual pageID
				// deferred=false for ADD operations (read-your-own-writes consistency)
				s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", document.DocumentID, fieldValue, pageID, nil, false)
				s.logger.Debugf("Scheduled BTree index update for document '%s' on field '%s' (page %d)",
					document.DocumentID, indexRef.BTreeIndexField.FieldName, pageID)
			}
		}
	}

	// WRITE-THROUGH CACHE: Immediately update page cache so reads see this document
	// Use the write-through helper which handles sharded locking and page creation
	s.updatePageCacheWithDocument(bundle.Name, pageID, document)
	s.logger.Debugf("Added document '%s' to page cache via write-through (page %d)", document.DocumentID, pageID)

	return nil
}

// UpdateDocumentInBundle updates documents in a bundle based on the update command.
// TASK 2: Document-level locking support - if lockInfo is provided, uses document locks instead of bundle write lock.
// PHASE 1: ctx may carry snapshot (e.g. planner.WithSnapshotInfo) for MVCC visibility; use context.Background() if none.

// ============================================================================
// OCC (Optimistic Concurrency Control) Write-Write Conflict Detection
// ============================================================================

func (s *BundleService) createUpdatedDocumentRCU(oldDoc *models.Document, updates []models.KeyValue, schema *models.BundleFieldSchema) *models.Document {
	newDoc := &models.Document{
		DocumentID:      oldDoc.DocumentID,
		CreatedAt:       oldDoc.CreatedAt,
		UpdatedAt:       time.Now(),
		CommitSequence:  0,
		SupersededAt:    time.Time{},
		CreatedByTxID:   0,
		DeletedByTxID:   0,
		VersionSequence: 0,
	}

	if schema != nil && len(oldDoc.Values) > 0 {
		newDoc.Values = make([]models.FieldValue, len(oldDoc.Values))
		copy(newDoc.Values, oldDoc.Values)
		for _, kv := range updates {
			if idx, ok := schema.NameToIndex[kv.Key]; ok && idx < len(newDoc.Values) {
				newDoc.Values[idx] = models.NewInterfaceValue(kv.Value)
			}
		}
	} else {
		newDoc.Data = make(map[string]interface{})
		if oldDoc.Data != nil {
			for k, v := range oldDoc.Data {
				newDoc.Data[k] = v
			}
		}
		for _, kv := range updates {
			newDoc.Data[kv.Key] = kv.Value
		}
	}

	helpers.BuildCachedJSON(newDoc, schema)

	// CommitSequence is now set by AppendVersionToBundleFile using SnapshotManager.GetNextCommitSequence()
	// This ensures globally ordered, atomic commit sequence allocation

	return newDoc
}

// ============================================================================
// RCU (Read-Copy-Update) DELETE - Lock-Free Concurrent Deletes
// ============================================================================

// DeleteDocumentFromBundle is the public interface for deleting documents from a bundle.
// When lockInfo is provided with LockManager and LockedDocIDs, skips bundle write lock (Phase 4: document-level locking).
// preFetchedDocs: when provided (e.g. from GetDocumentsAndIDsByFilter), harvest uses these and skips GetDocument in the harvest loop.

// deleteDocumentsInternal performs the actual document deletion logic without acquiring locks.
// When lockInfo is provided with LockedDocIDs, uses AppendDeletionMarkersBatchWithLocks (no bundle-level lock).
//

// DeleteAllDocumentsFromBundle performs a bulk delete of all documents in a bundle with referential integrity checks
// This method implements the CONFIRMED bulk delete operation: DELETE FROM "BundleName" CONFIRMED
//

// RCU MODE: When settings.EnableRCUWrites is true, uses lock-free RCU updates for better concurrency.
func (s *BundleService) UpdateDocumentInBundle(ctx context.Context, database *models.Database, bundle *models.Bundle, docCommand *models.DocumentUpdateCommand, lockInfo ...*DocumentLockInfo) (err error) {
	// PERFORMANCE TIMING: Track where time is spent in UPDATE operations
	updateStart := time.Now()
	defer func() {
		totalTime := time.Since(updateStart)
		if totalTime > 500*time.Millisecond {
			s.logger.Infof("⚠️⚠️⚠️UPDATE SLOW: Total time %v for bundle '%s' WHERE '%s'",
				totalTime, docCommand.BundleName, docCommand.WhereClause)
		}
	}()

	if ctx == nil {
		ctx = context.Background()
	}
	args := settings.GetSettings()

	// RCU MODE: Use lock-free updates when enabled and no document-level locks provided
	// Document-level locks indicate transaction mode which needs the old lock-based path
	useRCU := args.EnableRCUWrites //&& len(lockInfo) == 0
	if useRCU {
		s.logger.Debugf("Using RCU (lock-free) update path for bundle '%s'", bundle.Name)
		// OCC Retry Loop: If we detect a write-write conflict, retry with a fresh snapshot
		maxRetries := args.MaxOCCRetries
		if maxRetries <= 0 {
			maxRetries = 3 // Default fallback
		}
		var lastErr error
		for attempt := 1; attempt <= maxRetries; attempt++ {
			lastErr = s.UpdateDocumentInBundleRCU(ctx, database, bundle, docCommand)
			if lastErr == nil {
				return nil // Success
			}
			if !stderrors.Is(lastErr, ErrWriteConflict) {
				// Non-conflict error, don't retry
				return lastErr
			}
			s.logger.Debugf("OCC: Write conflict on attempt %d/%d for bundle '%s', retrying with fresh snapshot",
				attempt, maxRetries, bundle.Name)
		}
		// All retries exhausted
		return fmt.Errorf("update failed after %d OCC retries: %w", maxRetries, lastErr)
	}
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot update document")
		return fmt.Errorf("bundle '%s' is nil, cannot update document", docCommand.BundleName)
	}

	// SAFETY CHECK: Bulk update (empty WHERE clause) requires CONFIRMED keyword
	if docCommand.WhereClause == "" || strings.TrimSpace(docCommand.WhereClause) == "" {
		if !docCommand.Confirmed {
			return fmt.Errorf("bulk update requires CONFIRMED keyword for safety. "+
				"Syntax: UPDATE DOCUMENTS IN BUNDLE \"%s\" (field = value, ...) CONFIRMED\n"+
				"This safety mechanism prevents accidental modification of all documents in a bundle. "+
				"Use a WHERE clause to update specific documents without CONFIRMED keyword",
				docCommand.BundleName)
		}
		s.logger.Debugf("Bulk update CONFIRMED for bundle '%s' - proceeding to update all documents", bundle.Name)
	}

	// PHASE 1.1: READ PHASE - Perform read operations under read lock
	// This allows concurrent reads during WHERE clause evaluation and FK validation
	if err = s.AcquireBundleReadLock(bundle.Name); err != nil {
		return fmt.Errorf("failed to acquire read lock: %w", err)
	}

	// TASK 1: Use query planner for WHERE clause processing (same fast path as SELECT)
	// OPTIMIZATION: If lockInfo contains pre-fetched document IDs AND we have actual locks,
	// use those directly to avoid duplicate WHERE clause evaluation.
	// IMPORTANT: Only use pre-fetched IDs when we have actual document locks (LockManager != nil)
	// because locks guarantee the IDs are still valid. Without locks, concurrent transactions
	// could delete/modify documents, making the IDs stale.
	var filteredDocs []*models.Document

	// TIMING: WHERE clause evaluation
	whereStart := time.Now()

	// Use pre-fetched docs when available (avoids GetDocument-by-ID and index→page lookup failures)
	li := len(lockInfo) > 0 && lockInfo[0] != nil
	hasLocks := li && lockInfo[0].LockManager != nil && len(lockInfo[0].LockedDocIDs) > 0
	hasPreFetched := li && len(lockInfo[0].PreFetchedDocs) > 0

	if hasLocks && hasPreFetched {
		// Use PreFetchedDocs from single WHERE scan; no GetDocument-by-ID. Ensures accurate updates.
		lockedSet := make(map[string]bool, len(lockInfo[0].LockedDocIDs))
		for _, id := range lockInfo[0].LockedDocIDs {
			lockedSet[id] = true
		}
		filteredDocs = make([]*models.Document, 0, len(lockInfo[0].PreFetchedDocs))
		for _, doc := range lockInfo[0].PreFetchedDocs {
			if doc != nil && lockedSet[doc.DocumentID] {
				filteredDocs = append(filteredDocs, doc)
			}
		}
		s.logger.Debugf("OPTIMIZATION: Used pre-fetched documents with locks (%d docs), no GetDocument-by-ID", len(filteredDocs))
	} else if hasPreFetched {
		// OCC path: PreFetchedDocs available but no locks (bundle-level lock used instead)
		// Use the pre-fetched docs directly to avoid re-running the WHERE clause
		filteredDocs = lockInfo[0].PreFetchedDocs
		s.logger.Debugf("OPTIMIZATION: Used pre-fetched documents from OCC (%d docs), no re-filtering", len(filteredDocs))
	} else if hasLocks && docCommand.WhereClause != "" && strings.TrimSpace(docCommand.WhereClause) != "" {
		// Locked IDs but no PreFetchedDocs (e.g. legacy caller): re-run WHERE via filter path.
		// Use GetDocumentsByFilter only; do not use query planner (avoids JOIN/aggregation on write-only UPDATE).
		filteredDocs, err = s.GetDocumentsByFilter(bundle, docCommand.WhereClause, nil)
		if err != nil {
			s.ReleaseBundleReadLock(bundle.Name)
			return fmt.Errorf("failed to filter documents: %w", err)
		}
	} else if docCommand.WhereClause != "" && docCommand.WhereClause != strings.TrimSpace("") {
		// Non-empty WHERE: use GetDocumentsByFilter only. Do not use query planner (avoids JOIN/aggregation on write-only UPDATE).
		filteredDocs, err = s.GetDocumentsByFilter(bundle, docCommand.WhereClause, nil)
		if err != nil {
			s.ReleaseBundleReadLock(bundle.Name)
			return fmt.Errorf("failed to filter documents: %w", err)
		}
	} else {
		// Empty WHERE clause - get all documents (only if CONFIRMED)
		// Fallback to GetDocumentsByFilter for this case
		filteredDocs, err = s.GetDocumentsByFilter(bundle, docCommand.WhereClause, nil)
		if err != nil {
			s.ReleaseBundleReadLock(bundle.Name)
			return fmt.Errorf("failed to filter documents: %w", err)
		}
	}

	// TIMING: Log WHERE clause evaluation time
	whereTime := time.Since(whereStart)
	if whereTime > 100*time.Millisecond {
		s.logger.Warnf("UPDATE TIMING: WHERE clause took %v for %d docs, bundle '%s'",
			whereTime, len(filteredDocs), bundle.Name)
	}

	if args.Debug {
		s.logger.Debugf("Updating %d documents from bundle '%s' with filter '%s'", len(filteredDocs), docCommand.BundleName, docCommand.WhereClause)
	}

	// Validate document update fields against bundle field definitions
	err = s.validateUpdateFields(bundle, docCommand)
	if err != nil {
		s.ReleaseBundleReadLock(bundle.Name)
		return fmt.Errorf("document field validation failed: %w", err)
	}

	// PHASE 1.2: Move FK validation to read lock phase (it only reads data)
	// ==========  VALIDATE REFERENTIAL INTEGRITY FOR FOREIGN KEY UPDATES ==========
	// Check if any fields being updated are foreign keys and validate the new values
	// Note: Must check BOTH outgoing relationships (stored in bundle.Relationships)
	//       AND incoming relationships (stored in other bundles pointing to this one)
	s.logger.Debugf("[REFINT-UPDATE] Starting FK validation for bundle '%s', database=%v, bundle.Relationships=%d",
		bundle.Name, database != nil, len(bundle.Relationships))

	var docIDs []string
	if len(bundle.Relationships) > 0 || database != nil {
		// Create operation-scoped validation cache to avoid redundant hash lookups
		validationCache := make(map[string]*ForeignKeyViolation)
		bundleCache := make(map[string]*models.Bundle)

		// Create validator
		validator := NewReferentialIntegrityValidator(s, s.logger)

		// Build map of field updates for easier lookup
		updateFields := make(map[string]string)
		for _, kv := range docCommand.Fields {
			if kv.Value == nil {
				continue
			}
			updateFields[kv.Key] = fmt.Sprintf("%v", kv.Value)
		}
		s.logger.Debugf("[REFINT-UPDATE] Update fields: %v", updateFields)

		// Identify which fields are foreign keys (checks BOTH directions)
		foreignKeyUpdates := validator.IdentifyForeignKeyFields(database, bundle, updateFields, bundleCache)
		s.logger.Debugf("[REFINT-UPDATE] Identified %d FK fields being updated", len(foreignKeyUpdates))

		if len(foreignKeyUpdates) > 0 {
			// Extract document IDs being updated
			docIDs = make([]string, len(filteredDocs))
			for i, doc := range filteredDocs {
				docIDs[i] = doc.DocumentID
			}
			s.logger.Debugf("[REFINT-UPDATE] Validating %d document(s): %v", len(docIDs), docIDs)

			// Perform batch validation with caching (under read lock - only reads data)
			violation := validator.batchValidateForeignKeys(bundle, docIDs, foreignKeyUpdates, validationCache)
			if violation != nil {
				s.ReleaseBundleReadLock(bundle.Name)
				// Log the violation at WARN level with suggested action
				s.logger.Warnf("[REFINT] %s | Suggested: %s", violation.Error(), violation.SuggestedAction)
				return fmt.Errorf("%s", violation.Error())
			}

			s.logger.Debugf("[REFINT] Foreign key validation passed for %d document(s) updating %d FK field(s)",
				len(docIDs), len(foreignKeyUpdates))
		}
	}

	// Collect document IDs for re-validation under write lock
	if docIDs == nil {
		docIDs = make([]string, len(filteredDocs))
		for i, doc := range filteredDocs {
			docIDs[i] = doc.DocumentID
		}
	}

	// Release read lock before acquiring write lock
	s.ReleaseBundleReadLock(bundle.Name)

	// TASK 2: Document-level locking - use document locks if provided, otherwise use bundle write lock.
	// MUST match document_operations.lockEscalationThreshold (100_000). Phase 5: no bundle-level locking for typical updates.
	var useDocumentLocks bool
	var lockedDocIDsSet map[string]bool
	const lockEscalationThreshold = 100_000

	if len(lockInfo) > 0 && lockInfo[0] != nil && len(lockInfo[0].LockedDocIDs) > 0 {
		if len(docIDs) <= lockEscalationThreshold {
			useDocumentLocks = true
			lockedDocIDsSet = make(map[string]bool, len(lockInfo[0].LockedDocIDs))
			for _, docID := range lockInfo[0].LockedDocIDs {
				lockedDocIDsSet[docID] = true
			}
			s.logger.Debugf("Using document-level locks for %d documents (below threshold %d)", len(docIDs), lockEscalationThreshold)
		} else {
			s.logger.Debugf("Lock escalation: %d documents exceeds threshold %d, using bundle write lock", len(docIDs), lockEscalationThreshold)
		}
	}

	// Acquire bundle write lock only if not using document locks or if lock escalation needed
	if !useDocumentLocks {
		if err = s.AcquireBundleWriteLock(bundle.Name); err != nil {
			return fmt.Errorf("failed to acquire write lock: %w", err)
		}
		defer s.ReleaseBundleWriteLock(bundle.Name)
	}

	// CRITICAL PERFORMANCE FIX: Use documents we already have from query planner
	// Re-fetching via GetDocument for each document ID was causing 2-20 second delays
	// However, we need to handle race condition: DELETE can happen between read and write lock acquisition
	// Solution: Do lightweight existence check using memtable and documentPageMap (O(1) lookups)
	// This is much faster than GetDocument which does I/O
	// TASK 2: If using document locks, filter to only documents that are locked
	if useDocumentLocks {
		// Filter to only documents that have locks acquired
		filteredLockedDocs := make([]*models.Document, 0, len(filteredDocs))
		for _, doc := range filteredDocs {
			if lockedDocIDsSet[doc.DocumentID] {
				filteredLockedDocs = append(filteredLockedDocs, doc)
			}
		}
		filteredDocs = filteredLockedDocs
	}
	filteredDocs = s.filterDeletedDocuments(bundle, filteredDocs)
	if len(filteredDocs) == 0 {
		// DEBUG level: This is expected behavior under high concurrency, not an error
		// Early return is efficient - avoids all update work when all documents were deleted
		s.logger.Debugf("All documents were deleted between read and write phases - skipping update (expected under high concurrency)")
		return nil // No documents to update
	}

	// TASK 3: Identify which fields have B-tree indexes for deferred update scheduling
	// We don't need to pre-load indexes since updates are deferred via scheduleIndexUpdate
	updatedFieldsSet := make(map[string]bool)
	for _, kv := range docCommand.Fields {
		updatedFieldsSet[kv.Key] = true
	}

	// Track which indexes need updates (for logging/debugging)
	btreeIndexesToUpdate := make(map[string]string) // indexName -> fieldName
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "btree" {
				fieldName := indexRef.BTreeIndexField.FieldName
				if updatedFieldsSet[fieldName] {
					btreeIndexesToUpdate[indexName] = fieldName
				}
			}
		}
	}

	// TASK 3: B-tree index updates are now deferred via scheduleIndexUpdate
	// Rollback for deferred updates is handled by the index update buffer system
	// If document update fails, the deferred index updates won't be applied (they're in buffer, not yet executed)

	// PHASE 2b: Resolve txID for CreatedByTxID (VersionTxID or TxID from lockInfo)
	var createdByTxIDUint64 uint64
	if len(lockInfo) > 0 && lockInfo[0] != nil {
		tidStr := lockInfo[0].VersionTxID
		if tidStr == "" {
			tidStr = lockInfo[0].TxID
		}
		if tidStr != "" {
			_, _ = fmt.Sscanf(tidStr, "%016x", &createdByTxIDUint64)
		}
	}

	perDocStart := time.Now()
	updatedDocs := make([]*models.Document, 0, len(filteredDocs))
	schema := bundle.DocumentStructure.FieldSchema()
	for _, doc := range filteredDocs {
		originalDoc := *doc

		if schema != nil && len(doc.Values) > 0 {
			newValues := make([]models.FieldValue, len(doc.Values))
			copy(newValues, doc.Values)
			for _, kv := range docCommand.Fields {
				if idx, ok := schema.NameToIndex[kv.Key]; ok && idx < len(newValues) {
					newValues[idx] = models.NewInterfaceValue(kv.Value)
				}
			}
			doc.Values = newValues
		} else {
			if doc.Data == nil {
				doc.Data = make(map[string]interface{})
			}
			for _, kv := range docCommand.Fields {
				doc.Data[kv.Key] = kv.Value
			}
		}

		helpers.BuildCachedJSON(doc, schema)

		// TASK 3: Use deferred index updates for B-tree indexes instead of synchronous operations
		// This reduces per-document index overhead by batching updates
		for indexName, fieldName := range btreeIndexesToUpdate {
			s.logger.Debugf("Indexed field '%s' was updated, scheduling deferred BTree index '%s' update", fieldName, indexName)

			oldFieldValue, extErr := extractFieldValueForIndex(originalDoc, fieldName, schema)
			if extErr != nil {
				s.logger.Warnf("Failed to extract old field value for document '%s': %v", doc.DocumentID, extErr)
				continue
			}

			newFieldValue, extErr := extractFieldValueForIndex(*doc, fieldName, schema)
			if extErr != nil {
				s.logger.Warnf("Failed to extract new field value for document '%s': %v", doc.DocumentID, extErr)
				continue
			}

			// PHASE 5: Get pageID using sharded cache (no global lock)
			pageID, _ := s.documentPageCache.GetPageID(bundle.Name, doc.DocumentID)

			// TASK 3: Schedule deferred B-tree index updates (delete old, insert new)
			// The indexUpdateBuffer will batch these and apply them later via flushIndexUpdates
			// This reduces per-document index overhead significantly
			// deferred=true for UPDATE operations (batch performance, no read-your-own-writes needed)
			// Schedule delete for old value
			s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "delete", doc.DocumentID, oldFieldValue, pageID, nil, true)
			// Schedule insert for new value
			s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", doc.DocumentID, newFieldValue, pageID, nil, true)
			s.logger.Debugf("Scheduled deferred B-tree index updates (delete+insert) for document '%s' on field '%s'", doc.DocumentID, fieldName)
		}

		// PHASE 2b: MVCC version metadata for new version (append-only semantics)
		if doc.VersionSequence == 0 {
			doc.VersionSequence = 1
		} else {
			doc.VersionSequence++
		}
		doc.CreatedByTxID = createdByTxIDUint64
		doc.CommitSequence = 0

		updatedDocs = append(updatedDocs, doc)
	}

	// TIMING: Log per-document processing time
	perDocTime := time.Since(perDocStart)
	if perDocTime > 100*time.Millisecond {
		s.logger.Warnf("UPDATE TIMING: Per-doc loop took %v for %d docs, bundle '%s'",
			perDocTime, len(updatedDocs), bundle.Name)
	}

	// TASK 3: B-tree index updates are now deferred via scheduleIndexUpdate
	// The indexUpdateBuffer will batch these updates and apply them later via flushIndexUpdates
	// This reduces per-document index overhead significantly
	// No need to apply B-tree operations here - they're scheduled for deferred execution
	// Each document with B-tree indexed fields schedules 2 updates (delete old + insert new) per index
	if len(btreeIndexesToUpdate) > 0 && len(updatedDocs) > 0 {
		totalBTreeUpdates := len(btreeIndexesToUpdate) * len(updatedDocs) * 2 // 2 = delete + insert per update
		s.logger.Debugf("Scheduled %d B-tree index updates (delete+insert) for deferred execution (will be batched)", totalBTreeUpdates)
	}

	// TIMING: UpdateDocumentsBatch
	batchStart := time.Now()

	// R1: Single UpdateDocumentsBatch for all updated docs (was N calls to UpdateDocumentInBundleFile).
	// R7 audit: UpdateDocumentInBundle holds AcquireBundleWriteLock (application) before this call, which
	// acquires getWriteLock (storage) inside UpdateDocumentsBatch. Lock order: application then storage.
	// Other callers of UpdateDocumentInBundleFile: applyDefaultToExistingDocuments, removeFieldFromExistingDocuments,
	// renameFieldInDocuments, convertFieldType, applyDefaultToMissingField (via ApplyFieldChanges). Those do not
	// hold AcquireBundleWriteLock; they use only the storage lock. No deadlock: no path holds application then
	// waits on storage while another holds storage then waits on application.
	//
	// DOCUMENT-LEVEL LOCKING: Use UpdateDocumentsBatchWithLocks ONLY when caller has ACTUAL pre-acquired document locks
	// (LockManager != nil). If lockInfo only contains docIDs for optimization (LockManager == nil), use bundle lock.
	// P0d: Pass preLockedDocIDs that exactly match updatedDocs (same length, same IDs) to avoid fallback to bundle lock.
	if len(lockInfo) > 0 && lockInfo[0] != nil && lockInfo[0].LockManager != nil && len(lockInfo[0].LockedDocIDs) > 0 {
		updatedDocIDs := make([]string, 0, len(updatedDocs))
		for _, d := range updatedDocs {
			updatedDocIDs = append(updatedDocIDs, d.DocumentID)
		}
		err = s.store.UpdateDocumentsBatchWithLocks(bundle, updatedDocs, updatedDocIDs)
		if err != nil {
			return fmt.Errorf("failed to update documents with document locks: %w", err)
		}
		s.logger.Debugf("Updated %d documents using document-level locks", len(updatedDocs))
	} else {
		// Fall back to bundle-level lock (either no lockInfo, or lockInfo only has docIDs for optimization)
		err = s.store.UpdateDocumentsBatch(bundle, updatedDocs)
		if err != nil {
			return fmt.Errorf("failed to update documents in bundle: %w", err)
		}
	}

	// TIMING: Log UpdateDocumentsBatch time
	batchTime := time.Since(batchStart)
	if batchTime > 100*time.Millisecond {
		s.logger.Warnf("UPDATE TIMING: UpdateDocumentsBatch took %v for %d docs, bundle '%s'",
			batchTime, len(updatedDocs), bundle.Name)
	}

	// Invalidate documentPageMap for each updated docID (pageID is stale after UPDATE).
	for _, d := range updatedDocs {
		s.invalidateDocumentPageMapEntry(bundle.Name, d.DocumentID)
	}

	// WRITE-THROUGH CACHE: Update page cache with new document versions immediately
	// This replaces the old invalidation approach - instead of invalidating and forcing
	// a disk read, we update the cache directly so reads see the new data immediately
	for _, doc := range updatedDocs {
		// Look up the pageID from documentPageMap (before it was invalidated)
		// If not found, the update will just skip - cache will load from disk on next read
		pageID, err := s.findDocumentPage(bundle.Name, doc.DocumentID)
		if err == nil {
			s.updatePageCacheWithDocument(bundle.Name, pageID, doc)
		}
	}
	s.logger.Debugf("Write-through: Updated %d documents in page cache for bundle '%s'", len(updatedDocs), bundle.Name)

	if s.statsUpdater != nil && schema != nil {
		for _, doc := range updatedDocs {
			if len(doc.Values) > 0 {
				for i, name := range schema.Names {
					if i < len(doc.Values) {
						s.statsUpdater.IncrementalUpdate(bundle.Name, name, nil, doc.Values[i].AsInterface(), bundle.TotalDocuments)
					}
				}
			} else if doc.Data != nil {
				for fieldName, v := range doc.Data {
					s.statsUpdater.IncrementalUpdate(bundle.Name, fieldName, nil, v, bundle.TotalDocuments)
				}
			}
		}
	}

	// NOTE: Metadata updates (TotalDocuments, PageCount) are handled via scheduleMetadataUpdate
	// which is atomic and uses its own mutex. No bundle write lock needed here.
	// The deferred metadata flush handles concurrent access safely.

	// TASK 3: Flush deferred index updates for this bundle only (P4a scoped flush)
	if len(btreeIndexesToUpdate) > 0 {
		s.flushIndexUpdatesForBundle(bundle.Name)
		s.logger.Debugf("Flushed deferred B-tree index updates for bundle %s", bundle.Name)
	}

	// PERFORMANCE: Invalidate JOIN hash table cache for this bundle
	// This ensures subsequent JOINs rebuild hash tables with updated data
	s.invalidateBundleCaches(bundle.Name)

	// TODO(P5): Memtable-first return — return after memtable+WAL, persist WriteBuffer→disk in background.
	// Requires explicit durability policy and recovery design. See plan §5 / §8.
	return nil
}

// ============================================================================
// RCU (Read-Copy-Update) UPDATE - Lock-Free Concurrent Writes
// ============================================================================
//
// UpdateDocumentInBundleRCU implements lock-free updates using Read-Copy-Update pattern:
//
// 1. Read: Find documents matching WHERE clause (no lock needed for read-committed)
// 2. Copy: Create new document versions with updated fields
// 3. Update: Append new versions to storage (concurrent-safe append)
// 4. Swap: Atomically update index pointers to new locations
// 5. Mark: Set SupersededAt on old versions for cleanup
//
// This eliminates the bundle-wide write lock that was causing serialization.
// PostgreSQL-inspired: Writers don't block writers, readers see consistent state.
//
// Performance: ~60x faster than lock-based approach at high concurrency
// - Old: 30ms @ 150 concurrent updates (serialized)
// - New: ~0.5ms @ 150 concurrent updates (parallel)
//
// Trade-offs:
// - Brief staleness window (~100ms grace period)
// - Old versions consume space until VACUUM
// - Last-writer-wins conflict resolution
func (s *BundleService) UpdateDocumentInBundleRCU(ctx context.Context, database *models.Database, bundle *models.Bundle, docCommand *models.DocumentUpdateCommand) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if bundle == nil {
		return fmt.Errorf("bundle '%s' is nil, cannot update document", docCommand.BundleName)
	}

	// SAFETY CHECK: Bulk update requires CONFIRMED keyword
	if (docCommand.WhereClause == "" || strings.TrimSpace(docCommand.WhereClause) == "") && !docCommand.Confirmed {
		return fmt.Errorf("bulk update requires CONFIRMED keyword for safety")
	}

	// SNAPSHOT ISOLATION: Create snapshot for autocommit operation
	// This enables lock-free WHERE evaluation with consistent visibility
	serviceRegistry := registry.GetRegistry()
	var snapshotSequence uint64
	var activeTxIDs map[uint64]bool

	if walManager := serviceRegistry.GetWALManager(); walManager != nil {
		if snapshotMgrFull := walManager.GetSnapshotManager(); snapshotMgrFull != nil {
			// Get current sequence as snapshot boundary (autocommit uses txID=0)
			snapshotSequence = snapshotMgrFull.GetCurrentSequence()
			// For autocommit, no active transactions to exclude
			activeTxIDs = make(map[uint64]bool)
		}
	}

	// Create snapshot info and add to context for lock-free visibility filtering
	args := settings.GetSettings()
	snapshotInfo := &models.SnapshotInfo{
		SnapshotSequence: snapshotSequence,
		TransactionID:    0, // txID=0 for autocommit
		ActiveTxIDs:      activeTxIDs,
		GracePeriodMs:    args.RCUGracePeriodMs,
	}
	ctx = models.WithSnapshotInfo(ctx, snapshotInfo)

	// STEP 1: READ - Find documents matching WHERE clause using snapshot isolation
	// Lock-free read with MVCC visibility filtering
	filteredDocs, err := s.GetDocumentsByFilterWithContext(ctx, bundle, docCommand.WhereClause, nil)
	if err != nil {
		return fmt.Errorf("failed to filter documents: %w", err)
	}

	if len(filteredDocs) == 0 {
		s.logger.Debugf("RCU UPDATE: No documents match WHERE clause, nothing to update")
		return nil
	}

	s.logger.Debugf("RCU UPDATE: Found %d documents to update in bundle '%s'", len(filteredDocs), bundle.Name)

	// Validate update fields against bundle schema
	if err := s.validateUpdateFields(bundle, docCommand); err != nil {
		return fmt.Errorf("field validation failed: %w", err)
	}

	// Get hash index for DocumentID (for atomic pointer swap)
	var hashIndex interface{}
	if bundle.Indexes != nil {
		if indexRef, exists := bundle.Indexes["DocumentID_idx"]; exists && indexRef.IndexType == "hash" {
			hashIndex = indexRef.IndexInstance
		}
	}

	// HOT OPTIMIZATION: Check if update touches any indexed field (excluding DocumentID).
	// If not, we can skip the hash index UpdatePageLocation since the SortedIndex
	// gives the same pageID for the same DocumentID — the pointer doesn't change.
	// This is the SyndrDB equivalent of PostgreSQL's Heap-Only Tuple (HOT) optimization.
	isHOT := !updatesIndexedField(bundle, docCommand.Fields)

	// STEP 2-5: For each document, create new version and swap atomically
	successCount := 0

	// Get SnapshotManager for CommitSequence allocation
	// Reuse the registry lookup from above
	var snapshotMgr interface {
		GetNextCommitSequence() uint64
	}
	if walManager := serviceRegistry.GetWALManager(); walManager != nil {
		snapshotMgr = walManager.GetSnapshotManager()
	}

	// OCC VALIDATION: Check for write-write conflicts before modifying any documents
	// This is the "validate" phase of Optimistic Concurrency Control.
	// If any document was modified after our snapshot, return ErrWriteConflict
	// so the caller can retry with a fresh snapshot.
	if err := s.ValidateNoConflict(filteredDocs, snapshotSequence); err != nil {
		return err // ErrWriteConflict - caller should retry
	}

	schema := bundle.DocumentStructure.FieldSchema()
	for _, oldDoc := range filteredDocs {
		// Documents already filtered by IsVisibleToSnapshot in GetDocumentsByFilterWithContext
		// Double-check with read-committed for any in-flight changes
		if !oldDoc.IsVisibleReadCommitted() {
			s.logger.Debugf("RCU UPDATE: Skipping document %s - no longer visible", oldDoc.DocumentID)
			continue
		}

		// STEP 2: COPY - Create new document with updated fields
		newDoc := s.createUpdatedDocumentRCU(oldDoc, docCommand.Fields, schema)

		// Get next commit sequence for this version (atomic, globally ordered)
		var commitSequence uint64
		if snapshotMgr != nil {
			commitSequence = snapshotMgr.GetNextCommitSequence()
		} else {
			// Fallback: Use nanosecond timestamp (less ideal but functional)
			commitSequence = uint64(time.Now().UnixNano())
		}

		// STEP 3: UPDATE - Append new version to storage using RCU path (no lock needed)
		// AppendVersionToBundleFile handles: VersionSequence increment, CommitSequence assignment
		pageID, err := s.store.AppendVersionToBundleFile(bundle, newDoc, oldDoc, commitSequence)
		if err != nil {
			s.logger.Warnf("RCU UPDATE: Failed to append new version for %s: %v", newDoc.DocumentID, err)
			continue
		}

		// STEP 4: SWAP - Atomically update index pointer to new location
		// HOT OPTIMIZATION: Skip hash index update when no indexed field changed.
		// The SortedIndex gives the same pageID for the same DocumentID, so the
		// pointer doesn't need updating. CommitSequence is tracked on the document itself.
		if !isHOT && hashIndex != nil {
			if idx, ok := hashIndex.(interface {
				UpdatePageLocation(keyValue, documentID string, newPageID uint32, commitSequence uint64) error
			}); ok {
				if err := idx.UpdatePageLocation(newDoc.DocumentID, newDoc.DocumentID, pageID, newDoc.CommitSequence); err != nil {
					s.logger.Warnf("RCU UPDATE: Failed to update index for %s: %v", newDoc.DocumentID, err)
					// Continue anyway - document is on disk, just index is stale
				}
			}
		}

		// STEP 5: MARK - Set SupersededAt on old version for cleanup
		// Note: oldDoc points to page cache - this marks it for VACUUM
		oldDoc.MarkSuperseded()

		// Update page cache with new document (write-through)
		s.updatePageCacheWithDocument(bundle.Name, pageID, newDoc)

		// Invalidate old page mapping (force re-lookup)
		s.invalidateDocumentPageMapEntry(bundle.Name, newDoc.DocumentID)

		successCount++
	}

	if successCount == 0 {
		return fmt.Errorf("RCU UPDATE: No documents were updated")
	}

	s.logger.Debugf("RCU UPDATE: Successfully updated %d/%d documents in bundle '%s'",
		successCount, len(filteredDocs), bundle.Name)

	// Invalidate JOIN caches for this bundle
	s.invalidateBundleCaches(bundle.Name)

	return nil
}

// DeleteDocumentFromBundleRCU implements lock-free deletes using RCU pattern:
//
// 1. Validate: Check referential integrity (FK constraints still enforced)
// 2. Find: Locate documents matching WHERE clause (no lock needed)
// 3. Mark: Write deletion tombstones (append-only, concurrent-safe)
// 4. Index: Atomically update indexes to mark as deleted
// 5. Cleanup: Mark documents as superseded for VACUUM
//
// This eliminates the bundle-wide write lock that was causing serialization.
// DELETE already uses append-only tombstones which are naturally RCU-compatible.
//
// Performance: ~60x faster than lock-based approach at high concurrency
// - Old: 30ms @ 150 concurrent deletes (serialized)
// - New: ~0.5ms @ 150 concurrent deletes (parallel)
func (s *BundleService) DeleteDocumentFromBundleRCU(bundle *models.Bundle, docCommand *models.DocumentDeleteCommand, docIDs []string, preFetchedDocs []*models.Document) error {
	args := settings.GetSettings()

	if args.Debug {
		s.logger.Debugf("RCU DELETE: Starting lock-free deletion from bundle '%s' with WHERE clause: %s",
			docCommand.BundleName, docCommand.WhereClause)
	}

	// ========== STEP 1: VALIDATE - Check for bulk delete safety ==========
	if (docCommand.WhereClause == "" || strings.TrimSpace(docCommand.WhereClause) == "") && len(docIDs) == 0 {
		if !docCommand.Confirmed {
			return fmt.Errorf("bulk delete requires CONFIRMED keyword for safety")
		}

		// Get all document IDs for bulk delete
		allDocs, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
		if err != nil {
			return fmt.Errorf("failed to retrieve documents for bulk delete: %w", err)
		}

		if len(allDocs) == 0 {
			s.logger.Debugf("RCU DELETE: Bundle '%s' is already empty", bundle.Name)
			docCommand.DeletedDocumentIDs = []string{}
			return nil
		}

		docIDs = make([]string, 0, len(allDocs))
		for _, doc := range allDocs {
			docIDs = append(docIDs, doc.DocumentID)
		}
		preFetchedDocs = allDocs
	}

	if len(docIDs) == 0 {
		s.logger.Debugf("RCU DELETE: No documents to delete")
		return nil
	}

	// ========== STEP 2: VALIDATE REFERENTIAL INTEGRITY ==========
	// FK validation is still required even in RCU mode
	fkStart := time.Now()
	validator := NewReferentialIntegrityValidator(s, s.logger)
	if err := validator.ValidateBulkDeleteOptimized(bundle, docIDs); err != nil {
		return fmt.Errorf("referential integrity: %w", err)
	}
	fkTime := time.Since(fkStart)
	if fkTime > 100*time.Millisecond {
		s.logger.Warnf("RCU DELETE TIMING: FK validation took %v for %d docs", fkTime, len(docIDs))
	}

	// ========== STEP 3: HARVEST - Collect index keys before deletion ==========
	harvestStart := time.Now()
	// Build lookup map from pre-fetched docs
	var docIDToDoc map[string]*models.Document
	if len(preFetchedDocs) > 0 {
		docIDToDoc = make(map[string]*models.Document, len(preFetchedDocs))
		for _, d := range preFetchedDocs {
			if d != nil && d.DocumentID != "" {
				docIDToDoc[d.DocumentID] = d
			}
		}
	}

	// Harvest B-tree keys and commit sequences
	btreeKeys := make(map[string]map[string][]byte)
	commitSeqs := make(map[string]uint64)
	harvestFailedDocIDs := make(map[string]struct{})

	const harvestSkipThreshold = 500
	schema := bundle.DocumentStructure.FieldSchema()
	if bundle.Indexes != nil && bundle.Database != nil {
		if len(docIDs) > harvestSkipThreshold && docIDToDoc == nil {
			for _, docID := range docIDs {
				harvestFailedDocIDs[docID] = struct{}{}
			}
			s.logger.Debugf("RCU DELETE: Skipped harvest for %d docs (>%d threshold)", len(docIDs), harvestSkipThreshold)
		} else {
			for _, docID := range docIDs {
				var doc *models.Document
				if docIDToDoc != nil {
					doc = docIDToDoc[docID]
				}
				if doc == nil {
					var err error
					doc, err = s.GetDocument(bundle.Name, bundle.Database.Name, docID)
					if err != nil {
						harvestFailedDocIDs[docID] = struct{}{}
						continue
					}
				}
				commitSeqs[docID] = doc.CommitSequence

				// Mark document as superseded for VACUUM cleanup
				doc.MarkSuperseded()

				// Harvest B-tree keys
				for indexName, indexRef := range bundle.Indexes {
					if indexRef.IndexType != "btree" {
						continue
					}
					fieldName := indexRef.BTreeIndexField.FieldName
					fv, err := extractFieldValueForIndex(*doc, fieldName, schema)
					if err != nil {
						continue
					}
					kb, err := convertValueToBytes(fv)
					if err != nil {
						continue
					}
					if btreeKeys[docID] == nil {
						btreeKeys[docID] = make(map[string][]byte)
					}
					btreeKeys[docID][indexName] = kb
				}
			}
		}
	}

	harvestTime := time.Since(harvestStart)
	if harvestTime > 100*time.Millisecond {
		s.logger.Warnf("RCU DELETE TIMING: Harvest took %v for %d docs", harvestTime, len(docIDs))
	}

	// ========== STEP 4: WRITE TOMBSTONES (No Lock Required) ==========
	tombstoneStart := time.Now()
	// Flush write buffer first to ensure pending writes are on disk
	if err := s.store.FlushWriteBuffers(docCommand.BundleName); err != nil {
		s.logger.Warnf("RCU DELETE: Failed to flush write buffers: %v", err)
	}

	// Append deletion markers (tombstones) - this is already concurrent-safe
	// No bundle lock needed - append is atomic at file level
	if err := s.store.AppendDeletionMarkersBatch(bundle, docIDs); err != nil {
		return fmt.Errorf("failed to append deletion markers: %w", err)
	}
	s.logger.Debugf("RCU DELETE: Wrote %d deletion markers", len(docIDs))

	// Close write buffer to ensure tombstones are visible
	if err := s.store.CloseWriteBuffer(docCommand.BundleName); err != nil {
		s.logger.Warnf("RCU DELETE: Failed to close write buffer: %v", err)
	}

	tombstoneTime := time.Since(tombstoneStart)
	if tombstoneTime > 100*time.Millisecond {
		s.logger.Warnf("RCU DELETE TIMING: Tombstones took %v for %d docs", tombstoneTime, len(docIDs))
	}

	// ========== STEP 5: UPDATE INDEXES (Atomic Operations) ==========
	indexStart := time.Now()
	// Pre-load indexes once
	hashIndexes := make(map[string]*hashindex.HashIndexV3)
	btreeIndexes := make(map[string]*btreeindexV2.BTreeIndex)
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				idx, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err == nil {
					hashIndexes[indexName] = idx
				}
			} else if indexRef.IndexType == "btree" {
				idx, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err == nil {
					btreeIndexes[indexName] = idx
				}
			}
		}
	}

	// Delete from indexes - these operations are atomic per-entry
	// Track which documents had successful B-tree deletes (via harvested keys)
	btreeDeletedDocs := make(map[string]bool)
	for _, documentID := range docIDs {
		for indexName, hashIndex := range hashIndexes {
			commitSeq := commitSeqs[documentID]
			if _, err := hashIndex.Delete(documentID, commitSeq); err != nil {
				s.logger.Warnf("RCU DELETE: Failed to delete '%s' from hash index '%s': %v", documentID, indexName, err)
			}
		}

		for indexName, btreeIndex := range btreeIndexes {
			if m := btreeKeys[documentID]; m != nil {
				if keyBytes, ok := m[indexName]; ok {
					if err := btreeIndex.Delete(keyBytes, documentID); err != nil {
						s.logger.Warnf("RCU DELETE: Failed to delete '%s' from B-tree '%s': %v", documentID, indexName, err)
					} else {
						btreeDeletedDocs[documentID] = true
					}
				}
			}
		}
	}

	// Cleanup B-tree entries ONLY for documents where harvest failed (no keys extracted)
	// This avoids redundant work for documents already deleted above
	if len(harvestFailedDocIDs) > 0 {
		failedIDs := make([]string, 0, len(harvestFailedDocIDs))
		for docID := range harvestFailedDocIDs {
			if !btreeDeletedDocs[docID] {
				failedIDs = append(failedIDs, docID)
			}
		}
		if len(failedIDs) > 0 {
			for _, btreeIndex := range btreeIndexes {
				if btreeIndex != nil {
					btreeIndex.DeleteByDocumentIDs(failedIDs)
				}
			}
			s.logger.Debugf("RCU DELETE: Cleaned up %d B-tree entries via fallback path", len(failedIDs))
		}
	}

	indexTime := time.Since(indexStart)
	if indexTime > 100*time.Millisecond {
		s.logger.Warnf("RCU DELETE TIMING: Index updates took %v for %d docs", indexTime, len(docIDs))
	}

	// Update SortedIndex
	if bundle.SortedIndex != nil {
		for _, documentID := range docIDs {
			bundle.SortedIndex.Delete(documentID)
		}
	}

	// ========== STEP 6: INVALIDATE CACHES ==========
	// PHASE 5: Use sharded cache for invalidation
	invalidatedPages := make(map[uint32]bool)
	for _, docID := range docIDs {
		// Get page ID before invalidating the cache entry
		if pageID, found := s.documentPageCache.GetPageID(docCommand.BundleName, docID); found {
			if !invalidatedPages[pageID] {
				pageKey := fmt.Sprintf("%s:%d", docCommand.BundleName, pageID)
				shardIdx := s.getPageShardIndex(pageKey)
				shard := s.pageShards[shardIdx]
				shard.mu.Lock()
				shard.deleteLocked(pageKey)
				shard.mu.Unlock()
				invalidatedPages[pageID] = true
				// Clear visibility map bit for deleted page
				s.clearVisibilityForPage(docCommand.BundleName, pageID)
			}
		}
		// Invalidate the document->page cache entry
		s.documentPageCache.InvalidateDocument(docCommand.BundleName, docID)
	}

	// Invalidate scanner cache
	if v, exists := s.bundleScanners.Load(docCommand.BundleName); exists {
		if smartScanner, ok := v.(*documentscanner.SmartBundleScanner); ok {
			smartScanner.RemoveDocumentsFromCache(docIDs)
		} else {
			s.RemoveDocumentScanner(docCommand.BundleName)
		}
	}

	// Invalidate JOIN caches
	s.invalidateBundleCaches(docCommand.BundleName)

	// ========== STEP 7: SET RESPONSE ==========
	docCommand.DeletedDocumentIDs = docIDs

	s.logger.Debugf("RCU DELETE: Successfully deleted %d documents from bundle '%s'", len(docIDs), bundle.Name)
	return nil
}

// RCU MODE: When settings.EnableRCUWrites is true, uses lock-free RCU deletes for better concurrency.
func (s *BundleService) DeleteDocumentFromBundle(bundle *models.Bundle, docCommand *models.DocumentDeleteCommand, docIDs []string, preFetchedDocs []*models.Document, lockInfo ...*DocumentLockInfo) error {
	args := settings.GetSettings()

	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot delete document")
		return fmt.Errorf("bundle '%s' is nil, cannot delete document", docCommand.BundleName)
	}

	// RCU MODE: Use lock-free deletes when enabled and no document-level locks provided
	// Document-level locks indicate transaction mode which needs the old lock-based path
	useRCU := args.EnableRCUWrites && len(lockInfo) == 0
	if useRCU {
		s.logger.Debugf("Using RCU (lock-free) delete path for bundle '%s'", bundle.Name)
		return s.DeleteDocumentFromBundleRCU(bundle, docCommand, docIDs, preFetchedDocs)
	}

	useDocLocks := len(lockInfo) > 0 && lockInfo[0] != nil && lockInfo[0].LockManager != nil && len(lockInfo[0].LockedDocIDs) > 0
	if !useDocLocks {
		if err := s.AcquireBundleWriteLock(bundle.Name); err != nil {
			return fmt.Errorf("failed to acquire write lock: %w", err)
		}
		defer s.ReleaseBundleWriteLock(bundle.Name)
	}

	if args.Debug {
		s.logger.Debugf("Starting document deletion from bundle '%s' with WHERE clause: %s",
			docCommand.BundleName, docCommand.WhereClause)
	}

	// ========== FIND DOCUMENTS MATCHING WHERE CLAUSE ==========

	// STEP 2 Notes: Check for empty WHERE clause (bulk delete all documents)
	// If docIDs are explicitly provided, skip this check - the caller knows what they're doing
	if (docCommand.WhereClause == "" || strings.TrimSpace(docCommand.WhereClause) == "") && len(docIDs) == 0 {
		// Bulk delete requires CONFIRMED keyword for safety
		if !docCommand.Confirmed {
			return fmt.Errorf("bulk delete requires CONFIRMED keyword for safety. "+
				"Syntax: DELETE FROM \"%s\" CONFIRMED\n"+
				"This safety mechanism prevents accidental data loss when deleting all documents in a bundle. "+
				"The operation will validate referential integrity and cascade deletes as configured",
				docCommand.BundleName)
		}

		// Get all document IDs from the bundle
		allDocs, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
		if err != nil {
			return fmt.Errorf("failed to retrieve documents for bulk delete: %w", err)
		}

		if len(allDocs) == 0 {
			s.logger.Debugf("Bundle '%s' is already empty, nothing to delete", bundle.Name)
			docCommand.DeletedDocumentIDs = []string{}
			return nil
		}

		// Extract document IDs for validation and deletion
		bulkDocIDs := make([]string, 0, len(allDocs))
		for _, doc := range allDocs {
			bulkDocIDs = append(bulkDocIDs, doc.DocumentID)
		}

		s.logger.Debugf("Bulk delete will validate and delete %d documents from bundle '%s'", len(bulkDocIDs), bundle.Name)

		// Perform batch referential integrity validation
		validator := NewReferentialIntegrityValidator(s, s.logger)
		if err := validator.ValidateBulkDeleteOptimized(bundle, bulkDocIDs); err != nil {
			return fmt.Errorf("bulk delete failed referential integrity check: %w", err)
		}

		// All validations passed - proceed with deletion using internal method (lock already held)
		// Skip individual metadata updates - we'll do a single bulk update after all deletions.
		// Pass allDocs as preFetchedDocs so harvest avoids N×GetDocument.
		if err := s.deleteDocumentsInternal(bundle, docCommand, bulkDocIDs, true, allDocs); err != nil {
			return fmt.Errorf("bulk delete execution failed: %w", err)
		}

		// CRITICAL FIX: Do NOT schedule decrement_docs metadata update
		// In append-only storage, tombstones are still entries on disk, so TotalDocuments
		// should represent total document entries (including tombstones), not active documents.
		// See DeleteDocumentFromBundleFile for detailed explanation.
		// s.scheduleMetadataUpdate(docCommand.BundleName, "decrement_docs", int64(len(bulkDocIDs))) // REMOVED: Causes corruption

		// Flush all pending operations to ensure consistency
		if err := s.FlushAllBuffers(); err != nil {
			s.logger.Warnf("Failed to flush buffers after bulk delete: %v", err)
		}

		s.logger.Debugf("Successfully deleted %d documents from bundle '%s'", len(bulkDocIDs), bundle.Name)
		return nil
	}

	// D9: Use ValidateBulkDeleteOptimized for all deletes (not only >10). Removes N×ValidateDelete
	// (N×GetBundleByName + N×GetOrLoadHashIndex per relationship) for 1–10 doc deletes.
	s.logger.Debugf("[REFINT] Starting referential integrity validation for %d document(s) in bundle '%s'", len(docIDs), bundle.Name)
	validator := NewReferentialIntegrityValidator(s, s.logger)
	if err := validator.ValidateBulkDeleteOptimized(bundle, docIDs); err != nil {
		return fmt.Errorf("referential integrity: %w", err)
	}
	s.logger.Debugf("[REFINT] Referential integrity validated successfully for %d document(s) in bundle '%s'", len(docIDs), bundle.Name)

	// Delegate to internal delete logic (lock already held when useDocLocks)
	// Pass lockInfo when using document-level locks so we skip storage bundle lock (AppendDeletionMarkersBatchWithLocks).
	return s.deleteDocumentsInternal(bundle, docCommand, docIDs, false, preFetchedDocs, lockInfo...)
}

// Parameters:
//   - skipMetadataUpdate: if true, caller is responsible for scheduling metadata updates (used for bulk operations)
//   - preFetchedDocs: when non-nil, harvest uses these for B-tree keys and commitSeqs instead of GetDocument
//   - lockInfo: optional; when present with LockedDocIDs, storage skips bundle write lock (Phase 4).
func (s *BundleService) deleteDocumentsInternal(bundle *models.Bundle, docCommand *models.DocumentDeleteCommand, docIDs []string, skipMetadataUpdate bool, preFetchedDocs []*models.Document, lockInfo ...*DocumentLockInfo) error {
	// D1: Use AppendDeletionMarkersBatch for all deletes (threshold 1). Removes N×DeleteDocumentFromBundleFile,
	// N×verifyDocumentExistsStreaming, N×appendDeletionMarker, N×lock. Delete is idempotent: tombstones for
	// non-existent docs are acceptable in append-only storage; callers should pass IDs from a valid WHERE result.
	if len(docIDs) == 0 {
		return nil
	}

	// D6 (Phase 1a): Harvest B-tree index keys BEFORE in-memory removal. GetDocument after removal is
	// semantically broken (doc already cleared). Use WHERE result; here we GetDocument while docs are still
	// in bundle.Documents/documentPageMap. If harvest fails for a doc, we skip B-tree delete for that doc.
	// A: Log at Debug to avoid WARN storms. B: Hash delete for this docID still runs in the index cleanup loop.
	// C: B-tree entries for harvest-failed docIDs are removed via DeleteByDocumentIDs after the per-doc loop.
	//
	// Large-delete optimization: when len(docIDs) > harvestSkipThreshold, skip the harvest loop entirely.
	// N×GetDocument (each with findDocumentPage, GetDocumentPage, possible eviction) causes timeouts and
	// documentPages contention. Put all docIDs in harvestFailedDocIDs; C (DeleteByDocumentIDs) does one
	// full B-tree scan per index instead. Trade: N×GetDocument vs M×fullScan (M = number of B-trees).
	const harvestSkipThreshold = 500
	btreeKeys := make(map[string]map[string][]byte)  // docID -> indexName -> keyBytes
	harvestFailedDocIDs := make(map[string]struct{}) // C: docIDs where GetDocument failed; DeleteByDocumentIDs will clean B-trees
	commitSeqs := make(map[string]uint64)            // docID -> CommitSequence for hash index Delete; 0 when harvest skipped or not found
	// Build docIDToDoc from preFetchedDocs when provided so harvest can skip GetDocument
	var docIDToDoc map[string]*models.Document
	if len(preFetchedDocs) > 0 {
		docIDToDoc = make(map[string]*models.Document, len(preFetchedDocs))
		for _, d := range preFetchedDocs {
			if d != nil && d.DocumentID != "" {
				docIDToDoc[d.DocumentID] = d
			}
		}
	}
	harvestSchema := bundle.DocumentStructure.FieldSchema()
	if bundle.Indexes != nil && bundle.Database != nil {
		// Skip harvest loop only when over threshold AND no preFetchedDocs (avoids N×GetDocument)
		if len(docIDs) > harvestSkipThreshold && docIDToDoc == nil {
			for _, docID := range docIDs {
				harvestFailedDocIDs[docID] = struct{}{}
			}
			s.logger.Debugf("B-tree harvest: skipped for %d docs (>%d); C (DeleteByDocumentIDs) will clean B-trees", len(docIDs), harvestSkipThreshold)
		} else {
			for _, docID := range docIDs {
				var doc *models.Document
				if docIDToDoc != nil {
					if d, ok := docIDToDoc[docID]; ok {
						doc = d
					}
				}
				if doc == nil {
					var err error
					doc, err = s.GetDocument(bundle.Name, bundle.Database.Name, docID)
					if err != nil {
						harvestFailedDocIDs[docID] = struct{}{}
						s.logger.Debugf("B-tree harvest: failed to load document '%s': %v; B will clean hash, C will clean B-tree", docID, err)
						continue
					}
				}
				commitSeqs[docID] = doc.CommitSequence // PERF: harvest once for hash index cleanup; avoids N×GetDocument in hash loop
				for indexName, indexRef := range bundle.Indexes {
					if indexRef.IndexType != "btree" {
						continue
					}
					fieldName := indexRef.BTreeIndexField.FieldName
					fv, err := extractFieldValueForIndex(*doc, fieldName, harvestSchema)
					if err != nil {
						s.logger.Warnf("B-tree harvest: extract %s for %s: %v; skipping", fieldName, docID, err)
						continue
					}
					kb, err := convertValueToBytes(fv)
					if err != nil {
						s.logger.Warnf("B-tree harvest: convert for %s: %v; skipping", docID, err)
						continue
					}
					if btreeKeys[docID] == nil {
						btreeKeys[docID] = make(map[string][]byte)
					}
					btreeKeys[docID][indexName] = kb
				}
			}
		}
	}

	// Flush write buffer FIRST so pending ADDs/UPDATEs are on disk before tombstones (D7: keep FlushWriteBuffers).
	// Otherwise a crash could leave a tombstone for a "never existed" doc.
	err := s.store.FlushWriteBuffers(docCommand.BundleName)
	if err != nil {
		s.logger.Warnf("Failed to flush write buffers before delete: %v", err)
	}

	// Write ALL deletion markers in one batch. D2: delete is idempotent.
	// When lockInfo has LockedDocIDs, use WithLocks to skip storage bundle lock (no bundle-level locking).
	if len(lockInfo) > 0 && lockInfo[0] != nil && len(lockInfo[0].LockedDocIDs) > 0 {
		err = s.store.AppendDeletionMarkersBatchWithLocks(bundle, docIDs, lockInfo[0].LockedDocIDs)
	} else {
		err = s.store.AppendDeletionMarkersBatch(bundle, docIDs)
	}
	if err != nil {
		return fmt.Errorf("failed to append batch deletion markers: %w", err)
	}
	s.logger.Debugf("DELETE: Wrote %d deletion markers to disk", len(docIDs))
	// Observability (Phase 1b): log batch delete. TODO: metrics.DeleteBatchDuration, DeleteVerifySkipCount, requested vs deleted.
	s.logger.Infow("Delete batch", "bundle", docCommand.BundleName, "docCount", len(docIDs))

	if s.statsUpdater != nil && docIDToDoc != nil {
		delSchema := bundle.DocumentStructure.FieldSchema()
		for _, doc := range docIDToDoc {
			if doc == nil {
				continue
			}
			if delSchema != nil && len(doc.Values) > 0 {
				for i, name := range delSchema.Names {
					if i < len(doc.Values) {
						s.statsUpdater.IncrementalUpdate(bundle.Name, name, doc.Values[i].AsInterface(), nil, bundle.TotalDocuments)
					}
				}
			} else if doc.Data != nil {
				for fieldName, v := range doc.Data {
					s.statsUpdater.IncrementalUpdate(bundle.Name, fieldName, v, nil, bundle.TotalDocuments)
				}
			}
		}
	}

	// Close the write buffer so subsequent opens see the correct file size (including tombstones).
	// Subsequent ADDs will recreate the buffer. Documented in plan §1.5 CloseWriteBuffer.
	err = s.store.CloseWriteBuffer(docCommand.BundleName)
	if err != nil {
		s.logger.Warnf("Failed to close write buffer after tombstones: %v", err)
	}

	// D5: In-memory and cache invalidation once per batch (unify bulk and non-bulk). Remove from all structures
	// after disk write to maintain durability.
	// CRITICAL ORDERING: Delete from indexes FIRST (Step 1), then invalidate caches (Step 2).
	// This ensures queries immediately see documents as missing via hash index lookup,
	// avoiding slow page scans when documentPageMap entries are removed.

	// STEP 1: Delete from all indexes IMMEDIATELY (before cache invalidation)
	// D6: Pre-load each index once per batch (Phase 2). TODO: batched B-tree Delete and BatchDelete for hash.
	hashIndexes := make(map[string]*hashindex.HashIndexV3)
	btreeIndexes := make(map[string]*btreeindexV2.BTreeIndex)
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				idx, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Errorf("Failed to load hash index '%s': %v", indexName, err)
					continue
				}
				hashIndexes[indexName] = idx
			} else if indexRef.IndexType == "btree" {
				idx, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Errorf("Failed to load BTree index '%s': %v", indexName, err)
					continue
				}
				btreeIndexes[indexName] = idx
			}
		}
	}

	// Process each document for index cleanup (hash Delete, B-tree Delete using harvested keys).
	for _, documentID := range docIDs {
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
					hashIndex := hashIndexes[indexName]
					if hashIndex == nil {
						continue
					}
					// PHASE 4: MVCC - use commitSeq harvested in D6 (or 0 when harvest skipped / doc not found)
					commitSeq := commitSeqs[documentID]
					deleted, err := hashIndex.Delete(documentID, commitSeq)
					if err != nil {
						s.logger.Warnf("Failed to delete DocumentID '%s' from hash index '%s': %v", documentID, indexName, err)
					} else if deleted {
						s.logger.Debugf("Successfully deleted DocumentID '%s' from hash index '%s'", documentID, indexName)
					}
				} else if indexRef.IndexType == "btree" {
					btreeIndex := btreeIndexes[indexName]
					if btreeIndex == nil {
						continue
					}
					if m := btreeKeys[documentID]; m != nil {
						if keyBytes, ok := m[indexName]; ok {
							err := btreeIndex.Delete(keyBytes, documentID)
							if err != nil {
								s.logger.Warnf("Failed to delete document '%s' from BTree index '%s': %v", documentID, indexName, err)
							} else {
								s.logger.Debugf("Successfully deleted document '%s' from BTree index '%s'", documentID, indexName)
							}
						}
					}
				}
			}
		}
	}

	// C: ALWAYS run DeleteByDocumentIDs for ALL deleted docIDs to clean any stale B-tree entries.
	for _, btreeIndex := range btreeIndexes {
		if btreeIndex != nil {
			n, err := btreeIndex.DeleteByDocumentIDs(docIDs)
			if err != nil {
				s.logger.Warnf("B-tree DeleteByDocumentIDs cleanup: %v", err)
			} else if n > 0 {
				s.logger.Debugf("B-tree DeleteByDocumentIDs: removed %d total entries for deleted docIDs", n)
			}
		}
	}

	// Update SortedIndex to decrement count for COUNT(*) optimization
	if bundle.SortedIndex != nil {
		for _, documentID := range docIDs {
			if bundle.SortedIndex.Delete(documentID) {
				s.logger.Debugf("Removed DocumentID '%s' from SortedIndex", documentID)
			}
		}
	}

	// NOTE: Hash index flush to disk is deferred to avoid I/O bottleneck on every delete.
	// In-memory deletions above are sufficient for query correctness.
	// Indexes will be flushed during: clean shutdown, background compaction, or batch completion.
	// If server crashes, indexes may be stale but will be rebuilt automatically on next load.

	// STEP 2: WRITE-THROUGH CACHE: Invalidate pages containing deleted documents
	//    We invalidate entire pages to ensure deleted documents are not visible to subsequent reads.
	//    On next read, pages will be reloaded from disk with tombstone filtering applied.
	// PHASE 5: Use sharded cache for invalidation
	invalidatedPages := make(map[uint32]bool) // Track unique pages to invalidate
	for _, docID := range docIDs {
		if pageID, found := s.documentPageCache.GetPageID(docCommand.BundleName, docID); found {
			if !invalidatedPages[pageID] {
				// Invalidate the entire page containing this document
				pageKey := fmt.Sprintf("%s:%d", docCommand.BundleName, pageID)
				shardIdx := s.getPageShardIndex(pageKey)
				shard := s.pageShards[shardIdx]
				shard.mu.Lock()
				shard.deleteLocked(pageKey)
				shard.mu.Unlock()
				invalidatedPages[pageID] = true
				// Clear visibility map bit for deleted page
				s.clearVisibilityForPage(docCommand.BundleName, pageID)
			}
		}
		// Invalidate the document->page cache entry
		s.documentPageCache.InvalidateDocument(docCommand.BundleName, docID)
	}
	s.logger.Debugf("Write-through: Invalidated %d pages containing %d deleted documents for bundle '%s'",
		len(invalidatedPages), len(docIDs), docCommand.BundleName)

	// 3. Remove deleted docs from scanner's cache when SmartBundleScanner; keep scanner alive to
	//    avoid 20–30s stall on next SELECT (new scanner would have empty cachedPages + cold
	//    documentPages → full reload from disk). Only tear down when we can't do targeted invalidation.
	if v, exists := s.bundleScanners.Load(docCommand.BundleName); exists {
		if smartScanner, ok := v.(*documentscanner.SmartBundleScanner); ok {
			smartScanner.RemoveDocumentsFromCache(docIDs)
			// do NOT call RemoveDocumentScanner — keep scanner and its cachedPages
		} else {
			s.RemoveDocumentScanner(docCommand.BundleName)
		}
	}

	// PERFORMANCE: Invalidate all caches for this bundle (query planner + JOIN hash tables)
	s.invalidateBundleCaches(docCommand.BundleName)

	// STEP 7: Update command with deleted document IDs for response
	docCommand.DeletedDocumentIDs = docIDs //deletedDocumentIDs

	return nil
}

// Performance: Uses batch validation with HashIndexV3.BatchGet() for O(1) parallel lookups
// Safety: Requires CONFIRMED keyword (validated by caller) and performs full referential integrity validation
// Transaction: Caller must wrap in WAL transaction at server layer for atomicity
//
// Error Format: Returns aggregated violation counts (e.g., "423 references in 'Books' via 'author_id'")
// instead of individual document errors for better UX with large datasets.
//
// TODO: I will add configurable soft-delete flag for bulk operations to enable tombstone-only mode
// with background compaction instead of immediate physical deletion
func (s *BundleService) DeleteAllDocumentsFromBundle(
	docCommand *models.DocumentDeleteCommand,
	bundle *models.Bundle,
) error {
	args := settings.GetSettings()
	if args.Debug {
		s.logger.Debugf("Starting bulk delete of all documents from bundle '%s'", docCommand.BundleName)
	}

	// Acquire write lock for the bundle
	if err := s.AcquireBundleWriteLock(bundle.Name); err != nil {
		return fmt.Errorf("failed to acquire write lock for bundle '%s': %w", bundle.Name, err)
	}
	defer s.ReleaseBundleWriteLock(bundle.Name)

	// Get all document IDs from the bundle
	allDocs, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
	if err != nil {
		return fmt.Errorf("failed to retrieve documents for bulk delete: %w", err)
	}

	if len(allDocs) == 0 {
		s.logger.Debugf("Bundle '%s' is already empty, nothing to delete", bundle.Name)
		docCommand.DeletedDocumentIDs = []string{}
		return nil
	}

	// Extract document IDs for validation and deletion
	docIDs := make([]string, 0, len(allDocs))
	for _, doc := range allDocs {
		docIDs = append(docIDs, doc.DocumentID)
	}

	s.logger.Debugf("Bulk delete will validate and delete %d documents from bundle '%s'", len(docIDs), bundle.Name)

	// Perform batch referential integrity validation
	validator := NewReferentialIntegrityValidator(s, s.logger)
	if err := validator.ValidateBulkDeleteOptimized(bundle, docIDs); err != nil {
		return fmt.Errorf("bulk delete failed referential integrity check: %w", err)
	}

	// All validations passed - proceed with deletion using internal method (lock already held)
	// Skip individual metadata updates - we'll do a single bulk update after all deletions.
	// Pass allDocs as preFetchedDocs so harvest avoids N×GetDocument.
	if err := s.deleteDocumentsInternal(bundle, docCommand, docIDs, true, allDocs); err != nil {
		return fmt.Errorf("bulk delete execution failed: %w", err)
	}

	// CRITICAL FIX: Do NOT schedule decrement_docs metadata update
	// In append-only storage, tombstones are still entries on disk, so TotalDocuments
	// should represent total document entries (including tombstones), not active documents.
	// See DeleteDocumentFromBundleFile for detailed explanation.
	// s.scheduleMetadataUpdate(docCommand.BundleName, "decrement_docs", int64(len(docIDs))) // REMOVED: Causes corruption

	// Flush all pending operations to ensure consistency
	if err := s.FlushAllBuffers(); err != nil {
		s.logger.Warnf("Failed to flush buffers after bulk delete: %v", err)
	}

	s.logger.Debugf("Successfully deleted %d documents from bundle '%s'", len(docIDs), bundle.Name)
	return nil
}
