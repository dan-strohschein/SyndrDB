package server

import (
	"context"
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"
	"time"

	"go.uber.org/zap"
)

// IsCursorCommand checks if a command is a cursor-related command.
func IsCursorCommand(command string) bool {
	upper := strings.ToUpper(strings.TrimSpace(command))
	return strings.HasPrefix(upper, "DECLARE ") ||
		strings.HasPrefix(upper, "FETCH ") ||
		(strings.HasPrefix(upper, "CLOSE ") && !strings.HasPrefix(upper, "CLOSE CONNECTION"))
}

// HandleCursorCommand dispatches cursor commands (DECLARE, FETCH, CLOSE).
func HandleCursorCommand(
	ctx context.Context,
	command string,
	session *Session,
	serviceManager ServiceManager,
	database *models.Database,
	logger *zap.SugaredLogger,
	startTime time.Time,
) (interface{}, error) {
	if session == nil {
		return nil, errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			"no active session for cursor operation", errors.LayerTransaction)
	}

	if session.Cursors == nil {
		return nil, errors.New(errors.ERR_INTERNAL,
			"cursor manager not initialized", errors.LayerTransaction)
	}

	// Tokenize
	tokenizer := syndrQL.NewTokenizer(command)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to tokenize cursor command", errors.LayerParser)
	}

	// Remove EOF token
	if len(tokens) > 0 && tokens[len(tokens)-1].Type == syndrQL.TOKEN_EOF {
		tokens = tokens[:len(tokens)-1]
	}

	// Parse cursor command
	cursorNode, err := syndrQL.ParseCursorCommand(tokens, command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse cursor command", errors.LayerParser)
	}

	switch cursorNode.Type {
	case syndrQL.CursorDeclare:
		return handleDeclare(ctx, cursorNode, session, serviceManager, database, logger)
	case syndrQL.CursorFetch:
		return handleFetch(cursorNode, session, logger, startTime)
	case syndrQL.CursorClose:
		return handleClose(cursorNode, session, logger)
	default:
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("unknown cursor command type: %s", cursorNode.Type),
			errors.LayerParser)
	}
}

// handleDeclare handles DECLARE cursor_name CURSOR FOR <query>
func handleDeclare(
	ctx context.Context,
	node *syndrQL.CursorNode,
	session *Session,
	serviceManager ServiceManager,
	database *models.Database,
	logger *zap.SugaredLogger,
) (interface{}, error) {
	// Create a cancellable context for the cursor's query execution.
	// This context lives as long as the cursor is open.
	cursorCtx, cancel := context.WithCancel(ctx)

	// Step 1: Parse the query
	query, err := ParseQuery(node.Query, logger)
	if err != nil {
		cancel()
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse cursor query", errors.LayerParser)
	}

	// Step 2: Create execution plan
	plan, err := serviceManager.UnifiedPlanner.CreatePlan(query, database)
	if err != nil {
		cancel()
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", query.FromBundle)
	}

	// Step 3: Add MVCC snapshot to context
	if session.IsInTransaction() {
		snapshot := session.GetMVCCSnapshot()
		if snapshot != nil {
			snapshotInfo := &planner.SnapshotInfo{
				SnapshotSequence: snapshot.SnapshotSequence,
				TransactionID:    snapshot.TransactionID,
				ActiveTxIDs:      snapshot.ActiveTxIDs,
			}
			cursorCtx = planner.WithSnapshotInfo(cursorCtx, snapshotInfo)
		}
	}
	// For non-transactional, set read-committed snapshot
	if planner.GetSnapshotInfoFromContext(cursorCtx) == nil && serviceManager.WALManager != nil {
		if snapshotMgr := serviceManager.WALManager.GetSnapshotManager(); snapshotMgr != nil {
			currentSeq := snapshotMgr.GetCurrentSequence()
			if currentSeq > 0 {
				cursorCtx = planner.WithSnapshotInfo(cursorCtx, &planner.SnapshotInfo{
					SnapshotSequence: currentSeq,
				})
			}
		}
	}

	// Step 4: Get an iterator from the plan
	var iterNode planner.IteratorNode
	fields := query.SelectFields

	if plan.UseIterator && plan.IteratorFactory != nil {
		// Plan supports native iteration
		iterNode = plan.IteratorFactory()
	} else if iterableRoot, ok := plan.RootNode.(planner.IterableNode); ok {
		// Root node can produce an iterator
		iterNode = iterableRoot.AsIterator()
	} else {
		// Fallback: materialize the plan and wrap in SliceIterator
		results, execErr := plan.RootNode.Execute(cursorCtx)
		if execErr != nil {
			cancel()
			return nil, errors.ConvertError(execErr, errors.LayerQuery)
		}
		docs := make([]*models.Document, 0, len(results))
		for _, doc := range results {
			docs = append(docs, doc)
		}
		iterNode = NewSliceIterator(docs)
	}

	// Step 5: Initialize the iterator
	if err := iterNode.Init(cursorCtx); err != nil {
		iterNode.Close()
		cancel()
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL,
			"failed to initialize cursor iterator", errors.LayerQuery)
	}

	// Step 6: Register in session's CursorManager
	if err := session.Cursors.Declare(node.CursorName, iterNode, fields, cancel); err != nil {
		iterNode.Close()
		cancel()
		return nil, err
	}

	return &CommandResponse{
		Result:      fmt.Sprintf("Cursor declared: %s", node.CursorName),
		ResultCount: 0,
	}, nil
}

// handleFetch handles FETCH N FROM cursor_name
func handleFetch(
	node *syndrQL.CursorNode,
	session *Session,
	logger *zap.SugaredLogger,
	startTime time.Time,
) (interface{}, error) {
	fetchCount := node.FetchCount
	if fetchCount == 0 {
		fetchCount = 10000 // FETCH ALL caps at 10K per call to avoid OOM
	}

	docs, fields, hasMore, err := session.Cursors.Fetch(node.CursorName, fetchCount)
	if err != nil {
		return nil, err
	}

	executionTime := float64(time.Since(startTime).Microseconds()) / 1000.0

	resp := &CommandResponse{
		ResultCount:     len(docs),
		ExecutionTimeMS: executionTime,
		StreamSlice:     docs,
		StreamFields:    fields,
	}

	if !hasMore {
		resp.Result = "CURSOR_EXHAUSTED"
	}

	return resp, nil
}

// handleClose handles CLOSE cursor_name
func handleClose(
	node *syndrQL.CursorNode,
	session *Session,
	logger *zap.SugaredLogger,
) (interface{}, error) {
	if err := session.Cursors.Close(node.CursorName); err != nil {
		return nil, err
	}

	return &CommandResponse{
		Result:      fmt.Sprintf("Cursor closed: %s", node.CursorName),
		ResultCount: 0,
	}, nil
}

// SliceIterator wraps a materialized []*Document slice as an IteratorNode.
// Used as a fallback when the execution plan cannot produce a true iterator.
type SliceIterator struct {
	docs  []*models.Document
	index int
}

func NewSliceIterator(docs []*models.Document) *SliceIterator {
	return &SliceIterator{docs: docs, index: 0}
}

func (si *SliceIterator) Init(ctx context.Context) error {
	si.index = 0
	return nil
}

func (si *SliceIterator) Next() (*models.Document, error) {
	if si.index >= len(si.docs) {
		return nil, nil // EOF
	}
	doc := si.docs[si.index]
	si.index++
	return doc, nil
}

func (si *SliceIterator) Close() error {
	si.docs = nil
	si.index = 0
	return nil
}
