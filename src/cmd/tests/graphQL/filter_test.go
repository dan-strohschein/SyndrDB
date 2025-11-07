package main

import (
	"strings"
	"testing"

	"syndrdb/src/internal/domain/models"
	graphql "syndrdb/src/internal/graphQL"
)

// TestFilterParsingBasic tests basic filter parsing
func TestFilterParsingBasic(t *testing.T) {
	input := map[string]interface{}{
		"status": map[string]interface{}{
			"eq": "active",
		},
	}

	result, err := graphql.ParseWhereInput(input)
	if err != nil {
		t.Fatalf("ParseWhereInput failed: %v", err)
	}

	if result == nil || len(result.Fields) == 0 {
		t.Error("Expected parsed fields")
	}

	t.Log("✓ Successfully parsed basic filter")
}

// TestFilterTranslation tests filter translation to WHERE clauses
func TestFilterTranslation(t *testing.T) {
	translator := graphql.NewFilterTranslator()

	input := &graphql.WhereInput{
		Fields: map[string]*graphql.FieldFilter{
			"status": {Eq: "active"},
		},
	}

	result, err := translator.TranslateWhereInput(input)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	if len(result.Clauses) == 0 {
		t.Error("Expected at least 1 clause")
	}

	t.Log("✓ Successfully translated filter")
}

// TestFilterWithAND tests AND operator
func TestFilterWithAND(t *testing.T) {
	translator := graphql.NewFilterTranslator()

	input := &graphql.WhereInput{
		AND: []*graphql.WhereInput{
			{Fields: map[string]*graphql.FieldFilter{"status": {Eq: "active"}}},
			{Fields: map[string]*graphql.FieldFilter{"age": {Gt: 18}}},
		},
	}

	result, err := translator.TranslateWhereInput(input)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	if result.Logic != "AND" {
		t.Errorf("Expected logic 'AND', got '%s'", result.Logic)
	}

	t.Log("✓ Successfully translated AND operator")
}

// TestFilterWithOR tests OR operator
func TestFilterWithOR(t *testing.T) {
	translator := graphql.NewFilterTranslator()

	input := &graphql.WhereInput{
		OR: []*graphql.WhereInput{
			{Fields: map[string]*graphql.FieldFilter{"status": {Eq: "active"}}},
			{Fields: map[string]*graphql.FieldFilter{"status": {Eq: "pending"}}},
		},
	}

	result, err := translator.TranslateWhereInput(input)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	if result.Logic != "OR" {
		t.Errorf("Expected logic 'OR', got '%s'", result.Logic)
	}

	t.Log("✓ Successfully translated OR operator")
}

// TestBuildWhereString tests WHERE clause string generation
func TestBuildWhereString(t *testing.T) {
	translator := graphql.NewFilterTranslator()

	whereGroup := &models.WhereGroup{
		Clauses: []models.WhereClause{
			{Field: "status", Operator: "=", Value: "active", Logic: "AND"},
		},
		Logic: "AND",
	}

	whereStr, err := translator.BuildWhereClauseString(whereGroup)
	if err != nil {
		t.Fatalf("BuildWhereClauseString failed: %v", err)
	}

	if whereStr == "" {
		t.Error("Expected non-empty WHERE clause")
	}

	if !strings.Contains(whereStr, "status") {
		t.Error("Expected WHERE clause to contain 'status'")
	}

	t.Logf("✓ Generated WHERE clause: %s", whereStr)
}

// TestOrderByParsing tests OrderBy parsing
func TestOrderByParsing(t *testing.T) {
	input := map[string]interface{}{
		"field":     "name",
		"direction": "ASC",
	}

	result, err := graphql.ParseOrderByInput(input)
	if err != nil {
		t.Fatalf("ParseOrderByInput failed: %v", err)
	}

	if len(result) == 0 {
		t.Error("Expected at least one order by")
	}

	t.Log("✓ Successfully parsed OrderBy")
}

// TestOrderByTranslation tests OrderBy translation
func TestOrderByTranslation(t *testing.T) {
	translator := graphql.NewFilterTranslator()

	input := []graphql.OrderByInput{
		{Field: "name", Direction: "ASC"},
		{Field: "age", Direction: "DESC"},
	}

	result := translator.TranslateOrderBy(input)

	if result != "name ASC, age DESC" {
		t.Errorf("Expected 'name ASC, age DESC', got '%s'", result)
	}

	t.Log("✓ Successfully translated OrderBy")
}

// TestAllOperators tests all comparison operators
func TestAllOperators(t *testing.T) {
	translator := graphql.NewFilterTranslator()

	operators := []struct {
		name  string
		setup func() *graphql.WhereInput
		check string
	}{
		{
			name: "Eq operator",
			setup: func() *graphql.WhereInput {
				return &graphql.WhereInput{
					Fields: map[string]*graphql.FieldFilter{
						"field": {Eq: "value"},
					},
				}
			},
			check: "=",
		},
		{
			name: "Ne operator",
			setup: func() *graphql.WhereInput {
				return &graphql.WhereInput{
					Fields: map[string]*graphql.FieldFilter{
						"field": {Ne: "value"},
					},
				}
			},
			check: "!=",
		},
		{
			name: "Gt operator",
			setup: func() *graphql.WhereInput {
				return &graphql.WhereInput{
					Fields: map[string]*graphql.FieldFilter{
						"field": {Gt: 10},
					},
				}
			},
			check: ">",
		},
		{
			name: "Gte operator",
			setup: func() *graphql.WhereInput {
				return &graphql.WhereInput{
					Fields: map[string]*graphql.FieldFilter{
						"field": {Gte: 10},
					},
				}
			},
			check: ">=",
		},
		{
			name: "Lt operator",
			setup: func() *graphql.WhereInput {
				return &graphql.WhereInput{
					Fields: map[string]*graphql.FieldFilter{
						"field": {Lt: 100},
					},
				}
			},
			check: "<",
		},
		{
			name: "Lte operator",
			setup: func() *graphql.WhereInput {
				return &graphql.WhereInput{
					Fields: map[string]*graphql.FieldFilter{
						"field": {Lte: 100},
					},
				}
			},
			check: "<=",
		},
	}

	for _, op := range operators {
		t.Run(op.name, func(t *testing.T) {
			input := op.setup()
			result, err := translator.TranslateWhereInput(input)
			if err != nil {
				t.Fatalf("Translation failed: %v", err)
			}

			if len(result.Clauses) == 0 {
				t.Fatal("Expected at least one clause")
			}

			if result.Clauses[0].Operator != op.check {
				t.Errorf("Expected operator '%s', got '%s'", op.check, result.Clauses[0].Operator)
			}

			t.Logf("✓ Correctly mapped to '%s'", op.check)
		})
	}
}

// TestComplexFilter tests a complex nested filter
func TestComplexFilter(t *testing.T) {
	translator := graphql.NewFilterTranslator()

	input := &graphql.WhereInput{
		OR: []*graphql.WhereInput{
			{
				AND: []*graphql.WhereInput{
					{Fields: map[string]*graphql.FieldFilter{"status": {Eq: "active"}}},
					{Fields: map[string]*graphql.FieldFilter{"age": {Gt: 18}}},
				},
			},
			{Fields: map[string]*graphql.FieldFilter{"status": {Eq: "premium"}}},
		},
	}

	result, err := translator.TranslateWhereInput(input)
	if err != nil {
		t.Fatalf("Translation failed: %v", err)
	}

	if result.Logic != "OR" {
		t.Errorf("Expected root logic 'OR', got '%s'", result.Logic)
	}

	if len(result.SubGroups) < 2 {
		t.Errorf("Expected at least 2 subgroups, got %d", len(result.SubGroups))
	}

	t.Log("✓ Successfully translated complex nested filter")
}
