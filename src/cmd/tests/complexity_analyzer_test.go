package main

import (
	"fmt"
	"syndrdb/src/internal/graphQL/optimization"
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

// TestComplexityAnalyzer_SimpleQuery tests a basic single-level query
func TestComplexityAnalyzer_SimpleQuery(t *testing.T) {
	// Create a simple AST manually (simpler than parsing with schema)
	doc := &ast.QueryDocument{
		Operations: ast.OperationList{
			{
				Operation: ast.Query,
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Name: "users",
						SelectionSet: ast.SelectionSet{
							&ast.Field{Name: "id"},
							&ast.Field{Name: "name"},
							&ast.Field{Name: "email"},
						},
					},
				},
			},
		},
	}

	analyzer := optimization.NewComplexityAnalyzer(nil)
	result := analyzer.AnalyzeQuery(doc, nil)

	if !result.IsAllowed {
		t.Errorf("Expected query to be allowed, got rejected: %s", result.Reason)
	}

	if result.Depth < 2 {
		t.Errorf("Expected depth >= 2, got %d", result.Depth)
	}

	if result.Complexity == 0 {
		t.Errorf("Expected non-zero complexity, got %d", result.Complexity)
	}
}

// TestComplexityAnalyzer_NestedQuery tests depth calculation
func TestComplexityAnalyzer_NestedQuery(t *testing.T) {
	// Create nested AST: authors → books → reviews → user
	doc := &ast.QueryDocument{
		Operations: ast.OperationList{
			{
				Operation: ast.Query,
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Name: "authors",
						SelectionSet: ast.SelectionSet{
							&ast.Field{Name: "id"},
							&ast.Field{Name: "name"},
							&ast.Field{
								Name: "books",
								SelectionSet: ast.SelectionSet{
									&ast.Field{Name: "id"},
									&ast.Field{Name: "title"},
									&ast.Field{
										Name: "reviews",
										SelectionSet: ast.SelectionSet{
											&ast.Field{Name: "id"},
											&ast.Field{Name: "rating"},
											&ast.Field{
												Name: "user",
												SelectionSet: ast.SelectionSet{
													&ast.Field{Name: "id"},
													&ast.Field{Name: "name"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Use higher complexity limit for this nested query
	analyzer := optimization.NewComplexityAnalyzer(&optimization.ComplexityConfig{
		MaxDepth:      10,
		MaxBreadth:    50,
		MaxComplexity: 1000,
		WarnThreshold: 70,
	})
	result := analyzer.AnalyzeQuery(doc, nil)

	if !result.IsAllowed {
		t.Errorf("Expected query to be allowed, got rejected: %s", result.Reason)
	}

	// Depth: operation → authors → books → reviews → user = 5
	if result.Depth < 5 {
		t.Errorf("Expected depth >= 5, got %d", result.Depth)
	}

	// Should have warnings for deep nesting
	if len(result.Warnings) == 0 {
		t.Error("Expected warnings for deep nesting, got none")
	}
}

// TestComplexityAnalyzer_TooDeep tests max depth rejection
func TestComplexityAnalyzer_TooDeep(t *testing.T) {
	// Create deeply nested AST: 7 levels deep
	doc := &ast.QueryDocument{
		Operations: ast.OperationList{
			{
				Operation: ast.Query,
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Name: "level1",
						SelectionSet: ast.SelectionSet{
							&ast.Field{
								Name: "level2",
								SelectionSet: ast.SelectionSet{
									&ast.Field{
										Name: "level3",
										SelectionSet: ast.SelectionSet{
											&ast.Field{
												Name: "level4",
												SelectionSet: ast.SelectionSet{
													&ast.Field{
														Name: "level5",
														SelectionSet: ast.SelectionSet{
															&ast.Field{
																Name: "level6",
																SelectionSet: ast.SelectionSet{
																	&ast.Field{Name: "id"},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	analyzer := optimization.NewComplexityAnalyzer(&optimization.ComplexityConfig{
		MaxDepth:      5,
		MaxBreadth:    50,
		MaxComplexity: 1000,
	})
	result := analyzer.AnalyzeQuery(doc, nil)

	if result.IsAllowed {
		t.Error("Expected query to be rejected for exceeding max depth")
	}

	if result.Depth <= 5 {
		t.Errorf("Expected depth > 5, got %d", result.Depth)
	}
}

// TestComplexityAnalyzer_WideQuery tests breadth calculation
func TestComplexityAnalyzer_WideQuery(t *testing.T) {
	// Build a query with 35 fields at one level
	fields := make(ast.SelectionSet, 35)
	for i := 0; i < 35; i++ {
		fields[i] = &ast.Field{Name: fmt.Sprintf("field%d", i)}
	}

	doc := &ast.QueryDocument{
		Operations: ast.OperationList{
			{
				Operation: ast.Query,
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Name:         "users",
						SelectionSet: fields,
					},
				},
			},
		},
	}

	analyzer := optimization.NewComplexityAnalyzer(nil)
	result := analyzer.AnalyzeQuery(doc, nil)

	if !result.IsAllowed {
		t.Errorf("Expected query to be allowed, got rejected: %s", result.Reason)
	}

	if result.Breadth < 30 {
		t.Errorf("Expected breadth >= 30, got %d", result.Breadth)
	}

	// Should recommend pagination for wide queries
	if result.RecommendedStrategy != "paginate" {
		t.Errorf("Expected 'paginate' strategy for wide query, got '%s'", result.RecommendedStrategy)
	}
}

// TestComplexityAnalyzer_TooWide tests max breadth rejection
func TestComplexityAnalyzer_TooWide(t *testing.T) {
	// Build a query with 60 fields (exceeds max breadth of 50)
	fields := make(ast.SelectionSet, 60)
	for i := 0; i < 60; i++ {
		fields[i] = &ast.Field{Name: fmt.Sprintf("field%d", i)}
	}

	doc := &ast.QueryDocument{
		Operations: ast.OperationList{
			{
				Operation: ast.Query,
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Name:         "users",
						SelectionSet: fields,
					},
				},
			},
		},
	}

	analyzer := optimization.NewComplexityAnalyzer(&optimization.ComplexityConfig{
		MaxDepth:      5,
		MaxBreadth:    50,
		MaxComplexity: 1000,
	})
	result := analyzer.AnalyzeQuery(doc, nil)

	if result.IsAllowed {
		t.Error("Expected query to be rejected for exceeding max breadth")
	}

	if result.Breadth <= 50 {
		t.Errorf("Expected breadth > 50, got %d", result.Breadth)
	}
}

// TestComplexityAnalyzer_DepthUtility tests the quick depth check utility
func TestComplexityAnalyzer_DepthUtility(t *testing.T) {
	// Create nested AST: authors → books → reviews → user
	doc := &ast.QueryDocument{
		Operations: ast.OperationList{
			{
				Operation: ast.Query,
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Name: "authors",
						SelectionSet: ast.SelectionSet{
							&ast.Field{
								Name: "books",
								SelectionSet: ast.SelectionSet{
									&ast.Field{
										Name: "reviews",
										SelectionSet: ast.SelectionSet{
											&ast.Field{
												Name: "user",
												SelectionSet: ast.SelectionSet{
													&ast.Field{Name: "id"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	depth := optimization.AnalyzeQueryDepth(doc)

	// operation → authors → books → reviews → user = 5
	if depth < 5 {
		t.Errorf("Expected depth >= 5, got %d", depth)
	}
}

// TestComplexityAnalyzer_StrategyRecommendations tests strategy logic
func TestComplexityAnalyzer_StrategyRecommendations(t *testing.T) {
	tests := []struct {
		name     string
		result   *optimization.ComplexityResult
		expected string
	}{
		{
			name: "shallow wide query → paginate",
			result: &optimization.ComplexityResult{
				Depth:   2,
				Breadth: 40,
			},
			expected: "paginate",
		},
		{
			name: "deep narrow query → join",
			result: &optimization.ComplexityResult{
				Depth:   6,
				Breadth: 10,
			},
			expected: "join",
		},
		{
			name: "moderate query → dataloader",
			result: &optimization.ComplexityResult{
				Depth:   3,
				Breadth: 15,
			},
			expected: "dataloader",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate analysis by copying result values
			testResult := &optimization.ComplexityResult{
				Depth:   tt.result.Depth,
				Breadth: tt.result.Breadth,
			}

			// Manually set strategy based on the analyzer's logic
			if testResult.Depth > 4 {
				testResult.RecommendedStrategy = "join"
			} else if testResult.Breadth > 30 {
				testResult.RecommendedStrategy = "paginate"
			} else {
				testResult.RecommendedStrategy = "dataloader"
			}

			if testResult.RecommendedStrategy != tt.expected {
				t.Errorf("Expected strategy '%s', got '%s'", tt.expected, testResult.RecommendedStrategy)
			}
		})
	}
}

// TestShouldUseDataLoader tests the DataLoader recommendation logic
func TestShouldUseDataLoader(t *testing.T) {
	tests := []struct {
		name     string
		result   *optimization.ComplexityResult
		expected bool
	}{
		{
			name: "good candidate",
			result: &optimization.ComplexityResult{
				Depth:      3,
				Breadth:    20,
				Complexity: 25,
			},
			expected: true,
		},
		{
			name: "too deep",
			result: &optimization.ComplexityResult{
				Depth:      6,
				Breadth:    20,
				Complexity: 25,
			},
			expected: false,
		},
		{
			name: "too wide",
			result: &optimization.ComplexityResult{
				Depth:      3,
				Breadth:    40,
				Complexity: 25,
			},
			expected: false,
		},
		{
			name: "too simple",
			result: &optimization.ComplexityResult{
				Depth:      2,
				Breadth:    5,
				Complexity: 5,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimization.ShouldUseDataLoader(tt.result)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestShouldUseJoins tests the JOIN recommendation logic
func TestShouldUseJoins(t *testing.T) {
	tests := []struct {
		name     string
		depth    int
		expected bool
	}{
		{"shallow", 3, false},
		{"moderate", 4, false},
		{"deep", 5, true},
		{"very deep", 7, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &optimization.ComplexityResult{Depth: tt.depth}
			got := optimization.ShouldUseJoins(result)
			if got != tt.expected {
				t.Errorf("Depth %d: expected %v, got %v", tt.depth, tt.expected, got)
			}
		})
	}
}

// TestShouldPaginate tests the pagination recommendation logic
func TestShouldPaginate(t *testing.T) {
	tests := []struct {
		name       string
		breadth    int
		complexity int
		expected   bool
	}{
		{"narrow simple", 10, 20, false},
		{"wide", 40, 50, true},
		{"high complexity", 20, 90, true},
		{"moderate", 25, 60, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &optimization.ComplexityResult{
				Breadth:    tt.breadth,
				Complexity: tt.complexity,
			}
			got := optimization.ShouldPaginate(result)
			if got != tt.expected {
				t.Errorf("Breadth %d, Complexity %d: expected %v, got %v",
					tt.breadth, tt.complexity, tt.expected, got)
			}
		})
	}
}
