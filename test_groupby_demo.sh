#!/bin/bash

# GROUP BY Test Script
# This script demonstrates the GROUP BY functionality in SyndrDB
# It creates sample data and runs various GROUP BY queries

echo "=== SyndrDB GROUP BY Demonstration ==="
echo ""

# Build the test executable
echo "Building test executable..."
go build -o ./bin/tests/test_runner ./src/cmd/tests/

# Run the GROUP BY tests
echo "Running GROUP BY tests..."
./bin/tests/test_runner

echo ""
echo "=== GROUP BY Demonstration Complete ==="

# Example queries that would work with a running SyndrDB instance:
echo ""
echo "Example GROUP BY queries for SyndrDB:"
echo ""
echo "1. Basic COUNT by category:"
echo "   SELECT category, COUNT(*) FROM \"Products\" GROUP BY category"
echo ""
echo "2. Sales summary by region:"
echo "   SELECT region, SUM(amount), AVG(amount) FROM \"Sales\" GROUP BY region"
echo ""
echo "3. Employee statistics by department:"
echo "   SELECT dept, COUNT(*) as emp_count, AVG(salary) as avg_salary FROM \"Employees\" GROUP BY dept"
echo ""
echo "4. Complex query with HAVING and ORDER BY:"
echo "   SELECT category, COUNT(*) as total, SUM(price) as revenue FROM \"Products\" GROUP BY category HAVING COUNT(*) > 5 ORDER BY revenue DESC"
echo ""
echo "5. Multiple grouping fields:"
echo "   SELECT region, status, COUNT(*), AVG(value) FROM \"Orders\" GROUP BY region, status"
