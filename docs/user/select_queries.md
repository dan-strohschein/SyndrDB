# 📘 SELECT Queries in SyndrDB

Complete guide to querying data in SyndrDB using SELECT statements.

---

## 📑 Table of Contents

- [**SELECT Basics**](#-select-basics)
  - [Simple SELECT](#simple-select)
  - [Field Selection](#field-selection)
  - [DISTINCT](#distinct)
- [**FROM Clause**](#-from-clause)
- [**WHERE Clause**](#-where-clause)
  - [Comparison Operators](#comparison-operators)
  - [Logical Operators](#logical-operators)
  - [IN Operator](#in-operator)
  - [LIKE Pattern Matching](#like-pattern-matching)
- [**JOIN Operations**](#-join-operations)
  - [INNER JOIN](#inner-join)
  - [LEFT JOIN](#left-join)
  - [RIGHT JOIN](#right-join)
- [**WITH RELATIONSHIP**](#-with-relationship)
- [**ORDER BY Clause**](#-order-by-clause)
- [**GROUP BY Clause**](#-group-by-clause)
- [**HAVING Clause**](#-having-clause)
- [**LIMIT & TOP**](#-limit--top)
- [**Aggregate Functions**](#-aggregate-functions)
  - [COUNT()](#count)
  - [SUM()](#sum)
  - [AVG()](#avg)
  - [MIN()](#min)
  - [MAX()](#max)
- [**Scalar Functions**](#-scalar-functions)
- [**Subqueries**](#-subqueries)
- [**Complex Query Examples**](#-complex-query-examples)

---

## 🔍 SELECT Basics

### Simple SELECT

Retrieve all documents from a bundle:

```sql
SELECT * FROM Authors
```

**Output:**
```json
[
  {"id": "uuid-1", "name": "Jane Austen", "age": 41},
  {"id": "uuid-2", "name": "Mark Twain", "age": 74}
]
```

### Field Selection

Specify which fields to return:

```sql
SELECT name, age FROM Authors
```

**Output:**
```json
[
  {"name": "Jane Austen", "age": 41},
  {"name": "Mark Twain", "age": 74}
]
```

**Multiple fields with aliases:**

```sql
SELECT 
    name AS author_name,
    age AS years_old,
    country AS origin
FROM Authors
```

### DISTINCT

Remove duplicate values:

```sql
SELECT DISTINCT country FROM Authors
```

```sql
SELECT DISTINCT age, country FROM Authors
```

> 💡 **Tip:** DISTINCT applies to the entire row when multiple fields are selected.

---

## 📦 FROM Clause

The `FROM` clause specifies which bundle (table) to query.

**Syntax:**
```sql
FROM <bundle_name>
```

**Examples:**

```sql
SELECT * FROM Books

SELECT title, price FROM Products

SELECT id, created_at FROM Orders
```

**Bundle Aliases:**

```sql
SELECT a.name, a.age 
FROM Authors AS a
```

> ⚠️ **Note:** Bundle names are case-sensitive in SyndrDB.

---

## 🔎 WHERE Clause

Filter documents based on conditions.

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal to | `age = 25` |
| `!=` or `<>` | Not equal to | `status != 'draft'` |
| `>` | Greater than | `price > 100` |
| `>=` | Greater than or equal | `age >= 18` |
| `<` | Less than | `quantity < 10` |
| `<=` | Less than or equal | `rating <= 5` |

**Examples:**

```sql
SELECT * FROM Books WHERE price > 20

SELECT * FROM Users WHERE age >= 18

SELECT * FROM Products WHERE category = 'Electronics'
```

### Logical Operators

**AND:**
```sql
SELECT * FROM Books 
WHERE price > 10 AND price < 50
```

**OR:**
```sql
SELECT * FROM Books 
WHERE genre = 'Fiction' OR genre = 'Mystery'
```

**NOT:**
```sql
SELECT * FROM Books 
WHERE NOT genre = 'Romance'
```

**Combined:**
```sql
SELECT * FROM Books 
WHERE (genre = 'Fiction' OR genre = 'Mystery') 
  AND price < 30 
  AND NOT out_of_stock = true
```

### IN Operator

Check if a value matches any value in a list:

```sql
SELECT * FROM Books 
WHERE genre IN ('Fiction', 'Mystery', 'Thriller')
```

```sql
SELECT * FROM Orders 
WHERE status IN ('pending', 'processing')
```

**NOT IN:**
```sql
SELECT * FROM Products 
WHERE category NOT IN ('Discontinued', 'Archived')
```

### LIKE Pattern Matching

Match string patterns using wildcards:

| Pattern | Description | Example |
|---------|-------------|---------|
| `%` | Match any sequence of characters | `'%son'` matches "Johnson", "Anderson" |
| `_` | Match single character | `'J_ne'` matches "Jane", "June" |

**Examples:**

```sql
-- Names starting with 'A'
SELECT * FROM Authors WHERE name LIKE 'A%'

-- Names ending with 'son'
SELECT * FROM Authors WHERE name LIKE '%son'

-- Names containing 'ann'
SELECT * FROM Authors WHERE name LIKE '%ann%'

-- Four-letter names starting with 'J'
SELECT * FROM Authors WHERE name LIKE 'J___'
```

**Case-insensitive matching:**
```sql
SELECT * FROM Books WHERE LOWER(title) LIKE '%harry potter%'
```

---

## 🔗 JOIN Operations

Combine documents from multiple bundles based on related fields.

### INNER JOIN

Returns documents that have matching values in both bundles:

**Syntax:**
```sql
SELECT <fields>
FROM <bundle1>
INNER JOIN <bundle2> ON <bundle1.field> = <bundle2.field>
```

**Example:**

```sql
SELECT 
    Books.title,
    Authors.name AS author_name
FROM Books
INNER JOIN Authors ON Books.author_id = Authors.id
```

**Output:**
```json
[
  {"title": "Pride and Prejudice", "author_name": "Jane Austen"},
  {"title": "Tom Sawyer", "author_name": "Mark Twain"}
]
```

**Multiple JOINs:**

```sql
SELECT 
    Orders.order_number,
    Customers.name AS customer_name,
    Products.title AS product_name
FROM Orders
INNER JOIN Customers ON Orders.customer_id = Customers.id
INNER JOIN Products ON Orders.product_id = Products.id
```

### LEFT JOIN

Returns all documents from the left bundle and matching documents from the right bundle:

```sql
SELECT 
    Authors.name,
    Books.title
FROM Authors
LEFT JOIN Books ON Authors.id = Books.author_id
```

> 📝 **Note:** Authors without books will appear with `null` for book fields.

**Example with WHERE:**
```sql
SELECT 
    Authors.name
FROM Authors
LEFT JOIN Books ON Authors.id = Books.author_id
WHERE Books.id IS NULL
```
*Returns authors who haven't written any books.*

### RIGHT JOIN

Returns all documents from the right bundle and matching documents from the left bundle:

```sql
SELECT 
    Books.title,
    Publishers.name AS publisher
FROM Books
RIGHT JOIN Publishers ON Books.publisher_id = Publishers.id
```

**JOIN with conditions:**

```sql
SELECT 
    a.name AS author,
    b.title AS book
FROM Authors AS a
INNER JOIN Books AS b ON a.id = b.author_id
WHERE b.published_year > 2020
ORDER BY a.name
```

---

## 🔀 WITH RELATIONSHIP

SyndrDB's native way to traverse predefined relationships between bundles.

**Syntax:**
```sql
SELECT <fields>
FROM <bundle>
WITH RELATIONSHIP <relationship_name>
```

**Single Relationship:**

```sql
SELECT 
    title,
    books.title AS related_books
FROM Authors
WITH RELATIONSHIP books
```

**Multiple Relationships:**

```sql
SELECT 
    name,
    books.title,
    awards.name AS award_name
FROM Authors
WITH RELATIONSHIP books, awards
```

**Nested Relationships:**

```sql
SELECT 
    name,
    books.title,
    books.reviews.rating
FROM Authors
WITH RELATIONSHIP books.reviews
```

**Filtering with Relationships:**

```sql
SELECT 
    name,
    books.title
FROM Authors
WITH RELATIONSHIP books
WHERE books.published_year > 2020
```

> 💡 **Tip:** `WITH RELATIONSHIP` uses pre-defined relationships in your schema and automatically handles the joins behind the scenes.

---

## 📊 ORDER BY Clause

Sort query results by one or more fields.

**Syntax:**
```sql
ORDER BY <field> [ASC|DESC]
```

**Ascending Order (default):**

```sql
SELECT * FROM Books ORDER BY price
```

**Descending Order:**

```sql
SELECT * FROM Books ORDER BY price DESC
```

**Multiple Fields:**

```sql
SELECT * FROM Books 
ORDER BY genre ASC, price DESC
```

*Sorts by genre first (A-Z), then by price (high to low) within each genre.*

**Order by Computed Values:**

```sql
SELECT 
    name,
    (price * quantity) AS total_value
FROM Products
ORDER BY total_value DESC
```

**Order with LIMIT:**

```sql
-- Top 10 most expensive books
SELECT * FROM Books 
ORDER BY price DESC 
LIMIT 10
```

**NULL Handling:**

```sql
-- NULLs last
SELECT * FROM Books 
ORDER BY published_date DESC NULLS LAST

-- NULLs first
SELECT * FROM Books 
ORDER BY published_date ASC NULLS FIRST
```

---

## 📈 GROUP BY Clause

Group documents by one or more fields for aggregation.

**Syntax:**
```sql
GROUP BY <field1>, <field2>, ...
```

**Basic Grouping:**

```sql
SELECT 
    genre,
    COUNT(*) AS book_count
FROM Books
GROUP BY genre
```

**Output:**
```json
[
  {"genre": "Fiction", "book_count": 45},
  {"genre": "Mystery", "book_count": 23},
  {"genre": "Biography", "book_count": 12}
]
```

**Multiple Fields:**

```sql
SELECT 
    author_id,
    genre,
    COUNT(*) AS count,
    AVG(price) AS avg_price
FROM Books
GROUP BY author_id, genre
```

**GROUP BY with JOIN:**

```sql
SELECT 
    Authors.name,
    COUNT(Books.id) AS book_count,
    AVG(Books.price) AS avg_book_price
FROM Authors
INNER JOIN Books ON Authors.id = Books.author_id
GROUP BY Authors.name
```

**GROUP BY with ORDER BY:**

```sql
SELECT 
    category,
    COUNT(*) AS product_count
FROM Products
GROUP BY category
ORDER BY product_count DESC
```

---

## 🎯 HAVING Clause

Filter grouped results (use after `GROUP BY`).

**Syntax:**
```sql
HAVING <condition>
```

> ⚠️ **Important:** `WHERE` filters individual documents before grouping; `HAVING` filters groups after aggregation.

**Examples:**

```sql
-- Authors with more than 5 books
SELECT 
    author_id,
    COUNT(*) AS book_count
FROM Books
GROUP BY author_id
HAVING COUNT(*) > 5
```

```sql
-- Genres with average price over $25
SELECT 
    genre,
    AVG(price) AS avg_price
FROM Books
GROUP BY genre
HAVING AVG(price) > 25
```

**HAVING with Multiple Conditions:**

```sql
SELECT 
    category,
    COUNT(*) AS count,
    AVG(price) AS avg_price
FROM Products
GROUP BY category
HAVING COUNT(*) >= 10 AND AVG(price) < 100
```

**WHERE + GROUP BY + HAVING:**

```sql
SELECT 
    genre,
    COUNT(*) AS count
FROM Books
WHERE published_year >= 2020
GROUP BY genre
HAVING COUNT(*) > 3
ORDER BY count DESC
```

---

## 🔢 LIMIT & TOP

Restrict the number of documents returned.

### LIMIT

**Syntax:**
```sql
LIMIT <number>
```

**Examples:**

```sql
-- First 10 books
SELECT * FROM Books LIMIT 10

-- Top 5 most expensive products
SELECT * FROM Products 
ORDER BY price DESC 
LIMIT 5
```

**LIMIT with OFFSET:**

```sql
-- Skip first 10, return next 10 (pagination)
SELECT * FROM Books 
LIMIT 10 OFFSET 10

-- Page 3 (20 per page)
SELECT * FROM Books 
LIMIT 20 OFFSET 40
```

### TOP

**Syntax:**
```sql
SELECT TOP <number> <fields> FROM <bundle>
```

**Examples:**

```sql
-- Top 10 records
SELECT TOP 10 * FROM Books

-- Top 5 by price
SELECT TOP 5 title, price 
FROM Books 
ORDER BY price DESC
```

**TOP with PERCENT:**

```sql
-- Top 10% of records
SELECT TOP 10 PERCENT * FROM Books
```

> 💡 **Tip:** `LIMIT` is SQL standard; `TOP` is included for compatibility with other databases.

---

## 🧮 Aggregate Functions

Perform calculations on groups of documents.

### COUNT()

Count the number of documents or non-null values.

**Count all documents:**
```sql
SELECT COUNT(*) AS total_books FROM Books
```

**Count specific field:**
```sql
SELECT COUNT(isbn) AS books_with_isbn FROM Books
```

**Count distinct values:**
```sql
SELECT COUNT(DISTINCT author_id) AS unique_authors FROM Books
```

**Count with GROUP BY:**
```sql
SELECT 
    genre,
    COUNT(*) AS count
FROM Books
GROUP BY genre
```

**Output:**
```json
[
  {"genre": "Fiction", "count": 45},
  {"genre": "Mystery", "count": 23}
]
```

### SUM()

Calculate the total of numeric values.

**Total sales:**
```sql
SELECT SUM(amount) AS total_revenue FROM Orders
```

**Sum by group:**
```sql
SELECT 
    product_id,
    SUM(quantity) AS total_sold
FROM OrderItems
GROUP BY product_id
```

**Sum with WHERE:**
```sql
SELECT SUM(price) AS total_value
FROM Books
WHERE genre = 'Fiction'
```

### AVG()

Calculate the arithmetic mean of numeric values.

**Average price:**
```sql
SELECT AVG(price) AS average_price FROM Books
```

**Average by group:**
```sql
SELECT 
    genre,
    AVG(price) AS avg_price
FROM Books
GROUP BY genre
```

**Average with rounding:**
```sql
SELECT 
    ROUND(AVG(rating), 2) AS avg_rating
FROM Books
```

### MIN()

Find the minimum value.

**Lowest price:**
```sql
SELECT MIN(price) AS lowest_price FROM Books
```

**Earliest date:**
```sql
SELECT MIN(published_date) AS first_publication FROM Books
```

**Min by group:**
```sql
SELECT 
    author_id,
    MIN(price) AS cheapest_book
FROM Books
GROUP BY author_id
```

### MAX()

Find the maximum value.

**Highest price:**
```sql
SELECT MAX(price) AS highest_price FROM Books
```

**Latest date:**
```sql
SELECT MAX(order_date) AS most_recent_order FROM Orders
```

**Max by group:**
```sql
SELECT 
    category,
    MAX(price) AS most_expensive
FROM Products
GROUP BY category
```

**Combined Aggregates:**

```sql
SELECT 
    COUNT(*) AS total_books,
    SUM(price) AS total_value,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM Books
WHERE published_year = 2024
```

**Output:**
```json
{
  "total_books": 127,
  "total_value": 3421.50,
  "avg_price": 26.94,
  "min_price": 9.99,
  "max_price": 89.99
}
```

---

## ⚡ Scalar Functions (Coming soon!)

Functions that operate on individual values.

### String Functions

**UPPER():**
```sql
SELECT UPPER(name) AS uppercase_name FROM Authors
```

**LOWER():**
```sql
SELECT LOWER(email) AS lowercase_email FROM Users
```

**LENGTH():**
```sql
SELECT title, LENGTH(title) AS title_length FROM Books
```

**SUBSTRING():**
```sql
SELECT SUBSTRING(isbn, 1, 3) AS isbn_prefix FROM Books
```

**CONCAT():**
```sql
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM Authors
```

**TRIM():**
```sql
SELECT TRIM(name) AS trimmed_name FROM Products
```

### Numeric Functions

**ROUND():**
```sql
SELECT ROUND(price, 2) AS rounded_price FROM Books
```

**CEILING():**
```sql
SELECT CEILING(rating) AS rounded_up FROM Books
```

**FLOOR():**
```sql
SELECT FLOOR(price) AS rounded_down FROM Products
```

**ABS():**
```sql
SELECT ABS(balance) AS absolute_balance FROM Accounts
```

### Date Functions

**NOW():**
```sql
SELECT NOW() AS current_timestamp
```

**DATE():**
```sql
SELECT DATE(created_at) AS creation_date FROM Orders
```

**YEAR(), MONTH(), DAY():**
```sql
SELECT 
    YEAR(published_date) AS year,
    MONTH(published_date) AS month,
    DAY(published_date) AS day
FROM Books
```

**DATEDIFF():**
```sql
SELECT 
    order_date,
    DATEDIFF(NOW(), order_date) AS days_ago
FROM Orders
```

### Conditional Functions

**CASE:**
```sql
SELECT 
    title,
    price,
    CASE 
        WHEN price < 10 THEN 'Budget'
        WHEN price < 30 THEN 'Standard'
        ELSE 'Premium'
    END AS price_category
FROM Books
```

**COALESCE():**
```sql
SELECT COALESCE(phone, email, 'No contact') AS contact_info FROM Users
```

**NULLIF():**
```sql
SELECT NULLIF(discount, 0) AS active_discount FROM Products
```

---

## 🔍 Subqueries (Coming Soon!)

Use query results within another query.

### Subquery in WHERE

**Find books more expensive than average:**
```sql
SELECT title, price
FROM Books
WHERE price > (SELECT AVG(price) FROM Books)
```

**Authors who have written books:**
```sql
SELECT name
FROM Authors
WHERE id IN (SELECT DISTINCT author_id FROM Books)
```

### Subquery in FROM

```sql
SELECT 
    genre,
    avg_price
FROM (
    SELECT 
        genre,
        AVG(price) AS avg_price
    FROM Books
    GROUP BY genre
) AS genre_stats
WHERE avg_price > 20
```

### Subquery in SELECT

```sql
SELECT 
    name,
    (SELECT COUNT(*) FROM Books WHERE Books.author_id = Authors.id) AS book_count
FROM Authors
```

### Correlated Subquery

```sql
SELECT 
    b1.title,
    b1.price
FROM Books b1
WHERE b1.price > (
    SELECT AVG(b2.price)
    FROM Books b2
    WHERE b2.genre = b1.genre
)
```

---

## 🌟 Complex Query Examples

### Example 1: E-commerce Analytics

```sql
SELECT 
    p.category,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    COUNT(o.id) AS total_orders,
    SUM(o.quantity) AS items_sold,
    SUM(o.quantity * p.price) AS revenue,
    AVG(o.quantity * p.price) AS avg_order_value
FROM Orders o
INNER JOIN Products p ON o.product_id = p.id
WHERE o.order_date >= '2024-01-01'
GROUP BY p.category
HAVING SUM(o.quantity * p.price) > 10000
ORDER BY revenue DESC
LIMIT 10
```

### Example 2: Author Rankings

```sql
SELECT 
    a.name AS author,
    COUNT(b.id) AS book_count,
    AVG(b.rating) AS avg_rating,
    SUM(b.sales) AS total_sales,
    CASE 
        WHEN AVG(b.rating) >= 4.5 THEN 'Bestseller'
        WHEN AVG(b.rating) >= 4.0 THEN 'Popular'
        WHEN AVG(b.rating) >= 3.5 THEN 'Good'
        ELSE 'Average'
    END AS tier
FROM Authors a
LEFT JOIN Books b ON a.id = b.author_id
GROUP BY a.name
HAVING COUNT(b.id) >= 3
ORDER BY total_sales DESC, avg_rating DESC
```

### Example 3: Customer Segmentation

```sql
SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(total_spent) AS avg_lifetime_value
FROM (
    SELECT 
        c.id,
        c.name,
        SUM(o.amount) AS total_spent,
        CASE 
            WHEN SUM(o.amount) > 1000 THEN 'VIP'
            WHEN SUM(o.amount) > 500 THEN 'Premium'
            WHEN SUM(o.amount) > 100 THEN 'Regular'
            ELSE 'Occasional'
        END AS customer_segment
    FROM Customers c
    LEFT JOIN Orders o ON c.id = o.customer_id
    GROUP BY c.id, c.name
) AS customer_stats
GROUP BY customer_segment
ORDER BY avg_lifetime_value DESC
```

### Example 4: Inventory Management

```sql
SELECT 
    p.id,
    p.name,
    p.category,
    p.stock_quantity,
    COALESCE(recent_sales.sold_last_30_days, 0) AS sold_last_30_days,
    CASE 
        WHEN p.stock_quantity = 0 THEN 'Out of Stock'
        WHEN p.stock_quantity < 10 THEN 'Low Stock'
        WHEN p.stock_quantity < 50 THEN 'Adequate'
        ELSE 'Well Stocked'
    END AS stock_status
FROM Products p
LEFT JOIN (
    SELECT 
        product_id,
        SUM(quantity) AS sold_last_30_days
    FROM OrderItems
    WHERE order_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
    GROUP BY product_id
) AS recent_sales ON p.id = recent_sales.product_id
WHERE p.active = true
ORDER BY sold_last_30_days DESC
```

### Example 5: Monthly Sales Trend

```sql
SELECT 
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) AS order_count,
    SUM(total_amount) AS revenue,
    AVG(total_amount) AS avg_order_value,
    MIN(total_amount) AS min_order,
    MAX(total_amount) AS max_order
FROM Orders
WHERE order_date >= '2023-01-01'
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year DESC, month DESC
```

---

## 📚 Best Practices

### ✅ DO:
- Use specific field names instead of `SELECT *` for better performance
- Create indexes on frequently filtered/joined fields
- Use `LIMIT` to restrict large result sets during development
- Use table aliases for readability in complex queries
- Test queries on small datasets first

### ❌ DON'T:
- Overuse `SELECT *` in production queries
- Create unnecessary subqueries when JOIN would work
- Forget to add `WHERE` clauses on large tables
- Use `HAVING` when `WHERE` would suffice
- Chain too many joins without proper indexes

---

## 🎓 Quick Reference

| Operation | Syntax | Example |
|-----------|--------|---------|
| **Basic SELECT** | `SELECT fields FROM bundle` | `SELECT * FROM Books` |
| **Filter** | `WHERE condition` | `WHERE price > 20` |
| **Join** | `JOIN bundle ON condition` | `JOIN Authors ON Books.author_id = Authors.id` |
| **Relationship** | `WITH RELATIONSHIP name` | `WITH RELATIONSHIP books` |
| **Sort** | `ORDER BY field [ASC\|DESC]` | `ORDER BY price DESC` |
| **Group** | `GROUP BY field` | `GROUP BY genre` |
| **Filter Groups** | `HAVING condition` | `HAVING COUNT(*) > 5` |
| **Limit Results** | `LIMIT n` or `TOP n` | `LIMIT 10` |
| **Count** | `COUNT(*)` or `COUNT(field)` | `COUNT(DISTINCT author_id)` |
| **Sum** | `SUM(field)` | `SUM(price)` |
| **Average** | `AVG(field)` | `AVG(rating)` |
| **Min/Max** | `MIN(field)` / `MAX(field)` | `MAX(published_year)` |

---

## 🆘 Getting Help

For more information:
- 📖 [SyndrDB Documentation](../README.md)
- 🔗 [GraphQL Queries](./graphql_queries.md)
- 💾 [Data Manipulation (INSERT/UPDATE/DELETE)](./data_manipulation.md)
- 🏗️ [Schema Management](./schema_management.md)

---

*Last updated: November 20, 2025* 🚀
