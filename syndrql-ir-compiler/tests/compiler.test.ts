import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { compile } from '../src/compiler';
import { Statement, Expr, Value, SelectStmt } from '../src/generated/types';

// Helper constructors matching Go test helpers
function ident(name: string): Expr { return { type: 'identifier', name }; }
function literal(v: Value): Expr { return { type: 'literal', value: v }; }
function strVal(s: string): Value { return { type: 'string', raw: s }; }
function intVal(n: number): Value { return { type: 'int', raw: String(n) }; }
function floatVal(f: number): Value { return { type: 'float', raw: String(f) }; }
function boolVal(b: boolean): Value { return { type: 'bool', raw: b ? 'true' : 'false' }; }
function binary(left: Expr, operator: string, right: Expr): Expr { return { type: 'binary', left, operator, right }; }
function isNull(field: Expr): Expr { return { type: 'isNull', left: field }; }
function inExpr(field: Expr, args: Expr[]): Expr { return { type: 'in', left: field, args }; }
function funcExpr(name: string, isBuiltIn: boolean, ...args: Expr[]): Expr { return { type: 'function', name, isBuiltIn, args }; }
function paramExpr(index: number): Expr { return { type: 'parameter', index }; }

describe('IR Compiler', () => {

  it('SELECT *', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Customers',
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Customers";');
  });

  it('SELECT fields', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('name') }, { expression: ident('email') }],
        bundle: 'Customers',
      },
    };
    assert.equal(compile(stmt), 'SELECT name, email FROM "Customers";');
  });

  it('SELECT DISTINCT', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('status') }],
        bundle: 'Orders',
        distinct: true,
      },
    };
    assert.equal(compile(stmt), 'SELECT DISTINCT status FROM "Orders";');
  });

  it('SELECT WHERE', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Customers',
        where: binary(ident('status'), '==', literal(strVal('active'))),
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Customers" WHERE status == "active";');
  });

  it('SELECT compound WHERE', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Products',
        where: binary(
          binary(ident('price'), '>', literal(intVal(100))),
          'AND',
          binary(ident('category'), '==', literal(strVal('Electronics'))),
        ),
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Products" WHERE price > 100 AND category == "Electronics";');
  });

  it('SELECT ORDER BY', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Customers',
        orderBy: [{ field: 'name', direction: 'ASC' }],
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Customers" ORDER BY name ASC;');
  });

  it('SELECT LIMIT OFFSET', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Products',
        limit: 10,
        offset: 20,
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Products" LIMIT 10 OFFSET 20;');
  });

  it('SELECT GROUP BY', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [
          { expression: ident('category') },
          { expression: funcExpr('COUNT', false, ident('*')), alias: 'total' },
        ],
        bundle: 'Products',
        groupBy: ['category'],
      },
    };
    assert.equal(compile(stmt), 'SELECT category, COUNT(*) AS total FROM "Products" GROUP BY category;');
  });

  it('SELECT HAVING', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [
          { expression: ident('status') },
          { expression: funcExpr('COUNT', false, ident('*')), alias: 'cnt' },
        ],
        bundle: 'Orders',
        groupBy: ['status'],
        having: binary(funcExpr('COUNT', false, ident('*')), '>', literal(intVal(5))),
      },
    };
    assert.equal(compile(stmt), 'SELECT status, COUNT(*) AS cnt FROM "Orders" GROUP BY status HAVING COUNT(*) > 5;');
  });

  it('SELECT JOIN', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Orders',
        joins: [{
          joinType: 'INNER',
          bundle: 'Customers',
          conditions: [{
            leftBundle: 'Orders', leftField: 'customerID',
            operator: '==',
            rightBundle: 'Customers', rightField: 'id',
          }],
        }],
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Orders" INNER JOIN "Customers" ON "Orders"."customerID" == "Customers"."id";');
  });

  it('SELECT FOR UPDATE', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Inventory',
        where: binary(ident('productID'), '==', literal(strVal('SKU-001'))),
        forUpdate: true,
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Inventory" WHERE productID == "SKU-001" FOR UPDATE;');
  });

  it('SELECT LIKE', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Customers',
        where: binary(ident('name'), 'LIKE', literal(strVal('John%'))),
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Customers" WHERE name LIKE "John%";');
  });

  it('SELECT IS NULL', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Customers',
        where: isNull(ident('email')),
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Customers" WHERE email IS NULL;');
  });

  it('SELECT IN', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Orders',
        where: inExpr(ident('status'), [literal(strVal('pending')), literal(strVal('shipped'))]),
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Orders" WHERE status IN ("pending", "shipped");');
  });

  it('SELECT F:UPPER', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: funcExpr('UPPER', true, ident('name')), alias: 'upper_name' }],
        bundle: 'Customers',
      },
    };
    assert.equal(compile(stmt), 'SELECT F:UPPER(name) AS upper_name FROM "Customers";');
  });

  it('SELECT parameter', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Users',
        where: binary(ident('age'), '>', paramExpr(1)),
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Users" WHERE age > $1;');
  });

  it('INSERT', () => {
    const stmt: Statement = {
      statementType: 'insert',
      insert: {
        bundle: 'Customers',
        fields: [
          { name: 'name', value: strVal('Alice') },
          { name: 'email', value: strVal('alice@example.com') },
          { name: 'age', value: intVal(30) },
        ],
      },
    };
    assert.equal(compile(stmt), 'ADD DOCUMENT TO BUNDLE "Customers" WITH ({name = "Alice"}, {email = "alice@example.com"}, {age = 30});');
  });

  it('UPDATE', () => {
    const stmt: Statement = {
      statementType: 'update',
      update: {
        bundle: 'Orders',
        fields: [{ name: 'status', value: strVal('shipped') }],
        where: binary(ident('orderID'), '==', literal(strVal('ORD-123'))),
      },
    };
    assert.equal(compile(stmt), 'UPDATE DOCUMENTS IN BUNDLE "Orders" (status = "shipped") WHERE orderID == "ORD-123";');
  });

  it('UPDATE CONFIRMED', () => {
    const stmt: Statement = {
      statementType: 'update',
      update: {
        bundle: 'Logs',
        fields: [{ name: 'archived', value: boolVal(true) }],
        confirmed: true,
      },
    };
    assert.equal(compile(stmt), 'UPDATE DOCUMENTS IN BUNDLE "Logs" (archived = true) CONFIRMED;');
  });

  it('DELETE', () => {
    const stmt: Statement = {
      statementType: 'delete',
      delete: {
        bundle: 'Sessions',
        where: binary(ident('expired'), '==', literal(boolVal(true))),
      },
    };
    assert.equal(compile(stmt), 'DELETE DOCUMENTS FROM "Sessions" WHERE expired == true;');
  });

  it('DELETE CONFIRMED', () => {
    const stmt: Statement = {
      statementType: 'delete',
      delete: {
        bundle: 'Logs',
        confirmed: true,
      },
    };
    assert.equal(compile(stmt), 'DELETE DOCUMENTS FROM "Logs" CONFIRMED;');
  });

  it('CREATE BUNDLE', () => {
    const stmt: Statement = {
      statementType: 'createBundle',
      createBundle: {
        bundle: 'Products',
        fields: [
          { name: 'name', type: 'STRING', required: true, unique: false },
          { name: 'price', type: 'FLOAT', required: true, unique: false },
          { name: 'inStock', type: 'BOOL', required: false, unique: false, defaultValue: boolVal(true) },
        ],
      },
    };
    assert.equal(compile(stmt), 'CREATE BUNDLE "Products" WITH FIELDS ({"name", "STRING", true, false}, {"price", "FLOAT", true, false}, {"inStock", "BOOL", false, false, true});');
  });

  it('DROP BUNDLE', () => {
    const stmt: Statement = {
      statementType: 'dropBundle',
      dropBundle: { bundle: 'TempData', force: true },
    };
    assert.equal(compile(stmt), 'DROP BUNDLE "TempData" FORCE;');
  });

  it('CREATE B-INDEX', () => {
    const stmt: Statement = {
      statementType: 'createIndex',
      createIndex: {
        indexType: 'B-INDEX',
        indexName: 'idx_products_name',
        bundle: 'Products',
        fields: [{ name: 'name', required: true, unique: false }],
      },
    };
    assert.equal(compile(stmt), 'CREATE B-INDEX "idx_products_name" ON BUNDLE "Products" WITH FIELDS ({"name", true, false});');
  });

  it('CREATE HASH INDEX', () => {
    const stmt: Statement = {
      statementType: 'createIndex',
      createIndex: {
        indexType: 'HASH INDEX',
        indexName: 'idx_customers_email',
        bundle: 'Customers',
        fields: [{ name: 'email', required: true, unique: true }],
      },
    };
    assert.equal(compile(stmt), 'CREATE HASH INDEX "idx_customers_email" ON BUNDLE "Customers" WITH FIELDS ({"email", true, true});');
  });

  it('CREATE VIEW', () => {
    const stmt: Statement = {
      statementType: 'createView',
      createView: {
        viewName: 'ActiveCustomers',
        query: {
          fields: [{ expression: ident('*') }],
          bundle: 'Customers',
          where: binary(ident('active'), '==', literal(boolVal(true))),
        },
      },
    };
    assert.equal(compile(stmt), 'CREATE VIEW "ActiveCustomers" AS SELECT * FROM "Customers" WHERE active == true;');
  });

  it('CREATE MATERIALIZED VIEW', () => {
    const stmt: Statement = {
      statementType: 'createView',
      createView: {
        viewName: 'OrderSummary',
        isMaterialized: true,
        query: {
          fields: [
            { expression: ident('status') },
            { expression: funcExpr('COUNT', false, ident('*')), alias: 'total' },
          ],
          bundle: 'Orders',
          groupBy: ['status'],
        },
      },
    };
    assert.equal(compile(stmt), 'CREATE MATERIALIZED VIEW "OrderSummary" AS SELECT status, COUNT(*) AS total FROM "Orders" GROUP BY status;');
  });

  it('DROP VIEW', () => {
    const stmt: Statement = { statementType: 'dropView', dropView: { viewName: 'OldView' } };
    assert.equal(compile(stmt), 'DROP VIEW "OldView";');
  });

  it('REFRESH VIEW', () => {
    const stmt: Statement = { statementType: 'refreshView', refreshView: { viewName: 'OrderSummary' } };
    assert.equal(compile(stmt), 'REFRESH MATERIALIZED VIEW "OrderSummary";');
  });

  it('BEGIN TRANSACTION', () => {
    assert.equal(compile({ statementType: 'beginTransaction' }), 'BEGIN TRANSACTION;');
  });

  it('COMMIT', () => {
    assert.equal(compile({ statementType: 'commit' }), 'COMMIT;');
  });

  it('ROLLBACK', () => {
    assert.equal(compile({ statementType: 'rollback' }), 'ROLLBACK;');
  });

  it('SAVEPOINT', () => {
    assert.equal(compile({ statementType: 'savepoint', savepoint: { name: 'sp1' } }), 'SAVEPOINT "sp1";');
  });

  it('ROLLBACK TO SAVEPOINT', () => {
    assert.equal(compile({ statementType: 'rollbackToSavepoint', rollbackToSavepoint: { name: 'sp1' } }), 'ROLLBACK TO SAVEPOINT "sp1";');
  });

  it('DECLARE CURSOR', () => {
    const stmt: Statement = {
      statementType: 'declareCursor',
      declareCursor: {
        cursorName: 'my_cursor',
        query: {
          fields: [{ expression: ident('*') }],
          bundle: 'Orders',
          orderBy: [{ field: 'createdAt', direction: 'DESC' }],
        },
      },
    };
    assert.equal(compile(stmt), 'DECLARE my_cursor CURSOR FOR SELECT * FROM "Orders" ORDER BY createdAt DESC;');
  });

  it('FETCH N', () => {
    assert.equal(compile({ statementType: 'fetchCursor', fetchCursor: { cursorName: 'c1', count: 10 } }), 'FETCH 10 FROM c1;');
  });

  it('FETCH NEXT', () => {
    assert.equal(compile({ statementType: 'fetchCursor', fetchCursor: { cursorName: 'c1', count: 1 } }), 'FETCH NEXT FROM c1;');
  });

  it('FETCH ALL', () => {
    assert.equal(compile({ statementType: 'fetchCursor', fetchCursor: { cursorName: 'c1', count: 0, fetchAll: true } }), 'FETCH ALL FROM c1;');
  });

  it('CLOSE CURSOR', () => {
    assert.equal(compile({ statementType: 'closeCursor', closeCursor: { cursorName: 'my_cursor' } }), 'CLOSE my_cursor;');
  });

  it('PREPARE', () => {
    const stmt: Statement = {
      statementType: 'prepare',
      prepare: {
        statementName: 'find_user',
        query: {
          fields: [{ expression: ident('*') }],
          bundle: 'Users',
          where: binary(ident('age'), '>', paramExpr(1)),
        },
      },
    };
    assert.equal(compile(stmt), 'PREPARE find_user AS SELECT * FROM "Users" WHERE age > $1;');
  });

  it('EXECUTE', () => {
    assert.equal(compile({ statementType: 'execute', execute: { statementName: 'find_user' } }), 'EXECUTE find_user;');
  });

  it('DEALLOCATE', () => {
    assert.equal(compile({ statementType: 'deallocate', deallocate: { statementName: 'find_user' } }), 'DEALLOCATE find_user;');
  });

  it('CREATE USER', () => {
    const stmt: Statement = { statementType: 'createUser', createUser: { username: 'admin', password: 'secret123' } };
    assert.equal(compile(stmt), 'CREATE USER "admin" WITH PASSWORD "secret123";');
  });

  it('GRANT PERMISSION', () => {
    const stmt: Statement = { statementType: 'grantPermission', grantPermission: { permission: 'SELECT', username: 'analyst' } };
    assert.equal(compile(stmt), 'GRANT "SELECT" TO USER "analyst";');
  });

  it('REVOKE PERMISSION', () => {
    const stmt: Statement = { statementType: 'revokePermission', revokePermission: { permission: 'DELETE', username: 'intern' } };
    assert.equal(compile(stmt), 'REVOKE "DELETE" FROM USER "intern";');
  });

  it('USE DATABASE', () => {
    const stmt: Statement = { statementType: 'useDatabase', useDatabase: { database: 'production' } };
    assert.equal(compile(stmt), 'USE DATABASE "production";');
  });

  it('CREATE DATABASE', () => {
    const stmt: Statement = { statementType: 'createDatabase', createDatabase: { database: 'analytics' } };
    assert.equal(compile(stmt), 'CREATE DATABASE "analytics";');
  });

  it('SHOW VERSIONS', () => {
    const stmt: Statement = { statementType: 'showVersions', showVersions: { documentId: 'doc-123', bundle: 'Orders' } };
    assert.equal(compile(stmt), 'SHOW VERSIONS FOR "doc-123" IN BUNDLE "Orders";');
  });

  it('EXPLAIN SELECT', () => {
    const stmt: Statement = {
      statementType: 'explainSelect',
      explainSelect: {
        query: {
          fields: [{ expression: ident('*') }],
          bundle: 'Products',
          where: binary(ident('price'), '>', literal(intVal(50))),
        },
      },
    };
    assert.equal(compile(stmt), 'EXPLAIN SELECT * FROM "Products" WHERE price > 50;');
  });

  it('CHECKPOINT', () => {
    assert.equal(compile({ statementType: 'checkpoint' }), 'CHECKPOINT;');
  });

  it('SHOW DATABASES', () => {
    assert.equal(compile({ statementType: 'showDatabases' }), 'SHOW DATABASES;');
  });

  it('CREATE ROLE', () => {
    const stmt: Statement = { statementType: 'createRole', createRole: { roleName: 'analyst', description: 'Data analysis role' } };
    assert.equal(compile(stmt), 'CREATE ROLE "analyst" WITH DESCRIPTION "Data analysis role";');
  });

  it('START MIGRATION', () => {
    const stmt: Statement = { statementType: 'startMigration', startMigration: { description: 'Add user preferences' } };
    assert.equal(compile(stmt), 'START MIGRATION WITH DESCRIPTION "Add user preferences";');
  });

  it('OR precedence with AND', () => {
    const stmt: Statement = {
      statementType: 'select',
      select: {
        fields: [{ expression: ident('*') }],
        bundle: 'Customers',
        where: binary(
          binary(
            binary(ident('status'), '==', literal(strVal('active'))),
            'OR',
            binary(ident('status'), '==', literal(strVal('pending'))),
          ),
          'AND',
          binary(ident('category'), '==', literal(strVal('VIP'))),
        ),
      },
    };
    assert.equal(compile(stmt), 'SELECT * FROM "Customers" WHERE (status == "active" OR status == "pending") AND category == "VIP";');
  });

  it('BACKUP DATABASE', () => {
    const stmt: Statement = { statementType: 'backupDatabase', backupDatabase: { database: 'production', path: '/backups/prod' } };
    assert.equal(compile(stmt), 'BACKUP DATABASE "production" TO "/backups/prod";');
  });

  it('INVALIDATE SESSION', () => {
    const stmt: Statement = { statementType: 'invalidateSession', invalidateSession: { sessionId: 'sess-abc-123' } };
    assert.equal(compile(stmt), 'INVALIDATE SESSION "sess-abc-123";');
  });

});
