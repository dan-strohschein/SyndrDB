"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const compiler_1 = require("../src/compiler");
// Helper constructors matching Go test helpers
function ident(name) { return { type: 'identifier', name }; }
function literal(v) { return { type: 'literal', value: v }; }
function strVal(s) { return { type: 'string', raw: s }; }
function intVal(n) { return { type: 'int', raw: String(n) }; }
function floatVal(f) { return { type: 'float', raw: String(f) }; }
function boolVal(b) { return { type: 'bool', raw: b ? 'true' : 'false' }; }
function binary(left, operator, right) { return { type: 'binary', left, operator, right }; }
function isNull(field) { return { type: 'isNull', left: field }; }
function inExpr(field, args) { return { type: 'in', left: field, args }; }
function funcExpr(name, isBuiltIn, ...args) { return { type: 'function', name, isBuiltIn, args }; }
function paramExpr(index) { return { type: 'parameter', index }; }
(0, node_test_1.describe)('IR Compiler', () => {
    (0, node_test_1.it)('SELECT *', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Customers',
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Customers";');
    });
    (0, node_test_1.it)('SELECT fields', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('name') }, { expression: ident('email') }],
                bundle: 'Customers',
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT name, email FROM "Customers";');
    });
    (0, node_test_1.it)('SELECT DISTINCT', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('status') }],
                bundle: 'Orders',
                distinct: true,
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT DISTINCT status FROM "Orders";');
    });
    (0, node_test_1.it)('SELECT WHERE', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Customers',
                where: binary(ident('status'), '==', literal(strVal('active'))),
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Customers" WHERE status == "active";');
    });
    (0, node_test_1.it)('SELECT compound WHERE', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Products',
                where: binary(binary(ident('price'), '>', literal(intVal(100))), 'AND', binary(ident('category'), '==', literal(strVal('Electronics')))),
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Products" WHERE price > 100 AND category == "Electronics";');
    });
    (0, node_test_1.it)('SELECT ORDER BY', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Customers',
                orderBy: [{ field: 'name', direction: 'ASC' }],
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Customers" ORDER BY name ASC;');
    });
    (0, node_test_1.it)('SELECT LIMIT OFFSET', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Products',
                limit: 10,
                offset: 20,
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Products" LIMIT 10 OFFSET 20;');
    });
    (0, node_test_1.it)('SELECT GROUP BY', () => {
        const stmt = {
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
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT category, COUNT(*) AS total FROM "Products" GROUP BY category;');
    });
    (0, node_test_1.it)('SELECT HAVING', () => {
        const stmt = {
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
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT status, COUNT(*) AS cnt FROM "Orders" GROUP BY status HAVING COUNT(*) > 5;');
    });
    (0, node_test_1.it)('SELECT JOIN', () => {
        const stmt = {
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
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Orders" INNER JOIN "Customers" ON "Orders"."customerID" == "Customers"."id";');
    });
    (0, node_test_1.it)('SELECT FOR UPDATE', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Inventory',
                where: binary(ident('productID'), '==', literal(strVal('SKU-001'))),
                forUpdate: true,
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Inventory" WHERE productID == "SKU-001" FOR UPDATE;');
    });
    (0, node_test_1.it)('SELECT LIKE', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Customers',
                where: binary(ident('name'), 'LIKE', literal(strVal('John%'))),
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Customers" WHERE name LIKE "John%";');
    });
    (0, node_test_1.it)('SELECT IS NULL', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Customers',
                where: isNull(ident('email')),
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Customers" WHERE email IS NULL;');
    });
    (0, node_test_1.it)('SELECT IN', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Orders',
                where: inExpr(ident('status'), [literal(strVal('pending')), literal(strVal('shipped'))]),
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Orders" WHERE status IN ("pending", "shipped");');
    });
    (0, node_test_1.it)('SELECT F:UPPER', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: funcExpr('UPPER', true, ident('name')), alias: 'upper_name' }],
                bundle: 'Customers',
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT F:UPPER(name) AS upper_name FROM "Customers";');
    });
    (0, node_test_1.it)('SELECT parameter', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Users',
                where: binary(ident('age'), '>', paramExpr(1)),
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Users" WHERE age > $1;');
    });
    (0, node_test_1.it)('INSERT', () => {
        const stmt = {
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
        assert.equal((0, compiler_1.compile)(stmt), 'ADD DOCUMENT TO BUNDLE "Customers" WITH ({name = "Alice"}, {email = "alice@example.com"}, {age = 30});');
    });
    (0, node_test_1.it)('UPDATE', () => {
        const stmt = {
            statementType: 'update',
            update: {
                bundle: 'Orders',
                fields: [{ name: 'status', value: strVal('shipped') }],
                where: binary(ident('orderID'), '==', literal(strVal('ORD-123'))),
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'UPDATE DOCUMENTS IN BUNDLE "Orders" (status = "shipped") WHERE orderID == "ORD-123";');
    });
    (0, node_test_1.it)('UPDATE CONFIRMED', () => {
        const stmt = {
            statementType: 'update',
            update: {
                bundle: 'Logs',
                fields: [{ name: 'archived', value: boolVal(true) }],
                confirmed: true,
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'UPDATE DOCUMENTS IN BUNDLE "Logs" (archived = true) CONFIRMED;');
    });
    (0, node_test_1.it)('DELETE', () => {
        const stmt = {
            statementType: 'delete',
            delete: {
                bundle: 'Sessions',
                where: binary(ident('expired'), '==', literal(boolVal(true))),
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'DELETE DOCUMENTS FROM "Sessions" WHERE expired == true;');
    });
    (0, node_test_1.it)('DELETE CONFIRMED', () => {
        const stmt = {
            statementType: 'delete',
            delete: {
                bundle: 'Logs',
                confirmed: true,
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'DELETE DOCUMENTS FROM "Logs" CONFIRMED;');
    });
    (0, node_test_1.it)('CREATE BUNDLE', () => {
        const stmt = {
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
        assert.equal((0, compiler_1.compile)(stmt), 'CREATE BUNDLE "Products" WITH FIELDS ({"name", "STRING", true, false}, {"price", "FLOAT", true, false}, {"inStock", "BOOL", false, false, true});');
    });
    (0, node_test_1.it)('DROP BUNDLE', () => {
        const stmt = {
            statementType: 'dropBundle',
            dropBundle: { bundle: 'TempData', force: true },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'DROP BUNDLE "TempData" FORCE;');
    });
    (0, node_test_1.it)('CREATE B-INDEX', () => {
        const stmt = {
            statementType: 'createIndex',
            createIndex: {
                indexType: 'B-INDEX',
                indexName: 'idx_products_name',
                bundle: 'Products',
                fields: [{ name: 'name', required: true, unique: false }],
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'CREATE B-INDEX "idx_products_name" ON BUNDLE "Products" WITH FIELDS ({"name", true, false});');
    });
    (0, node_test_1.it)('CREATE HASH INDEX', () => {
        const stmt = {
            statementType: 'createIndex',
            createIndex: {
                indexType: 'HASH INDEX',
                indexName: 'idx_customers_email',
                bundle: 'Customers',
                fields: [{ name: 'email', required: true, unique: true }],
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'CREATE HASH INDEX "idx_customers_email" ON BUNDLE "Customers" WITH FIELDS ({"email", true, true});');
    });
    (0, node_test_1.it)('CREATE VIEW', () => {
        const stmt = {
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
        assert.equal((0, compiler_1.compile)(stmt), 'CREATE VIEW "ActiveCustomers" AS SELECT * FROM "Customers" WHERE active == true;');
    });
    (0, node_test_1.it)('CREATE MATERIALIZED VIEW', () => {
        const stmt = {
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
        assert.equal((0, compiler_1.compile)(stmt), 'CREATE MATERIALIZED VIEW "OrderSummary" AS SELECT status, COUNT(*) AS total FROM "Orders" GROUP BY status;');
    });
    (0, node_test_1.it)('DROP VIEW', () => {
        const stmt = { statementType: 'dropView', dropView: { viewName: 'OldView' } };
        assert.equal((0, compiler_1.compile)(stmt), 'DROP VIEW "OldView";');
    });
    (0, node_test_1.it)('REFRESH VIEW', () => {
        const stmt = { statementType: 'refreshView', refreshView: { viewName: 'OrderSummary' } };
        assert.equal((0, compiler_1.compile)(stmt), 'REFRESH MATERIALIZED VIEW "OrderSummary";');
    });
    (0, node_test_1.it)('BEGIN TRANSACTION', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'beginTransaction' }), 'BEGIN TRANSACTION;');
    });
    (0, node_test_1.it)('COMMIT', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'commit' }), 'COMMIT;');
    });
    (0, node_test_1.it)('ROLLBACK', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'rollback' }), 'ROLLBACK;');
    });
    (0, node_test_1.it)('SAVEPOINT', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'savepoint', savepoint: { name: 'sp1' } }), 'SAVEPOINT "sp1";');
    });
    (0, node_test_1.it)('ROLLBACK TO SAVEPOINT', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'rollbackToSavepoint', rollbackToSavepoint: { name: 'sp1' } }), 'ROLLBACK TO SAVEPOINT "sp1";');
    });
    (0, node_test_1.it)('DECLARE CURSOR', () => {
        const stmt = {
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
        assert.equal((0, compiler_1.compile)(stmt), 'DECLARE my_cursor CURSOR FOR SELECT * FROM "Orders" ORDER BY createdAt DESC;');
    });
    (0, node_test_1.it)('FETCH N', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'fetchCursor', fetchCursor: { cursorName: 'c1', count: 10 } }), 'FETCH 10 FROM c1;');
    });
    (0, node_test_1.it)('FETCH NEXT', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'fetchCursor', fetchCursor: { cursorName: 'c1', count: 1 } }), 'FETCH NEXT FROM c1;');
    });
    (0, node_test_1.it)('FETCH ALL', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'fetchCursor', fetchCursor: { cursorName: 'c1', count: 0, fetchAll: true } }), 'FETCH ALL FROM c1;');
    });
    (0, node_test_1.it)('CLOSE CURSOR', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'closeCursor', closeCursor: { cursorName: 'my_cursor' } }), 'CLOSE my_cursor;');
    });
    (0, node_test_1.it)('PREPARE', () => {
        const stmt = {
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
        assert.equal((0, compiler_1.compile)(stmt), 'PREPARE find_user AS SELECT * FROM "Users" WHERE age > $1;');
    });
    (0, node_test_1.it)('EXECUTE', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'execute', execute: { statementName: 'find_user' } }), 'EXECUTE find_user;');
    });
    (0, node_test_1.it)('DEALLOCATE', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'deallocate', deallocate: { statementName: 'find_user' } }), 'DEALLOCATE find_user;');
    });
    (0, node_test_1.it)('CREATE USER', () => {
        const stmt = { statementType: 'createUser', createUser: { username: 'admin', password: 'secret123' } };
        assert.equal((0, compiler_1.compile)(stmt), 'CREATE USER "admin" WITH PASSWORD "secret123";');
    });
    (0, node_test_1.it)('GRANT PERMISSION', () => {
        const stmt = { statementType: 'grantPermission', grantPermission: { permission: 'SELECT', username: 'analyst' } };
        assert.equal((0, compiler_1.compile)(stmt), 'GRANT "SELECT" TO USER "analyst";');
    });
    (0, node_test_1.it)('REVOKE PERMISSION', () => {
        const stmt = { statementType: 'revokePermission', revokePermission: { permission: 'DELETE', username: 'intern' } };
        assert.equal((0, compiler_1.compile)(stmt), 'REVOKE "DELETE" FROM USER "intern";');
    });
    (0, node_test_1.it)('USE DATABASE', () => {
        const stmt = { statementType: 'useDatabase', useDatabase: { database: 'production' } };
        assert.equal((0, compiler_1.compile)(stmt), 'USE DATABASE "production";');
    });
    (0, node_test_1.it)('CREATE DATABASE', () => {
        const stmt = { statementType: 'createDatabase', createDatabase: { database: 'analytics' } };
        assert.equal((0, compiler_1.compile)(stmt), 'CREATE DATABASE "analytics";');
    });
    (0, node_test_1.it)('SHOW VERSIONS', () => {
        const stmt = { statementType: 'showVersions', showVersions: { documentId: 'doc-123', bundle: 'Orders' } };
        assert.equal((0, compiler_1.compile)(stmt), 'SHOW VERSIONS FOR "doc-123" IN BUNDLE "Orders";');
    });
    (0, node_test_1.it)('EXPLAIN SELECT', () => {
        const stmt = {
            statementType: 'explainSelect',
            explainSelect: {
                query: {
                    fields: [{ expression: ident('*') }],
                    bundle: 'Products',
                    where: binary(ident('price'), '>', literal(intVal(50))),
                },
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'EXPLAIN SELECT * FROM "Products" WHERE price > 50;');
    });
    (0, node_test_1.it)('CHECKPOINT', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'checkpoint' }), 'CHECKPOINT;');
    });
    (0, node_test_1.it)('SHOW DATABASES', () => {
        assert.equal((0, compiler_1.compile)({ statementType: 'showDatabases' }), 'SHOW DATABASES;');
    });
    (0, node_test_1.it)('CREATE ROLE', () => {
        const stmt = { statementType: 'createRole', createRole: { roleName: 'analyst', description: 'Data analysis role' } };
        assert.equal((0, compiler_1.compile)(stmt), 'CREATE ROLE "analyst" WITH DESCRIPTION "Data analysis role";');
    });
    (0, node_test_1.it)('START MIGRATION', () => {
        const stmt = { statementType: 'startMigration', startMigration: { description: 'Add user preferences' } };
        assert.equal((0, compiler_1.compile)(stmt), 'START MIGRATION WITH DESCRIPTION "Add user preferences";');
    });
    (0, node_test_1.it)('OR precedence with AND', () => {
        const stmt = {
            statementType: 'select',
            select: {
                fields: [{ expression: ident('*') }],
                bundle: 'Customers',
                where: binary(binary(binary(ident('status'), '==', literal(strVal('active'))), 'OR', binary(ident('status'), '==', literal(strVal('pending')))), 'AND', binary(ident('category'), '==', literal(strVal('VIP')))),
            },
        };
        assert.equal((0, compiler_1.compile)(stmt), 'SELECT * FROM "Customers" WHERE (status == "active" OR status == "pending") AND category == "VIP";');
    });
    (0, node_test_1.it)('BACKUP DATABASE', () => {
        const stmt = { statementType: 'backupDatabase', backupDatabase: { database: 'production', path: '/backups/prod' } };
        assert.equal((0, compiler_1.compile)(stmt), 'BACKUP DATABASE "production" TO "/backups/prod";');
    });
    (0, node_test_1.it)('INVALIDATE SESSION', () => {
        const stmt = { statementType: 'invalidateSession', invalidateSession: { sessionId: 'sess-abc-123' } };
        assert.equal((0, compiler_1.compile)(stmt), 'INVALIDATE SESSION "sess-abc-123";');
    });
});
