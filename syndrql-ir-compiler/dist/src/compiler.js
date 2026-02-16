"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.compile = compile;
exports.compileAll = compileAll;
const expression_compiler_1 = require("./expression-compiler");
function compile(stmt) {
    switch (stmt.statementType) {
        // DML
        case 'select': return compileSelect(stmt.select);
        case 'insert': return compileInsert(stmt);
        case 'update': return compileUpdate(stmt);
        case 'delete': return compileDelete(stmt);
        // DDL - Bundle
        case 'createBundle': return compileCreateBundle(stmt);
        case 'dropBundle': return compileDropBundle(stmt);
        case 'updateBundle': return compileUpdateBundle(stmt);
        // DDL - Index
        case 'createIndex': return compileCreateIndex(stmt);
        // DDL - View
        case 'createView': return compileCreateView(stmt);
        case 'dropView': return compileDropView(stmt);
        case 'refreshView': return `REFRESH MATERIALIZED VIEW "${stmt.refreshView.viewName}";`;
        // Transaction
        case 'beginTransaction': return 'BEGIN TRANSACTION;';
        case 'commit': return 'COMMIT;';
        case 'rollback': return 'ROLLBACK;';
        case 'savepoint': return `SAVEPOINT "${stmt.savepoint.name}";`;
        case 'rollbackToSavepoint': return `ROLLBACK TO SAVEPOINT "${stmt.rollbackToSavepoint.name}";`;
        // Cursor
        case 'declareCursor': return compileDeclareCursor(stmt);
        case 'fetchCursor': return compileFetchCursor(stmt);
        case 'closeCursor': return `CLOSE ${stmt.closeCursor.cursorName};`;
        // Prepared
        case 'prepare': return compilePrepare(stmt);
        case 'execute': return `EXECUTE ${stmt.execute.statementName};`;
        case 'deallocate': return `DEALLOCATE ${stmt.deallocate.statementName};`;
        // RBAC
        case 'createUser': return `CREATE USER "${stmt.createUser.username}" WITH PASSWORD "${stmt.createUser.password}";`;
        case 'updateUser': {
            let s = `UPDATE USER "${stmt.updateUser.username}" SET PASSWORD = "${stmt.updateUser.newPassword}"`;
            if (stmt.updateUser.force)
                s += ' FORCE';
            return s + ';';
        }
        case 'deleteUser': {
            let s = `DELETE USER "${stmt.deleteUser.username}"`;
            if (stmt.deleteUser.force)
                s += ' FORCE';
            return s + ';';
        }
        case 'createRole': {
            let s = `CREATE ROLE "${stmt.createRole.roleName}"`;
            if (stmt.createRole.description)
                s += ` WITH DESCRIPTION "${stmt.createRole.description}"`;
            return s + ';';
        }
        case 'updateRole': {
            let s = `UPDATE ROLE "${stmt.updateRole.roleName}" SET DESCRIPTION = "${stmt.updateRole.newDescription}"`;
            if (stmt.updateRole.force)
                s += ' FORCE';
            return s + ';';
        }
        case 'deleteRole': {
            let s = `DELETE ROLE "${stmt.deleteRole.roleName}"`;
            if (stmt.deleteRole.force)
                s += ' FORCE';
            return s + ';';
        }
        case 'grantPermission': return `GRANT "${stmt.grantPermission.permission}" TO USER "${stmt.grantPermission.username}";`;
        case 'grantRole': return `GRANT "${stmt.grantRole.roleName}" TO USER "${stmt.grantRole.username}";`;
        case 'revokePermission': return `REVOKE "${stmt.revokePermission.permission}" FROM USER "${stmt.revokePermission.username}";`;
        case 'revokeRole': return `REVOKE "${stmt.revokeRole.roleName}" FROM USER "${stmt.revokeRole.username}";`;
        // Database
        case 'useDatabase': return `USE DATABASE "${stmt.useDatabase.database}";`;
        case 'createDatabase': return `CREATE DATABASE "${stmt.createDatabase.database}";`;
        case 'dropDatabase': {
            let s = `DROP DATABASE "${stmt.dropDatabase.database}"`;
            if (stmt.dropDatabase.force)
                s += ' FORCE';
            return s + ';';
        }
        case 'renameDatabase': return `RENAME DATABASE "${stmt.renameDatabase.oldName}" TO "${stmt.renameDatabase.newName}";`;
        case 'attachDatabase': {
            let s = `ATTACH DATABASE "${stmt.attachDatabase.path}"`;
            if (stmt.attachDatabase.alias)
                s += ` AS "${stmt.attachDatabase.alias}"`;
            return s + ';';
        }
        // Utility
        case 'showDatabases': return 'SHOW DATABASES;';
        case 'showBundles': return 'SHOW BUNDLES;';
        case 'showBundle': return `SHOW BUNDLE "${stmt.showBundle.bundle}";`;
        case 'showUsers': return 'SHOW USERS;';
        case 'showSessions': return 'SHOW SESSIONS;';
        case 'showViews': {
            if (stmt.showViews?.database) {
                return `SHOW VIEWS IN DATABASE "${stmt.showViews.database}";`;
            }
            return 'SHOW VIEWS;';
        }
        case 'describeView': return `DESCRIBE VIEW "${stmt.describeView.viewName}";`;
        case 'showMigrations': return `SHOW MIGRATIONS FOR "${stmt.showMigrations.database}";`;
        case 'showRateLimit': return 'SHOW RATE LIMIT;';
        case 'showVersions': {
            let s = 'SHOW VERSIONS';
            if (stmt.showVersions?.documentId)
                s += ` FOR "${stmt.showVersions.documentId}"`;
            if (stmt.showVersions?.bundle)
                s += ` IN BUNDLE "${stmt.showVersions.bundle}"`;
            return s + ';';
        }
        case 'showActiveSnapshots': return 'SHOW ACTIVE SNAPSHOTS;';
        case 'showConflictLog': return 'SHOW CONFLICT LOG;';
        case 'explainSelect': return `EXPLAIN ${(0, expression_compiler_1.compileSelectInner)(stmt.explainSelect.query)};`;
        // Migration
        case 'startMigration': {
            if (stmt.startMigration?.description) {
                return `START MIGRATION WITH DESCRIPTION "${stmt.startMigration.description}";`;
            }
            return 'START MIGRATION;';
        }
        case 'applyMigration': return `APPLY MIGRATION "${stmt.applyMigration.migrationId}";`;
        case 'validateMigration': return `VALIDATE MIGRATION "${stmt.validateMigration.migrationId}";`;
        case 'validateRollback': return `VALIDATE ROLLBACK "${stmt.validateRollback.migrationId}";`;
        // Ops
        case 'checkpoint': return 'CHECKPOINT;';
        case 'backupDatabase': {
            let s = `BACKUP DATABASE "${stmt.backupDatabase.database}"`;
            if (stmt.backupDatabase.path)
                s += ` TO "${stmt.backupDatabase.path}"`;
            return s + ';';
        }
        case 'restoreDatabase': {
            let s = `RESTORE DATABASE FROM "${stmt.restoreDatabase.path}"`;
            if (stmt.restoreDatabase.database)
                s += ` AS "${stmt.restoreDatabase.database}"`;
            return s + ';';
        }
        case 'invalidateSession': return `INVALIDATE SESSION "${stmt.invalidateSession.sessionId}";`;
        default:
            throw new Error(`Unknown statement type: ${stmt.statementType}`);
    }
}
function compileAll(env) {
    return env.statements.map(s => compile(s));
}
function compileSelect(s) {
    return (0, expression_compiler_1.compileSelectInner)(s) + ';';
}
function compileValue(v) {
    if (!v)
        return 'NULL';
    switch (v.type) {
        case 'string': return `"${v.raw}"`;
        case 'int': return v.raw;
        case 'float': return v.raw;
        case 'bool': return v.raw;
        case 'null': return 'NULL';
        default: return 'NULL';
    }
}
function compileInsert(stmt) {
    const s = stmt.insert;
    const fieldStrs = s.fields.map(f => `{${f.name} = ${compileValue(f.value)}}`);
    return `ADD DOCUMENT TO BUNDLE "${s.bundle}" WITH (${fieldStrs.join(', ')});`;
}
function compileUpdate(stmt) {
    const s = stmt.update;
    const fieldStrs = s.fields.map(f => `${f.name} = ${compileValue(f.value)}`);
    let result = `UPDATE DOCUMENTS IN BUNDLE "${s.bundle}" (${fieldStrs.join(', ')})`;
    if (s.confirmed)
        result += ' CONFIRMED';
    if (s.where)
        result += ' WHERE ' + (0, expression_compiler_1.compileExpr)(s.where);
    return result + ';';
}
function compileDelete(stmt) {
    const s = stmt.delete;
    let result = `DELETE DOCUMENTS FROM "${s.bundle}"`;
    if (s.confirmed)
        result += ' CONFIRMED';
    if (s.where)
        result += ' WHERE ' + (0, expression_compiler_1.compileExpr)(s.where);
    return result + ';';
}
function compileCreateBundle(stmt) {
    const s = stmt.createBundle;
    const fieldStrs = s.fields.map(f => {
        let str = `{"${f.name}", "${f.type}", ${f.required}, ${f.unique}`;
        if (f.defaultValue)
            str += ', ' + compileValue(f.defaultValue);
        return str + '}';
    });
    return `CREATE BUNDLE "${s.bundle}" WITH FIELDS (${fieldStrs.join(', ')});`;
}
function compileDropBundle(stmt) {
    let result = `DROP BUNDLE "${stmt.dropBundle.bundle}"`;
    if (stmt.dropBundle.force)
        result += ' FORCE';
    return result + ';';
}
function compileUpdateBundle(stmt) {
    const s = stmt.updateBundle;
    let result = `UPDATE BUNDLE "${s.bundle}"`;
    if (s.newBundleName)
        result += ` RENAME TO "${s.newBundleName}"`;
    if (s.changes) {
        for (const c of s.changes) {
            switch (c.action) {
                case 'ADD':
                    if (c.fieldDef) {
                        result += ` ADD FIELD "${c.field}" {"${c.fieldDef.name}", "${c.fieldDef.type}", ${c.fieldDef.required}, ${c.fieldDef.unique}}`;
                    }
                    break;
                case 'REMOVE':
                    result += ` REMOVE FIELD "${c.field}"`;
                    break;
                case 'MODIFY':
                    if (c.fieldDef) {
                        result += ` MODIFY FIELD "${c.field}" {"${c.fieldDef.name}", "${c.fieldDef.type}", ${c.fieldDef.required}, ${c.fieldDef.unique}}`;
                    }
                    break;
            }
        }
    }
    return result + ';';
}
function compileCreateIndex(stmt) {
    const s = stmt.createIndex;
    const fieldStrs = s.fields.map(f => `{"${f.name}", ${f.required}, ${f.unique}}`);
    return `CREATE ${s.indexType} "${s.indexName}" ON BUNDLE "${s.bundle}" WITH FIELDS (${fieldStrs.join(', ')});`;
}
function compileCreateView(stmt) {
    const s = stmt.createView;
    const prefix = s.isMaterialized ? 'CREATE MATERIALIZED VIEW' : 'CREATE VIEW';
    return `${prefix} "${s.viewName}" AS ${(0, expression_compiler_1.compileSelectInner)(s.query)};`;
}
function compileDropView(stmt) {
    const s = stmt.dropView;
    if (s.isMaterialized)
        return `DROP MATERIALIZED VIEW "${s.viewName}";`;
    return `DROP VIEW "${s.viewName}";`;
}
function compileDeclareCursor(stmt) {
    const s = stmt.declareCursor;
    return `DECLARE ${s.cursorName} CURSOR FOR ${(0, expression_compiler_1.compileSelectInner)(s.query)};`;
}
function compileFetchCursor(stmt) {
    const s = stmt.fetchCursor;
    if (s.fetchAll)
        return `FETCH ALL FROM ${s.cursorName};`;
    if (s.count === 1)
        return `FETCH NEXT FROM ${s.cursorName};`;
    return `FETCH ${s.count} FROM ${s.cursorName};`;
}
function compilePrepare(stmt) {
    const s = stmt.prepare;
    return `PREPARE ${s.statementName} AS ${(0, expression_compiler_1.compileSelectInner)(s.query)};`;
}
