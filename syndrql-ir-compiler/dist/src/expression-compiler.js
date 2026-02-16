"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.compileExpr = compileExpr;
exports.compileSelectInner = compileSelectInner;
function compileExpr(e) {
    if (!e)
        return '';
    switch (e.type) {
        case 'literal':
            return compileLiteral(e.value);
        case 'identifier':
            return e.name ?? '';
        case 'qualified':
            return `"${e.bundle}"."${e.name}"`;
        case 'binary':
            return compileBinary(e);
        case 'not':
            return 'NOT ' + compileExpr(e.left);
        case 'isNull':
            return compileExpr(e.left) + ' IS NULL';
        case 'isNotNull':
            return compileExpr(e.left) + ' IS NOT NULL';
        case 'in':
            return compileIn(e, 'IN');
        case 'notIn':
            return compileIn(e, 'NOT IN');
        case 'function':
            return compileFunction(e);
        case 'parameter':
            return `$${e.index}`;
        case 'exists':
            if (e.query) {
                const inner = compileSelectInner(e.query);
                return `EXISTS (${inner})`;
            }
            return 'EXISTS';
        default:
            return '';
    }
}
function compileLiteral(v) {
    if (!v)
        return 'NULL';
    switch (v.type) {
        case 'string':
            return `"${v.raw}"`;
        case 'int':
            return v.raw;
        case 'float':
            return v.raw;
        case 'bool':
            return v.raw;
        case 'null':
            return 'NULL';
        default:
            return 'NULL';
    }
}
function compileBinary(e) {
    let left = compileExpr(e.left);
    let right = compileExpr(e.right);
    if (needsParens(e.left, e.operator ?? '')) {
        left = `(${left})`;
    }
    if (needsParens(e.right, e.operator ?? '')) {
        right = `(${right})`;
    }
    return `${left} ${e.operator} ${right}`;
}
function needsParens(child, parentOp) {
    if (!child || child.type !== 'binary')
        return false;
    if (parentOp === 'AND' && child.operator === 'OR')
        return true;
    return false;
}
function compileIn(e, keyword) {
    const left = compileExpr(e.left);
    const values = (e.args ?? []).map(arg => compileExpr(arg));
    return `${left} ${keyword} (${values.join(', ')})`;
}
function compileFunction(e) {
    const args = (e.args ?? []).map(arg => compileExpr(arg));
    let name = e.name ?? '';
    if (e.isBuiltIn) {
        name = 'F:' + name;
    }
    return `${name}(${args.join(', ')})`;
}
// Exported for reuse by view/cursor/prepare
function compileSelectInner(s) {
    const parts = [];
    // SELECT [DISTINCT]
    let selectClause = 'SELECT ';
    if (s.distinct) {
        selectClause += 'DISTINCT ';
    }
    // Fields
    if (!s.fields || s.fields.length === 0) {
        selectClause += '*';
    }
    else {
        const fieldStrs = s.fields.map(f => {
            let str = compileExpr(f.expression);
            if (f.alias) {
                str += ' AS ' + f.alias;
            }
            return str;
        });
        selectClause += fieldStrs.join(', ');
    }
    parts.push(selectClause);
    // FROM
    parts.push(`FROM "${s.bundle}"`);
    // JOIN
    if (s.joins) {
        for (const j of s.joins) {
            const condStrs = j.conditions.map(c => `"${c.leftBundle}"."${c.leftField}" ${c.operator} "${c.rightBundle}"."${c.rightField}"`);
            parts.push(`${j.joinType} JOIN "${j.bundle}" ON ${condStrs.join(' AND ')}`);
        }
    }
    // WHERE
    if (s.where) {
        parts.push('WHERE ' + compileExpr(s.where));
    }
    // GROUP BY
    if (s.groupBy && s.groupBy.length > 0) {
        parts.push('GROUP BY ' + s.groupBy.join(', '));
    }
    // HAVING
    if (s.having) {
        parts.push('HAVING ' + compileExpr(s.having));
    }
    // ORDER BY
    if (s.orderBy && s.orderBy.length > 0) {
        const orderStrs = s.orderBy.map(o => `${o.field} ${o.direction}`);
        parts.push('ORDER BY ' + orderStrs.join(', '));
    }
    // LIMIT
    if (s.limit && s.limit > 0) {
        parts.push(`LIMIT ${s.limit}`);
    }
    // OFFSET
    if (s.offset && s.offset > 0) {
        parts.push(`OFFSET ${s.offset}`);
    }
    // FOR UPDATE
    if (s.forUpdate) {
        parts.push('FOR UPDATE');
    }
    return parts.join(' ');
}
