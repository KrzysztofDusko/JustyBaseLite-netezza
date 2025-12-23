/**
 * Unit tests for core/variableUtils.ts
 */

import {
    extractVariables,
    parseSetVariables,
    replaceVariablesInSql,
    processVariables
} from '../core/variableUtils';

describe('core/variableUtils', () => {
    describe('extractVariables', () => {
        it('should return empty set for empty string', () => {
            expect(extractVariables('')).toEqual(new Set());
        });

        it('should return empty set for null-like input', () => {
            expect(extractVariables('')).toEqual(new Set());
        });

        it('should return empty set for SQL without variables', () => {
            expect(extractVariables('SELECT * FROM users')).toEqual(new Set());
        });

        it('should extract single variable with braces', () => {
            const result = extractVariables('SELECT * FROM ${TABLE}');
            expect(result).toEqual(new Set(['TABLE']));
        });

        it('should extract single variable without braces', () => {
            const result = extractVariables('SELECT * FROM $TABLE');
            expect(result).toEqual(new Set(['TABLE']));
        });

        it('should extract multiple variables with braces', () => {
            const result = extractVariables('SELECT * FROM ${TABLE} WHERE id = ${ID}');
            expect(result).toEqual(new Set(['TABLE', 'ID']));
        });

        it('should extract multiple variables without braces', () => {
            const result = extractVariables('SELECT * FROM $TABLE WHERE id = $ID');
            expect(result).toEqual(new Set(['TABLE', 'ID']));
        });

        it('should extract mixed format variables', () => {
            const result = extractVariables('SELECT * FROM ${TABLE} WHERE id = $ID');
            expect(result).toEqual(new Set(['TABLE', 'ID']));
        });

        it('should deduplicate repeated variables', () => {
            const result = extractVariables('SELECT ${COL}, ${COL} FROM ${TABLE}');
            expect(result).toEqual(new Set(['COL', 'TABLE']));
        });

        it('should deduplicate same variable in different formats', () => {
            const result = extractVariables('SELECT ${TABLE}, $TABLE FROM db');
            expect(result).toEqual(new Set(['TABLE']));
        });

        it('should handle variables with underscores', () => {
            const result = extractVariables('SELECT * FROM ${MY_TABLE_NAME}');
            expect(result).toEqual(new Set(['MY_TABLE_NAME']));
        });

        it('should handle $VAR with underscores', () => {
            const result = extractVariables('SELECT * FROM $MY_TABLE_NAME');
            expect(result).toEqual(new Set(['MY_TABLE_NAME']));
        });

        it('should handle variables starting with underscore', () => {
            const result = extractVariables('SELECT * FROM $_PRIVATE_TABLE');
            expect(result).toEqual(new Set(['_PRIVATE_TABLE']));
        });

        it('should handle variables with numbers (not at start)', () => {
            const result = extractVariables('SELECT * FROM ${TABLE1} JOIN ${TABLE2}');
            expect(result).toEqual(new Set(['TABLE1', 'TABLE2']));
        });

        it('should handle $VAR with numbers (not at start)', () => {
            const result = extractVariables('SELECT * FROM $TABLE1 JOIN $TABLE2');
            expect(result).toEqual(new Set(['TABLE1', 'TABLE2']));
        });

        it('should not extract $VAR starting with number', () => {
            // $123TABLE should not be extracted as variable
            const result = extractVariables('SELECT $123 FROM ${VALID}');
            expect(result).toEqual(new Set(['VALID']));
        });

        it('should handle multiline SQL', () => {
            const sql = `SELECT * 
FROM \${TABLE}
WHERE id = $ID`;
            expect(extractVariables(sql)).toEqual(new Set(['TABLE', 'ID']));
        });

        it('should stop $VAR at word boundary', () => {
            // $TABLE.column - should only extract TABLE
            const result = extractVariables('SELECT $TABLE.column FROM db');
            expect(result).toEqual(new Set(['TABLE']));
        });

        it('should handle $VAR at end of line', () => {
            const result = extractVariables('SELECT * FROM $TABLE');
            expect(result).toEqual(new Set(['TABLE']));
        });
    });

    describe('parseSetVariables', () => {
        it('should return empty result for empty string', () => {
            const result = parseSetVariables('');
            expect(result).toEqual({ sql: '', setValues: {} });
        });

        it('should return unchanged SQL when no @SET present', () => {
            const sql = 'SELECT * FROM users';
            const result = parseSetVariables(sql);
            expect(result.sql).toBe(sql);
            expect(result.setValues).toEqual({});
        });

        it('should parse single @SET definition', () => {
            const sql = '@SET TABLE = users\nSELECT * FROM ${TABLE}';
            const result = parseSetVariables(sql);
            expect(result.sql).toBe('SELECT * FROM ${TABLE}');
            expect(result.setValues).toEqual({ TABLE: 'users' });
        });

        it('should parse multiple @SET definitions', () => {
            const sql = '@SET DB = mydb\n@SET TABLE = users\nSELECT * FROM ${DB}.${TABLE}';
            const result = parseSetVariables(sql);
            expect(result.sql).toBe('SELECT * FROM ${DB}.${TABLE}');
            expect(result.setValues).toEqual({ DB: 'mydb', TABLE: 'users' });
        });

        it('should be case-insensitive for @SET keyword', () => {
            const sql = '@set TABLE = users\n@SET DB = mydb\n@Set SCHEMA = admin';
            const result = parseSetVariables(sql);
            expect(result.setValues).toEqual({ TABLE: 'users', DB: 'mydb', SCHEMA: 'admin' });
        });

        it('should handle @SET with trailing semicolon', () => {
            const result = parseSetVariables('@SET TABLE = users;');
            expect(result.setValues).toEqual({ TABLE: 'users' });
        });

        it('should handle @SET with quoted values (single quotes)', () => {
            const result = parseSetVariables("@SET NAME = 'John Doe'");
            expect(result.setValues).toEqual({ NAME: 'John Doe' });
        });

        it('should handle @SET with quoted values (double quotes)', () => {
            const result = parseSetVariables('@SET NAME = "John Doe"');
            expect(result.setValues).toEqual({ NAME: 'John Doe' });
        });

        it('should handle @SET with spaces around equals', () => {
            const result = parseSetVariables('@SET  TABLE  =  users');
            expect(result.setValues).toEqual({ TABLE: 'users' });
        });

        it('should handle @SET at any position in SQL', () => {
            const sql = 'SELECT * FROM foo\n@SET BAR = baz\nWHERE x = 1';
            const result = parseSetVariables(sql);
            expect(result.sql).toBe('SELECT * FROM foo\nWHERE x = 1');
            expect(result.setValues).toEqual({ BAR: 'baz' });
        });

        it('should handle Windows-style line endings', () => {
            const sql = '@SET TABLE = users\r\nSELECT * FROM ${TABLE}';
            const result = parseSetVariables(sql);
            expect(result.sql).toBe('SELECT * FROM ${TABLE}');
            expect(result.setValues).toEqual({ TABLE: 'users' });
        });

        it('should handle leading whitespace before @SET', () => {
            const result = parseSetVariables('   @SET TABLE = users');
            expect(result.setValues).toEqual({ TABLE: 'users' });
        });
    });

    describe('replaceVariablesInSql', () => {
        it('should return unchanged SQL when no variables', () => {
            const sql = 'SELECT * FROM users';
            expect(replaceVariablesInSql(sql, {})).toBe(sql);
        });

        it('should replace single variable', () => {
            const sql = 'SELECT * FROM ${TABLE}';
            const result = replaceVariablesInSql(sql, { TABLE: 'users' });
            expect(result).toBe('SELECT * FROM users');
        });

        it('should replace multiple variables', () => {
            const sql = 'SELECT * FROM ${DB}.${SCHEMA}.${TABLE}';
            const result = replaceVariablesInSql(sql, {
                DB: 'mydb',
                SCHEMA: 'admin',
                TABLE: 'users'
            });
            expect(result).toBe('SELECT * FROM mydb.admin.users');
        });

        it('should replace same variable multiple times', () => {
            const sql = 'SELECT ${COL}, ${COL} FROM ${TABLE}';
            const result = replaceVariablesInSql(sql, { COL: 'name', TABLE: 'users' });
            expect(result).toBe('SELECT name, name FROM users');
        });

        it('should replace missing variable with empty string', () => {
            const sql = 'SELECT * FROM ${TABLE} WHERE ${MISSING}';
            const result = replaceVariablesInSql(sql, { TABLE: 'users' });
            expect(result).toBe('SELECT * FROM users WHERE ');
        });

        it('should handle value with special characters', () => {
            const sql = 'SELECT * FROM ${TABLE}';
            const result = replaceVariablesInSql(sql, { TABLE: 'my-special_table$name' });
            expect(result).toBe('SELECT * FROM my-special_table$name');
        });

        it('should handle value with SQL keywords', () => {
            const sql = 'SELECT * FROM ${TABLE}';
            const result = replaceVariablesInSql(sql, { TABLE: 'SELECT FROM WHERE' });
            expect(result).toBe('SELECT * FROM SELECT FROM WHERE');
        });

        it('should handle numeric values as strings', () => {
            const sql = 'SELECT * FROM users WHERE id = ${ID}';
            const result = replaceVariablesInSql(sql, { ID: '42' });
            expect(result).toBe('SELECT * FROM users WHERE id = 42');
        });

        // $VAR syntax tests
        it('should replace $VAR without braces', () => {
            const sql = 'SELECT * FROM $TABLE';
            const result = replaceVariablesInSql(sql, { TABLE: 'users' });
            expect(result).toBe('SELECT * FROM users');
        });

        it('should replace multiple $VAR without braces', () => {
            const sql = 'SELECT * FROM $DB.$SCHEMA.$TABLE';
            const result = replaceVariablesInSql(sql, {
                DB: 'mydb',
                SCHEMA: 'admin',
                TABLE: 'users'
            });
            expect(result).toBe('SELECT * FROM mydb.admin.users');
        });

        it('should replace mixed ${VAR} and $VAR', () => {
            const sql = 'SELECT * FROM ${TABLE} WHERE id = $ID';
            const result = replaceVariablesInSql(sql, { TABLE: 'users', ID: '42' });
            expect(result).toBe('SELECT * FROM users WHERE id = 42');
        });

        it('should not replace $VAR if not in values', () => {
            const sql = 'SELECT * FROM $UNKNOWN';
            const result = replaceVariablesInSql(sql, { TABLE: 'users' });
            expect(result).toBe('SELECT * FROM $UNKNOWN');
        });

        it('should replace longer variable name first', () => {
            // $TABLE_NAME should be replaced before $TABLE to avoid partial match
            const sql = 'SELECT * FROM $TABLE_NAME WHERE $TABLE = 1';
            const result = replaceVariablesInSql(sql, {
                TABLE: 'tab',
                TABLE_NAME: 'my_table'
            });
            expect(result).toBe('SELECT * FROM my_table WHERE tab = 1');
        });

        it('should handle $VAR starting with underscore', () => {
            const sql = 'SELECT * FROM $_PRIVATE';
            const result = replaceVariablesInSql(sql, { _PRIVATE: 'secret_table' });
            expect(result).toBe('SELECT * FROM secret_table');
        });

        it('should stop $VAR replacement at word boundary', () => {
            const sql = 'SELECT $COL.subfield FROM $TABLE';
            const result = replaceVariablesInSql(sql, { COL: 'data', TABLE: 'users' });
            expect(result).toBe('SELECT data.subfield FROM users');
        });
    });

    describe('processVariables', () => {
        it('should handle SQL without any variables', () => {
            const result = processVariables('SELECT * FROM users');
            expect(result.processedSql).toBe('SELECT * FROM users');
            expect(result.unresolvedVars).toEqual([]);
        });

        it('should resolve variables from @SET definitions', () => {
            const sql = '@SET TABLE = users\nSELECT * FROM ${TABLE}';
            const result = processVariables(sql);
            expect(result.processedSql).toBe('SELECT * FROM users');
            expect(result.unresolvedVars).toEqual([]);
        });

        it('should report unresolved variables', () => {
            const sql = 'SELECT * FROM ${TABLE} WHERE id = ${ID}';
            const result = processVariables(sql);
            expect(result.unresolvedVars).toContain('TABLE');
            expect(result.unresolvedVars).toContain('ID');
        });

        it('should allow overrides to replace @SET defaults', () => {
            const sql = '@SET TABLE = default_table\nSELECT * FROM ${TABLE}';
            const result = processVariables(sql, { TABLE: 'override_table' });
            expect(result.processedSql).toBe('SELECT * FROM override_table');
        });

        it('should merge @SET defaults with overrides', () => {
            const sql = '@SET DB = mydb\nSELECT * FROM ${DB}.${TABLE}';
            const result = processVariables(sql, { TABLE: 'users' });
            expect(result.processedSql).toBe('SELECT * FROM mydb.users');
            expect(result.unresolvedVars).toEqual([]);
        });

        it('should partially resolve when some variables missing', () => {
            const sql = '@SET DB = mydb\nSELECT * FROM ${DB}.${SCHEMA}.${TABLE}';
            const result = processVariables(sql, { TABLE: 'users' });
            expect(result.processedSql).toBe('SELECT * FROM mydb..users');
            expect(result.unresolvedVars).toEqual(['SCHEMA']);
        });
    });
});
