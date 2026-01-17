/**
 * Unit tests for SQL Linter
 */

// Mock vscode module
jest.mock('vscode', () => ({
    DiagnosticSeverity: {
        Error: 0,
        Warning: 1,
        Information: 2,
        Hint: 3
    }
}), { virtual: true });

import {
    ruleNZ001,
    ruleNZ002,
    ruleNZ003,
    ruleNZ004,
    ruleNZ005,
    ruleNZ006,
    ruleNZ007,
    ruleNZ008,
    ruleNZ009,
    ruleNZ010,
    ruleNZ011,
    allRules,
    parseSeverity
} from '../providers/linterRules';

describe('SQL Linter Rules', () => {
    describe('NZ001 - SELECT *', () => {
        it('should detect SELECT *', () => {
            const sql = 'SELECT * FROM table1';
            const issues = ruleNZ001.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].ruleId).toBe('NZ001');
        });

        it('should detect multiple SELECT *', () => {
            const sql = 'SELECT * FROM table1; SELECT * FROM table2;';
            const issues = ruleNZ001.check(sql);
            expect(issues.length).toBe(2);
        });

        it('should not flag SELECT with explicit columns', () => {
            const sql = 'SELECT col1, col2 FROM table1';
            const issues = ruleNZ001.check(sql);
            expect(issues.length).toBe(0);
        });

        it('should not flag * inside string', () => {
            const sql = "SELECT 'SELECT *' FROM table1";
            const issues = ruleNZ001.check(sql);
            expect(issues.length).toBe(0);
        });

        it('should not flag * inside comment', () => {
            const sql = 'SELECT col1 -- SELECT * is bad\nFROM table1';
            const issues = ruleNZ001.check(sql);
            expect(issues.length).toBe(0);
        });
    });

    describe('NZ002 - DELETE without WHERE', () => {
        it('should detect DELETE without WHERE', () => {
            const sql = 'DELETE FROM table1';
            const issues = ruleNZ002.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].ruleId).toBe('NZ002');
        });

        it('should not flag DELETE with WHERE', () => {
            const sql = 'DELETE FROM table1 WHERE id = 1';
            const issues = ruleNZ002.check(sql);
            expect(issues.length).toBe(0);
        });

        it('should handle multiple statements correctly', () => {
            const sql = 'DELETE FROM table1 WHERE id = 1; DELETE FROM table2;';
            const issues = ruleNZ002.check(sql);
            expect(issues.length).toBe(1);
        });
    });

    describe('NZ003 - UPDATE without WHERE', () => {
        it('should detect UPDATE without WHERE', () => {
            const sql = 'UPDATE table1 SET col1 = 1';
            const issues = ruleNZ003.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].ruleId).toBe('NZ003');
        });

        it('should not flag UPDATE with WHERE', () => {
            const sql = 'UPDATE table1 SET col1 = 1 WHERE id = 1';
            const issues = ruleNZ003.check(sql);
            expect(issues.length).toBe(0);
        });
    });

    describe('NZ004 - CROSS JOIN', () => {
        it('should detect CROSS JOIN', () => {
            const sql = 'SELECT * FROM table1 CROSS JOIN table2';
            const issues = ruleNZ004.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].ruleId).toBe('NZ004');
        });

        it('should not flag regular JOIN', () => {
            const sql = 'SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id';
            const issues = ruleNZ004.check(sql);
            expect(issues.length).toBe(0);
        });
    });

    describe('NZ005 - Leading wildcard LIKE', () => {
        it('should detect LIKE with leading wildcard', () => {
            const sql = "SELECT * FROM table1 WHERE name LIKE '%test'";
            const issues = ruleNZ005.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].ruleId).toBe('NZ005');
        });

        it('should not flag LIKE with trailing wildcard only', () => {
            const sql = "SELECT * FROM table1 WHERE name LIKE 'test%'";
            const issues = ruleNZ005.check(sql);
            expect(issues.length).toBe(0);
        });
    });

    describe('NZ006 - ORDER BY without LIMIT', () => {
        it('should detect ORDER BY without LIMIT', () => {
            const sql = 'SELECT * FROM table1 ORDER BY col1';
            const issues = ruleNZ006.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].ruleId).toBe('NZ006');
        });

        it('should not flag ORDER BY with LIMIT', () => {
            const sql = 'SELECT * FROM table1 ORDER BY col1 LIMIT 10';
            const issues = ruleNZ006.check(sql);
            expect(issues.length).toBe(0);
        });

        it('should not flag ORDER BY with FETCH', () => {
            const sql = 'SELECT * FROM table1 ORDER BY col1 FETCH FIRST 10 ROWS ONLY';
            const issues = ruleNZ006.check(sql);
            expect(issues.length).toBe(0);
        });
    });

    describe('NZ007 - Inconsistent keyword casing', () => {
        it('should detect mixed case (inconsistent) keywords', () => {
            const sql = 'SELECT col1 from table1 WHERE id = 1';
            const issues = ruleNZ007.check(sql);
            // Should report 'from' as inconsistent (majority is UPPER)
            expect(issues.length).toBeGreaterThan(0);
            expect(issues[0].ruleId).toBe('NZ007');
            expect(issues[0].message).toContain('UPPERCASE');
        });

        it('should detect Mixed Case (e.g. Select) as error', () => {
            const sql = 'Select * FROM table1';
            const issues = ruleNZ007.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].message).toContain('mixed casing');
        });

        it('should enforce dominant style correctly', () => {
            // 2 lower, 1 UPPER -> dominant is lower
            const sql = 'select * from table1 WHERE id = 1';
            const issues = ruleNZ007.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].message).toContain('lowercase');
        });

        it('should not flag consistent uppercase', () => {
            const sql = 'SELECT COL1 FROM TABLE1 WHERE ID = 1';
            const issues = ruleNZ007.check(sql);
            expect(issues.length).toBe(0);
        });

        it('should not flag consistent lowercase', () => {
            const sql = 'select col1 from table1 where id = 1';
            const issues = ruleNZ007.check(sql);
            expect(issues.length).toBe(0);
        });
    });

    describe('NZ008 - TRUNCATE statement', () => {
        it('should detect TRUNCATE', () => {
            const sql = 'TRUNCATE TABLE table1';
            const issues = ruleNZ008.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].ruleId).toBe('NZ008');
        });

        it('should detect TRUNCATE without TABLE keyword', () => {
            const sql = 'TRUNCATE table1';
            const issues = ruleNZ008.check(sql);
            expect(issues.length).toBe(1);
        });
    });

    describe('NZ009 - Multiple OR conditions', () => {
        it('should detect multiple OR in WHERE', () => {
            const sql = 'SELECT * FROM table1 WHERE id = 1 OR name = "test" OR status = 1';
            const issues = ruleNZ009.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].ruleId).toBe('NZ009');
        });

        it('should not flag single OR', () => {
            const sql = 'SELECT * FROM table1 WHERE id = 1 OR name = "test"';
            const issues = ruleNZ009.check(sql);
            expect(issues.length).toBe(0);
        });
    });

    describe('NZ010 - Missing table alias in JOIN', () => {
        it('should detect JOIN without alias', () => {
            const sql = 'SELECT * FROM table1 t1 JOIN table2 ON t1.id = table2.id';
            const issues = ruleNZ010.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].ruleId).toBe('NZ010');
        });

        it('should not flag JOIN with alias', () => {
            const sql = 'SELECT * FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id';
            const issues = ruleNZ010.check(sql);
            expect(issues.length).toBe(0);
        });
    });

    describe('NZ011 - CTAS Missing Distribution', () => {
        it('should detect CTAS without DISTRIBUTE ON', () => {
            const sql = 'CREATE TABLE new_table AS SELECT * FROM old_table';
            const issues = ruleNZ011.check(sql);
            expect(issues.length).toBe(1);
            expect(issues[0].ruleId).toBe('NZ011');
        });

        it('should not flag CTAS with DISTRIBUTE ON', () => {
            const sql = 'CREATE TABLE new_table AS SELECT * FROM old_table DISTRIBUTE ON RANDOM';
            const issues = ruleNZ011.check(sql);
            expect(issues.length).toBe(0);
        });

        it('should not flag regular CREATE TABLE', () => {
            const sql = 'CREATE TABLE new_table (id int)';
            const issues = ruleNZ011.check(sql);
            expect(issues.length).toBe(0);
        });

        it('should not flag CTAS with explicit column distribution', () => {
            const sql = 'CREATE TABLE new_table AS SELECT * FROM old_table DISTRIBUTE ON (id)';
            const issues = ruleNZ011.check(sql);
            expect(issues.length).toBe(0);
        });

        it('should handle case insensitivity', () => {
            const sql = 'create table t as select * from old distribute on random';
            const issues = ruleNZ011.check(sql);
            expect(issues.length).toBe(0);
        });

        it('should detect CTAS with parentheses', () => {
            const sql = 'CREATE TABLE t AS (SELECT * FROM old)';
            const issues = ruleNZ011.check(sql);
            expect(issues.length).toBe(1);
        });

        it('should handle multiline CTAS', () => {
            const sql = `
                CREATE TABLE t AS 
                SELECT * FROM old
            `;
            const issues = ruleNZ011.check(sql);
            expect(issues.length).toBe(1);
        });

        it('should ignore DISTRIBUTE ON inside comments', () => {
            const sql = 'CREATE TABLE t AS SELECT * FROM old; -- DISTRIBUTE ON RANDOM';
            // This technically ends at semicolon, so statement content excludes the comment.
            // Should report missing distribution
            const issues1 = ruleNZ011.check(sql);
            expect(issues1.length).toBe(1);

            // But let's test inline comment inside statement
            const sql2 = 'CREATE TABLE t AS SELECT /* DISTRIBUTE ON RANDOM */ * FROM old';
            const issues2 = ruleNZ011.check(sql2);
            expect(issues2.length).toBe(1);
        });
    });

    describe('parseSeverity', () => {
        it('should parse error', () => {
            expect(parseSeverity('error')).toBe(0); // DiagnosticSeverity.Error
        });

        it('should parse warning', () => {
            expect(parseSeverity('warning')).toBe(1); // DiagnosticSeverity.Warning
        });

        it('should parse off', () => {
            expect(parseSeverity('off')).toBeNull();
        });
    });

    describe('allRules', () => {
        it('should contain 13 rules', () => {
            expect(allRules.length).toBe(13);
        });

        it('should have unique rule IDs', () => {
            const ids = allRules.map(r => r.id);
            const uniqueIds = new Set(ids);
            expect(uniqueIds.size).toBe(ids.length);
        });
    });
});

describe('Complex SQL scenarios', () => {
    it('should handle subqueries correctly', () => {
        const sql = `
            SELECT * FROM (
                SELECT id, name FROM users WHERE active = 1
            ) AS subquery
        `;
        // Should detect SELECT * but not the inner SELECT with explicit columns
        const issues = ruleNZ001.check(sql);
        expect(issues.length).toBe(1);
    });

    it('should ignore patterns inside block comments', () => {
        const sql = `
            /* 
             * SELECT * FROM dangerous_table
             * DELETE FROM important_table
             */
            SELECT col1 FROM table1 WHERE id = 1
        `;
        const selectStarIssues = ruleNZ001.check(sql);
        const deleteIssues = ruleNZ002.check(sql);
        expect(selectStarIssues.length).toBe(0);
        expect(deleteIssues.length).toBe(0);
    });

    it('should handle nested quotes correctly', () => {
        const sql = `SELECT 'it''s a test' FROM table1`;
        const issues = ruleNZ001.check(sql);
        expect(issues.length).toBe(0);
    });
});
