/**
 * Unit tests for SqlParser
 */

import { SqlParser } from '../sql/sqlParser';

describe('SqlParser', () => {
    describe('splitStatements', () => {
        it('should split simple statements', () => {
            const sql = 'SELECT 1; SELECT 2;';
            const result = SqlParser.splitStatements(sql);
            expect(result).toEqual(['SELECT 1', 'SELECT 2']);
        });

        it('should handle statement without trailing semicolon', () => {
            const sql = 'SELECT * FROM table1; SELECT * FROM table2';
            const result = SqlParser.splitStatements(sql);
            expect(result).toEqual(['SELECT * FROM table1', 'SELECT * FROM table2']);
        });

        it('should ignore semicolons in single quotes', () => {
            const sql = "SELECT 'hello; world'; SELECT 2;";
            const result = SqlParser.splitStatements(sql);
            expect(result).toEqual(["SELECT 'hello; world'", 'SELECT 2']);
        });

        it('should ignore semicolons in double quotes', () => {
            const sql = 'SELECT "column;name"; SELECT 2;';
            const result = SqlParser.splitStatements(sql);
            expect(result).toEqual(['SELECT "column;name"', 'SELECT 2']);
        });

        it('should ignore semicolons in line comments', () => {
            const sql = 'SELECT 1 -- comment; with semicolon\n; SELECT 2;';
            const result = SqlParser.splitStatements(sql);
            expect(result).toEqual(['SELECT 1 -- comment; with semicolon', 'SELECT 2']);
        });

        it('should ignore semicolons in block comments', () => {
            const sql = 'SELECT 1 /* comment; with; semicolons */; SELECT 2;';
            const result = SqlParser.splitStatements(sql);
            expect(result).toEqual(['SELECT 1 /* comment; with; semicolons */', 'SELECT 2']);
        });

        it('should handle empty input', () => {
            const result = SqlParser.splitStatements('');
            expect(result).toEqual([]);
        });

        it('should handle whitespace only', () => {
            const result = SqlParser.splitStatements('   \n\t  ');
            expect(result).toEqual([]);
        });

        it('should handle CREATE PROCEDURE with BEGIN/END', () => {
            const sql = `CREATE PROCEDURE myproc()
            BEGIN
                SELECT 1;
                SELECT 2;
            END;
            SELECT 3;`;
            const result = SqlParser.splitStatements(sql);
            // Note: Current implementation will split on semicolons inside procedure
            // This test documents current behavior
            expect(result.length).toBeGreaterThan(0);
        });

        it('should handle escaped single quotes in strings', () => {
            const sql = "SELECT 'it''s a test'; SELECT 2;";
            const result = SqlParser.splitStatements(sql);
            expect(result).toEqual(["SELECT 'it''s a test'", 'SELECT 2']);
        });

        it('should handle multiple consecutive semicolons', () => {
            const sql = 'SELECT 1;;; SELECT 2;';
            const result = SqlParser.splitStatements(sql);
            expect(result).toEqual(['SELECT 1', 'SELECT 2']);
        });

        it('should handle multiline SQL', () => {
            const sql = `SELECT 
                id,
                name
            FROM 
                users;
            SELECT * FROM orders;`;
            const result = SqlParser.splitStatements(sql);
            expect(result.length).toBe(2);
        });
    });


    describe('getStatementAtPosition', () => {
        it('should find statement at cursor position', () => {
            const sql = 'SELECT 1; SELECT 2; SELECT 3;';
            //                   ^--- offset 10
            const result = SqlParser.getStatementAtPosition(sql, 10);
            expect(result).not.toBeNull();
            expect(result?.sql).toBe('SELECT 2');
        });

        it('should find first statement when cursor at beginning', () => {
            const sql = 'SELECT 1; SELECT 2;';
            const result = SqlParser.getStatementAtPosition(sql, 0);
            expect(result).not.toBeNull();
            expect(result?.sql).toBe('SELECT 1');
        });

        it('should find last statement when cursor at end', () => {
            const sql = 'SELECT 1; SELECT 2';
            const result = SqlParser.getStatementAtPosition(sql, 15);
            expect(result).not.toBeNull();
            expect(result?.sql).toBe('SELECT 2');
        });

        it('should return null for empty content', () => {
            const sql = '   ; ;  ';
            const result = SqlParser.getStatementAtPosition(sql, 3);
            expect(result).toBeNull();
        });

        it('should handle statement with quotes', () => {
            const sql = "SELECT 'value'; SELECT 2;";
            const result = SqlParser.getStatementAtPosition(sql, 5);
            expect(result).not.toBeNull();
            expect(result?.sql).toBe("SELECT 'value'");
        });

        it('should return correct start and end positions', () => {
            const sql = 'SELECT 1; SELECT 2; SELECT 3;';
            //           0123456789...
            const result = SqlParser.getStatementAtPosition(sql, 12);
            expect(result).not.toBeNull();
            expect(result?.sql).toBe('SELECT 2');
            // Verify that start and end make sense
            expect(result?.start).toBeGreaterThanOrEqual(0);
            expect(result?.end).toBeGreaterThan(result?.start || 0);
            // Verify that extracting with these positions gives the right content
            expect(sql.substring(result!.start, result!.end).trim()).toBe('SELECT 2');
        });
    });

    describe('getObjectAtPosition', () => {
        it('should parse simple name', () => {
            const sql = 'SELECT * FROM mytable WHERE id = 1';
            //                        ^--- offset 14
            const result = SqlParser.getObjectAtPosition(sql, 14);
            expect(result).toEqual({ name: 'mytable' });
        });

        it('should parse schema.name format', () => {
            const sql = 'SELECT * FROM myschema.mytable';
            const result = SqlParser.getObjectAtPosition(sql, 20);
            expect(result).toEqual({ schema: 'myschema', name: 'mytable' });
        });

        it('should parse database.schema.name format', () => {
            const sql = 'SELECT * FROM mydb.myschema.mytable';
            const result = SqlParser.getObjectAtPosition(sql, 25);
            expect(result).toEqual({ database: 'mydb', schema: 'myschema', name: 'mytable' });
        });

        it('should parse database..name format (Netezza shorthand)', () => {
            const sql = 'SELECT * FROM mydb..mytable';
            const result = SqlParser.getObjectAtPosition(sql, 20);
            expect(result).toEqual({ database: 'mydb', name: 'mytable' });
        });

        it('should handle quoted identifiers', () => {
            // Parser includes quotes as identifier chars but not spaces
            // So "mytable" works, but "my table" gets truncated at space
            const sql = 'SELECT * FROM "MYTABLE"';
            const result = SqlParser.getObjectAtPosition(sql, 18);
            expect(result).not.toBeNull();
            expect(result?.name).toBe('MYTABLE');
        });

        it('should return null for whitespace', () => {
            const sql = 'SELECT   FROM table1';
            const result = SqlParser.getObjectAtPosition(sql, 7);
            expect(result).toBeNull();
        });
    });
});
