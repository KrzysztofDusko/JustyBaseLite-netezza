export class SqlParser {
    /**
     * Splits a SQL script into individual statements, respecting quotes and comments.
     */
    public static splitStatements(text: string): string[] {
        const statements: string[] = [];
        let currentStatement = '';
        let inSingleQuote = false;
        let inDoubleQuote = false;
        let inLineComment = false;
        let inBlockComment = false;
        let i = 0;

        while (i < text.length) {
            const char = text[i];
            const nextChar = i + 1 < text.length ? text[i + 1] : '';

            // Handle comments and quotes
            if (inLineComment) {
                if (char === '\n') inLineComment = false;
            } else if (inBlockComment) {
                if (char === '*' && nextChar === '/') {
                    inBlockComment = false;
                    currentStatement += char + nextChar;
                    i++;
                    i++;
                    continue;
                }
            } else if (inSingleQuote) {
                if (char === "'" && text[i - 1] !== '\\') inSingleQuote = false; // Simple check, might need better escaping
            } else if (inDoubleQuote) {
                if (char === '"' && text[i - 1] !== '\\') inDoubleQuote = false;
            } else {
                // Not in any special block
                if (char === '-' && nextChar === '-') {
                    inLineComment = true;
                } else if (char === '/' && nextChar === '*') {
                    inBlockComment = true;
                } else if (char === "'") {
                    inSingleQuote = true;
                } else if (char === '"') {
                    inDoubleQuote = true;
                } else if (char === ';') {
                    // Found statement terminator
                    if (currentStatement.trim()) {
                        statements.push(currentStatement.trim());
                    }
                    currentStatement = '';
                    i++;
                    continue;
                }
            }

            currentStatement += char;
            i++;
        }

        if (currentStatement.trim()) {
            statements.push(currentStatement.trim());
        }

        return statements;
    }

    /**
     * Finds the SQL statement at the given offset.
     */
    public static getStatementAtPosition(
        text: string,
        offset: number
    ): { sql: string; start: number; end: number } | null {
        let start = 0;
        let end = text.length;
        let inSingleQuote = false;
        let inDoubleQuote = false;
        let inLineComment = false;
        let inBlockComment = false;
        let lastSemi = -1;

        // First pass: find the start of the statement (after the previous semicolon)
        for (let i = 0; i < offset; i++) {
            const char = text[i];
            const nextChar = i + 1 < text.length ? text[i + 1] : '';

            if (inLineComment) {
                if (char === '\n') inLineComment = false;
            } else if (inBlockComment) {
                if (char === '*' && nextChar === '/') {
                    inBlockComment = false;
                    i++;
                }
            } else if (inSingleQuote) {
                if (char === "'" && text[i - 1] !== '\\') inSingleQuote = false;
            } else if (inDoubleQuote) {
                if (char === '"' && text[i - 1] !== '\\') inDoubleQuote = false;
            } else {
                if (char === '-' && nextChar === '-') {
                    inLineComment = true;
                } else if (char === '/' && nextChar === '*') {
                    inBlockComment = true;
                } else if (char === "'") {
                    inSingleQuote = true;
                } else if (char === '"') {
                    inDoubleQuote = true;
                } else if (char === ';') {
                    lastSemi = i;
                }
            }
        }

        start = lastSemi + 1;

        // Reset state for second pass from start
        inSingleQuote = false;
        inDoubleQuote = false;
        inLineComment = false;
        inBlockComment = false;

        // Second pass: find the end of the statement
        for (let i = start; i < text.length; i++) {
            const char = text[i];
            const nextChar = i + 1 < text.length ? text[i + 1] : '';

            if (inLineComment) {
                if (char === '\n') inLineComment = false;
            } else if (inBlockComment) {
                if (char === '*' && nextChar === '/') {
                    inBlockComment = false;
                    i++;
                }
            } else if (inSingleQuote) {
                if (char === "'" && text[i - 1] !== '\\') inSingleQuote = false;
            } else if (inDoubleQuote) {
                if (char === '"' && text[i - 1] !== '\\') inDoubleQuote = false;
            } else {
                if (char === '-' && nextChar === '-') {
                    inLineComment = true;
                } else if (char === '/' && nextChar === '*') {
                    inBlockComment = true;
                } else if (char === "'") {
                    inSingleQuote = true;
                } else if (char === '"') {
                    inDoubleQuote = true;
                } else if (char === ';') {
                    end = i;
                    break;
                }
            }
        }

        const sql = text.substring(start, end).trim();
        if (!sql) return null;

        return { sql, start, end };
    }

    /**
     * Extracts the database object reference at the given offset.
     * Supports formats: NAME, SCHEMA.NAME, DB.SCHEMA.NAME, DB..NAME
     */
    public static getObjectAtPosition(
        text: string,
        offset: number
    ): { database?: string; schema?: string; name: string } | null {
        // Find the boundaries of the identifier at the cursor
        // We allow alphanumeric, underscore, dot, and double quotes
        const isIdentifierChar = (char: string) => /[a-zA-Z0-9_."]/i.test(char);

        let start = offset;
        while (start > 0 && isIdentifierChar(text[start - 1])) {
            start--;
        }

        let end = offset;
        while (end < text.length && isIdentifierChar(text[end])) {
            end++;
        }

        const identifier = text.substring(start, end);
        if (!identifier) return null;

        // Clean up quotes if present (simple handling)
        const clean = (s: string) => (s ? s.replace(/"/g, '') : undefined);

        // Parse parts
        // Handle DB..NAME case specifically
        if (identifier.includes('..')) {
            const parts = identifier.split('..');
            if (parts.length === 2) {
                return {
                    database: clean(parts[0]),
                    name: clean(parts[1])!
                };
            }
        }

        const parts = identifier.split('.');
        if (parts.length === 1) {
            return { name: clean(parts[0])! };
        } else if (parts.length === 2) {
            return {
                schema: clean(parts[0]),
                name: clean(parts[1])!
            };
        } else if (parts.length === 3) {
            return {
                database: clean(parts[0]),
                schema: clean(parts[1]),
                name: clean(parts[2])!
            };
        }

        return null;
    }
}
