/**
 * SQL Text Utilities for processing source code
 * Used by Object Search to filter out comments and string literals
 */

/**
 * Strips comments and string literals from SQL source code.
 * Removes:
 * - Single-line comments: -- comment
 * - Multi-line comments: slash-star ... star-slash
 * - String literals: 'text' (including escaped '' inside)
 *
 * @param sql The SQL source code to process
 * @returns The SQL with comments and literals removed
 */
export function stripCommentsAndLiterals(sql: string): string {
    let result = '';
    let i = 0;

    while (i < sql.length) {
        // Single-line comment: --
        if (sql[i] === '-' && i + 1 < sql.length && sql[i + 1] === '-') {
            // Skip until newline
            while (i < sql.length && sql[i] !== '\n') {
                i++;
            }
            // Keep the newline for proper spacing
            if (i < sql.length) {
                result += ' ';
                i++;
            }
            continue;
        }

        // Multi-line comment: /* */
        if (sql[i] === '/' && i + 1 < sql.length && sql[i + 1] === '*') {
            i += 2; // Skip /*
            while (i < sql.length - 1 && !(sql[i] === '*' && sql[i + 1] === '/')) {
                i++;
            }
            if (i < sql.length - 1) {
                i += 2; // Skip */
            }
            result += ' '; // Replace with space to maintain word boundaries
            continue;
        }

        // String literal: 'text' (handle escaped '' inside)
        if (sql[i] === "'") {
            i++; // Skip opening quote
            while (i < sql.length) {
                if (sql[i] === "'" && i + 1 < sql.length && sql[i + 1] === "'") {
                    i += 2; // Skip escaped quote ''
                } else if (sql[i] === "'") {
                    i++; // Skip closing quote
                    break;
                } else {
                    i++;
                }
            }
            result += ' '; // Replace with space to maintain word boundaries
            continue;
        }

        result += sql[i];
        i++;
    }

    return result;
}

/**
 * Checks if the search term exists in the SQL source code,
 * excluding comments and string literals.
 *
 * @param sql The SQL source code to search in
 * @param term The search term (case-insensitive)
 * @returns true if term is found in code (not in comments/literals)
 */
export function searchInCode(sql: string, term: string): boolean {
    const cleanedSql = stripCommentsAndLiterals(sql);
    return cleanedSql.toUpperCase().includes(term.toUpperCase());
}
