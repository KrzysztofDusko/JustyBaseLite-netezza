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

/**
 * Search mode for source code search
 * - 'raw': Search in entire source (including comments and strings)
 * - 'noComments': Search excluding comments (-- and block comments)
 * - 'noCommentsNoLiterals': Search excluding comments and string literals
 */
export type SourceSearchMode = 'raw' | 'noComments' | 'noCommentsNoLiterals';

/**
 * Strips only comments from SQL source code (preserves string literals).
 * Removes:
 * - Single-line comments: -- comment
 * - Multi-line comments: slash-star ... star-slash
 *
 * @param sql The SQL source code to process
 * @returns The SQL with only comments removed
 */
export function stripComments(sql: string): string {
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

        // String literal: 'text' - PRESERVE (just skip over without removing)
        if (sql[i] === "'") {
            result += sql[i];
            i++; // Skip opening quote
            while (i < sql.length) {
                if (sql[i] === "'" && i + 1 < sql.length && sql[i + 1] === "'") {
                    result += "''"; // Keep escaped quote
                    i += 2;
                } else if (sql[i] === "'") {
                    result += sql[i]; // Keep closing quote
                    i++;
                    break;
                } else {
                    result += sql[i];
                    i++;
                }
            }
            continue;
        }

        result += sql[i];
        i++;
    }

    return result;
}

/**
 * Searches for a term in SQL source code using the specified mode.
 *
 * @param sql The SQL source code to search in
 * @param term The search term (case-insensitive)
 * @param mode The search mode: 'raw', 'noComments', or 'noCommentsNoLiterals'
 * @returns true if term is found according to the mode
 */
export function searchInCodeWithMode(sql: string, term: string, mode: SourceSearchMode): boolean {
    let searchText: string;

    switch (mode) {
        case 'raw':
            searchText = sql;
            break;
        case 'noComments':
            searchText = stripComments(sql);
            break;
        case 'noCommentsNoLiterals':
        default:
            searchText = stripCommentsAndLiterals(sql);
            break;
    }

    return searchText.toUpperCase().includes(term.toUpperCase());
}
