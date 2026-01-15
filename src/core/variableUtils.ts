/**
 * SQL Variable Utilities
 * Handles SQL variable extraction, parsing @SET definitions, and replacement
 */

/**
 * Extract placeholder variable names from SQL.
 * Supports two formats:
 * - ${VAR_NAME} - variable in curly braces
 * - $VAR_NAME - variable without braces (must start with letter or underscore)
 * 
 * @param sql - The SQL string to scan for variables
 * @returns Set of variable names found in the SQL
 * 
 * @example
 * extractVariables("SELECT * FROM ${TABLE} WHERE id = $ID")
 * // Returns: Set(['TABLE', 'ID'])
 */
export function extractVariables(sql: string): Set<string> {
    const vars = new Set<string>();
    if (!sql) return vars;

    // Match ${VAR_NAME} format (letters, digits, underscores)
    for (const m of sql.matchAll(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g)) {
        if (m[1]) vars.add(m[1]);
    }

    // Match $VAR_NAME format (must start with letter or underscore)
    // Use negative lookbehind to avoid matching ${VAR} pattern
    // and negative lookahead to not match if followed by {
    for (const m of sql.matchAll(/\$([A-Za-z_][A-Za-z0-9_]*)(?!\s*\})/g)) {
        // Skip if this is part of ${...} pattern (check if preceded by ${ )
        const fullMatch = m[0];
        const idx = m.index!;
        // Check if previous char is { (would mean we're inside ${VAR})
        if (idx > 0 && sql[idx - 1] === '{') continue;
        // Check if next char after match is } (would mean ${VAR})
        const afterIdx = idx + fullMatch.length;
        if (afterIdx < sql.length && sql[afterIdx] === '}') continue;

        if (m[1]) vars.add(m[1]);
    }

    return vars;
}

/**
 * Parsed result from @SET statement scanning
 */
export interface ParseSetResult {
    /** SQL with @SET lines removed */
    sql: string;
    /** Map of variable name to default value */
    setValues: Record<string, string>;
}

/**
 * Parse lines like `@SET NAME = value` (case-insensitive) at the top/anywhere in SQL.
 * Removes those lines from the SQL and returns a map of defaults.
 * 
 * @param sql - The SQL string to parse
 * @returns Object with cleaned SQL and extracted set values
 * 
 * @example
 * parseSetVariables("@SET TABLE = users\nSELECT * FROM ${TABLE}")
 * // Returns: { sql: "SELECT * FROM ${TABLE}", setValues: { TABLE: "users" } }
 */
export function parseSetVariables(sql: string): ParseSetResult {
    if (!sql) return { sql: '', setValues: {} };

    const lines = sql.split(/\r?\n/);
    const remaining: string[] = [];
    const setValues: Record<string, string> = {};

    for (const line of lines) {
        const m = line.match(/^\s*@SET\s+([A-Za-z0-9_]+)\s*=\s*(.+)$/i);
        if (m) {
            let val = m[2].trim();

            // Check if there's a semicolon followed by more SQL on the same line
            // e.g., "@SET A=1; SELECT ${A}" -> val="1", rest="SELECT ${A}"
            const semiIndex = val.indexOf(';');
            let restOfLine = '';
            if (semiIndex !== -1) {
                restOfLine = val.substring(semiIndex + 1).trim();
                val = val.substring(0, semiIndex).trim();
            }

            // Remove trailing semicolon from value if present (after split)
            if (val.endsWith(';')) val = val.slice(0, -1).trim();
            // Remove surrounding quotes if present
            const qm = val.match(/^'(.*)'$/s) || val.match(/^"(.*)"$/s);
            if (qm) val = qm[1];
            setValues[m[1]] = val;

            // If there was content after the semicolon, add it to remaining SQL
            if (restOfLine) {
                remaining.push(restOfLine);
            }
        } else {
            remaining.push(line);
        }
    }

    return { sql: remaining.join('\n'), setValues };
}


/**
 * Replace variable placeholders in SQL with provided values.
 * Supports two formats:
 * - ${VAR_NAME} - variable in curly braces
 * - $VAR_NAME - variable without braces
 * 
 * @param sql - The SQL string with placeholders
 * @param values - Record mapping variable names to their values
 * @returns SQL string with all placeholders replaced
 * 
 * @example
 * replaceVariablesInSql("SELECT * FROM ${TABLE} WHERE id = $ID", { TABLE: "users", ID: "42" })
 * // Returns: "SELECT * FROM users WHERE id = 42"
 */
export function replaceVariablesInSql(sql: string, values: Record<string, string>): string {
    // First replace ${VAR_NAME} format
    let result = sql.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (_: string, name: string) => {
        return values[name] ?? '';
    });

    // Then replace $VAR_NAME format (must start with letter or underscore)
    // Sort keys by length descending to replace longer names first (e.g., $TABLE_NAME before $TABLE)
    const sortedKeys = Object.keys(values).sort((a, b) => b.length - a.length);
    for (const key of sortedKeys) {
        // Only replace if key starts with letter or underscore
        if (/^[A-Za-z_]/.test(key)) {
            // Use word boundary to avoid partial replacements
            const regex = new RegExp(`\\$${key}(?![A-Za-z0-9_])`, 'g');
            result = result.replace(regex, values[key] ?? '');
        }
    }

    return result;
}

/**
 * Extract all unique variables from multiple SQL queries.
 * This is useful when you want to prompt for all variables once before executing multiple queries.
 * 
 * @param queries - Array of SQL query strings
 * @returns Set of all unique variable names found across all queries
 * 
 * @example
 * extractVariablesFromQueries(['SELECT $VAR1', 'SELECT ${VAR2}', 'SELECT $VAR1'])
 * // Returns: Set(['VAR1', 'VAR2'])
 */
export function extractVariablesFromQueries(queries: string[]): Set<string> {
    const allVars = new Set<string>();
    for (const query of queries) {
        const parsed = parseSetVariables(query);
        const vars = extractVariables(parsed.sql);
        vars.forEach(v => allVars.add(v));
    }
    return allVars;
}

/**
 * Full variable processing pipeline:
 * 1. Parse @SET definitions
 * 2. Extract ${VAR} placeholders
 * 3. Merge defaults with provided overrides
 * 4. Replace all placeholders
 * 
 * @param sql - The SQL string to process
 * @param overrides - Optional overrides for variable values
 * @returns Processed SQL with all variables resolved
 */
export function processVariables(
    sql: string,
    overrides?: Record<string, string>
): { processedSql: string; unresolvedVars: string[] } {
    // Step 1: Parse @SET definitions
    const parsed = parseSetVariables(sql);

    // Step 2: Extract variables from remaining SQL
    const vars = extractVariables(parsed.sql);

    // Step 3: Merge defaults with overrides
    const values: Record<string, string> = { ...parsed.setValues, ...overrides };

    // Step 4: Find unresolved variables
    const unresolvedVars = Array.from(vars).filter(v => values[v] === undefined);

    // Step 5: Replace all resolvable placeholders
    const processedSql = replaceVariablesInSql(parsed.sql, values);

    return { processedSql, unresolvedVars };
}
