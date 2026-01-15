/**
 * SQL Linter Rules for Netezza
 * 
 * Modular, configurable lint rules for SQL analysis.
 * Each rule is a pure function that can be tested independently.
 */

import * as vscode from 'vscode';

/**
 * Represents a lint issue found in SQL code
 */
export interface LintIssue {
    ruleId: string;
    message: string;
    severity: vscode.DiagnosticSeverity;
    startOffset: number;
    endOffset: number;
}

/**
 * Lint rule definition
 */
export interface LintRule {
    id: string;
    name: string;
    description: string;
    defaultSeverity: vscode.DiagnosticSeverity;
    check(sql: string): LintIssue[];
}

/**
 * Rule severity configuration from user settings
 */
export type RuleSeverityConfig = 'error' | 'warning' | 'information' | 'hint' | 'off';

/**
 * Convert string severity to VS Code DiagnosticSeverity
 */
export function parseSeverity(severity: RuleSeverityConfig): vscode.DiagnosticSeverity | null {
    switch (severity) {
        case 'error': return vscode.DiagnosticSeverity.Error;
        case 'warning': return vscode.DiagnosticSeverity.Warning;
        case 'information': return vscode.DiagnosticSeverity.Information;
        case 'hint': return vscode.DiagnosticSeverity.Hint;
        case 'off': return null;
        default: return vscode.DiagnosticSeverity.Warning;
    }
}

/**
 * Check if a position is inside a string literal or comment
 */
function isInsideStringOrComment(sql: string, position: number): boolean {
    let inSingleQuote = false;
    let inDoubleQuote = false;
    let inLineComment = false;
    let inBlockComment = false;

    for (let i = 0; i < position && i < sql.length; i++) {
        const char = sql[i];
        const nextChar = i + 1 < sql.length ? sql[i + 1] : '';

        if (inLineComment) {
            if (char === '\n') inLineComment = false;
        } else if (inBlockComment) {
            if (char === '*' && nextChar === '/') {
                inBlockComment = false;
                i++; // Skip the '/'
            }
        } else if (inSingleQuote) {
            if (char === "'") inSingleQuote = false;
        } else if (inDoubleQuote) {
            if (char === '"') inDoubleQuote = false;
        } else {
            if (char === '-' && nextChar === '-') {
                inLineComment = true;
            } else if (char === '/' && nextChar === '*') {
                inBlockComment = true;
            } else if (char === "'") {
                inSingleQuote = true;
            } else if (char === '"') {
                inDoubleQuote = true;
            }
        }
    }

    return inSingleQuote || inDoubleQuote || inLineComment || inBlockComment;
}

/**
 * Find all matches of a pattern, excluding those inside strings/comments
 */
function findPatternMatches(sql: string, pattern: RegExp): RegExpExecArray[] {
    const matches: RegExpExecArray[] = [];
    let match;
    const regex = new RegExp(pattern.source, pattern.flags.includes('g') ? pattern.flags : pattern.flags + 'g');

    while ((match = regex.exec(sql)) !== null) {
        if (!isInsideStringOrComment(sql, match.index)) {
            matches.push(match);
        }
    }

    return matches;
}

// ============================================================================
// LINT RULES
// ============================================================================

/**
 * NZ001: SELECT * usage
 */
export const ruleNZ001: LintRule = {
    id: 'NZ001',
    name: 'Select Star',
    description: 'Avoid using SELECT * - specify explicit column names for better performance and maintainability',
    defaultSeverity: vscode.DiagnosticSeverity.Warning,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        const pattern = /\bSELECT\s+\*/gi;
        const matches = findPatternMatches(sql, pattern);

        for (const match of matches) {
            // Find the position of * within the match
            const starPos = match[0].indexOf('*');
            issues.push({
                ruleId: this.id,
                message: `${this.id}: ${this.description}`,
                severity: this.defaultSeverity,
                startOffset: match.index + starPos,
                endOffset: match.index + starPos + 1
            });
        }

        return issues;
    }
};

/**
 * NZ002: DELETE without WHERE
 */
export const ruleNZ002: LintRule = {
    id: 'NZ002',
    name: 'Delete Without Where',
    description: 'DELETE statement without WHERE clause will delete all rows',
    defaultSeverity: vscode.DiagnosticSeverity.Error,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        // Match DELETE FROM ... but check if WHERE follows before the next statement or end
        const pattern = /\bDELETE\s+FROM\s+[\w."]+/gi;
        const matches = findPatternMatches(sql, pattern);

        for (const match of matches) {
            const afterDelete = sql.substring(match.index + match[0].length);
            // Check if WHERE appears before ; or end of statement
            const nextSemicolon = afterDelete.indexOf(';');
            const textToCheck = nextSemicolon >= 0 ? afterDelete.substring(0, nextSemicolon) : afterDelete;

            if (!/\bWHERE\b/i.test(textToCheck)) {
                issues.push({
                    ruleId: this.id,
                    message: `${this.id}: ${this.description}`,
                    severity: this.defaultSeverity,
                    startOffset: match.index,
                    endOffset: match.index + 6 // Just highlight "DELETE"
                });
            }
        }

        return issues;
    }
};

/**
 * NZ003: UPDATE without WHERE
 */
export const ruleNZ003: LintRule = {
    id: 'NZ003',
    name: 'Update Without Where',
    description: 'UPDATE statement without WHERE clause will update all rows',
    defaultSeverity: vscode.DiagnosticSeverity.Error,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        const pattern = /\bUPDATE\s+[\w."]+\s+SET\b/gi;
        const matches = findPatternMatches(sql, pattern);

        for (const match of matches) {
            const afterUpdate = sql.substring(match.index + match[0].length);
            const nextSemicolon = afterUpdate.indexOf(';');
            const textToCheck = nextSemicolon >= 0 ? afterUpdate.substring(0, nextSemicolon) : afterUpdate;

            if (!/\bWHERE\b/i.test(textToCheck)) {
                issues.push({
                    ruleId: this.id,
                    message: `${this.id}: ${this.description}`,
                    severity: this.defaultSeverity,
                    startOffset: match.index,
                    endOffset: match.index + 6 // Just highlight "UPDATE"
                });
            }
        }

        return issues;
    }
};

/**
 * NZ004: CROSS JOIN detected
 */
export const ruleNZ004: LintRule = {
    id: 'NZ004',
    name: 'Cross Join',
    description: 'CROSS JOIN produces a Cartesian product - verify this is intentional',
    defaultSeverity: vscode.DiagnosticSeverity.Warning,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        const pattern = /\bCROSS\s+JOIN\b/gi;
        const matches = findPatternMatches(sql, pattern);

        for (const match of matches) {
            issues.push({
                ruleId: this.id,
                message: `${this.id}: ${this.description}`,
                severity: this.defaultSeverity,
                startOffset: match.index,
                endOffset: match.index + match[0].length
            });
        }

        return issues;
    }
};

/**
 * NZ005: Leading wildcard in LIKE
 */
export const ruleNZ005: LintRule = {
    id: 'NZ005',
    name: 'Leading Wildcard Like',
    description: "LIKE pattern with leading wildcard ('%...') prevents index usage",
    defaultSeverity: vscode.DiagnosticSeverity.Hint,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        // Match LIKE '%something' or LIKE '%'
        const pattern = /\bLIKE\s+'%/gi;
        const matches = findPatternMatches(sql, pattern);

        for (const match of matches) {
            issues.push({
                ruleId: this.id,
                message: `${this.id}: ${this.description}`,
                severity: this.defaultSeverity,
                startOffset: match.index,
                endOffset: match.index + match[0].length
            });
        }

        return issues;
    }
};

/**
 * NZ006: ORDER BY without LIMIT
 */
export const ruleNZ006: LintRule = {
    id: 'NZ006',
    name: 'Order By Without Limit',
    description: 'ORDER BY without LIMIT/FETCH may cause performance issues on large datasets',
    defaultSeverity: vscode.DiagnosticSeverity.Information,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        const pattern = /\bORDER\s+BY\b/gi;
        const matches = findPatternMatches(sql, pattern);

        for (const match of matches) {
            const afterOrderBy = sql.substring(match.index + match[0].length);
            const nextSemicolon = afterOrderBy.indexOf(';');
            const textToCheck = nextSemicolon >= 0 ? afterOrderBy.substring(0, nextSemicolon) : afterOrderBy;

            // Check for LIMIT, FETCH, or TOP
            if (!/\b(LIMIT|FETCH|TOP)\b/i.test(textToCheck)) {
                // Also check before ORDER BY for TOP
                const beforeOrderBy = sql.substring(0, match.index);
                if (!/\bTOP\s+\d+\b/i.test(beforeOrderBy)) {
                    issues.push({
                        ruleId: this.id,
                        message: `${this.id}: ${this.description}`,
                        severity: this.defaultSeverity,
                        startOffset: match.index,
                        endOffset: match.index + match[0].length
                    });
                }
            }
        }

        return issues;
    }
};

/**
 * NZ007: Inconsistent keyword casing
 */
export const ruleNZ007: LintRule = {
    id: 'NZ007',
    name: 'Inconsistent Keyword Case',
    description: 'SQL keywords have inconsistent casing - consider using consistent UPPER or lower case',
    defaultSeverity: vscode.DiagnosticSeverity.Warning,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        const keywords = ['SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
            'ON', 'AND', 'OR', 'INSERT', 'INTO', 'UPDATE', 'DELETE', 'CREATE',
            'DROP', 'ALTER', 'TABLE', 'VIEW', 'INDEX', 'ORDER', 'BY', 'GROUP',
            'HAVING', 'UNION', 'ALL', 'DISTINCT', 'AS', 'SET', 'VALUES', 'NULL',
            'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'EXISTS', 'CASE', 'WHEN',
            'THEN', 'ELSE', 'END', 'LIMIT', 'OFFSET'];

        let upperCount = 0;
        let lowerCount = 0;
        const foundKeywords: { keyword: string; index: number; type: 'UPPER' | 'lower' | 'Mixed' }[] = [];

        // Helper to avoid double counting if patterns overlap (though keywords usually don't)
        // Set of start indices processed
        const processedIndices = new Set<number>();

        for (const keyword of keywords) {
            // Use case-insensitive global match
            const pattern = new RegExp(`\\b${keyword}\\b`, 'gi');
            const matches = findPatternMatches(sql, pattern);

            for (const match of matches) {
                // Ensure we don't process same location twice (unlikely with this keyword list but good for safety)
                if (processedIndices.has(match.index)) continue;
                processedIndices.add(match.index);

                const text = match[0];
                let type: 'UPPER' | 'lower' | 'Mixed';

                if (text === text.toUpperCase()) {
                    upperCount++;
                    type = 'UPPER';
                } else if (text === text.toLowerCase()) {
                    lowerCount++;
                    type = 'lower';
                } else {
                    type = 'Mixed';
                }

                foundKeywords.push({ keyword: text, index: match.index, type });
            }
        }

        // Determine dominant style
        // If count is equal, prefer UPPER as it's standard SQL convention
        const dominantIsUpper = upperCount >= lowerCount;
        const targetType = dominantIsUpper ? 'UPPER' : 'lower';

        // Check consistency
        for (const item of foundKeywords) {
            if (item.type === 'Mixed') {
                issues.push({
                    ruleId: this.id,
                    message: `${this.id}: Keyword '${item.keyword}' has mixed casing (expected ${dominantIsUpper ? 'UPPERCASE' : 'lowercase'})`,
                    severity: this.defaultSeverity,
                    startOffset: item.index,
                    endOffset: item.index + item.keyword.length
                });
            } else if (item.type !== targetType) {
                // Only report if it deviates from the dominant style derived from non-mixed keywords
                // If we have 0 upper and 0 lower (only mixed), we default to UPPER, so mixed will be reported above
                // If we have legitimate different casing, report it here
                issues.push({
                    ruleId: this.id,
                    message: `${this.id}: Keyword '${item.keyword}' should be ${dominantIsUpper ? 'UPPERCASE' : 'lowercase'}`,
                    severity: this.defaultSeverity,
                    startOffset: item.index,
                    endOffset: item.index + item.keyword.length
                });
            }
        }

        return issues;
    }
};

/**
 * NZ008: TRUNCATE statement
 */
export const ruleNZ008: LintRule = {
    id: 'NZ008',
    name: 'Truncate Table',
    description: 'TRUNCATE removes all data and cannot be rolled back - use with caution',
    defaultSeverity: vscode.DiagnosticSeverity.Warning,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        const pattern = /\bTRUNCATE\s+(TABLE\s+)?[\w."]+/gi;
        const matches = findPatternMatches(sql, pattern);

        for (const match of matches) {
            issues.push({
                ruleId: this.id,
                message: `${this.id}: ${this.description}`,
                severity: this.defaultSeverity,
                startOffset: match.index,
                endOffset: match.index + 8 // Just highlight "TRUNCATE"
            });
        }

        return issues;
    }
};

/**
 * NZ009: OR in WHERE clause
 */
export const ruleNZ009: LintRule = {
    id: 'NZ009',
    name: 'Or In Where Clause',
    description: 'Multiple OR conditions may prevent index usage - consider UNION for better performance',
    defaultSeverity: vscode.DiagnosticSeverity.Hint,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        // Look for WHERE ... OR patterns
        const wherePattern = /\bWHERE\b/gi;
        const whereMatches = findPatternMatches(sql, wherePattern);

        for (const whereMatch of whereMatches) {
            const afterWhere = sql.substring(whereMatch.index);
            const nextClause = afterWhere.search(/\b(GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|UNION|;)/i);
            const whereClause = nextClause >= 0 ? afterWhere.substring(0, nextClause) : afterWhere;

            // Count OR occurrences
            const orMatches = findPatternMatches(whereClause, /\bOR\b/gi);
            if (orMatches.length >= 2) {
                // Report the first OR
                const firstOr = orMatches[0];
                issues.push({
                    ruleId: this.id,
                    message: `${this.id}: ${this.description} (${orMatches.length} OR conditions found)`,
                    severity: this.defaultSeverity,
                    startOffset: whereMatch.index + firstOr.index,
                    endOffset: whereMatch.index + firstOr.index + 2
                });
            }
        }

        return issues;
    }
};

/**
 * NZ010: Missing table alias in JOIN
 */
export const ruleNZ010: LintRule = {
    id: 'NZ010',
    name: 'Missing Table Alias',
    description: 'Consider using table aliases in JOINs for better readability',
    defaultSeverity: vscode.DiagnosticSeverity.Information,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        // Look for JOIN table_name followed directly by ON (no alias)
        const pattern = /\bJOIN\s+([\w."]+)\s+ON\b/gi;
        const matches = findPatternMatches(sql, pattern);

        for (const match of matches) {
            issues.push({
                ruleId: this.id,
                message: `${this.id}: Table '${match[1]}' in JOIN has no alias - ${this.description}`,
                severity: this.defaultSeverity,
                startOffset: match.index,
                endOffset: match.index + match[0].length
            });
        }

        return issues;
    }
};

/**
 * NZ011: CTAS missing DISTRIBUTE ON
 */
export const ruleNZ011: LintRule = {
    id: 'NZ011',
    name: 'CTAS Missing Distribution',
    description: 'CREATE TABLE AS SELECT should specify explicit data distribution',
    defaultSeverity: vscode.DiagnosticSeverity.Warning,
    check(sql: string): LintIssue[] {
        const issues: LintIssue[] = [];
        // Match CREATE TABLE [IF NOT EXISTS] table_name AS [ ( ] SELECT
        // pattern covers:
        // CREATE TABLE t AS SELECT
        // CREATE TABLE t AS (SELECT
        // CREATE TABLE IF NOT EXISTS t AS SELECT
        const pattern = /\bCREATE\s+TABLE\s+(?:(?:IF\s+NOT\s+EXISTS\s+)?[\w."]+\s+)?AS\s+(?:\(\s*)?SELECT\b/gi;
        const matches = findPatternMatches(sql, pattern);

        for (const match of matches) {
            // Find the end of this statement (next ; or EOF) respecting quotes/comments
            let endIndex = sql.length;
            for (let i = match.index; i < sql.length; i++) {
                if (sql[i] === ';' && !isInsideStringOrComment(sql, i)) {
                    endIndex = i;
                    break;
                }
            }

            const statementContent = sql.substring(match.index, endIndex);

            // Check for DISTRIBUTE ON in this statement
            // We reuse findPatternMatches on the substring to safely ignore comments inside
            // But findPatternMatches expects global position or strict string. 
            // Let's just Regex test the substring, false negatives in comments are rare for keywords
            // but for correctness let's verify match isn't in comment.

            const distributePattern = /\bDISTRIBUTE\s+ON\b/i;
            const distMatch = distributePattern.exec(statementContent);

            let hasDistribute = false;
            if (distMatch) {
                // Verify the match isn't inside a comment relative to the original SQL
                if (!isInsideStringOrComment(sql, match.index + distMatch.index)) {
                    hasDistribute = true;
                }
            }

            if (!hasDistribute) {
                issues.push({
                    ruleId: this.id,
                    message: `${this.id}: ${this.description} - Add 'DISTRIBUTE ON (...)' or 'DISTRIBUTE ON RANDOM'`,
                    severity: this.defaultSeverity,
                    startOffset: match.index,
                    endOffset: match.index + match[0].length
                });
            }
        }
        return issues;
    }
};

/**
 * All available lint rules
 */
export const allRules: LintRule[] = [
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
    ruleNZ011
];

/**
 * Get a rule by its ID
 */
export function getRuleById(id: string): LintRule | undefined {
    return allRules.find(rule => rule.id === id);
}
