/**
 * SQL Linter Provider for Netezza
 * 
 * Provides real-time SQL linting integrated with VS Code's diagnostics system.
 */

import * as vscode from 'vscode';
import { allRules, LintIssue, parseSeverity, RuleSeverityConfig } from './linterRules';

/**
 * SQL Linter Provider
 * Manages VS Code diagnostics for SQL files
 */
export class SqlLinterProvider {
    private diagnosticCollection: vscode.DiagnosticCollection;
    private disposables: vscode.Disposable[] = [];
    private lintTimers: Map<string, NodeJS.Timeout> = new Map();
    private readonly lintDebounceMs = 400;

    constructor() {
        this.diagnosticCollection = vscode.languages.createDiagnosticCollection('netezza-sql-linter');
    }

    /**
     * Activate the linter
     */
    public activate(context: vscode.ExtensionContext): void {
        // Add diagnostic collection to disposables
        context.subscriptions.push(this.diagnosticCollection);

        // Lint on document open
        this.disposables.push(
            vscode.workspace.onDidOpenTextDocument(doc => {
                if (this.shouldLint(doc)) {
                    this.lintDocument(doc);
                }
            })
        );

        // Lint on document change
        this.disposables.push(
            vscode.workspace.onDidChangeTextDocument(event => {
                if (this.shouldLint(event.document)) {
                    this.scheduleLint(event.document);
                }
            })
        );

        // Clear diagnostics when document is closed
        this.disposables.push(
            vscode.workspace.onDidCloseTextDocument(doc => {
                this.diagnosticCollection.delete(doc.uri);
            })
        );

        // Lint on configuration change
        this.disposables.push(
            vscode.workspace.onDidChangeConfiguration(event => {
                if (event.affectsConfiguration('netezza.linter')) {
                    this.lintAllOpenDocuments();
                }
            })
        );

        // Register disposables
        context.subscriptions.push(...this.disposables);

        // Lint all currently open SQL documents
        this.lintAllOpenDocuments();
    }

    /**
     * Check if a document should be linted
     */
    private shouldLint(document: vscode.TextDocument): boolean {
        return document.languageId === 'sql';
    }

    /**
     * Get linter configuration
     */
    private getConfig(): { enabled: boolean; rules: Record<string, RuleSeverityConfig> } {
        const config = vscode.workspace.getConfiguration('netezza.linter');
        return {
            enabled: config.get<boolean>('enabled', true),
            rules: config.get<Record<string, RuleSeverityConfig>>('rules', {})
        };
    }

    /**
     * Lint all open SQL documents
     */
    private lintAllOpenDocuments(): void {
        for (const doc of vscode.workspace.textDocuments) {
            if (this.shouldLint(doc)) {
                this.lintDocument(doc);
            }
        }
    }

    /**
     * Lint a document and update diagnostics
     */
    public lintDocument(document: vscode.TextDocument): void {
        const config = this.getConfig();

        // If linter is disabled, clear diagnostics
        if (!config.enabled) {
            this.diagnosticCollection.delete(document.uri);
            return;
        }

        const sql = document.getText();
        const issues = this.lintSql(sql, config.rules);
        const diagnostics = this.issuesToDiagnostics(document, issues);

        this.diagnosticCollection.set(document.uri, diagnostics);
    }

    private scheduleLint(document: vscode.TextDocument): void {
        const key = document.uri.toString();
        const existing = this.lintTimers.get(key);
        if (existing) {
            clearTimeout(existing);
        }

        const timer = setTimeout(() => {
            this.lintTimers.delete(key);
            this.lintDocument(document);
        }, this.lintDebounceMs);

        this.lintTimers.set(key, timer);
    }

    /**
     * Lint SQL text and return issues
     * This is a pure function that can be used for testing
     */
    public lintSql(sql: string, rulesConfig: Record<string, RuleSeverityConfig> = {}): LintIssue[] {
        const issues: LintIssue[] = [];

        for (const rule of allRules) {
            // Get configured severity or use default
            const configuredSeverity = rulesConfig[rule.id];
            const severity = configuredSeverity !== undefined
                ? parseSeverity(configuredSeverity)
                : rule.defaultSeverity;

            // Skip if rule is disabled
            if (severity === null) {
                continue;
            }

            // Run the rule
            const ruleIssues = rule.check(sql);

            // Override severity based on configuration
            for (const issue of ruleIssues) {
                issues.push({
                    ...issue,
                    severity
                });
            }
        }

        return issues;
    }

    /**
     * Convert lint issues to VS Code diagnostics
     */
    private issuesToDiagnostics(document: vscode.TextDocument, issues: LintIssue[]): vscode.Diagnostic[] {
        return issues.map(issue => {
            const startPos = document.positionAt(issue.startOffset);
            const endPos = document.positionAt(issue.endOffset);
            const range = new vscode.Range(startPos, endPos);

            const diagnostic = new vscode.Diagnostic(range, issue.message, issue.severity);
            diagnostic.source = 'Netezza SQL Linter';
            diagnostic.code = issue.ruleId;

            return diagnostic;
        });
    }

    /**
     * Dispose of the linter
     */
    public dispose(): void {
        this.diagnosticCollection.dispose();
        for (const timer of this.lintTimers.values()) {
            clearTimeout(timer);
        }
        this.lintTimers.clear();
        for (const disposable of this.disposables) {
            disposable.dispose();
        }
    }
}

/**
 * Singleton instance
 */
let linterInstance: SqlLinterProvider | undefined;

/**
 * Get or create the linter instance
 */
export function getSqlLinter(): SqlLinterProvider {
    if (!linterInstance) {
        linterInstance = new SqlLinterProvider();
    }
    return linterInstance;
}

/**
 * Activate the SQL linter
 */
export function activateSqlLinter(context: vscode.ExtensionContext): SqlLinterProvider {
    const linter = getSqlLinter();
    linter.activate(context);
    return linter;
}
