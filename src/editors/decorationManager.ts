/**
 * Decoration Manager - manages SQL highlighting and script decorations
 */

import * as vscode from 'vscode';
import { SqlParser } from '../sql/sqlParser';

/**
 * Create decoration type for SQL statement highlighting
 */
export function createSqlStatementDecoration(): vscode.TextEditorDecorationType {
    return vscode.window.createTextEditorDecorationType({
        backgroundColor: 'rgba(5, 115, 201, 0.10)',
        isWholeLine: false,
        rangeBehavior: vscode.DecorationRangeBehavior.ClosedClosed
    });
}

/**
 * Update script decorations for an editor
 */
// Script invocation decorations and CodeLens were removed intentionally.

/**
 * Update SQL statement highlighting based on cursor position
 */
export function updateSqlHighlight(
    sqlStatementDecoration: vscode.TextEditorDecorationType,
    editor: vscode.TextEditor | undefined
): void {
    const config = vscode.workspace.getConfiguration('netezza');
    const enabled = config.get<boolean>('highlightActiveStatement', true);

    if (!enabled || !editor || (editor.document.languageId !== 'sql' && editor.document.languageId !== 'mssql')) {
        if (editor) {
            editor.setDecorations(sqlStatementDecoration, []);
        }
        return;
    }

    try {
        const document = editor.document;
        const position = editor.selection.active;
        const offset = document.offsetAt(position);
        const text = document.getText();

        const stmt = SqlParser.getStatementAtPosition(text, offset);

        if (stmt) {
            const startPos = document.positionAt(stmt.start);
            const endPos = document.positionAt(stmt.end);
            const range = new vscode.Range(startPos, endPos);
            editor.setDecorations(sqlStatementDecoration, [range]);
        } else {
            editor.setDecorations(sqlStatementDecoration, []);
        }
    } catch (e) {
        console.error('Error updating SQL highlight:', e);
    }
}

/**
 * Register all decoration-related subscriptions
 */
export function registerDecorationSubscriptions(
    context: vscode.ExtensionContext,
    sqlStatementDecoration: vscode.TextEditorDecorationType
): void {
    // SQL statement highlighting
    context.subscriptions.push(
        vscode.window.onDidChangeTextEditorSelection(e => {
            updateSqlHighlight(sqlStatementDecoration, e.textEditor);
        }),
        vscode.window.onDidChangeActiveTextEditor(e => {
            updateSqlHighlight(sqlStatementDecoration, e);
        }),
        vscode.workspace.onDidChangeConfiguration(e => {
            if (e.affectsConfiguration('netezza.highlightActiveStatement')) {
                updateSqlHighlight(sqlStatementDecoration, vscode.window.activeTextEditor);
            }
        })
    );

    // Initial update for SQL highlighting
    updateSqlHighlight(sqlStatementDecoration, vscode.window.activeTextEditor);
}
