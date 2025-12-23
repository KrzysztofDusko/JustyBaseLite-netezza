/**
 * Decoration Manager - manages SQL highlighting and script decorations
 */

import * as vscode from 'vscode';
import { SqlParser } from '../sql/sqlParser';

// Regex for detecting Python script invocations
const scriptRegex = /(^|\s)(?:[A-Za-z]:\\|\\|\/)?[\w.\-\\/]+\.py\b|(^|\s)python(?:\.exe)?\s+[^\n]*\.py\b/i;

/**
 * CodeLens provider for Python script lines
 */
export class ScriptCodeLensProvider implements vscode.CodeLensProvider {
    private _onDidChange = new vscode.EventEmitter<void>();
    readonly onDidChangeCodeLenses = this._onDidChange.event;

    public provideCodeLenses(document: vscode.TextDocument): vscode.CodeLens[] {
        const lenses: vscode.CodeLens[] = [];
        for (let i = 0; i < document.lineCount; i++) {
            const line = document.lineAt(i);
            if (scriptRegex.test(line.text)) {
                const range = line.range;
                const cmd: vscode.Command = {
                    title: 'Run as script',
                    command: 'netezza.runScriptFromLens',
                    arguments: [document.uri, range]
                };
                lenses.push(new vscode.CodeLens(range, cmd));
            }
        }
        return lenses;
    }

    public refresh() {
        this._onDidChange.fire();
    }
}

/**
 * Create decoration type for script lines
 */
export function createScriptDecoration(): vscode.TextEditorDecorationType {
    return vscode.window.createTextEditorDecorationType({
        backgroundColor: new vscode.ThemeColor('editor.rangeHighlightBackground'),
        borderRadius: '3px'
    });
}

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
export function updateScriptDecorations(
    scriptDecoration: vscode.TextEditorDecorationType,
    editor?: vscode.TextEditor
): void {
    const active = editor || vscode.window.activeTextEditor;
    if (!active) return;

    const doc = active.document;
    const ranges: vscode.DecorationOptions[] = [];

    for (let i = 0; i < doc.lineCount; i++) {
        const line = doc.lineAt(i);
        if (scriptRegex.test(line.text)) {
            ranges.push({ range: line.range, hoverMessage: 'Python script invocation' });
        }
    }

    active.setDecorations(scriptDecoration, ranges);
}

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
    scriptDecoration: vscode.TextEditorDecorationType,
    sqlStatementDecoration: vscode.TextEditorDecorationType
): void {
    // Script decorations
    context.subscriptions.push(
        vscode.window.onDidChangeActiveTextEditor(() => updateScriptDecorations(scriptDecoration)),
        vscode.workspace.onDidChangeTextDocument(e => {
            if (vscode.window.activeTextEditor && e.document === vscode.window.activeTextEditor.document) {
                updateScriptDecorations(scriptDecoration, vscode.window.activeTextEditor);
            }
        })
    );

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

    // Initial updates
    updateScriptDecorations(scriptDecoration, vscode.window.activeTextEditor);
    updateSqlHighlight(sqlStatementDecoration, vscode.window.activeTextEditor);
}
