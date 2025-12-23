/**
 * SQL Shortcuts - Auto-expand shortcuts like SX -> SELECT
 */

import * as vscode from 'vscode';

// SQL shortcuts mapping
const shortcuts = new Map<string, string>([
    ['SX', 'SELECT'],
    ['WX', 'WHERE'],
    ['GX', 'GROUP BY'],
    ['HX', 'HAVING'],
    ['OX', 'ORDER BY'],
    ['FX', 'FROM'],
    ['JX', 'JOIN'],
    ['LX', 'LIMIT'],
    ['IX', 'INSERT INTO'],
    ['UX', 'UPDATE'],
    ['DX', 'DELETE FROM'],
    ['CX', 'CREATE TABLE']
]);

/**
 * Register SQL shortcuts handler
 */
export function registerSqlShortcuts(context: vscode.ExtensionContext): void {
    const disposable = vscode.workspace.onDidChangeTextDocument(async event => {
        // Only process SQL files
        if (event.document.languageId !== 'sql' && event.document.languageId !== 'mssql') {
            return;
        }

        // Only process single character additions (typing)
        if (event.contentChanges.length !== 1) {
            return;
        }

        const change = event.contentChanges[0];

        // Check if user typed a space (trigger for shortcuts)
        if (change.text !== ' ') {
            return;
        }

        // Get the active editor
        const editor = vscode.window.activeTextEditor;
        if (!editor || editor.document !== event.document) {
            return;
        }

        // Get the line where the change occurred
        const line = event.document.lineAt(change.range.start.line);
        const lineText = line.text;

        // Check if any shortcut should be expanded
        for (const [trigger, replacement] of shortcuts) {
            const pattern = new RegExp(`\\b${trigger}\\s$`, 'i');

            if (pattern.test(lineText)) {
                // Found a shortcut to expand
                const triggerStart = lineText.toUpperCase().lastIndexOf(trigger.toUpperCase());
                if (triggerStart >= 0) {
                    // Calculate positions
                    const startPos = new vscode.Position(change.range.start.line, triggerStart);
                    const endPos = new vscode.Position(change.range.start.line, triggerStart + trigger.length + 1); // +1 for space

                    // Replace the shortcut + space with the full text + space
                    await editor.edit(editBuilder => {
                        editBuilder.replace(new vscode.Range(startPos, endPos), replacement + ' ');
                    });

                    // Trigger IntelliSense for SELECT, FROM, JOIN
                    if (['SELECT', 'FROM', 'JOIN'].includes(replacement)) {
                        setTimeout(() => {
                            vscode.commands.executeCommand('editor.action.triggerSuggest');
                        }, 100);
                    }

                    break; // Only process one shortcut at a time
                }
            }
        }
    });

    context.subscriptions.push(disposable);
}
