/**
 * Code Actions for Netezza SQL Linter
 * 
 * Provides Quick Fixes for lint issues found by SqlLinterProvider.
 */

import * as vscode from 'vscode';
import { SqlParser } from '../sql/sqlParser';

export class NetezzaLinterCodeActionProvider implements vscode.CodeActionProvider {
    public static readonly providedCodeActionKinds = [
        vscode.CodeActionKind.QuickFix
    ];

    public provideCodeActions(
        document: vscode.TextDocument,
        _range: vscode.Range | vscode.Selection,
        context: vscode.CodeActionContext,
        _token: vscode.CancellationToken
    ): vscode.CodeAction[] {
        const actions: vscode.CodeAction[] = [];

        for (const diagnostic of context.diagnostics) {
            if (diagnostic.code === 'NZ011') {
                const action = this.createDistributeOnRandomFix(document, diagnostic);
                if (action) {
                    actions.push(action);
                }
            }
        }

        return actions;
    }

    /**
     * Creates a Quick Fix to add "DISTRIBUTE ON RANDOM" to a CTAS statement
     */
    private createDistributeOnRandomFix(document: vscode.TextDocument, diagnostic: vscode.Diagnostic): vscode.CodeAction | undefined {
        const action = new vscode.CodeAction('Add DISTRIBUTE ON RANDOM', vscode.CodeActionKind.QuickFix);
        action.diagnostics = [diagnostic];
        action.isPreferred = true;

        // Find where the statement ends to insert the clause
        const offset = document.offsetAt(diagnostic.range.start);
        const text = document.getText();

        // Use SqlParser to find the statement boundaries
        const statement = SqlParser.getStatementAtPosition(text, offset);

        if (!statement) return undefined;

        // Logic: SqlParser returns 'end' as the index of the semicolon (or EOF)
        // So inserting at 'end' positions it correctly before the semicolon.
        // We ensure there is a space before.

        action.edit = new vscode.WorkspaceEdit();
        action.edit.insert(document.uri, document.positionAt(statement.end), ' DISTRIBUTE ON RANDOM');

        return action;
    }
}
