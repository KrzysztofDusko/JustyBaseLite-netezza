/**
 * Schema Commands - Copy/Clipboard Operations
 * Commands: copySelectAll, copyDrop, copyName
 */

import * as vscode from 'vscode';
import { runQuery } from '../../core/queryRunner';
import { SchemaCommandsDependencies, SchemaItemData } from './types';
import { getFullName, requireConnection, executeWithProgress } from './helpers';

/**
 * Register copy-related commands
 */
export function registerCopyCommands(deps: SchemaCommandsDependencies): vscode.Disposable[] {
    const { context, connectionManager, schemaProvider } = deps;

    return [
        // Copy Select All
        vscode.commands.registerCommand('netezza.copySelectAll', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema) {
                const sql = `SELECT * FROM ${item.dbName}.${item.schema}.${item.label} LIMIT 1000;`;

                const action = await vscode.window.showQuickPick(
                    [
                        { label: 'Open in Editor', description: 'Open SQL in a new editor', value: 'editor' },
                        { label: 'Copy to Clipboard', description: 'Copy SQL to clipboard', value: 'clipboard' }
                    ],
                    {
                        placeHolder: 'How would you like to access the SQL?'
                    }
                );

                if (action) {
                    if (action.value === 'editor') {
                        const doc = await vscode.workspace.openTextDocument({
                            content: sql,
                            language: 'sql'
                        });
                        await vscode.window.showTextDocument(doc);
                    } else {
                        await vscode.env.clipboard.writeText(sql);
                        vscode.window.showInformationMessage('Copied to clipboard');
                    }
                }
            }
        }),

        // Copy Drop
        vscode.commands.registerCommand('netezza.copyDrop', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema && item.objType) {
                const fullName = getFullName(item);
                const sql = `DROP ${item.objType} ${fullName};`;

                const confirmation = await vscode.window.showWarningMessage(
                    `Are you sure you want to delete ${item.objType.toLowerCase()} "${fullName}"?`,
                    { modal: true },
                    'Yes, delete',
                    'Cancel'
                );

                if (confirmation === 'Yes, delete') {
                    const connectionString = await requireConnection(connectionManager);
                    if (!connectionString) return;

                    try {
                        await executeWithProgress(
                            `Deleting ${item.objType.toLowerCase()} ${fullName}...`,
                            async () => {
                                await runQuery(context, sql, true, item.connectionName, connectionManager);
                            }
                        );

                        vscode.window.showInformationMessage(`Deleted ${item.objType.toLowerCase()}: ${fullName}`);
                        schemaProvider.refresh();
                    } catch (err: unknown) {
                        const message = err instanceof Error ? err.message : String(err);
                        vscode.window.showErrorMessage(`Error during deletion: ${message}`);
                    }
                }
            }
        }),

        // Copy Name
        vscode.commands.registerCommand('netezza.copyName', (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema) {
                const name = getFullName(item);
                vscode.env.clipboard.writeText(name);
                vscode.window.showInformationMessage('Copied to clipboard');
            }
        })
    ];
}
