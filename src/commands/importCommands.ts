/**
 * Import Commands - commands for importing data to Netezza
 */

import * as vscode from 'vscode';
import * as path from 'path';
import { ConnectionManager } from '../core/connectionManager';
import { runQuery } from '../core/queryRunner';

export interface ImportCommandsDependencies {
    context: vscode.ExtensionContext;
    connectionManager: ConnectionManager;
    outputChannel: vscode.OutputChannel;
}

/**
 * Helper to log execution time
 */
function logExecutionTime(outputChannel: vscode.OutputChannel, operation: string, startTime: number): void {
    const duration = Date.now() - startTime;
    outputChannel.appendLine(`[${new Date().toLocaleTimeString()}] ${operation} completed in ${duration}ms`);
    outputChannel.show(true);
}

/**
 * Generate auto table name from current database and schema
 */
async function generateAutoTableName(
    context: vscode.ExtensionContext,
    connectionName: string | undefined,
    connectionManager: ConnectionManager
): Promise<string | null> {
    try {
        const currentDbQuery = 'SELECT CURRENT_CATALOG, CURRENT_SCHEMA';
        const currentDbResult = await runQuery(context, currentDbQuery, true, connectionName, connectionManager);

        if (currentDbResult) {
            const dbInfo = JSON.parse(currentDbResult);
            if (dbInfo && dbInfo.length > 0) {
                const database = dbInfo[0].CURRENT_CATALOG || 'SYSTEM';
                const schema = dbInfo[0].CURRENT_SCHEMA || 'ADMIN';

                const now = new Date();
                const dateStr = now.toISOString().slice(0, 10).replace(/-/g, '');
                const random = Math.floor(Math.random() * 10000)
                    .toString()
                    .padStart(4, '0');

                return `${database}.${schema}.IMPORT_${dateStr}_${random}`;
            }
        }
    } catch (err: any) {
        vscode.window.showErrorMessage(`Error getting current database/schema: ${err.message}`);
    }
    return null;
}

/**
 * Register all import-related commands
 */
export function registerImportCommands(deps: ImportCommandsDependencies): vscode.Disposable[] {
    const { context, connectionManager, outputChannel } = deps;

    return [
        // Import Data from Clipboard
        vscode.commands.registerCommand('netezza.importClipboard', async () => {
            try {
                const editor = vscode.window.activeTextEditor;
                const documentUri = editor?.document?.uri?.toString();
                const connectionName = connectionManager.getConnectionForExecution(documentUri);
                const connectionString = await connectionManager.getConnectionString(connectionName);
                if (!connectionString) {
                    throw new Error('Connection not configured. Please connect via Netezza: Connect...');
                }

                const targetTable = await vscode.window.showInputBox({
                    prompt: 'Enter target table name (leave empty for auto-generated name)',
                    placeHolder: 'e.g. my_schema.my_table or leave empty',
                    validateInput: value => {
                        if (!value || value.trim().length === 0) {
                            return null;
                        }
                        if (
                            !/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(
                                value.trim()
                            )
                        ) {
                            return 'Invalid table name format. Use: [database.]schema.table';
                        }
                        return null;
                    }
                });

                if (targetTable === undefined) return;

                let finalTableName: string;
                if (!targetTable || targetTable.trim().length === 0) {
                    const autoName = await generateAutoTableName(context, connectionName, connectionManager);
                    if (!autoName) return;
                    finalTableName = autoName;
                    vscode.window.showInformationMessage(`Auto-generated table name: ${finalTableName}`);
                } else {
                    finalTableName = targetTable.trim();
                }

                const formatOptions = await vscode.window.showQuickPick(
                    [
                        {
                            label: 'Auto-detect',
                            description: 'Automatically detect clipboard format (text or Excel XML)',
                            value: null
                        },
                        {
                            label: 'Excel XML Spreadsheet',
                            description: 'Force Excel XML format processing',
                            value: 'XML Spreadsheet'
                        },
                        {
                            label: 'Plain Text',
                            description: 'Force plain text processing with delimiter detection',
                            value: 'TEXT'
                        }
                    ],
                    {
                        placeHolder: 'Select clipboard data format'
                    }
                );

                if (!formatOptions) return;

                const startTime = Date.now();

                await vscode.window.withProgress(
                    {
                        location: vscode.ProgressLocation.Notification,
                        title: 'Importing clipboard data...',
                        cancellable: false
                    },
                    async progress => {
                        const { importClipboardDataToNetezza } = await import('../import/clipboardImporter');

                        const result = await importClipboardDataToNetezza(
                            finalTableName,
                            connectionString,
                            formatOptions.value,
                            {},
                            (message: string) => {
                                progress.report({ message });
                                outputChannel.appendLine(`[Clipboard Import] ${message}`);
                            }
                        );

                        if (!result.success) {
                            throw new Error(result.message);
                        }

                        if (result.details) {
                            outputChannel.appendLine(
                                `[Clipboard Import] Rows processed: ${result.details.rowsProcessed}`
                            );
                            outputChannel.appendLine(`[Clipboard Import] Columns: ${result.details.columns}`);
                            outputChannel.appendLine(`[Clipboard Import] Format: ${result.details.format}`);
                        }
                    }
                );

                logExecutionTime(outputChannel, 'Import Clipboard Data', startTime);
                vscode.window.showInformationMessage(
                    `Clipboard data imported successfully to table: ${finalTableName}`
                );
            } catch (err: any) {
                vscode.window.showErrorMessage(`Error importing clipboard data: ${err.message}`);
            }
        }),

        // Import Data from File
        vscode.commands.registerCommand('netezza.importData', async () => {
            try {
                const editor = vscode.window.activeTextEditor;
                const documentUri = editor?.document?.uri?.toString();
                const connectionName = connectionManager.getConnectionForExecution(documentUri);
                const connectionString = await connectionManager.getConnectionString(connectionName);
                if (!connectionString) {
                    throw new Error('Connection not configured. Please connect via Netezza: Connect...');
                }

                const fileUris = await vscode.window.showOpenDialog({
                    canSelectFiles: true,
                    canSelectFolders: false,
                    canSelectMany: false,
                    filters: {
                        'Data Files': ['csv', 'txt', 'xlsx', 'xlsb', 'json'],
                        'CSV Files': ['csv'],
                        'Excel Files': ['xlsx', 'xlsb'],
                        'Text Files': ['txt'],
                        'JSON Files': ['json'],
                        'All Files': ['*']
                    },
                    openLabel: 'Select file to import'
                });

                if (!fileUris || fileUris.length === 0) return;

                const sourceFile = fileUris[0].fsPath;

                const targetTable = await vscode.window.showInputBox({
                    prompt: 'Enter target table name (leave empty for auto-generated name)',
                    placeHolder: 'e.g. my_schema.my_table or leave empty',
                    validateInput: value => {
                        if (!value || value.trim().length === 0) {
                            return null;
                        }
                        if (
                            !/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(
                                value.trim()
                            )
                        ) {
                            return 'Invalid table name format. Use: [database.]schema.table';
                        }
                        return null;
                    }
                });

                if (targetTable === undefined) return;

                let finalTableName: string;
                if (!targetTable || targetTable.trim().length === 0) {
                    const autoName = await generateAutoTableName(context, connectionName, connectionManager);
                    if (!autoName) return;
                    finalTableName = autoName;
                    vscode.window.showInformationMessage(`Auto-generated table name: ${finalTableName}`);
                } else {
                    finalTableName = targetTable.trim();
                }

                const importOptions = await vscode.window.showQuickPick(
                    [
                        { label: 'Default Import', description: 'Use default settings', value: {} },
                        { label: 'Custom Options', description: 'Configure import settings (coming soon)', value: null }
                    ],
                    {
                        placeHolder: 'Select import options'
                    }
                );

                if (!importOptions) return;

                if (importOptions.value === null) {
                    vscode.window.showInformationMessage('Custom options will be available in future version');
                    return;
                }

                const startTime = Date.now();

                await vscode.window.withProgress(
                    {
                        location: vscode.ProgressLocation.Notification,
                        title: 'Importing data...',
                        cancellable: false
                    },
                    async progress => {
                        const { importDataToNetezza } = await import('../import/dataImporter');

                        const result = await importDataToNetezza(
                            sourceFile,
                            finalTableName,
                            connectionString,
                            (message: string) => {
                                progress.report({ message });
                                outputChannel.appendLine(`[Import] ${message}`);
                            }
                        );

                        if (!result.success) {
                            throw new Error(result.message);
                        }

                        if (result.details) {
                            outputChannel.appendLine(`[Import] Rows processed: ${result.details.rowsProcessed}`);
                            outputChannel.appendLine(`[Import] Columns: ${result.details.columns}`);
                            outputChannel.appendLine(`[Import] Delimiter: ${result.details.detectedDelimiter}`);
                        }
                    }
                );

                logExecutionTime(outputChannel, 'Import Data', startTime);
                vscode.window.showInformationMessage(`Data imported successfully to table: ${finalTableName}`);
            } catch (err: any) {
                vscode.window.showErrorMessage(`Error importing data: ${err.message}`);
            }
        }),

        // Smart Paste (Auto-detect Excel XML)
        vscode.commands.registerCommand('netezza.smartPaste', async () => {
            try {
                const activeEditor = vscode.window.activeTextEditor;
                if (!activeEditor) return;

                const config = vscode.workspace.getConfiguration('netezza');
                const pythonPath = config.get<string>('pythonPath') || 'python';
                const checkScriptPath = path.join(context.extensionPath, 'python', 'check_clipboard_format.py');

                const cp = require('child_process');

                const hasXmlSpreadsheet = await new Promise<boolean>(resolve => {
                    const checkProcess = cp.spawn(pythonPath, [checkScriptPath]);
                    checkProcess.on('close', (code: number) => {
                        resolve(code === 1);
                    });
                    checkProcess.on('error', () => {
                        resolve(false);
                    });
                });

                if (hasXmlSpreadsheet) {
                    const action = await vscode.window.showQuickPick(
                        [
                            {
                                label: '📊 import to Netezza table',
                                description: 'Detected "XML Spreadsheet" format - import data to database',
                                value: 'import'
                            },
                            {
                                label: '📝 Paste as text',
                                description: 'Paste clipboard content as plain text',
                                value: 'paste'
                            }
                        ],
                        {
                            placeHolder: 'Detected "XML Spreadsheet" format in clipboard - choose an action'
                        }
                    );

                    if (action?.value === 'import') {
                        vscode.commands.executeCommand('netezza.importClipboard');
                    } else if (action?.value === 'paste') {
                        const clipboardContent = await vscode.env.clipboard.readText();
                        const selection = activeEditor.selection;
                        await activeEditor.edit(editBuilder => {
                            editBuilder.replace(selection, clipboardContent);
                        });
                    }
                } else {
                    const clipboardContent = await vscode.env.clipboard.readText();
                    const selection = activeEditor.selection;
                    await activeEditor.edit(editBuilder => {
                        editBuilder.replace(selection, clipboardContent);
                    });
                }
            } catch (error: any) {
                vscode.window.showErrorMessage(`Error during paste: ${error.message}`);
            }
        })
    ];
}
