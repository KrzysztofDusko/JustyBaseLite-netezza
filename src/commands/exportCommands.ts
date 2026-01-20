/**
 * Export Commands - commands for exporting data to various formats
 */

import * as vscode from 'vscode';
import * as path from 'path';
import * as os from 'os';
import { ConnectionManager } from '../core/connectionManager';
import { CsvExportItem, StructuredExportItem } from '../export/xlsbExporter';

export interface ExportCommandsDependencies {
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
 * Register all export-related commands
 */
export function registerExportCommands(deps: ExportCommandsDependencies): vscode.Disposable[] {
    const { connectionManager, outputChannel } = deps;

    return [
        // Export to XLSB
        vscode.commands.registerCommand('netezza.exportToXlsb', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor) {
                vscode.window.showErrorMessage('No active editor found');
                return;
            }

            const selection = editor.selection;
            const text = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);

            if (!text.trim()) {
                vscode.window.showWarningMessage('No SQL query to export');
                return;
            }

            const uri = await vscode.window.showSaveDialog({
                filters: { 'Excel Binary Workbook': ['xlsb'] },
                saveLabel: 'Export to XLSB'
            });

            if (!uri) return;

            const startTime = Date.now();

            try {
                const documentUri = editor.document.uri.toString();
                const connectionName = connectionManager.getConnectionForExecution(documentUri);
                const connectionDetails = await connectionManager.getConnection(connectionName || '');
                if (!connectionDetails) {
                    throw new Error('Connection not configured. Please connect via Netezza: Connect...');
                }

                // Get query timeout from configuration
                const config = vscode.workspace.getConfiguration('netezza');
                const queryTimeout = config.get<number>('queryTimeout', 1800);

                await vscode.window.withProgress(
                    {
                        location: vscode.ProgressLocation.Notification,
                        title: 'Exporting to XLSB...',
                        cancellable: true
                    },
                    async (progress, token) => {
                        const { exportQueryToXlsb } = await import('../export/xlsbExporter');

                        const result = await exportQueryToXlsb(
                            connectionDetails,
                            text,
                            uri.fsPath,
                            false,
                            (message: string) => {
                                progress.report({ message });
                                outputChannel.appendLine(`[XLSB Export] ${message}`);
                            },
                            queryTimeout,
                            token
                        );

                        if (!result.success) {
                            throw new Error(result.message);
                        }
                    }
                );

                logExecutionTime(outputChannel, 'Export to XLSB', startTime);
                vscode.window.showInformationMessage(`Results exported to ${uri.fsPath}`);
            } catch (err: unknown) {
                const errorMsg = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error exporting to XLSB: ${errorMsg}`);
            }
        }),

        // Export to CSV
        vscode.commands.registerCommand('netezza.exportToCsv', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor) {
                vscode.window.showErrorMessage('No active editor found');
                return;
            }

            const selection = editor.selection;
            const text = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);

            if (!text.trim()) {
                vscode.window.showWarningMessage('No SQL query to export');
                return;
            }

            const uri = await vscode.window.showSaveDialog({
                filters: { 'CSV Files': ['csv'] },
                saveLabel: 'Export to CSV'
            });

            if (!uri) return;

            const startTime = Date.now();

            try {
                const documentUri = editor.document.uri.toString();
                const connectionName = connectionManager.getConnectionForExecution(documentUri);
                const connectionDetails = await connectionManager.getConnection(connectionName || '');
                if (!connectionDetails) {
                    throw new Error('Connection not configured. Please connect via Netezza: Connect...');
                }

                // Get query timeout from configuration
                const config = vscode.workspace.getConfiguration('netezza');
                const queryTimeout = config.get<number>('queryTimeout', 1800);

                await vscode.window.withProgress(
                    {
                        location: vscode.ProgressLocation.Notification,
                        title: 'Exporting to CSV...',
                        cancellable: true
                    },
                    async (progress, token) => {
                        const { exportToCsv } = await import('../export/csvExporter');
                        await exportToCsv(connectionDetails, text, uri.fsPath, progress, queryTimeout, token);
                    }
                );

                logExecutionTime(outputChannel, 'Export to CSV', startTime);
                vscode.window.showInformationMessage(`Results exported to ${uri.fsPath}`);
            } catch (err: unknown) {
                const errorMsg = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error exporting to CSV: ${errorMsg}`);
            }
        }),

        // Copy XLSB to Clipboard
        vscode.commands.registerCommand('netezza.copyXlsbToClipboard', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor) {
                vscode.window.showErrorMessage('No active editor found');
                return;
            }

            const selection = editor.selection;
            const text = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);

            if (!text.trim()) {
                vscode.window.showWarningMessage('No SQL query to export');
                return;
            }

            try {
                const documentUri = editor.document.uri.toString();
                const connectionName = connectionManager.getConnectionForExecution(documentUri);
                const connectionDetails = await connectionManager.getConnection(connectionName || '');
                if (!connectionDetails) {
                    throw new Error('Connection not configured. Please connect via Netezza: Connect...');
                }

                // Get query timeout from configuration
                const config = vscode.workspace.getConfiguration('netezza');
                const queryTimeout = config.get<number>('queryTimeout', 1800);

                const startTime = Date.now();

                await vscode.window.withProgress(
                    {
                        location: vscode.ProgressLocation.Notification,
                        title: 'Exporting to XLSB and copying to clipboard...',
                        cancellable: true
                    },
                    async (progress, token) => {
                        const { exportQueryToXlsb, getTempFilePath } = await import('../export/xlsbExporter');

                        const tempPath = getTempFilePath();

                        const result = await exportQueryToXlsb(
                            connectionDetails,
                            text,
                            tempPath,
                            true,
                            (message: string) => {
                                progress.report({ message });
                                outputChannel.appendLine(`[XLSB Clipboard] ${message}`);
                            },
                            queryTimeout,
                            token
                        );

                        if (!result.success) {
                            throw new Error(result.message);
                        }

                        if (!result.details?.clipboard_success) {
                            throw new Error('Failed to copy file to clipboard');
                        }
                    }
                );

                logExecutionTime(outputChannel, 'Copy XLSB to Clipboard', startTime);

                const action = await vscode.window.showInformationMessage(
                    'Excel file copied to clipboard! You can now paste it into Excel or Windows Explorer.',
                    'Show Temp Folder',
                    'OK'
                );

                if (action === 'Show Temp Folder') {
                    const tempDir = os.tmpdir();
                    await vscode.env.openExternal(vscode.Uri.file(tempDir));
                }
            } catch (err: unknown) {
                const errorMsg = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error copying XLSB to clipboard: ${errorMsg}`);
            }
        }),

        // Export to XLSB and Open
        vscode.commands.registerCommand('netezza.exportToXlsbAndOpen', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor) {
                vscode.window.showErrorMessage('No active editor found');
                return;
            }

            const selection = editor.selection;
            const text = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);

            if (!text.trim()) {
                vscode.window.showWarningMessage('No SQL query to export');
                return;
            }

            const uri = await vscode.window.showSaveDialog({
                filters: { 'Excel Binary Workbook': ['xlsb'] },
                saveLabel: 'Export to XLSB and Open'
            });

            if (!uri) return;

            const startTime = Date.now();

            try {
                const documentUri = editor.document.uri.toString();
                const connectionName = connectionManager.getConnectionForExecution(documentUri);
                const connectionDetails = await connectionManager.getConnection(connectionName || '');
                if (!connectionDetails) {
                    throw new Error('Connection not configured. Please connect via Netezza: Connect...');
                }

                // Get query timeout from configuration
                const config = vscode.workspace.getConfiguration('netezza');
                const queryTimeout = config.get<number>('queryTimeout', 1800);

                await vscode.window.withProgress(
                    {
                        location: vscode.ProgressLocation.Notification,
                        title: 'Exporting to XLSB and opening...',
                        cancellable: true
                    },
                    async (progress, token) => {
                        const { exportQueryToXlsb } = await import('../export/xlsbExporter');

                        const result = await exportQueryToXlsb(
                            connectionDetails,
                            text,
                            uri.fsPath,
                            false,
                            (message: string) => {
                                progress.report({ message });
                                outputChannel.appendLine(`[XLSB Export] ${message}`);
                            },
                            queryTimeout,
                            token
                        );

                        if (!result.success) {
                            throw new Error(result.message);
                        }
                    }
                );

                logExecutionTime(outputChannel, 'Export to XLSB and Open', startTime);

                await vscode.env.openExternal(uri);
                vscode.window.showInformationMessage(`Results exported and opened: ${uri.fsPath}`);
            } catch (err: unknown) {
                const errorMsg = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error exporting to XLSB: ${errorMsg}`);
            }
        }),

        // Export Current Result to XLSB and Open (from datagrid)
        vscode.commands.registerCommand(
            'netezza.exportCurrentResultToXlsbAndOpen',
            async (csvContent: string | (CsvExportItem & { isActive?: boolean })[], sql?: string) => {
                try {
                    if (!csvContent || (Array.isArray(csvContent) && csvContent.length === 0)) {
                        vscode.window.showErrorMessage('No data to export');
                        return;
                    }

                    let dataToExport = csvContent;

                    if (Array.isArray(csvContent) && csvContent.length > 1) {
                        const choice = await vscode.window.showQuickPick(
                            ['Export All Results', 'Export Active Result Only'],
                            { placeHolder: 'Multiple results available. What would you like to export?' }
                        );

                        if (!choice) return;

                        if (choice === 'Export Active Result Only') {
                            const activeItem = csvContent.find(item => item.isActive);
                            if (activeItem) {
                                dataToExport = [activeItem];
                            } else {
                                dataToExport = [csvContent[0]];
                            }
                        }
                    }

                    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
                    const tempPath = path.join(os.tmpdir(), `netezza_results_${timestamp}.xlsb`);

                    const startTime = Date.now();

                    await vscode.window.withProgress(
                        {
                            location: vscode.ProgressLocation.Notification,
                            title: 'Creating Excel file...',
                            cancellable: true
                        },
                        async (progress) => {
                            // Detect if data is structured (has columns/rows) vs CSV (has csv property)
                            const isStructuredData = Array.isArray(dataToExport) &&
                                dataToExport.length > 0 &&
                                'columns' in dataToExport[0] &&
                                'rows' in dataToExport[0];

                            if (isStructuredData) {
                                // Use type-aware structured export
                                const { exportStructuredToXlsb } = await import('../export/xlsbExporter');
                                const result = await exportStructuredToXlsb(
                                    dataToExport as unknown as (StructuredExportItem & { isActive?: boolean })[],
                                    tempPath,
                                    false,
                                    (message: string) => {
                                        progress.report({ message });
                                        outputChannel.appendLine(`[Structured to XLSB] ${message}`);
                                    }
                                );
                                if (!result.success) {
                                    throw new Error(result.message);
                                }
                            } else {
                                // Legacy CSV export (fallback)
                                const { exportCsvToXlsb } = await import('../export/xlsbExporter');
                                const result = await exportCsvToXlsb(
                                    dataToExport,
                                    tempPath,
                                    false,
                                    { source: 'Query Results Panel', sql },
                                    (message: string) => {
                                        progress.report({ message });
                                        outputChannel.appendLine(`[CSV to XLSB] ${message}`);
                                    }
                                );
                                if (!result.success) {
                                    throw new Error(result.message);
                                }
                            }
                        }
                    );

                    const duration = Date.now() - startTime;
                    outputChannel.appendLine(
                        `[${new Date().toLocaleTimeString()}] Export Current Result to Excel completed in ${duration}ms`
                    );

                    await vscode.env.openExternal(vscode.Uri.file(tempPath));
                    vscode.window.showInformationMessage(`Results exported and opened: ${tempPath}`);
                } catch (err: unknown) {
                    const errorMsg = err instanceof Error ? err.message : String(err);
                    vscode.window.showErrorMessage(`Error exporting to Excel: ${errorMsg}`);
                }
            }
        ),

        // Copy Current Result to Clipboard as XLSB
        vscode.commands.registerCommand(
            'netezza.copyCurrentResultToXlsbClipboard',
            async (csvContent: string | (CsvExportItem & { isActive?: boolean })[], sql?: string) => {
                try {
                    if (!csvContent || (Array.isArray(csvContent) && csvContent.length === 0)) {
                        vscode.window.showErrorMessage('No data to copy');
                        return;
                    }

                    let dataToExport = csvContent;

                    if (Array.isArray(csvContent) && csvContent.length > 1) {
                        const choice = await vscode.window.showQuickPick(
                            ['Export All Results', 'Export Active Result Only'],
                            { placeHolder: 'Multiple results available. What would you like to export?' }
                        );

                        if (!choice) return;

                        if (choice === 'Export Active Result Only') {
                            const activeItem = csvContent.find(item => item.isActive);
                            if (activeItem) {
                                dataToExport = [activeItem];
                            } else {
                                dataToExport = [csvContent[0]];
                            }
                        }
                    }

                    const { getTempFilePath } = await import('../export/xlsbExporter');
                    const tempPath = getTempFilePath();

                    const startTime = Date.now();

                    await vscode.window.withProgress(
                        {
                            location: vscode.ProgressLocation.Notification,
                            title: 'Copying to clipboard as Excel...',
                            cancellable: true
                        },
                        async (progress) => {
                            // Detect if data is structured (has columns/rows) vs CSV (has csv property)
                            const isStructuredData = Array.isArray(dataToExport) &&
                                dataToExport.length > 0 &&
                                'columns' in dataToExport[0] &&
                                'rows' in dataToExport[0];

                            if (isStructuredData) {
                                // Use type-aware structured export
                                const { exportStructuredToXlsb } = await import('../export/xlsbExporter');
                                const result = await exportStructuredToXlsb(
                                    dataToExport as unknown as (StructuredExportItem & { isActive?: boolean })[],
                                    tempPath,
                                    true, // copyToClipboard=true
                                    (message: string) => {
                                        progress.report({ message });
                                        outputChannel.appendLine(`[Clipboard Structured XLSB] ${message}`);
                                    }
                                );
                                if (!result.success) {
                                    throw new Error(result.message);
                                }
                            } else {
                                // Legacy CSV export (fallback)
                                const { exportCsvToXlsb } = await import('../export/xlsbExporter');
                                const result = await exportCsvToXlsb(
                                    dataToExport,
                                    tempPath,
                                    true, // copyToClipboard=true
                                    { source: 'Query Results Panel', sql },
                                    (message: string) => {
                                        progress.report({ message });
                                        outputChannel.appendLine(`[Clipboard XLSB] ${message}`);
                                    }
                                );
                                if (!result.success) {
                                    throw new Error(result.message);
                                }
                            }
                        }
                    );

                    logExecutionTime(outputChannel, 'Copy Result as Excel', startTime);

                    const action = await vscode.window.showInformationMessage(
                        'Excel file copied to clipboard! You can now paste it into Excel or Windows Explorer.',
                        'Show Temp Folder',
                        'OK'
                    );

                    if (action === 'Show Temp Folder') {
                        const tempDir = os.tmpdir();
                        await vscode.env.openExternal(vscode.Uri.file(tempDir));
                    }
                } catch (err: unknown) {
                    const errorMsg = err instanceof Error ? err.message : String(err);
                    vscode.window.showErrorMessage(`Error copying to Excel: ${errorMsg}`);
                }
            }
        ),

        // Export Current Result to XLSX and Open (from datagrid)
        vscode.commands.registerCommand(
            'netezza.exportCurrentResultToXlsxAndOpen',
            async (csvContent: string | (CsvExportItem & { isActive?: boolean })[], sql?: string) => {
                try {
                    if (!csvContent || (Array.isArray(csvContent) && csvContent.length === 0)) {
                        vscode.window.showErrorMessage('No data to export');
                        return;
                    }

                    let dataToExport = csvContent;

                    if (Array.isArray(csvContent) && csvContent.length > 1) {
                        const choice = await vscode.window.showQuickPick(
                            ['Export All Results', 'Export Active Result Only'],
                            { placeHolder: 'Multiple results available. What would you like to export?' }
                        );

                        if (!choice) return;

                        if (choice === 'Export Active Result Only') {
                            const activeItem = csvContent.find(item => item.isActive);
                            if (activeItem) {
                                dataToExport = [activeItem];
                            } else {
                                dataToExport = [csvContent[0]];
                            }
                        }
                    }

                    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
                    const tempPath = path.join(os.tmpdir(), `netezza_results_${timestamp}.xlsx`);

                    const startTime = Date.now();

                    await vscode.window.withProgress(
                        {
                            location: vscode.ProgressLocation.Notification,
                            title: 'Creating Excel XLSX file...',
                            cancellable: true
                        },
                        async (progress) => {
                            // Detect if data is structured (has columns/rows) vs CSV (has csv property)
                            const isStructuredData = Array.isArray(dataToExport) &&
                                dataToExport.length > 0 &&
                                'columns' in dataToExport[0] &&
                                'rows' in dataToExport[0];

                            if (isStructuredData) {
                                // Use type-aware structured export
                                const { exportStructuredToXlsx } = await import('../export/xlsxExporter');
                                const result = await exportStructuredToXlsx(
                                    dataToExport as unknown as (StructuredExportItem & { isActive?: boolean })[],
                                    tempPath,
                                    false,
                                    (message: string) => {
                                        progress.report({ message });
                                        outputChannel.appendLine(`[Structured to XLSX] ${message}`);
                                    }
                                );
                                if (!result.success) {
                                    throw new Error(result.message);
                                }
                            } else {
                                // Legacy CSV export (fallback)
                                const { exportCsvToXlsx } = await import('../export/xlsxExporter');
                                const result = await exportCsvToXlsx(
                                    dataToExport,
                                    tempPath,
                                    false,
                                    { source: 'Query Results Panel', sql },
                                    (message: string) => {
                                        progress.report({ message });
                                        outputChannel.appendLine(`[CSV to XLSX] ${message}`);
                                    }
                                );
                                if (!result.success) {
                                    throw new Error(result.message);
                                }
                            }
                        }
                    );

                    const duration = Date.now() - startTime;
                    outputChannel.appendLine(
                        `[${new Date().toLocaleTimeString()}] Export Current Result to XLSX completed in ${duration}ms`
                    );

                    await vscode.env.openExternal(vscode.Uri.file(tempPath));
                    vscode.window.showInformationMessage(`Results exported and opened: ${tempPath}`);
                } catch (err: unknown) {
                    const errorMsg = err instanceof Error ? err.message : String(err);
                    vscode.window.showErrorMessage(`Error exporting to Excel XLSX: ${errorMsg}`);
                }
            }
        )
    ];
}
