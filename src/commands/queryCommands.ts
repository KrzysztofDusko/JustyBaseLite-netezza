/**
 * Query Commands - commands for executing SQL queries
 */

import * as vscode from 'vscode';
import { ConnectionManager } from '../core/connectionManager';
import { runQueriesSequentially, runExplainQuery, runQueriesWithStreaming, StreamingChunk, cancelQueryByUri } from '../core/queryRunner';
import { SqlParser } from '../sql/sqlParser';
import { ResultPanelView } from '../views/resultPanelView';
// sql-formatter is lazy-loaded to reduce startup time
import { buildExecCommand } from '../utils/shellUtils';

export interface QueryCommandsDependencies {
    context: vscode.ExtensionContext;
    connectionManager: ConnectionManager;
    resultPanelProvider: ResultPanelView;
}

/**
 * Register all query execution commands
 */
export function registerQueryCommands(deps: QueryCommandsDependencies): vscode.Disposable[] {
    const { context, connectionManager, resultPanelProvider } = deps;

    return [
        vscode.commands.registerCommand('netezza.cancelQuery', async (sourceUri?: string | vscode.Uri, currentRowCounts?: number[]) => {
            let uriToCancel = typeof sourceUri === 'string' ? sourceUri : sourceUri?.toString();
            if (!uriToCancel) {
                const editor = vscode.window.activeTextEditor;
                if (editor) {
                    uriToCancel = editor.document.uri.toString();
                }
            }

            if (uriToCancel) {
                console.log(`[netezza.cancelQuery] Cancelling: ${uriToCancel}`);
                // 1. Update UI immediately (optimistic)
                resultPanelProvider.cancelExecution(uriToCancel, currentRowCounts);

                // 2. Perform backend cancellation (async)
                try {
                    await cancelQueryByUri(uriToCancel);
                } catch (err) {
                    console.error('[netezza.cancelQuery] Backend cancel failed:', err);
                }
            } else {
                // If we don't have a specific URI, try to cancel the "active" execution in the provider
                const activeUri = resultPanelProvider.getActiveSource();
                if (activeUri) {
                    console.log(`[netezza.cancelQuery] No URI provided, falling back to active source: ${activeUri}`);
                    resultPanelProvider.cancelExecution(activeUri, currentRowCounts);
                    await cancelQueryByUri(activeUri);
                } else {
                    vscode.window.showWarningMessage('No active query to cancel.');
                }
            }
        }),
        // Run Query (Smart/Sequential Execution)
        vscode.commands.registerCommand('netezza.runQuery', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor) {
                vscode.window.showErrorMessage('No active editor found');
                return;
            }

            const document = editor.document;
            const selection = editor.selection;
            const text = document.getText();
            const sourceUri = document.uri.toString();

            let queries: string[] = [];

            if (!selection.isEmpty) {
                const selectedText = document.getText(selection);
                if (!selectedText.trim()) {
                    vscode.window.showWarningMessage('No SQL query selected');
                    return;
                }
                if (/^\s*CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\b/i.test(selectedText)) {
                    queries = [selectedText];
                } else {
                    queries = SqlParser.splitStatements(selectedText).filter(q => q.trim().length > 0);
                }
            } else {
                const offset = document.offsetAt(selection.active);
                const statement = SqlParser.getStatementAtPosition(text, offset);

                if (statement) {
                    queries = [statement.sql];
                    const startPos = document.positionAt(statement.start);
                    const endPos = document.positionAt(statement.end);
                    editor.selection = new vscode.Selection(startPos, endPos);
                } else {
                    vscode.window.showWarningMessage('No SQL statement found at cursor');
                    return;
                }
            }

            if (queries.length === 0) return;

            // Check for Python script invocation
            const single = queries.length === 1 ? queries[0].trim() : null;
            if (single) {
                const tokens = single.split(/\s+/);
                const first = tokens[0] || '';

                const isPythonExec =
                    /python(\.exe)?$/i.test(first) && tokens.length >= 2 && tokens[1].toLowerCase().endsWith('.py');
                const isScriptDirect = first.toLowerCase().endsWith('.py');

                if (isPythonExec || isScriptDirect) {
                    const config = vscode.workspace.getConfiguration('netezza');
                    const pythonPath = config.get<string>('pythonPath') || 'python';

                    let cmd = '';
                    if (isPythonExec) {
                        const py = tokens[0];
                        const script = tokens[1];
                        const args = tokens.slice(2);
                        cmd = buildExecCommand(py, script, args);
                    } else {
                        const script = first;
                        const args = tokens.slice(1);
                        cmd = buildExecCommand(pythonPath, script, args);
                    }

                    const term = vscode.window.createTerminal({ name: 'Netezza: Script' });
                    term.show(true);
                    term.sendText(cmd, true);
                    vscode.window.showInformationMessage(`Running script: ${cmd}`);
                    return;
                }
            }

            try {
                resultPanelProvider.setActiveSource(sourceUri);
                resultPanelProvider.startExecution(sourceUri);

                // Check if streaming is enabled
                const config = vscode.workspace.getConfiguration('netezza');
                const enableStreaming = config.get<boolean>('enableStreaming', false);
                const streamingChunkSize = config.get<number>('streamingChunkSize', 5000);

                await vscode.window.withProgress({
                    location: vscode.ProgressLocation.Notification,
                    title: `Executing SQL for ${sourceUri.split(/[\\/]/).pop()}...`,
                    cancellable: false
                }, async (progress) => {
                    // Listen for cancel event to update progress message immediately
                    const cancelListener = resultPanelProvider.onDidCancel((cancelledUri) => {
                        if (cancelledUri === sourceUri) {
                            progress.report({ message: 'Cancelling query...' });
                        }
                    });

                    try {
                        if (enableStreaming) {
                            // Use streaming for large result sets
                            await runQueriesWithStreaming(
                                context,
                                queries,
                                connectionManager,
                                sourceUri,
                                msg => resultPanelProvider.log(sourceUri, msg),
                                (queryIndex: number, chunk: StreamingChunk, sql: string) => {
                                    resultPanelProvider.appendStreamingChunk(sourceUri, queryIndex, chunk, sql);
                                },
                                streamingChunkSize
                            );
                        } else {
                            // Use traditional batch loading
                            await runQueriesSequentially(
                                context,
                                queries,
                                connectionManager,
                                sourceUri,
                                msg => resultPanelProvider.log(sourceUri, msg),
                                queryResults => resultPanelProvider.updateResults(queryResults, sourceUri, true)
                            );
                        }
                    } finally {
                        cancelListener.dispose();
                    }
                });

                resultPanelProvider.finalizeExecution(sourceUri);
                vscode.commands.executeCommand('netezza.results.focus');
            } catch (err: unknown) {
                const msg = err instanceof Error ? err.message : String(err);
                
                // If it's a cancellation error, log info but don't show error dialog or error result
                if (msg.includes('Query cancelled')) {
                    resultPanelProvider.log(sourceUri, 'Query execution cancelled by user.');
                    resultPanelProvider.finalizeExecution(sourceUri);
                    return;
                }

                resultPanelProvider.log(sourceUri, `Error: ${msg}`);

                // Add error result BEFORE finalizing so it gets properly pinned
                resultPanelProvider.updateResults([{
                    columns: [],
                    data: [],
                    message: msg,
                    isError: true,
                    sql: queries.length === 1 ? queries[0] : undefined
                }], sourceUri, true);

                // Finalize AFTER adding error so the error pin is preserved
                resultPanelProvider.finalizeExecution(sourceUri);
                vscode.window.showErrorMessage(`Error executing query: ${msg}`);
            }
        }),

        // Run Query Batch
        vscode.commands.registerCommand('netezza.runQueryBatch', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor) {
                vscode.window.showErrorMessage('No active editor found');
                return;
            }

            const document = editor.document;
            const selection = editor.selection;
            const sourceUri = document.uri.toString();

            let text: string;
            if (!selection.isEmpty) {
                text = document.getText(selection);
            } else {
                text = document.getText();
            }

            if (!text.trim()) {
                vscode.window.showWarningMessage('No SQL query to execute');
                return;
            }

            // Check for Python script
            const full = text.trim();
            const tokens = full.split(/\s+/);
            const first = tokens[0] || '';
            const isPythonExec =
                /python(\.exe)?$/i.test(first) && tokens.length >= 2 && tokens[1].toLowerCase().endsWith('.py');
            const isScriptDirect = first.toLowerCase().endsWith('.py');

            if (isPythonExec || isScriptDirect) {
                const config = vscode.workspace.getConfiguration('netezza');
                const pythonPath = config.get<string>('pythonPath') || 'python';

                let cmd = '';
                if (isPythonExec) {
                    const py = tokens[0];
                    const script = tokens[1];
                    const args = tokens.slice(2);
                    cmd = buildExecCommand(py, script, args);
                } else {
                    const script = first;
                    const args = tokens.slice(1);
                    cmd = buildExecCommand(pythonPath, script, args);
                }

                const term = vscode.window.createTerminal({ name: 'Netezza: Script' });
                term.show(true);
                term.sendText(cmd, true);
                vscode.window.showInformationMessage(`Running script: ${cmd}`);
                return;
            }

            try {
                resultPanelProvider.startExecution(sourceUri);

                await vscode.window.withProgress({
                    location: vscode.ProgressLocation.Notification,
                    title: `Executing batch SQL for ${sourceUri.split(/[\\/]/).pop()}...`,
                    cancellable: false
                }, async () => {
                    await runQueriesSequentially(
                        context,
                        [text],
                        connectionManager,
                        sourceUri,
                        msg => resultPanelProvider.log(sourceUri, msg),
                        queryResults => resultPanelProvider.updateResults(queryResults, sourceUri, true)
                    );
                });

                resultPanelProvider.finalizeExecution(sourceUri);
                vscode.commands.executeCommand('netezza.results.focus');
            } catch (err: unknown) {
                const msg = err instanceof Error ? err.message : String(err);
                
                // If it's a cancellation error, log info but don't show error dialog or error result
                if (msg.includes('Query cancelled')) {
                    resultPanelProvider.log(sourceUri, 'Query execution cancelled by user.');
                    resultPanelProvider.finalizeExecution(sourceUri);
                    return;
                }

                resultPanelProvider.log(sourceUri, `Error: ${msg}`);

                // Add error result BEFORE finalizing so it gets properly pinned
                resultPanelProvider.updateResults([{
                    columns: [],
                    data: [],
                    message: msg,
                    isError: true,
                    sql: text
                }], sourceUri, true);

                // Finalize AFTER adding error so the error pin is preserved
                resultPanelProvider.finalizeExecution(sourceUri);
                vscode.window.showErrorMessage(`Error executing query: ${msg}`);
            }
        }),

        // Explain Query
        vscode.commands.registerCommand('netezza.explainQuery', async () => {
            await executeExplainQuery(context, connectionManager, false);
        }),

        // Explain Query Verbose
        vscode.commands.registerCommand('netezza.explainQueryVerbose', async () => {
            await executeExplainQuery(context, connectionManager, true);
        }),

        // Format SQL
        vscode.commands.registerCommand('netezza.formatSQL', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor) {
                vscode.window.showErrorMessage('No active editor');
                return;
            }

            if (editor.document.languageId !== 'sql' && editor.document.languageId !== 'mssql') {
                vscode.window.showWarningMessage('Format SQL is only available for SQL files');
                return;
            }

            const config = vscode.workspace.getConfiguration('netezza');
            const tabWidth = config.get<number>('formatSQL.tabWidth', 4);
            const keywordCase = config.get<'upper' | 'lower' | 'preserve'>('formatSQL.keywordCase', 'upper');

            const selection = editor.selection;
            const text = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);

            try {
                // Lazy load sql-formatter only when needed
                const { format: formatSQL } = await import('sql-formatter');

                const doubleDotPlaceholder = '__NZ_DOUBLE_DOT__';
                const preprocessed = text.replace(/\.\.(?=[a-zA-Z_])/g, `.${doubleDotPlaceholder}.`);

                const formatted = formatSQL(preprocessed, {
                    language: 'sql',
                    tabWidth: tabWidth,
                    keywordCase: keywordCase,
                    linesBetweenQueries: 2
                });

                const result = formatted.replace(new RegExp(`\\.\\s*${doubleDotPlaceholder}\\s*\\.`, 'g'), '..');

                await editor.edit(editBuilder => {
                    if (selection.isEmpty) {
                        const fullRange = new vscode.Range(
                            editor.document.positionAt(0),
                            editor.document.positionAt(editor.document.getText().length)
                        );
                        editBuilder.replace(fullRange, result);
                    } else {
                        editBuilder.replace(selection, result);
                    }
                });

                vscode.window.showInformationMessage('SQL formatted successfully');
            } catch (err: unknown) {
                const errMsg = err instanceof Error ? err.message : String(err);
                if (errMsg.includes('Parse error')) {
                    vscode.window.showErrorMessage(
                        'SQL formatting failed: The SQL contains syntax not supported by the formatter. ' +
                        'Try selecting a simpler portion of the SQL to format.'
                    );
                } else {
                    vscode.window.showErrorMessage(`Format SQL failed: ${errMsg}`);
                }
            }
        })
    ];
}

/**
 * Execute EXPLAIN query
 */
async function executeExplainQuery(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    verbose: boolean
): Promise<void> {
    const editor = vscode.window.activeTextEditor;
    if (!editor) {
        vscode.window.showErrorMessage('No active editor found');
        return;
    }

    const document = editor.document;
    const selection = editor.selection;

    let text: string;
    if (!selection.isEmpty) {
        text = document.getText(selection);
    } else {
        const position = editor.selection.active;
        const offset = document.offsetAt(position);
        const fullText = document.getText();
        const stmt = SqlParser.getStatementAtPosition(fullText, offset);
        if (stmt) {
            text = fullText.substring(stmt.start, stmt.end);
        } else {
            text = document.getText();
        }
    }

    if (!text.trim()) {
        vscode.window.showWarningMessage('No SQL query to explain');
        return;
    }

    let cleanQuery = text.trim();
    if (cleanQuery.toUpperCase().startsWith('EXPLAIN')) {
        cleanQuery = cleanQuery.replace(/^EXPLAIN\s+(?:VERBOSE\s+)?/i, '');
    }

    const explainQueryText = verbose ? `EXPLAIN VERBOSE ${cleanQuery}` : `EXPLAIN ${cleanQuery}`;

    try {
        const documentUri = document.uri.toString();
        const connectionName = connectionManager.getConnectionForExecution(documentUri);

        if (!connectionName) {
            vscode.window.showErrorMessage('No database connection. Please connect first.');
            return;
        }

        await vscode.window.withProgress(
            {
                location: vscode.ProgressLocation.Notification,
                title: 'Generating query plan...',
                cancellable: false
            },
            async () => {
                const result = await runExplainQuery(
                    context,
                    explainQueryText,
                    connectionName,
                    connectionManager,
                    documentUri
                );

                if (result && result.trim()) {
                    const { parseExplainOutput, ExplainPlanView } = await import('../views/explainPlanView');
                    const parsed = parseExplainOutput(result);
                    ExplainPlanView.createOrShow(context.extensionUri, parsed, cleanQuery);
                } else {
                    vscode.window.showWarningMessage('No explain output received');
                }
            }
        );
    } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        vscode.window.showErrorMessage(`Error generating query plan: ${msg}`);
    }
}
