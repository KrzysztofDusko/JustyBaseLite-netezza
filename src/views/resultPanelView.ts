import * as vscode from 'vscode';
import { AnalysisPanelView } from './analysisPanelView';
import { ResultSet } from '../types';
import { ResultsHtmlGenerator, ViewData, ViewScriptUris } from './resultsHtmlGenerator';

export class ResultPanelView implements vscode.WebviewViewProvider {
    public static readonly viewType = 'netezza.results';
    private _view?: vscode.WebviewView;
    private _extensionUri: vscode.Uri;
    // Map<sourceUri, resultSets[]>
    private _resultsMap: Map<string, ResultSet[]> = new Map();
    private _pinnedSources: Set<string> = new Set();
    // Map<resultId, {sourceUri, resultSetIndex, timestamp, label}>
    private _pinnedResults: Map<
        string,
        { sourceUri: string; resultSetIndex: number; timestamp: number; label: string }
    > = new Map();
    private _autoPinnedResults: Set<string> = new Set(); // Track auto-pinned results for current execution
    private _activeSourceUri: string | undefined;
    private _activeResultSetIndexMap: Map<string, number> = new Map();
    private _resultIdCounter: number = 0;
    private _executingSources: Set<string> = new Set();
    private _cancelledSources: Set<string> = new Set();

    // Event emitter for cancel notifications
    private _onDidCancel = new vscode.EventEmitter<string>();
    public readonly onDidCancel = this._onDidCancel.event;

    constructor(extensionUri: vscode.Uri) {
        this._extensionUri = extensionUri;
    }

    public getActiveSource(): string | undefined {
        return this._activeSourceUri;
    }

    public resolveWebviewView(
        webviewView: vscode.WebviewView,
        _context: vscode.WebviewViewResolveContext,
        _token: vscode.CancellationToken
    ) {
        this._view = webviewView;

        webviewView.webview.options = {
            enableScripts: true,
            localResourceRoots: [vscode.Uri.joinPath(this._extensionUri, 'media')]
        };

        webviewView.webview.html = this._getHtmlForWebview();

        webviewView.webview.onDidReceiveMessage(message => {
            switch (message.command) {
                case 'analyze':
                    AnalysisPanelView.createOrShow(this._extensionUri, message.data);
                    return;
                case 'describeWithCopilot':
                    vscode.commands.executeCommand('netezza.describeDataWithCopilot', message.data, message.sql);
                    return;
                case 'fixSqlError':
                    vscode.commands.executeCommand('netezza.fixSqlError', message.errorMessage, message.sql);
                    return;
                case 'exportCsv':
                    this.exportCsv(message.data);
                    return;
                case 'openInExcel':
                    this.openInExcel(message.data, message.sql);
                    return;
                case 'copyAsExcel':
                    this.copyAsExcel(message.data, message.sql);
                    return;
                case 'openInExcelXlsx':
                    this.openInExcelXlsx(message.data, message.sql);
                    return;
                case 'exportJson':
                    this.exportJson(message.data);
                    return;
                case 'exportXml':
                    this.exportXml(message.data);
                    return;
                case 'exportSqlInsert':
                    this.exportSqlInsert(message.data);
                    return;
                case 'exportMarkdown':
                    this.exportMarkdown(message.data);
                    return;
                case 'switchSource':
                    this._activeSourceUri = message.sourceUri;
                    // Ensure the source exists in the results map
                    if (!this._resultsMap.has(message.sourceUri)) {
                        this._resultsMap.set(message.sourceUri, []);
                    }
                    this._updateWebview();
                    return;
                case 'togglePin':
                    if (this._pinnedSources.has(message.sourceUri)) {
                        this._pinnedSources.delete(message.sourceUri);
                    } else {
                        this._pinnedSources.add(message.sourceUri);
                    }
                    this._updateWebview();
                    return;
                case 'toggleResultPin':
                    this._toggleResultPin(message.sourceUri, message.resultSetIndex);
                    return;
                case 'switchToPinnedResult':
                    this._switchToPinnedResult(message.resultId);
                    return;
                case 'unpinResult':
                    this._pinnedResults.delete(message.resultId);
                    this._updateWebview();
                    return;
                case 'closeSource':
                    this.closeSource(message.sourceUri);
                    return;
                case 'closeResult':
                    this.closeResult(message.sourceUri, message.resultSetIndex);
                    return;
                case 'closeAllResults':
                    this.closeAllResults(message.sourceUri);
                    return;
                case 'cancelQuery':
                    if (message.sourceUri) {
                        // Call the command to cancel
                        console.log(`[resultPanelView] Received cancelQuery message for: ${message.sourceUri}`);
                        vscode.commands.executeCommand('netezza.cancelQuery', message.sourceUri, message.currentRowCounts);
                    }
                    return;
                case 'copyToClipboard':
                    vscode.env.clipboard.writeText(message.text);
                    vscode.window.showInformationMessage('Copied to clipboard');
                    return;
                case 'info':
                    vscode.window.showInformationMessage(message.text);
                    return;
                case 'error':
                    vscode.window.showErrorMessage(message.text);
                    return;
                case 'setContext':
                    vscode.commands.executeCommand('setContext', message.key, message.value);
                    return;
                case 'clearLogs':
                    this.clearLogs(message.sourceUri);
                    return;
                case 'switchResultSet':
                    this._activeResultSetIndexMap.set(message.sourceUri, message.resultSetIndex);
                    return;
            }
        });
    }

    public triggerCopySelection() {
        this._postMessageToWebview({ command: 'copySelection' });
    }

    /**
     * Check if a URI scheme is valid for result tracking.
     * Only 'file' (saved files) and 'untitled' (new unsaved files) are allowed.
     * This prevents creating result tabs for Copilot chat code blocks, output panels, etc.
     */
    private _isValidSourceUri(sourceUri: string): boolean {
        return sourceUri.startsWith('file:') || sourceUri.startsWith('untitled:');
    }

    public setActiveSource(sourceUri: string) {
        // Ignore invalid URI schemes (e.g., vscode-chat-code-block, output, etc.)
        if (!this._isValidSourceUri(sourceUri)) {
            return;
        }
        if (this._activeSourceUri === sourceUri) return;
        
        // Only switch to this source if it already has results
        // Don't create empty tabs for files that never ran SQL
        if (!this._resultsMap.has(sourceUri)) {
            return;
        }
        
        this._activeSourceUri = sourceUri;
        this._updateWebview();
    }

    /**
     * Start a new execution for the given source.
     * Clears unpinned results and sets up an initial "Execution Log" result set.
     */
    public startExecution(sourceUri: string) {
        // Ignore invalid URI schemes
        if (!this._isValidSourceUri(sourceUri)) {
            return;
        }
        this._executingSources.add(sourceUri);
        this._cancelledSources.delete(sourceUri);
        // Clear unpinned results for this source
        const existingResults = this._resultsMap.get(sourceUri) || [];

        let logResultSet: ResultSet;

        // Find existing log in results
        const existingLogIndex = existingResults.findIndex(r => r.isLog);
        if (existingLogIndex !== -1) {
            logResultSet = existingResults[existingLogIndex];
            // APPEND log for new execution instead of reset
            const timestamp = new Date().toLocaleTimeString();
            logResultSet.data.push(['', '']); // Spacer
            logResultSet.data.push([timestamp, '--- New Execution Started ---']);
            logResultSet.message = 'Execution started...';
            logResultSet.executionTimestamp = Date.now();
        } else {
            // Create new Log
            const timestamp = new Date().toLocaleTimeString();
            logResultSet = {
                columns: [
                    { name: 'Time', type: 'string' },
                    { name: 'Message', type: 'string' }
                ],
                data: [[timestamp, '--- New Execution Started ---']],
                message: 'Execution started...',
                executionTimestamp: Date.now(),
                isLog: true,
                name: 'Logs'
            } as ResultSet;
        }

        // Identify other pinned results (excluding the log if it was already there)
        // Also explicitly preserve error results
        const otherPinnedInfo = Array.from(this._pinnedResults.entries())
            .filter(
                ([_, info]) => info.sourceUri === sourceUri && existingResults[info.resultSetIndex] !== logResultSet
            )
            .sort((a, b) => a[1].resultSetIndex - b[1].resultSetIndex); // Keep relative order of others

        const otherPinnedResults: ResultSet[] = [];
        otherPinnedInfo.forEach(([_id, info]) => {
            if (info.resultSetIndex < existingResults.length) {
                const rs = existingResults[info.resultSetIndex];
                if (rs) {
                    otherPinnedResults.push(rs);
                }
            }
        });

        // Log is ALWAYS FIRST
        const finalResultSets = [logResultSet, ...otherPinnedResults];
        this._resultsMap.set(sourceUri, finalResultSets);

        // CLEAR old pins for this source and RE-PIN everything with new indices
        const oldPinIds = Array.from(this._pinnedResults.entries())
            .filter(([_, info]) => info.sourceUri === sourceUri)
            .map(([id, _]) => id);
        oldPinIds.forEach(id => this._pinnedResults.delete(id));

        // Re-pin Log at index 0
        const logResultId = `result_${++this._resultIdCounter} `;
        const filename = sourceUri.split(/[\\/]/).pop() || sourceUri;
        this._pinnedResults.set(logResultId, {
            sourceUri,
            resultSetIndex: 0,
            timestamp: Date.now(),
            label: `${filename} - Logs`
        });

        // Re-pin others (shifted by 1)
        otherPinnedInfo.forEach(([_oldId, _info], idx) => {
            const newId = `result_${++this._resultIdCounter} `;
            this._pinnedResults.set(newId, {
                sourceUri,
                resultSetIndex: idx + 1, // 0 is Log
                timestamp: Date.now(),
                label: `${filename} - Result ${idx + 1} ` // +1 because 0 is Log, 1 is Result 1
            });
        });

        this._pinnedSources.add(sourceUri);
        this._activeSourceUri = sourceUri;
        this._activeResultSetIndexMap.set(sourceUri, 0); // Switch to Logs

        this._updateWebview();
        this._view?.show?.(true);
    }

    /**
     * Append a log message to the active execution log for the source.
     */
    public log(sourceUri: string, message: string) {
        const results = this._resultsMap.get(sourceUri);
        if (!results || results.length === 0) return;

        // Find the result set with isLog=true
        const logResultSet = results.find(r => r.isLog);

        if (logResultSet) {
            const timestamp = new Date().toLocaleTimeString();
            logResultSet.data.push([timestamp, message]);

            // If we have too many logs, maybe trim? For now keep all.

            // Send partial update to frontend instead of full re-render if possible?
            // For now, full update is safer to implement quickly, but might be flashy.
            // Let's try full update first.
            this._updateWebview();
        }
    }

    /**
     * Check if execution for the given source has been cancelled.
     */
    public isCancelled(sourceUri: string): boolean {
        return this._cancelledSources.has(sourceUri);
    }

    /**
     * Notify the frontend that execution for the given source has been cancelled.
     * This allows the frontend to stop processing pending data chunks immediately.
     */
    public cancelExecution(sourceUri: string, currentRowCounts?: number[]) {
        if (this._executingSources.has(sourceUri)) {
            this._executingSources.delete(sourceUri);
        }
        this._cancelledSources.add(sourceUri);

        // Emit cancel event for progress notification update
        this._onDidCancel.fire(sourceUri);

        // Mark current result sets as cancelled in our state
        const results = this._resultsMap.get(sourceUri) || [];
        results.forEach((rs, index) => {
            rs.isCancelled = true;
            // If we have counts from the webview, truncate to what the user actually saw
            if (currentRowCounts && currentRowCounts[index] !== undefined) {
                rs.data = rs.data.slice(0, currentRowCounts[index]);
            }
        });

        // Force immediate UI update to show 'Cancelled' status and hide spinner
        this._updateWebview();

        // Notify webview to discard pending messages for this source
        this._postMessageToWebview({
            command: 'cancelExecution',
            sourceUri: sourceUri
        });
    }

    /**
     * Called after all queries in an execution complete.
     * Unpins the auto-pinned results so they behave normally on next run.
     */
    public finalizeExecution(sourceUri: string) {
        this._executingSources.delete(sourceUri);
        // Unpin all auto-pinned results for this source (including errors)

        for (const resultId of this._autoPinnedResults) {
            const pin = this._pinnedResults.get(resultId);
            if (pin && pin.sourceUri === sourceUri) {
                this._pinnedResults.delete(resultId);
            }
        }
        this._autoPinnedResults.clear();
        this._updateWebview();
    }

    public updateResults(results: ResultSet[], sourceUri: string, append: boolean = false) {
        // results is now QueryResult[] (array of { columns, data, ... }) or any[] (legacy)

        // Auto-pin the source by default if it's new
        if (!this._resultsMap.has(sourceUri)) {
            this._pinnedSources.add(sourceUri);
        }

        let newResultSets: ResultSet[] = [];
        if (Array.isArray(results)) {
            newResultSets = results;
        } else {
            newResultSets = [results];
        }

        const executionTimestamp = Date.now();
        newResultSets.forEach(rs => {
            rs.executionTimestamp = executionTimestamp;
        });

        const existingResults = this._resultsMap.get(sourceUri) || [];
        const existingLog = existingResults.find(r => r.isLog);

        // Identify currently pinned results
        const pinnedEntryPairs = Array.from(this._pinnedResults.entries())
            .filter((entry): entry is [string, { sourceUri: string; resultSetIndex: number; timestamp: number; label: string }] => {
                const info = entry[1];
                return info.sourceUri === sourceUri;
            })
            .sort((a, b) => a[1].resultSetIndex - b[1].resultSetIndex);

        const finalResultSets: ResultSet[] = [];
        const remappedPins = new Map<string, ResultSet>(); // resultId -> resultSetObject

        // 1. Logs (Always first if exists)
        if (existingLog) {
            finalResultSets.push(existingLog);
        }

        // 2. Add other pinned results (excluding log to avoid dupe)
        pinnedEntryPairs.forEach(([id, info]) => {
            const rs = existingResults[info.resultSetIndex];
            if (rs) {
                if (rs === existingLog) {
                    // Already added log logic, just track the pin ID
                    remappedPins.set(id, rs);
                } else {
                    // Add other pinned result
                    // Avoid duplicating if multiple pins point to same result
                    if (!finalResultSets.includes(rs)) {
                        finalResultSets.push(rs);
                    }
                    remappedPins.set(id, rs);
                }
            }
        });

        // 3. Append New Results
        const newResultsStartIndex = finalResultSets.length;
        console.log('[updateResults] Before append:', {
            existingResults: existingResults.map((rs, i) => `[${i}]isLog = ${rs.isLog}, isError = ${rs.isError} `),
            finalResultSetsSoFar: finalResultSets.map((rs, i) => `[${i}]isLog = ${rs.isLog}, isError = ${rs.isError} `),
            newResults: newResultSets.map(rs => `isLog = ${rs.isLog}, isError = ${rs.isError} `)
        });
        finalResultSets.push(...newResultSets);

        // Update the results map FIRST before manipulating pins
        this._resultsMap.set(sourceUri, finalResultSets);

        // 4. Update indices for existing pins BEFORE creating new pins
        remappedPins.forEach((rs, id) => {
            const newIndex = finalResultSets.indexOf(rs);
            if (newIndex !== -1) {
                const pin = this._pinnedResults.get(id);
                if (pin) pin.resultSetIndex = newIndex;
            }
        });

        // 5. Ensure Log is pinned ("Default pinned" behavior)
        if (existingLog) {
            // Check if we already have a pin pointing to existingLog
            const isLogPinned = Array.from(remappedPins.values()).includes(existingLog);
            if (!isLogPinned) {
                const logResultId = `result_${++this._resultIdCounter} `;
                const filename = sourceUri.split(/[\\\\/]/).pop() || sourceUri;
                this._pinnedResults.set(logResultId, {
                    sourceUri,
                    resultSetIndex: 0, // It is always at 0
                    timestamp: Date.now(),
                    label: `${filename} - Logs`
                });
            }
        }

        // 6. Auto-pin new results so they don't get lost on next updateResults call
        const filename = sourceUri.split(/[\\\\/]/).pop() || sourceUri;
        newResultSets.forEach((rs, idx) => {
            const resultId = `result_${++this._resultIdCounter} `;
            const resultIndex = newResultsStartIndex + idx;
            this._pinnedResults.set(resultId, {
                sourceUri,
                resultSetIndex: resultIndex,
                timestamp: Date.now(),
                label: rs.isLog ? `${filename} - Logs` : `${filename} - Result ${resultIndex} `
            });
            // Track as auto-pinned (will be unpinned after execution completes)
            this._autoPinnedResults.add(resultId);
        });

        // 7. Handle unpinned cleanup (if !append)
        if (!append) {
            const unpinnedSources = Array.from(this._resultsMap.keys()).filter(
                uri => uri !== sourceUri && !this._pinnedSources.has(uri)
            );
            unpinnedSources.forEach(uri => {
                this._resultsMap.delete(uri);
                const pinnedResultsToRemove = Array.from(this._pinnedResults.entries())
                    .filter((entry): entry is [string, { sourceUri: string; resultSetIndex: number; timestamp: number; label: string }] => {
                        const info = entry[1];
                        return info.sourceUri === uri;
                    })
                    .map(([id, _]) => id);
                pinnedResultsToRemove.forEach(id => this._pinnedResults.delete(id));
            });
        }

        this._activeSourceUri = sourceUri;
        this._activeResultSetIndexMap.set(sourceUri, newResultsStartIndex); // Switch to the first of new results

        if (this._view) {
            this._updateWebview();
            this._view.show?.(true);
        } else {
            vscode.window.showInformationMessage('Query completed. Please open "Query Results" panel to view data.');
        }
    }

    /**
     * Handle streaming chunk - append rows to the active result set without full re-render.
     * This is used for progressive loading of large result sets.
     */
    public appendStreamingChunk(
        sourceUri: string,
        _queryIndex: number,
        chunk: {
            columns: { name: string; type?: string }[];
            rows: unknown[][];
            isFirstChunk: boolean;
            isLastChunk: boolean;
            totalRowsSoFar: number;
            limitReached: boolean;
        },
        sql: string
    ) {
        // If the execution was cancelled, ignore any further chunks
        if (this._cancelledSources.has(sourceUri)) {
            return;
        }

        const existingResults = this._resultsMap.get(sourceUri) || [];

        if (chunk.isFirstChunk && chunk.columns.length > 0) {
            // Create a new result set for this query
            const newResultSet: ResultSet = {
                columns: chunk.columns,
                data: chunk.rows,
                executionTimestamp: Date.now(),
                sql,
                limitReached: chunk.limitReached
            };

            // Find log result set (always first)
            const logIndex = existingResults.findIndex(r => r.isLog);

            if (logIndex >= 0) {
                // Insert after log and other existing results
                existingResults.push(newResultSet);
            } else {
                existingResults.push(newResultSet);
            }

            this._resultsMap.set(sourceUri, existingResults);

            // Auto-pin the new result set
            const filename = sourceUri.split(/[\\/]/).pop() || sourceUri;
            const resultId = `result_${++this._resultIdCounter} `;
            const resultSetIndex = existingResults.length - 1;
            this._pinnedResults.set(resultId, {
                sourceUri,
                resultSetIndex,
                timestamp: Date.now(),
                label: `${filename} - Result ${resultSetIndex} `
            });
            this._autoPinnedResults.add(resultId);

            // Full render for first chunk (new result set created)
            this._activeSourceUri = sourceUri;
            this._activeResultSetIndexMap.set(sourceUri, resultSetIndex); // Switch to the new result set
            this._updateWebview();
            this._view?.show?.(true);
        } else if (chunk.rows.length > 0) {
            // Append rows to the last result set for this query
            const targetResultSet = existingResults[existingResults.length - 1];
            if (targetResultSet && !targetResultSet.isLog) {
                targetResultSet.data.push(...chunk.rows);
                targetResultSet.limitReached = chunk.limitReached;

                // Send incremental update to webview instead of full re-render
                this._postMessageToWebview({
                    command: 'appendRows',
                    resultSetIndex: existingResults.length - 1,
                    rows: chunk.rows,
                    totalRows: chunk.totalRowsSoFar,
                    isLastChunk: chunk.isLastChunk,
                    limitReached: chunk.limitReached
                });
            }
        }

        // Update row count on last chunk
        if (chunk.isLastChunk) {
            this._postMessageToWebview({
                command: 'streamingComplete',
                resultSetIndex: existingResults.length - 1,
                totalRows: chunk.totalRowsSoFar,
                limitReached: chunk.limitReached
            });
        }
    }

    private _postMessageToWebview(message: Record<string, unknown>) {
        if (this._view) {
            this._view.webview.postMessage(message);
        }
    }

    private _updateWebview() {
        if (this._view) {
            this._view.webview.html = this._getHtmlForWebview();
        }
    }

    private _toggleResultPin(sourceUri: string, resultSetIndex: number) {
        // Check if this result is already pinned
        const existingPin = Array.from(this._pinnedResults.entries()).find(
            (entry): entry is [string, { sourceUri: string; resultSetIndex: number; timestamp: number; label: string }] => {
                const info = entry[1];
                return info.sourceUri === sourceUri && info.resultSetIndex === resultSetIndex;
            }
        );

        if (existingPin) {
            // Unpin the result
            this._pinnedResults.delete(existingPin[0]);
        } else {
            // Pin the result
            const resultId = `result_${++this._resultIdCounter} `;
            const timestamp = Date.now();
            const filename = sourceUri.split(/[\\/]/).pop() || sourceUri;
            const label = `${filename} - Result ${resultSetIndex + 1} `;

            this._pinnedResults.set(resultId, {
                sourceUri,
                resultSetIndex,
                timestamp,
                label
            });
        }
        this._updateWebview();
    }

    private _switchToPinnedResult(resultId: string) {
        const pinnedResult = this._pinnedResults.get(resultId);
        if (pinnedResult) {
            this._activeSourceUri = pinnedResult.sourceUri;
            this._updateWebview();

            // Send message to frontend to switch to the correct result set
            if (this._view) {
                this._view.webview.postMessage({
                    command: 'switchToResultSet',
                    resultSetIndex: pinnedResult.resultSetIndex
                });
            }
        }
    }

    private async exportCsv(csvContent: string) {
        const uri = await vscode.window.showSaveDialog({
            filters: {
                'CSV Files': ['csv']
            },
            saveLabel: 'Export'
        });

        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(csvContent));
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath} `);
        }
    }

    private async openInExcel(data: unknown, sql?: string) {
        // Data can now be a string (CSV) OR an array of { csv, sql, name } objects
        // If it's the old format (string), we wrap it? Or just pass it?
        // The command handler will need to handle both provided we update it.
        vscode.commands.executeCommand('netezza.exportCurrentResultToXlsbAndOpen', data, sql);
    }

    private async copyAsExcel(data: unknown, sql?: string) {
        vscode.commands.executeCommand('netezza.copyCurrentResultToXlsbClipboard', data, sql);
    }

    private async openInExcelXlsx(data: unknown, sql?: string) {
        try {
            await vscode.commands.executeCommand('netezza.exportCurrentResultToXlsxAndOpen', data, sql);
        } catch (error: unknown) {
            const errorMsg = error instanceof Error ? error.message : String(error);
            vscode.window.showErrorMessage(`Failed to export to XLSX: ${errorMsg}. Please try reloading the window.`);
        }
    }

    private async exportJson(content: string) {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'JSON Files': ['json'] },
            saveLabel: 'Export JSON'
        });
        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(content));
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath} `);
        }
    }

    private async exportXml(content: string) {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'XML Files': ['xml'] },
            saveLabel: 'Export XML'
        });
        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(content));
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath} `);
        }
    }

    private async exportSqlInsert(content: string) {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'SQL Files': ['sql'] },
            saveLabel: 'Export SQL'
        });
        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(content));
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath} `);
        }
    }

    private async exportMarkdown(content: string) {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'Markdown Files': ['md'] },
            saveLabel: 'Export Markdown'
        });
        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(content));
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath} `);
        }
    }

    private closeSource(sourceUri: string) {
        if (this._resultsMap.has(sourceUri)) {
            this._resultsMap.delete(sourceUri);
            this._pinnedSources.delete(sourceUri);
            this._executingSources.delete(sourceUri);

            // Also remove any pinned results for this source
            const pinnedResultsToRemove = Array.from(this._pinnedResults.entries())
                .filter(([_, info]) => info.sourceUri === sourceUri)
                .map(([id, _]) => id);
            pinnedResultsToRemove.forEach(id => this._pinnedResults.delete(id));

            // If active source was closed, switch to another one
            if (this._activeSourceUri === sourceUri) {
                const remainingSources = Array.from(this._resultsMap.keys());
                this._activeSourceUri = remainingSources.length > 0 ? remainingSources[0] : undefined;
            }

            this._updateWebview();
        }
    }

    private closeResult(sourceUri: string, resultSetIndex: number) {
        const results = this._resultsMap.get(sourceUri);
        if (!results || resultSetIndex < 0 || resultSetIndex >= results.length) {
            return;
        }

        // Remove the result at this index
        results.splice(resultSetIndex, 1);

        // Remove any pinned results that pointed to this result
        const pinnedResultsToRemove = Array.from(this._pinnedResults.entries())
            .filter(([_, info]) => info.sourceUri === sourceUri && info.resultSetIndex === resultSetIndex)
            .map(([id, _]) => id);
        pinnedResultsToRemove.forEach(id => this._pinnedResults.delete(id));

        // Update indices for pinned results that are after the removed one
        for (const [, info] of this._pinnedResults.entries()) {
            if (info.sourceUri === sourceUri && info.resultSetIndex > resultSetIndex) {
                info.resultSetIndex -= 1;
            }
        }

        // If we closed all non-log results, or if active index is now out of bounds
        if (this._activeResultSetIndexMap.get(sourceUri) === resultSetIndex) {
            // Switch to the next available result or the one before it
            const newIndex = resultSetIndex < results.length ? resultSetIndex : Math.max(0, results.length - 1);
            this._activeResultSetIndexMap.set(sourceUri, newIndex);
        } else if (this._activeResultSetIndexMap.get(sourceUri)! > resultSetIndex) {
            // Decrement active index if it was after the removed result
            this._activeResultSetIndexMap.set(sourceUri, this._activeResultSetIndexMap.get(sourceUri)! - 1);
        }

        this._updateWebview();
    }

    private closeAllResults(sourceUri: string) {
        const results = this._resultsMap.get(sourceUri);
        if (!results || results.length === 0) {
            return;
        }

        // Keep only the log result (if it exists)
        const logIndex = results.findIndex(r => r.isLog);
        
        if (logIndex === -1) {
            // No log, remove all results
            results.splice(0);
        } else {
            // Keep only log, remove everything else
            const logResult = results[logIndex];
            results.splice(0);
            results.push(logResult);
        }

        // Remove all pinned results for non-log items for this source
        const pinnedResultsToRemove = Array.from(this._pinnedResults.entries())
            .filter(([_, info]) => {
                if (info.sourceUri !== sourceUri) return false;
                const rs = results[info.resultSetIndex];
                return !rs || !rs.isLog; // Remove if doesn't exist or is not log
            })
            .map(([id, _]) => id);
        pinnedResultsToRemove.forEach(id => this._pinnedResults.delete(id));

        // Reset active index to 0 (log is now at index 0 if it exists)
        this._activeResultSetIndexMap.set(sourceUri, 0);

        this._updateWebview();
    }

    private clearLogs(sourceUri: string) {
        const results = this._resultsMap.get(sourceUri);
        if (results) {
            const logResultSet = results.find(r => r.isLog);
            if (logResultSet) {
                logResultSet.data = [];
                const timestamp = new Date().toLocaleTimeString();
                logResultSet.data.push([timestamp, '--- Logs Cleared ---']);
                this._updateWebview();
            }
        }
    }

    private _getHtmlForWebview() {
        if (!this._view) {
            return '';
        }

        const uris = this._getScriptUris();
        const viewData = this._prepareViewData();
        const generator = new ResultsHtmlGenerator(this._view.webview.cspSource);

        return generator.generateHtml(uris, viewData);
    }

    private _getScriptUris(): ViewScriptUris {
        return {
            scriptUri: this._view!.webview.asWebviewUri(
                vscode.Uri.joinPath(this._extensionUri, 'media', 'tanstack-table-core.js')
            ),
            virtualUri: this._view!.webview.asWebviewUri(
                vscode.Uri.joinPath(this._extensionUri, 'media', 'tanstack-virtual-core.js')
            ),
            mainScriptUri: this._view!.webview.asWebviewUri(
                vscode.Uri.joinPath(this._extensionUri, 'media', 'resultPanel.js')
            ),
            workerUri: this._view!.webview.asWebviewUri(
                vscode.Uri.joinPath(this._extensionUri, 'media', 'searchWorker.js')
            ),
            styleUri: this._view!.webview.asWebviewUri(
                vscode.Uri.joinPath(this._extensionUri, 'media', 'resultPanel.css')
            )
        };
    }

    private _prepareViewData(): ViewData {
        const sources = Array.from(this._resultsMap.keys());
        const pinnedSources = Array.from(this._pinnedSources);
        const pinnedResults = Array.from(this._pinnedResults.entries()).map(([id, info]) => ({
            id,
            ...info
        }));
        const activeSource =
            this._activeSourceUri && this._resultsMap.has(this._activeSourceUri)
                ? this._activeSourceUri
                : sources.length > 0
                    ? sources[0]
                    : null;
        let activeResultSets = activeSource ? this._resultsMap.get(activeSource) || [] : [];

        // If no results exist for the active source, create an empty state with a message
        if (activeSource && activeResultSets.length === 0) {
            const timestamp = new Date().toLocaleTimeString();
            const emptyLog: ResultSet = {
                columns: [
                    { name: 'Time', type: 'string' },
                    { name: 'Message', type: 'string' }
                ],
                data: [[timestamp, 'No results yet']],
                message: 'No results yet',
                executionTimestamp: Date.now(),
                isLog: true,
                name: 'Logs'
            } as ResultSet;
            activeResultSets = [emptyLog];
            // Persist empty log into results map so subsequent _prepareViewData calls and webview render
            // consistently reflect that this source has an empty log result set.
            this._resultsMap.set(activeSource, activeResultSets);
        }

        const activeResultSetIndex =
            activeSource && this._activeResultSetIndexMap.has(activeSource)
                ? this._activeResultSetIndexMap.get(activeSource)!
                : activeResultSets && activeResultSets.length > 0
                    ? 0
                    : 0;

        // Serialize data safely with BigInt support
        const bigIntReplacer = (_key: string, value: unknown) => {
            if (typeof value === 'bigint') {
                if (value >= Number.MIN_SAFE_INTEGER && value <= Number.MAX_SAFE_INTEGER) {
                    return Number(value);
                }
                return value.toString();
            }
            return value;
        };

        return {
            sourcesJson: JSON.stringify(sources),
            pinnedSourcesJson: JSON.stringify(pinnedSources),
            pinnedResultsJson: JSON.stringify(pinnedResults),
            activeSourceJson: JSON.stringify(activeSource),
            resultSetsJson: JSON.stringify(activeResultSets, bigIntReplacer),
            activeResultSetIndex: activeResultSetIndex,
            executingSourcesJson: JSON.stringify(Array.from(this._executingSources))
        };
    }
}
