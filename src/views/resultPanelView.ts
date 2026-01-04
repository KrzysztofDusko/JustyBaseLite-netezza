import * as vscode from 'vscode';
import { AnalysisPanelView } from './analysisPanelView';
import { ResultSet } from '../types';
import { ResultsHtmlGenerator, ViewScriptUris, ViewData } from './resultsHtmlGenerator';

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
    private _resultIdCounter: number = 0;

    constructor(extensionUri: vscode.Uri) {
        this._extensionUri = extensionUri;
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
            }
        });
    }

    public triggerCopySelection() {
        this._postMessageToWebview({ command: 'copySelection' });
    }

    public setActiveSource(sourceUri: string) {
        if (this._resultsMap.has(sourceUri) && this._activeSourceUri !== sourceUri) {
            this._activeSourceUri = sourceUri;
            this._updateWebview();
        }
    }

    /**
     * Start a new execution for the given source.
     * Clears unpinned results and sets up an initial "Execution Log" result set.
     */
    public startExecution(sourceUri: string) {
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
            logResultSet = {
                columns: [
                    { name: 'Time', type: 'string' },
                    { name: 'Message', type: 'string' }
                ],
                data: [],
                message: 'Execution started...',
                executionTimestamp: Date.now(),
                isLog: true,
                name: 'Logs'
            } as ResultSet;
        }

        // Identify other pinned results (excluding the log if it was already there)
        const otherPinnedInfo = Array.from(this._pinnedResults.entries())
            .filter(
                ([_, info]) => info.sourceUri === sourceUri && existingResults[info.resultSetIndex] !== logResultSet
            )
            .sort((a, b) => a[1].resultSetIndex - b[1].resultSetIndex); // Keep relative order of others

        const otherPinnedResults: ResultSet[] = [];
        otherPinnedInfo.forEach(([_id, info]) => {
            if (info.resultSetIndex < existingResults.length) {
                otherPinnedResults.push(existingResults[info.resultSetIndex]);
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
        const logResultId = `result_${++this._resultIdCounter}`;
        const filename = sourceUri.split(/[\\/]/).pop() || sourceUri;
        this._pinnedResults.set(logResultId, {
            sourceUri,
            resultSetIndex: 0,
            timestamp: Date.now(),
            label: `${filename} - Logs`
        });

        // Re-pin others (shifted by 1)
        otherPinnedInfo.forEach(([_oldId, _info], idx) => {
            const newId = `result_${++this._resultIdCounter}`;
            this._pinnedResults.set(newId, {
                sourceUri,
                resultSetIndex: idx + 1, // 0 is Log
                timestamp: Date.now(),
                label: `${filename} - Result ${idx + 2}` // +2 because 0 is Log, 1 is Result 2 (visually)
            });
        });

        this._pinnedSources.add(sourceUri);
        this._activeSourceUri = sourceUri;

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
     * Called after all queries in an execution complete.
     * Unpins the auto-pinned results so they behave normally on next run.
     */
    public finalizeExecution(sourceUri: string) {
        // Unpin all auto-pinned results for this source
        for (const resultId of this._autoPinnedResults) {
            const pin = this._pinnedResults.get(resultId);
            if (pin && pin.sourceUri === sourceUri) {
                this._pinnedResults.delete(resultId);
            }
        }
        this._autoPinnedResults.clear();
        // Note: We don't call _updateWebview() here to avoid flicker
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
        finalResultSets.push(...newResultSets);

        // Auto-pin new results so they don't get lost on next updateResults call
        const filename = sourceUri.split(/[\\\\/]/).pop() || sourceUri;
        newResultSets.forEach((rs, idx) => {
            const resultId = `result_${++this._resultIdCounter}`;
            const resultIndex = newResultsStartIndex + idx;
            this._pinnedResults.set(resultId, {
                sourceUri,
                resultSetIndex: resultIndex,
                timestamp: Date.now(),
                label: rs.isLog ? `${filename} - Logs` : `${filename} - Result ${resultIndex}`
            });
            // Track as auto-pinned (will be unpinned after execution completes)
            this._autoPinnedResults.add(resultId);
        });

        this._resultsMap.set(sourceUri, finalResultSets);

        // 4. Update indices for existing pins
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
                const logResultId = `result_${++this._resultIdCounter}`;
                const filename = sourceUri.split(/[\\/]/).pop() || sourceUri;
                this._pinnedResults.set(logResultId, {
                    sourceUri,
                    resultSetIndex: 0, // It is always at 0
                    timestamp: Date.now(),
                    label: `${filename} - Logs`
                });
            }
        }

        // 6. Handle unpinned cleanup (if !append)
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
            const resultId = `result_${++this._resultIdCounter}`;
            const resultSetIndex = existingResults.length - 1;
            this._pinnedResults.set(resultId, {
                sourceUri,
                resultSetIndex,
                timestamp: Date.now(),
                label: `${filename} - Result ${resultSetIndex}`
            });
            this._autoPinnedResults.add(resultId);

            // Full render for first chunk (new result set created)
            this._activeSourceUri = sourceUri;
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
            const resultId = `result_${++this._resultIdCounter}`;
            const timestamp = Date.now();
            const filename = sourceUri.split(/[\\/]/).pop() || sourceUri;
            const label = `${filename} - Result ${resultSetIndex + 1}`;

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
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath}`);
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
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath}`);
        }
    }

    private async exportXml(content: string) {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'XML Files': ['xml'] },
            saveLabel: 'Export XML'
        });
        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(content));
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath}`);
        }
    }

    private async exportSqlInsert(content: string) {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'SQL Files': ['sql'] },
            saveLabel: 'Export SQL'
        });
        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(content));
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath}`);
        }
    }

    private async exportMarkdown(content: string) {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'Markdown Files': ['md'] },
            saveLabel: 'Export Markdown'
        });
        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(content));
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath}`);
        }
    }

    private closeSource(sourceUri: string) {
        if (this._resultsMap.has(sourceUri)) {
            this._resultsMap.delete(sourceUri);
            this._pinnedSources.delete(sourceUri);

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
        const activeResultSets = activeSource ? this._resultsMap.get(activeSource) : [];

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
            resultSetsJson: JSON.stringify(activeResultSets, bigIntReplacer)
        };
    }
}
