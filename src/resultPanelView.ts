import * as vscode from 'vscode';
import { AnalysisPanelView } from './analysisPanelView';

export class ResultPanelView implements vscode.WebviewViewProvider {
    public static readonly viewType = 'netezza.results';
    private _view?: vscode.WebviewView;
    private _extensionUri: vscode.Uri;
    // Map<sourceUri, resultSets[]>
    private _resultsMap: Map<string, any[]> = new Map();
    private _pinnedSources: Set<string> = new Set();
    // Map<resultId, {sourceUri, resultSetIndex, timestamp, label}>
    private _pinnedResults: Map<string, { sourceUri: string, resultSetIndex: number, timestamp: number, label: string }> = new Map();
    private _autoPinnedResults: Set<string> = new Set(); // Track auto-pinned results for current execution
    private _activeSourceUri: string | undefined;
    private _resultIdCounter: number = 0;

    constructor(extensionUri: vscode.Uri) {
        this._extensionUri = extensionUri;
    }

    public resolveWebviewView(
        webviewView: vscode.WebviewView,
        context: vscode.WebviewViewResolveContext,
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
            }
        });
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

        let logResultSet: any;

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
            };
        }

        // Identify other pinned results (excluding the log if it was already there)
        const otherPinnedInfo = Array.from(this._pinnedResults.entries())
            .filter(([_, info]) => info.sourceUri === sourceUri && existingResults[info.resultSetIndex] !== logResultSet)
            .sort((a, b) => a[1].resultSetIndex - b[1].resultSetIndex); // Keep relative order of others

        const otherPinnedResults: any[] = [];
        otherPinnedInfo.forEach(([id, info]) => {
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
        otherPinnedInfo.forEach(([oldId, info], idx) => {
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

    public updateResults(results: any[], sourceUri: string, append: boolean = false) {
        // results is now QueryResult[] (array of { columns, data, ... }) or any[] (legacy)

        // Auto-pin the source by default if it's new
        if (!this._resultsMap.has(sourceUri)) {
            this._pinnedSources.add(sourceUri);
        }

        let newResultSets: any[] = [];
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
            .filter(([_, info]) => info.sourceUri === sourceUri)
            .sort((a, b) => a[1].resultSetIndex - b[1].resultSetIndex);

        let finalResultSets: any[] = [];
        let remappedPins = new Map<string, any>(); // resultId -> resultSetObject

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
            const unpinnedSources = Array.from(this._resultsMap.keys()).filter(uri =>
                uri !== sourceUri && !this._pinnedSources.has(uri)
            );
            unpinnedSources.forEach(uri => {
                this._resultsMap.delete(uri);
                const pinnedResultsToRemove = Array.from(this._pinnedResults.entries())
                    .filter(([_, info]) => info.sourceUri === uri)
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


    private _updateWebview() {
        if (this._view) {
            this._view.webview.html = this._getHtmlForWebview();
        }
    }

    private _toggleResultPin(sourceUri: string, resultSetIndex: number) {
        // Check if this result is already pinned
        const existingPin = Array.from(this._pinnedResults.entries()).find(([_, info]) =>
            info.sourceUri === sourceUri && info.resultSetIndex === resultSetIndex
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

    private async openInExcel(data: any, sql?: string) {
        // Data can now be a string (CSV) OR an array of { csv, sql, name } objects
        // If it's the old format (string), we wrap it? Or just pass it?
        // The command handler will need to handle both provided we update it.
        vscode.commands.executeCommand('netezza.exportCurrentResultToXlsbAndOpen', data, sql);
    }

    private async copyAsExcel(data: any, sql?: string) {
        vscode.commands.executeCommand('netezza.copyCurrentResultToXlsbClipboard', data, sql);
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

        const { scriptUri, virtualUri, mainScriptUri, styleUri, workerUri } = this._getScriptUris();
        const viewData = this._prepareViewData();

        return this._buildHtmlDocument(scriptUri, virtualUri, mainScriptUri, styleUri, viewData, workerUri);
    }

    private _getScriptUris() {
        return {
            scriptUri: this._view!.webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'tanstack-table-core.js')),
            virtualUri: this._view!.webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'tanstack-virtual-core.js')),
            mainScriptUri: this._view!.webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'resultPanel.js')),
            workerUri: this._view!.webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'searchWorker.js')),
            styleUri: this._view!.webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'resultPanel.css'))
        };
    }

    private _prepareViewData() {
        const sources = Array.from(this._resultsMap.keys());
        const pinnedSources = Array.from(this._pinnedSources);
        const pinnedResults = Array.from(this._pinnedResults.entries()).map(([id, info]) => ({
            id,
            ...info
        }));
        const activeSource = this._activeSourceUri && this._resultsMap.has(this._activeSourceUri) ? this._activeSourceUri : (sources.length > 0 ? sources[0] : null);
        const activeResultSets = activeSource ? this._resultsMap.get(activeSource) : [];

        // Serialize data safely with BigInt support
        const bigIntReplacer = (key: string, value: any) => {
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

    private _buildHtmlDocument(scriptUri: vscode.Uri, virtualUri: vscode.Uri, mainScriptUri: vscode.Uri, styleUri: vscode.Uri, viewData: any, workerUri: vscode.Uri) {
        const cspSource = this._view!.webview.cspSource;

        // SVG Icons
        const icons = {
            eye: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M8 3c-3 0-6 2.5-6 5s3 5 6 5 6-2.5 6-5-3-5-6-5zm0 9c-2.5 0-4.5-2-4.5-4S5.5 4 8 4s4.5 2 4.5 4-2 4.5-4.5 4.5z"/><circle cx="8" cy="8" r="2"/></svg>`,
            excel: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M6 3h8v10H6V3zm-1 0H3v10h2V3zm-2-1h9a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V3a1 1 0 0 1 1-1z"/><path d="M6 6h8v1H6V6zm0 2h8v1H6V8zm0 2h8v1H6v-1z"/></svg>`,
            copy: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M4 4h7v2H4V4zm0 4h7v2H4V8zm0 4h7v2H4v-2zM2 1h12v14H2V1zm1 1v12h10V2H3z"/></svg>`,
            csv: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M13 2H6L2 6v8a1 1 0 0 0 1 1h10a1 1 0 0 0 1-1V3a1 1 0 0 0-1-1zm-1 11H4V7h3V4h5v9z"/></svg>`,
            json: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M5 2c0-1.1.9-2 2-2h2a2 2 0 0 1 2 2v2h-2V2H7v2H5V2zm0 12c0 1.1.9 2 2 2h2a2 2 0 0 0 2-2v-2h-2v2H7v-2H5v2zM2 7v2h2V7H2zm10 0v2h2V7h-2z"/></svg>`,
            xml: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M11.5 1L7 15h2l4.5-14h-2zM4.5 1L0 15h2l4.5-14h-2z"/></svg>`,
            sql: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M8 0C3.6 0 0 1.8 0 4v8c0 2.2 3.6 4 8 4s8-1.8 8-4V4c0-2.2-3.6-4-8-4zm0 2c3.3 0 6 1.3 6 3s-2.7 3-6 3-6-1.3-6-3 2.7-3 6-3zm0 12c-3.3 0-6-1.3-6-3V9c1.6 1.7 4.3 2 6 2s4.4-.3 6-2v2c0 1.7-2.7 3-6 3z"/></svg>`,
            markdown: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M14.5 2H1.5C.7 2 0 2.7 0 3.5v9C0 13.3.7 14 1.5 14h13c.8 0 1.5-.7 1.5-1.5v-9c0-.8-.7-1.5-1.5-1.5zM3 11V5l2 2 2-2v6H6V7l-1 1-1-1v4H3zm10 0h-2V9h-2v2H7V5h2v2h2V5h2v6z"/></svg>`,
            export: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M6 3h8v10H6V3zm-1 0H3v10h2V3zm-2-1h9a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V3a1 1 0 0 1 1-1z"/><path d="M6 6h8v1H6V6zm0 2h8v1H6V8zm0 2h8v1H6v-1z"/><path d="M10 12L8 14L6 12" stroke="currentColor" stroke-width="1.5" fill="none"/></svg>`, // Custom combo icon
            checkAll: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M13.485 1.929l1.414 1.414-9.9 9.9-4.243-4.242 1.415-1.415 2.828 2.829z"/></svg>`,
            clear: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M8 7.293l4.146-4.147.708.708L8.707 8l4.147 4.146-.708.708L8 8.707l-4.146 4.147-.708-.708L7.293 8 3.146 3.854l.708-.708L8 7.293z"/></svg>`
        };

        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src ${cspSource} 'unsafe-inline'; worker-src ${cspSource} blob:; connect-src ${cspSource}; style-src ${cspSource} 'unsafe-inline';">
            <title>Query Results</title>
            <script src="${scriptUri}"></script>
            <script src="${virtualUri}"></script>
            <link rel="stylesheet" href="${styleUri}">
        </head>
        <body>
            <div id="sourceTabs" class="source-tabs"></div>
            <div id="resultSetTabs" class="result-set-tabs" style="display: none;"></div>
            
            <div class="controls">
                <input type="text" id="globalFilter" placeholder="Filter..." onkeyup="onFilterChanged()" style="background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); padding: 4px;">
                <button onclick="toggleRowView()" title="Toggle Row View">${icons.eye} Row View</button>
                
                <!-- Split Button for Export -->
                <div class="split-btn-container">
                    <button id="exportMainBtn" class="split-btn-main" onclick="executeSplitExport()" title="Export results (Excel)">
                        ${icons.excel} Excel
                    </button>
                    <button id="exportArrowBtn" class="split-btn-arrow" onclick="toggleExportMenu()" title="Select export format">
                        <svg width="10" height="10" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor">
                            <path d="M8 11L3 6h10l-5 5z"/>
                        </svg>
                    </button>
                    <div id="exportMenu" class="split-btn-menu" style="display:none;">
                        <div class="split-btn-menu-item" onclick="selectExportFormat('excel')">
                            ${icons.excel} Excel (XLSB)
                        </div>
                        <div class="split-btn-menu-item" onclick="selectExportFormat('csv')">
                            ${icons.csv} CSV
                        </div>
                        <div class="split-btn-menu-item" onclick="selectExportFormat('json')">
                            ${icons.json} JSON
                        </div>
                        <div class="split-btn-menu-item" onclick="selectExportFormat('xml')">
                            ${icons.xml} XML
                        </div>
                        <div class="split-btn-menu-item" onclick="selectExportFormat('sql')">
                            ${icons.sql} SQL INSERT
                        </div>
                        <div class="split-btn-menu-item" onclick="selectExportFormat('markdown')">
                            ${icons.markdown} Markdown Table
                        </div>
                    </div>
                </div>

                <div style="width: 1px; height: 16px; background: var(--vscode-panel-border); margin: 0 2px;"></div>
                <button onclick="copyAsExcel()" title="Copy results as Excel to clipboard">${icons.excel} Excel Copy</button>
                <div style="width: 1px; height: 16px; background: var(--vscode-panel-border); margin: 0 2px;"></div>
                <button onclick="selectAll()" title="Select all rows">${icons.checkAll} Select All</button>
                <button onclick="copySelection(false)" title="Copy selected cells to clipboard">${icons.copy} Copy</button>
                <button onclick="copySelection(true)" title="Copy selected cells with headers">${icons.copy} Copy w/ Headers</button>
                <button onclick="clearAllFilters()" title="Clear all column filters">${icons.clear} Clear Filters</button>
                <span id="rowCountInfo" style="margin-left: auto; font-size: 12px; opacity: 0.8;"></span>
            </div>

            <div id="groupingPanel" class="grouping-panel" ondragover="onDragOverGroup(event)" ondragleave="onDragLeaveGroup(event)" ondrop="onDropGroup(event)">
                <span style="opacity: 0.5;">Drag headers here to group</span>
            </div>

            <div id="mainSplitView" class="main-split-view">
                <div id="gridContainer"></div>
                <div id="rowViewPanel" class="row-view-panel">
                    <div class="row-view-header">
                        <span>Row Details & Comparison</span>
                        <span class="row-view-close" onclick="toggleRowView()">×</span>
                    </div>
                    <div id="rowViewContent" class="row-view-content">
                        <div class="row-view-placeholder">Select 1 or 2 rows to view details or compare</div>
                    </div>
                </div>
            </div>
            
            <script>
                const vscode = acquireVsCodeApi();
                window.sources = ${viewData.sourcesJson};
                window.pinnedSources = new Set(${viewData.pinnedSourcesJson});
                window.pinnedResults = ${viewData.pinnedResultsJson};
                window.activeSource = ${viewData.activeSourceJson};
                window.resultSets = ${viewData.resultSetsJson};
                
                let grids = [];
                let activeGridIndex = window.resultSets && window.resultSets.length > 0 ? window.resultSets.length - 1 : 0;
                const workerUri = "${workerUri}";
            </script>
            <script src="${mainScriptUri}"></script>
            <script>
                // Initialize on load
                init();
            </script>
        </body>
        </html>`;
    }
}
