import * as vscode from 'vscode';

export class ResultPanelView implements vscode.WebviewViewProvider {
    public static readonly viewType = 'netezza.results';
    private _view?: vscode.WebviewView;
    private _extensionUri: vscode.Uri;
    // Map<sourceUri, resultSets[]>
    private _resultsMap: Map<string, any[][]> = new Map();
    private _pinnedSources: Set<string> = new Set();
    // Map<resultId, {sourceUri, resultSetIndex, timestamp, label}>
    private _pinnedResults: Map<string, { sourceUri: string, resultSetIndex: number, timestamp: number, label: string }> = new Map();
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
                case 'exportCsv':
                    this.exportCsv(message.data);
                    return;
                case 'openInExcel':
                    this.openInExcel(message.data, message.sql);
                    return;
                case 'copyAsExcel':
                    this.copyAsExcel(message.data, message.sql);
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

        // New logic: Preserve pinned individual results and add new ones
        const existingResults = this._resultsMap.get(sourceUri) || [];
        const pinnedResultsForSource = Array.from(this._pinnedResults.entries())
            .filter(([_, info]) => info.sourceUri === sourceUri)
            .sort((a, b) => a[1].resultSetIndex - b[1].resultSetIndex);

        // Build final result set array: pinned results + new results
        let finalResultSets: any[] = [];

        // First, add all pinned results from existing data (preserve original indices for identification)
        const validPinnedResults: Array<[string, any]> = [];
        pinnedResultsForSource.forEach(([resultId, pinnedInfo]) => {
            if (pinnedInfo.resultSetIndex < existingResults.length) {
                finalResultSets.push(existingResults[pinnedInfo.resultSetIndex]);
                validPinnedResults.push([resultId, pinnedInfo]);
            }
        });

        // Then add all new results
        finalResultSets.push(...newResultSets);

        // Update pinned result indices to match new positions (AFTER building the array)
        validPinnedResults.forEach(([resultId, _], newIndex) => {
            const pinnedEntry = this._pinnedResults.get(resultId);
            if (pinnedEntry) {
                pinnedEntry.resultSetIndex = newIndex;
            }
        });

        // Clear unpinned sources (other files)
        if (!append) {
            const unpinnedSources = Array.from(this._resultsMap.keys()).filter(uri =>
                uri !== sourceUri && !this._pinnedSources.has(uri)
            );
            unpinnedSources.forEach(uri => {
                this._resultsMap.delete(uri);
                // Also remove any pinned results for this source
                const pinnedResultsToRemove = Array.from(this._pinnedResults.entries())
                    .filter(([_, info]) => info.sourceUri === uri)
                    .map(([id, _]) => id);
                pinnedResultsToRemove.forEach(id => this._pinnedResults.delete(id));
            });
        }

        this._resultsMap.set(sourceUri, finalResultSets);
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

    private async openInExcel(csvContent: string, sql?: string) {
        // Use the existing XLSB export functionality to create and open Excel file
        vscode.commands.executeCommand('netezza.exportCurrentResultToXlsbAndOpen', csvContent, sql);
    }

    private async copyAsExcel(csvContent: string, sql?: string) {
        vscode.commands.executeCommand('netezza.copyCurrentResultToXlsbClipboard', csvContent, sql);
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
                <button onclick="toggleRowView()" title="Toggle Row View">👁️ Row View</button>
                <button onclick="openInExcel()" title="Open results in Excel">📊 Excel</button>
                <button onclick="copyAsExcel()" title="Copy results as Excel to clipboard">📋 Excel Copy</button>
                <button onclick="exportToCsv()" title="Export results to CSV">📄 CSV</button>
                <div style="width: 1px; height: 16px; background: var(--vscode-panel-border); margin: 0 4px;"></div>
                <button onclick="selectAll()" title="Select all rows">☑️ Select All</button>
                <button onclick="copySelection(false)" title="Copy selected cells to clipboard">📋 Copy</button>
                <button onclick="copySelection(true)" title="Copy selected cells with headers">📋 Copy w/ Headers</button>
                <button onclick="clearAllFilters()" title="Clear all column filters">🚫 Clear Filters</button>
                <input type="text" id="globalFilter" placeholder="Filter..." onkeyup="onFilterChanged()" style="background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); padding: 4px;">
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
