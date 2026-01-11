import * as vscode from 'vscode';

export interface ViewScriptUris {
    scriptUri: vscode.Uri;
    virtualUri: vscode.Uri;
    mainScriptUri: vscode.Uri;
    styleUri: vscode.Uri;
    workerUri: vscode.Uri;
}

export interface ViewData {
    sourcesJson: string;
    pinnedSourcesJson: string;
    pinnedResultsJson: string;
    activeSourceJson: string;
    resultSetsJson: string;
    activeResultSetIndex: number;
    executingSourcesJson: string;
}

export class ResultsHtmlGenerator {
    private _cspSource: string;

    constructor(cspSource: string) {
        this._cspSource = cspSource;
    }

    public generateHtml(uris: ViewScriptUris, viewData: ViewData): string {
        const icons = this._getIcons();
        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src ${this._cspSource} 'unsafe-inline'; worker-src ${this._cspSource} blob:; connect-src ${this._cspSource}; style-src ${this._cspSource} 'unsafe-inline';">
            <title>Query Results</title>
            <script src="${uris.scriptUri}"></script>
            <script src="${uris.virtualUri}"></script>
            <link rel="stylesheet" href="${uris.styleUri}">
        </head>
        <body>
            <div id="sourceTabs" class="source-tabs"></div>
            <div id="resultSetTabs" class="result-set-tabs" style="display: none;"></div>
            
            <div id="loadingOverlay" class="loading-overlay">
                <div class="spinner"></div>
                <div class="executing-text">Executing SQL...</div>
                <button id="cancelQueryBtn" class="secondary" title="Cancel the current query">Cancel</button>
            </div>
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
                        <div class="split-btn-menu-item" onclick="selectExportFormat('xlsx')">
                            ${icons.excel} Excel (XLSX)
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
                <button id="clearLogsBtn" onclick="clearLogs()" title="Clear execution logs" style="display: none;">${icons.trash} Clear Logs</button>
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
                window.executingSources = new Set(${viewData.executingSourcesJson});
                
                let grids = [];
                let activeGridIndex = ${viewData.activeResultSetIndex};
                const workerUri = "${uris.workerUri}";
            </script>
            <script src="${uris.mainScriptUri}"></script>
            <script>
                // Initialize on load
                init();
            </script>
        </body>
        </html>`;
    }

    private _getIcons() {
        return {
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
            clear: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M8 7.293l4.146-4.147.708.708L8.707 8l4.147 4.146-.708.708L8 8.707l-4.146 4.147-.708-.708L7.293 8 3.146 3.854l.708-.708L8 7.293z"/></svg>`,
            trash: `<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M6.5 1h3l.5.5V3h3v1h-1v10h-1v-10h-7v10h-1V4h-1V3h3V1.5l.5-.5zM7 2v1h2V2H7zm-2 2v9h6V4H5zm1 1h1v7H6V5zm2 0h1v7H8V5z"/></svg>`
        };
    }
}
