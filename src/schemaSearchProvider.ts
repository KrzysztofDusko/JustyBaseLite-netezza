import * as vscode from 'vscode';
import { runQuery } from './queryRunner';
import { MetadataCache } from './metadataCache';
import { ConnectionManager } from './connectionManager';

export class SchemaSearchProvider implements vscode.WebviewViewProvider {
    public static readonly viewType = 'netezza.search';
    private _view?: vscode.WebviewView;

    constructor(
        private readonly _extensionUri: vscode.Uri,
        private context: vscode.ExtensionContext,
        private metadataCache: MetadataCache,
        private connectionManager: ConnectionManager
    ) { }

    public resolveWebviewView(
        webviewView: vscode.WebviewView,
        context: vscode.WebviewViewResolveContext,
        _token: vscode.CancellationToken,
    ) {
        this._view = webviewView;

        webviewView.webview.options = {
            enableScripts: true,
            localResourceRoots: [this._extensionUri]
        };

        webviewView.webview.html = this._getHtmlForWebview(webviewView.webview);

        webviewView.webview.onDidReceiveMessage(async (data) => {
            switch (data.type) {
                case 'search':
                    await this.search(data.value);
                    break;
                case 'navigate':
                    // Execute command to reveal item in schema tree
                    vscode.commands.executeCommand('netezza.revealInSchema', data);
                    break;
            }
        });
    }

    private currentSearchId = 0;

    private async search(term: string) {
        if (!term || term.length < 2) {
            return;
        }

        // Determine active connection
        // Priority: Active Tab (if SQL) -> Global Active Connection
        let connectionName: string | undefined;

        const activeEditor = vscode.window.activeTextEditor;
        if (activeEditor && activeEditor.document.languageId === 'sql') {
            connectionName = this.connectionManager.getConnectionForExecution(activeEditor.document.uri.toString());
        }

        if (!connectionName) {
            connectionName = this.connectionManager.getActiveConnectionName() || undefined;
        }

        if (!connectionName) {
            this._view?.webview.postMessage({ type: 'results', data: [], append: false });
            // Optionally could notify user: vscode.window.showWarningMessage('No active connection for search');
            return;
        }

        const searchId = ++this.currentSearchId;
        const statusBarDisposable = vscode.window.setStatusBarMessage(`$(loading~spin) Searching for "${term}"...`);

        let sentIds = new Set<string>();

        // 1. Search in Cache first (Immediate results) - CONNECTION SCOPED
        if (this._view) {
            const cachedResults = this.metadataCache.search(term, connectionName);
            if (cachedResults.length > 0) {
                let mappedResults: any[] = [];

                cachedResults.forEach(item => {
                    // Generate ID to deduplicate later - normalized
                    const key = `${item.name.toUpperCase().trim()}|${item.type.toUpperCase().trim()}|${(item.parent || '').toUpperCase().trim()}`;

                    if (!sentIds.has(key)) {
                        sentIds.add(key);
                        mappedResults.push({
                            NAME: item.name,
                            SCHEMA: item.schema,
                            DATABASE: item.database,
                            TYPE: item.type,
                            PARENT: item.parent || '',
                            DESCRIPTION: 'Result from Cache',
                            MATCH_TYPE: 'NAME', // Cache mostly matches by name
                            connectionName // Pass connection name for Reveal
                        });
                    }
                });

                // Sort cached results by priority: Tables/Views (1) -> Columns (2)
                mappedResults.sort((a, b) => {
                    const getPrio = (t: string) => (t === 'COLUMN' ? 2 : 1);
                    return getPrio(b.TYPE) - getPrio(a.TYPE) ? getPrio(a.TYPE) - getPrio(b.TYPE) : a.NAME.localeCompare(b.NAME);
                });

                // Send cached results immediately
                if (mappedResults.length > 0 && searchId === this.currentSearchId) {
                    this._view.webview.postMessage({ type: 'results', data: mappedResults, append: false });
                }
            } else {
                // Clear previous results IF we haven't sent anything yet?
                // Actually if cache is empty, we send empty list to clear UI
                if (searchId === this.currentSearchId) {
                    this._view.webview.postMessage({ type: 'results', data: [], append: false });
                }
            }

            // Trigger background prefetch for this connection
            if (!this.metadataCache.hasAllObjectsPrefetchTriggered(connectionName)) {
                this.metadataCache.prefetchAllObjects(connectionName, async (q) => runQuery(this.context, q, true, connectionName, this.connectionManager));
            }
        }

        // 2. Search in Database (Comprehensive results)
        // Only if connection is available
        const safeTerm = term.replace(/'/g, "''").toUpperCase();
        const likeTerm = `%${safeTerm}%`;

        // Combined query to search Tables, Objects, Columns, Views, Procedures
        // Priority: 1=NAME (Tables/Views), 2=COLUMN, 3=DESC, 4=DEFINITION/SOURCE
        const query = `
            SELECT * FROM (
                SELECT 1 AS PRIORITY, OBJNAME AS NAME, SCHEMA, DBNAME AS DATABASE, OBJTYPE AS TYPE, '' AS PARENT, 
                       COALESCE(DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
                FROM _V_OBJECT_DATA 
                WHERE UPPER(OBJNAME) LIKE '${likeTerm}'
                UNION ALL
                SELECT 1 AS PRIORITY, OBJNAME AS NAME, SCHEMA, DBNAME AS DATABASE, OBJTYPE AS TYPE, '' AS PARENT, 
                       COALESCE(DESCRIPTION, '') AS DESCRIPTION, 'DESC' AS MATCH_TYPE
                FROM _V_OBJECT_DATA 
                WHERE UPPER(DESCRIPTION) LIKE '${likeTerm}' AND UPPER(OBJNAME) NOT LIKE '${likeTerm}'
                UNION ALL
                SELECT 2 AS PRIORITY, C.ATTNAME AS NAME, O.SCHEMA, O.DBNAME AS DATABASE, 'COLUMN' AS TYPE, O.OBJNAME AS PARENT,
                       COALESCE(C.DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
                FROM _V_RELATION_COLUMN C
                JOIN _V_OBJECT_DATA O ON C.OBJID = O.OBJID
                WHERE UPPER(C.ATTNAME) LIKE '${likeTerm}'
                UNION ALL
                SELECT 4 AS PRIORITY, V.VIEWNAME AS NAME, V.SCHEMA, V.DATABASE, 'VIEW' AS TYPE, '' AS PARENT, 
                       'Found in view definition' AS DESCRIPTION, 'DEFINITION' AS MATCH_TYPE
                FROM _V_VIEW V
                WHERE UPPER(V.DEFINITION) LIKE '${likeTerm}'
                UNION ALL
                SELECT 4 AS PRIORITY, P.PROCEDURE AS NAME, P.SCHEMA, P.DATABASE, 'PROCEDURE' AS TYPE, '' AS PARENT, 
                       'Found in procedure source' AS DESCRIPTION, 'SOURCE' AS MATCH_TYPE
                FROM _V_PROCEDURE P
                WHERE UPPER(P.PROCEDURESOURCE) LIKE '${likeTerm}'
                UNION ALL
                SELECT 3 AS PRIORITY, E1.TABLENAME AS NAME, E1.SCHEMA, E1.DATABASE, 'EXTERNAL TABLE' AS TYPE, '' AS PARENT,
                       COALESCE(E2.EXTOBJNAME, '') AS DESCRIPTION, 'DATAOBJECT' AS MATCH_TYPE
                FROM _V_EXTERNAL E1
                JOIN _V_EXTOBJECT E2 ON E1.DATABASE = E2.DATABASE AND E1.SCHEMA = E2.SCHEMA AND E1.TABLENAME = E2.TABLENAME
                WHERE UPPER(E2.EXTOBJNAME) LIKE '${likeTerm}'
            ) AS R
            ORDER BY PRIORITY, NAME
            LIMIT 100
        `;

        try {
            const resultJson = await runQuery(this.context, query, true, connectionName, this.connectionManager);

            if (searchId !== this.currentSearchId) {
                statusBarDisposable.dispose();
                return; // Old search, ignore
            }

            if (resultJson) {
                let results: any[] = [];
                if (resultJson === 'Query executed successfully (no results).' || resultJson.startsWith('Query executed successfully')) {
                    results = [];
                } else {
                    results = JSON.parse(resultJson);
                }
                const mappedResults: any[] = [];

                results.forEach((item: any) => {
                    const key = `${item.NAME.toUpperCase().trim()}|${item.TYPE.toUpperCase().trim()}|${(item.PARENT || '').toUpperCase().trim()}`;

                    if (!sentIds.has(key)) {
                        mappedResults.push({
                            NAME: item.NAME,
                            SCHEMA: item.SCHEMA,
                            DATABASE: item.DATABASE,
                            TYPE: item.TYPE,
                            PARENT: item.PARENT,
                            DESCRIPTION: item.DESCRIPTION,
                            MATCH_TYPE: item.MATCH_TYPE,
                            connectionName // Pass connection name
                        });
                        sentIds.add(key);
                    }
                });

                // Send DB results
                // As we might have sent cache results, we should append or refresh
                if (mappedResults.length > 0 && this._view) {
                    this._view.webview.postMessage({ type: 'results', data: mappedResults, append: true });
                }
            }
        } catch (e: any) {
            console.error('Search error:', e);
            if (this._view && searchId === this.currentSearchId) {
                this._view.webview.postMessage({ type: 'error', message: e.message });
            }
        } finally {
            statusBarDisposable.dispose();
        }
    }

    private _getHtmlForWebview(webview: vscode.Webview) {
        return `<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Schema Search</title>
        <style>
            body { 
                font-family: var(--vscode-font-family); 
                padding: 0; 
                margin: 0;
                color: var(--vscode-foreground); 
                display: flex;
                flex-direction: column;
                height: 100vh;
                overflow: hidden;
            }
            .search-box { 
                display: flex; 
                gap: 5px; 
                padding: 10px;
                flex-shrink: 0;
                border-bottom: 1px solid var(--vscode-panel-border);
            }
            input { flex-grow: 1; padding: 5px; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); }
            button {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 6px;
                background-color: var(--vscode-button-secondaryBackground);
                color: var(--vscode-button-secondaryForeground);
                border: 1px solid var(--vscode-contrastBorder, transparent);
                padding: 4px 10px;
                cursor: pointer;
                border-radius: 2px;
                font-family: var(--vscode-font-family);
                font-size: 12px;
                line-height: 18px;
            }
            button:hover { background-color: var(--vscode-button-secondaryHoverBackground); }
            button.primary { background-color: var(--vscode-button-background); color: var(--vscode-button-foreground); }
            button.primary:hover { background-color: var(--vscode-button-hoverBackground); }
            #status { padding: 5px 10px; flex-shrink: 0; }
            .results { 
                list-style: none; 
                padding: 0; 
                margin: 0; 
                flex-grow: 1; 
                overflow-y: auto; 
            }
            .result-item { padding: 8px 10px; border-bottom: 1px solid var(--vscode-panel-border); display: flex; flex-direction: column; cursor: pointer; position: relative; }
            .result-item:hover { background: var(--vscode-list-hoverBackground); }
            .item-header { display: flex; justify-content: space-between; font-weight: bold; }
            .item-details { font-size: 0.9em; opacity: 0.8; display: flex; gap: 10px; }
            .type-badge { font-size: 0.8em; background: var(--vscode-badge-background); color: var(--vscode-badge-foreground); padding: 2px 5px; border-radius: 3px; }
            .tooltip { position: absolute; background: var(--vscode-editorHoverWidget-background); color: var(--vscode-editorHoverWidget-foreground); border: 1px solid var(--vscode-editorHoverWidget-border); padding: 8px; border-radius: 4px; font-size: 0.9em; max-width: 300px; word-wrap: break-word; z-index: 1000; opacity: 0; visibility: hidden; transition: opacity 0.2s, visibility 0.2s; pointer-events: none; }
            .result-item:hover .tooltip { opacity: 1; visibility: visible; }
            .tooltip.top { bottom: 100%; left: 0; margin-bottom: 5px; }
            .tooltip.bottom { top: 100%; left: 0; margin-top: 5px; }
            .cache-badge { background-color: var(--vscode-charts-green); color: white; padding: 1px 4px; border-radius: 2px; font-size: 0.7em; margin-left: 5px; }
            .spinner {
                border: 2px solid transparent;
                border-top: 2px solid var(--vscode-progressBar-background);
                border-radius: 50%;
                width: 14px;
                height: 14px;
                animation: spin 1s linear infinite;
                display: inline-block;
                vertical-align: middle;
                margin-right: 8px;
            }
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
        </style>
    </head>
    <body>
        <div class="search-box">
            <input type="text" id="searchInput" placeholder="Search tables, columns, view definitions, procedure source..." />
            <button id="searchBtn" class="primary">Search</button>
        </div>
        <div id="status"></div>
        <ul class="results" id="resultsList"></ul>

        <script>
            try {
            const vscode = acquireVsCodeApi();
            const searchInput = document.getElementById('searchInput');
            const searchBtn = document.getElementById('searchBtn');
            const resultsList = document.getElementById('resultsList');
            const status = document.getElementById('status');

            searchBtn.addEventListener('click', () => {
                const term = searchInput.value;
                if (term) {
                    status.innerHTML = '<span class="spinner"></span> Searching...';
                    vscode.postMessage({ type: 'search', value: term });
                }
            });

            searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    searchBtn.click();
                }
            });

            window.addEventListener('message', event => {
                const message = event.data;
                switch (message.type) {
                    case 'results':
                        status.textContent = '';
                        renderResults(message.data, message.append);
                        break;
                    case 'error':
                        status.textContent = 'Error: ' + message.message;
                        break;
                }
            });

            function renderResults(data, append) {
                if (!append) {
                    resultsList.innerHTML = '';
                }

                if (!data || data.length === 0) {
                    if (!append && resultsList.children.length === 0) {
                        status.textContent = 'No results found.';
                    }
                    return;
                }

                data.forEach(item => {
                    const li = document.createElement('li');
                    li.className = 'result-item';

                    const parentInfo = item.PARENT ? \`Parent: \${item.PARENT}\` : '';
                    const schemaInfo = item.SCHEMA ? \`Schema: \${item.SCHEMA}\` : '';
                    const databaseInfo = item.DATABASE ? \`Database: \${item.DATABASE}\` : '';
                    const description = item.DESCRIPTION && item.DESCRIPTION.trim() ? item.DESCRIPTION : '';
                    
                    // Add match type indicator
                    const matchTypeInfo = item.MATCH_TYPE === 'DEFINITION' ? 'Match in view definition' :
                                        item.MATCH_TYPE === 'SOURCE' ? 'Match in procedure source' :
                                        item.MATCH_TYPE === 'NAME' ? 'Match in name' : '';
                    
                    li.innerHTML = \`
                        <div class="item-header">
                            <span>\${item.NAME}</span>
                            <span class="type-badge">\${item.TYPE}</span>
                        </div>
                        <div class="item-details">
                            <span>\${databaseInfo}</span>
                            <span>\${schemaInfo}</span>
                            <span>\${parentInfo}</span>
                            \${matchTypeInfo ? \`<span style="font-style: italic; color: var(--vscode-descriptionForeground);">\${matchTypeInfo}</span>\` : ''}
                        </div>
                        \${description ? \`<div class="tooltip bottom">\${description}</div>\` : ''}
                    \`;
                    
                    // Add double-click handler to navigate to schema tree
                    li.addEventListener('dblclick', () => {
                        vscode.postMessage({ 
                            type: 'navigate', 
                            name: item.NAME,
                            schema: item.SCHEMA,
                            database: item.DATABASE,
                            objType: item.TYPE,
                            parent: item.PARENT,
                            connectionName: item.connectionName // Pass back connection name
                        });
                    });
                    
                    resultsList.appendChild(li);
                });
            }
            } catch (e) {
                document.body.innerHTML = '<pre style="color:red;">Error loading Schema Search: ' + e.message + '\\n' + e.stack + '</pre>';
            }
        </script>
    </body>
    </html>`;
    }
}
