import * as vscode from 'vscode';
import { runQuery } from './queryRunner';
import { MetadataCache } from './metadataCache';

export class SchemaSearchProvider implements vscode.WebviewViewProvider {
    public static readonly viewType = 'netezza.search';
    private _view?: vscode.WebviewView;

    constructor(private readonly _extensionUri: vscode.Uri, private context: vscode.ExtensionContext, private metadataCache: MetadataCache) { }

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

        const searchId = ++this.currentSearchId;
        const statusBarDisposable = vscode.window.setStatusBarMessage(`$(loading~spin) Searching for "${term}"...`);

        let sentIds = new Set<string>();

        // 1. Search in Cache first (Immediate results)
        if (this._view) {
            const cachedResults = this.metadataCache.search(term);
            if (cachedResults.length > 0) {
                const mappedResults: any[] = [];

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
                            MATCH_TYPE: 'NAME' // Cache mostly matches by name
                        });
                    }
                });

                // Send cached results immediately
                if (mappedResults.length > 0 && searchId === this.currentSearchId) {
                    this._view.webview.postMessage({ type: 'results', data: mappedResults, append: false });
                } else if (mappedResults.length === 0 && searchId === this.currentSearchId) {
                    // If we had cached results but they were all duplicates of nothing (impossible) 
                    // or if we have no unique results from cache, maybe wait for DB?
                    // But we should clear if we are starting a new search.
                    this._view.webview.postMessage({ type: 'results', data: [], append: false });
                }
            } else {
                // Clear previous results
                if (searchId === this.currentSearchId) {
                    this._view.webview.postMessage({ type: 'results', data: [], append: false });
                }
            }
        }

        // 2. Search in Database (Comprehensive results)
        const safeTerm = term.replace(/'/g, "''").toUpperCase();
        const likeTerm = `%${safeTerm}%`;

        const query = `
            SELECT OBJNAME AS NAME, SCHEMA, DBNAME AS DATABASE, OBJTYPE AS TYPE, '' AS PARENT, 
                   COALESCE(DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
            FROM _V_OBJECT_DATA 
            WHERE UPPER(OBJNAME) LIKE '${likeTerm}'
            UNION ALL
            SELECT C.ATTNAME AS NAME, O.SCHEMA, O.DBNAME AS DATABASE, 'COLUMN' AS TYPE, O.OBJNAME AS PARENT,
                   COALESCE(C.DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
            FROM _V_RELATION_COLUMN C
            JOIN _V_OBJECT_DATA O ON C.OBJID = O.OBJID
            WHERE UPPER(C.ATTNAME) LIKE '${likeTerm}'
            UNION ALL
            SELECT V.VIEWNAME AS NAME, V.SCHEMA, V.DATABASE, 'VIEW' AS TYPE, '' AS PARENT,
                   'Found in view definition' AS DESCRIPTION, 'DEFINITION' AS MATCH_TYPE
            FROM _V_VIEW V
            WHERE UPPER(V.DEFINITION) LIKE '${likeTerm}'
            UNION ALL
            SELECT P.PROCEDURE AS NAME, P.SCHEMA, P.DATABASE, 'PROCEDURE' AS TYPE, '' AS PARENT,
                   'Found in procedure source' AS DESCRIPTION, 'SOURCE' AS MATCH_TYPE
            FROM _V_PROCEDURE P
            WHERE UPPER(P.PROCEDURESOURCE) LIKE '${likeTerm}'
            ORDER BY TYPE, NAME
        `;

        try {
            const resultJson = await runQuery(this.context, query, true);

            // If another search started, ignore this result
            if (searchId !== this.currentSearchId) {
                statusBarDisposable.dispose();
                return;
            }

            statusBarDisposable.dispose();

            if (this._view && resultJson) {
                let dbResults = JSON.parse(resultJson);

                // Deduplicate against what we already sent from cache
                if (sentIds.size > 0) {
                    dbResults = dbResults.filter((item: any) => {
                        const key = `${item.NAME.toUpperCase().trim()}|${item.TYPE.toUpperCase().trim()}|${(item.PARENT || '').toUpperCase().trim()}`;
                        if (sentIds.has(key)) return false;

                        // Also add to sentIds to prevent internal duplicates in DB results (unlikely with UNION but safe)
                        sentIds.add(key);
                        return true;
                    });
                }

                // Append non-duplicated DB results
                if (dbResults.length > 0) {
                    this._view.webview.postMessage({ type: 'results', data: dbResults, append: true });
                } else if (sentIds.size === 0) {
                    // Only if NOTHING found in cache AND NOTHING in DB, show no results
                }
            }

            // Trigger background prefetch of all objects after first search (non-blocking)
            if (!this.metadataCache.hasAllObjectsPrefetchTriggered()) {
                const context = this.context;
                const cache = this.metadataCache;
                setTimeout(async () => {
                    try {
                        await cache.prefetchAllObjects((q) => runQuery(context, q, true));
                    } catch (e) {
                        console.error('[SchemaSearch] Background prefetch error:', e);
                    }
                }, 500); // Delay to let UI settle
            }
        } catch (e: any) {
            statusBarDisposable.dispose();
            if (this._view && searchId === this.currentSearchId) {
                this._view.webview.postMessage({ type: 'error', message: e.message });
            }
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
                body { font-family: var(--vscode-font-family); padding: 10px; color: var(--vscode-foreground); }
                .search-box { display: flex; gap: 5px; margin-bottom: 10px; }
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
                .results { list-style: none; padding: 0; }
                .result-item { padding: 5px; border-bottom: 1px solid var(--vscode-panel-border); display: flex; flex-direction: column; cursor: pointer; position: relative; }
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
                const vscode = acquireVsCodeApi();
                const searchInput = document.getElementById('searchInput');
                const searchBtn = document.getElementById('searchBtn');
                const resultsList = document.getElementById('resultsList');
                const status = document.getElementById('status');

                searchBtn.addEventListener('click', () => {
                    const term = searchInput.value;
                    if (term) {
                        status.innerHTML = '<span class="spinner"></span> Searching...';
                        // Keep results until new ones come (or clear? we clear inside search handler if necessary or by postMessage)
                        // resultsList.innerHTML = ''; // Don't clear immediately, let the backend drive it
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
                                parent: item.PARENT
                            });
                        });
                        
                        resultsList.appendChild(li);
                    });
                }
            </script>
        </body>
        </html>`;
    }
}
