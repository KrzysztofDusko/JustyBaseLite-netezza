import * as vscode from 'vscode';
import { runQuery } from './queryRunner';

export class SchemaSearchProvider implements vscode.WebviewViewProvider {
    public static readonly viewType = 'netezza.search';
    private _view?: vscode.WebviewView;

    constructor(private readonly _extensionUri: vscode.Uri, private context: vscode.ExtensionContext) { }

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

    private async search(term: string) {
        if (!term || term.length < 2) {
            return;
        }

        // Escape single quotes
        const safeTerm = term.replace(/'/g, "''");
        const likeTerm = `%${safeTerm}%`;

        // Query to search for objects, columns, view definitions, and procedure source
        // We search _V_OBJECT_DATA for tables, views, synonyms, etc.
        // And _V_RELATION_COLUMN for columns.
        // And _V_VIEW for view definitions.
        // And _V_PROCEDURE for procedure source code.
        const query = `
            SELECT OBJNAME AS NAME, SCHEMA, OBJTYPE AS TYPE, '' AS PARENT, 
                   COALESCE(DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
            FROM _V_OBJECT_DATA 
            WHERE OBJNAME LIKE '${likeTerm}'
            UNION ALL
            SELECT C.ATTNAME AS NAME, O.SCHEMA, 'COLUMN' AS TYPE, O.OBJNAME AS PARENT,
                   COALESCE(C.DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
            FROM _V_RELATION_COLUMN C
            JOIN _V_OBJECT_DATA O ON C.OBJID = O.OBJID
            WHERE C.ATTNAME LIKE '${likeTerm}'
            UNION ALL
            SELECT V.VIEWNAME AS NAME, V.SCHEMA, 'VIEW' AS TYPE, '' AS PARENT,
                   'Found in view definition' AS DESCRIPTION, 'DEFINITION' AS MATCH_TYPE
            FROM _V_VIEW V
            WHERE V.DEFINITION LIKE '${likeTerm}'
            UNION ALL
            SELECT P.PROCEDURE AS NAME, P.SCHEMA, 'PROCEDURE' AS TYPE, '' AS PARENT,
                   'Found in procedure source' AS DESCRIPTION, 'SOURCE' AS MATCH_TYPE
            FROM _V_PROCEDURE P
            WHERE P.PROCEDURESOURCE LIKE '${likeTerm}'
            ORDER BY TYPE, NAME
            LIMIT 100
        `;

        try {
            const resultJson = await runQuery(this.context, query, true);
            if (this._view) {
                this._view.webview.postMessage({ type: 'results', data: resultJson ? JSON.parse(resultJson) : [] });
            }
        } catch (e: any) {
            if (this._view) {
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
                        status.textContent = 'Searching...';
                        resultsList.innerHTML = '';
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
                            renderResults(message.data);
                            break;
                        case 'error':
                            status.textContent = 'Error: ' + message.message;
                            break;
                    }
                });

                function renderResults(data) {
                    if (!data || data.length === 0) {
                        status.textContent = 'No results found.';
                        return;
                    }
                    
                    data.forEach(item => {
                        const li = document.createElement('li');
                        li.className = 'result-item';
                        
                        const parentInfo = item.PARENT ? \`Parent: \${item.PARENT}\` : '';
                        const schemaInfo = item.SCHEMA ? \`Schema: \${item.SCHEMA}\` : '';
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
