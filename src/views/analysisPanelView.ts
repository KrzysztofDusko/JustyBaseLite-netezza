import * as vscode from 'vscode';

export class AnalysisPanelView {
    public static readonly viewType = 'netezza.analysisPanel';
    private _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private _disposables: vscode.Disposable[] = [];

    private constructor(panel: vscode.WebviewPanel, extensionUri: vscode.Uri, data: Record<string, unknown>[]) {
        this._panel = panel;
        this._extensionUri = extensionUri;

        // Set the webview's initial html content
        this._update(data);

        // Listen for when the panel is disposed
        // This happens when the user closes the panel or when the panel is closed programmatically
        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);

        // Handle messages from the webview
        this._panel.webview.onDidReceiveMessage(
            message => {
                switch (message.command) {
                    case 'alert':
                        vscode.window.showErrorMessage(message.text);
                        return;
                    case 'info':
                        vscode.window.showInformationMessage(message.text);
                        return;
                }
            },
            null,
            this._disposables
        );
    }

    public static createOrShow(extensionUri: vscode.Uri, data: Record<string, unknown>[]) {
        const column = vscode.window.activeTextEditor ? vscode.ViewColumn.Beside : undefined;

        // Create a new panel
        const panel = vscode.window.createWebviewPanel(
            AnalysisPanelView.viewType,
            'Data Analysis',
            column || vscode.ViewColumn.One,
            {
                // Enable javascript in the webview
                enableScripts: true,
                // And restrict the webview to only loading content from our extension's media directory.
                localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'media')],
                retainContextWhenHidden: true
            }
        );

        new AnalysisPanelView(panel, extensionUri, data);
    }

    public dispose() {
        this._panel.dispose();

        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) {
                x.dispose();
            }
        }
    }

    private _update(data: Record<string, unknown>[]) {
        const webview = this._panel.webview;
        this._panel.title = 'Data Analysis';
        this._panel.webview.html = this._getHtmlForWebview(webview, data);
    }

    private _getHtmlForWebview(webview: vscode.Webview, initialData: Record<string, unknown>[]) {
        // Local path to main script run in the webview
        const scriptPathOnDisk = vscode.Uri.joinPath(this._extensionUri, 'media', 'analysisPanel.js');
        const scriptUri = webview.asWebviewUri(scriptPathOnDisk);

        // Local path to css styles
        const stylePath = vscode.Uri.joinPath(this._extensionUri, 'media', 'analysisPanel.css');
        const styleUri = webview.asWebviewUri(stylePath);

        // TanStack Table and Virtual (Assuming they are in media folder as seen in resultPanel.js references)
        // We reuse the same libraries
        const tableCoreUri = webview.asWebviewUri(
            vscode.Uri.joinPath(this._extensionUri, 'media', 'tanstack-table-core.js')
        );
        const virtualCoreUri = webview.asWebviewUri(
            vscode.Uri.joinPath(this._extensionUri, 'media', 'tanstack-virtual-core.js')
        );

        const nonce = getNonce();

        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}'; font-src ${webview.cspSource};">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <link href="${styleUri}" rel="stylesheet">

            <title>Data Analysis</title>
        </head>
        <body>
            <div id="app">
                <div class="sidebar">
                    <div class="section">
                        <h3>Fields</h3>
                        <div id="fieldList" class="field-list"></div>
                    </div>
                </div>
                <div class="main-content">
                    <div class="filter-section" style="margin-bottom: 15px; display: flex; gap: 10px; align-items: center;">
                        <label for="dataFilter" style="font-weight: bold;">Filter:</label>
                        <input type="text" id="dataFilter" placeholder="Type to filter data..." style="flex: 1; padding: 6px 10px; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); border-radius: 4px;">
                        <button id="applyFilter" style="padding: 6px 12px; cursor: pointer;">Apply</button>
                        <button id="clearFilter" style="padding: 6px 12px; cursor: pointer;">Clear</button>
                    </div>
                    <div class="config-zones">
                        <div class="zone-container">
                            <h4>Rows</h4>
                            <div id="rowsZone" class="drop-zone" data-zone="rows"></div>
                        </div>
                        <div class="zone-container">
                            <h4>Columns</h4>
                            <div id="colsZone" class="drop-zone" data-zone="cols"></div>
                        </div>
                        <div class="zone-container">
                            <h4>Values</h4>
                            <div id="valuesZone" class="drop-zone" data-zone="values"></div>
                        </div>
                    </div>
                    <div class="pivot-grid-container">
                        <div id="pivotGrid"></div>
                    </div>
                    <div class="chart-container" style="margin-top: 20px;">
                        <div class="chart-header" style="display: flex; align-items: center; gap: 10px; margin-bottom: 10px;">
                            <h4 style="margin: 0;">Chart</h4>
                            <select id="chartType" style="padding: 4px;">
                                <option value="bar">Bar Chart</option>
                                <option value="line">Line Chart</option>
                            </select>
                        </div>
                        <canvas id="pivotChart" width="600" height="300" style="background: var(--vscode-editor-background); border: 1px solid var(--vscode-panel-border); border-radius: 4px;"></canvas>
                    </div>
                </div>
            </div>
            
            <script nonce="${nonce}" src="${tableCoreUri}"></script>
            <script nonce="${nonce}" src="${virtualCoreUri}"></script>
            <script nonce="${nonce}">
                // Pass initial data to the script
                window.initialData = ${JSON.stringify(initialData)};
                window.TableCore = TableCore;
                window.VirtualCore = VirtualCore;
            </script>
            <script nonce="${nonce}" src="${scriptUri}"></script>
        </body>
        </html>`;
    }
}

function getNonce() {
    let text = '';
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    for (let i = 0; i < 32; i++) {
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
}
