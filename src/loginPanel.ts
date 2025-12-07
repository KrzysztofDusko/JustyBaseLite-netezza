import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';

export class LoginPanel {
    public static currentPanel: LoginPanel | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private _disposables: vscode.Disposable[] = [];

    private constructor(panel: vscode.WebviewPanel, private extensionUri: vscode.Uri, private connectionManager: ConnectionManager) {
        this._panel = panel;
        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);
        this._panel.webview.html = this._getHtmlForWebview(this._panel.webview);
        this._panel.webview.onDidReceiveMessage(
            async message => {
                switch (message.command) {
                    case 'save':
                        await this.connectionManager.saveConnection(message.data);
                        vscode.window.showInformationMessage('Connection saved!');
                        this.dispose();
                        return;
                }
            },
            null,
            this._disposables
        );
    }

    public static createOrShow(extensionUri: vscode.Uri, connectionManager: ConnectionManager) {
        const column = vscode.window.activeTextEditor ? vscode.window.activeTextEditor.viewColumn : undefined;

        if (LoginPanel.currentPanel) {
            LoginPanel.currentPanel._panel.reveal(column);
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'netezzaLogin',
            'Connect to Netezza',
            column || vscode.ViewColumn.One,
            {
                enableScripts: true
            }
        );

        LoginPanel.currentPanel = new LoginPanel(panel, extensionUri, connectionManager);
    }

    public dispose() {
        LoginPanel.currentPanel = undefined;
        this._panel.dispose();
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) {
                x.dispose();
            }
        }
    }

    private _getHtmlForWebview(webview: vscode.Webview) {
        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Connect to Netezza</title>
            <style>
                body { font-family: var(--vscode-font-family); padding: 20px; color: var(--vscode-editor-foreground); background-color: var(--vscode-editor-background); }
                .form-group { margin-bottom: 15px; }
                label { display: block; margin-bottom: 5px; }
                input { width: 100%; padding: 8px; box-sizing: border-box; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); }
                button { background: var(--vscode-button-background); color: var(--vscode-button-foreground); padding: 10px 20px; border: none; cursor: pointer; }
                button:hover { background: var(--vscode-button-hoverBackground); }
            </style>
        </head>
        <body>
            <h2>Connect to Netezza</h2>
            <div class="form-group">
                <label for="host">Host</label>
                <input type="text" id="host" placeholder="nzhost">
            </div>
            <div class="form-group">
                <label for="port">Port</label>
                <input type="number" id="port" value="5480">
            </div>
            <div class="form-group">
                <label for="database">Database</label>
                <input type="text" id="database" placeholder="system">
            </div>
            <div class="form-group">
                <label for="user">User</label>
                <input type="text" id="user" placeholder="admin">
            </div>
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password">
            </div>
            <button onclick="save()">Save Connection</button>

            <script>
                const vscode = acquireVsCodeApi();
                function save() {
                    const host = document.getElementById('host').value;
                    const port = document.getElementById('port').value;
                    const database = document.getElementById('database').value;
                    const user = document.getElementById('user').value;
                    const password = document.getElementById('password').value;
                    
                    vscode.postMessage({
                        command: 'save',
                        data: { host, port, database, user, password }
                    });
                }
            </script>
        </body>
        </html>`;
    }
}
