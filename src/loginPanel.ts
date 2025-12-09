import * as vscode from 'vscode';
import { ConnectionManager, ConnectionDetails } from './connectionManager';

export class LoginPanel {
    public static currentPanel: LoginPanel | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private _disposables: vscode.Disposable[] = [];

    private constructor(panel: vscode.WebviewPanel, private extensionUri: vscode.Uri, private connectionManager: ConnectionManager) {
        this._panel = panel;
        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);
        this._panel.webview.html = this._getHtmlForWebview(this._panel.webview);

        // Handle messages from the webview
        this._panel.webview.onDidReceiveMessage(
            async message => {
                switch (message.command) {
                    case 'save':
                        try {
                            await this.connectionManager.saveConnection(message.data);
                            vscode.window.showInformationMessage(`Connection '${message.data.name}' saved and activated!`);
                            this.sendConnectionsToWebview(); // Refresh list
                        } catch (e: any) {
                            vscode.window.showErrorMessage(`Error saving: ${e.message}`);
                        }
                        return;
                    case 'delete':
                        try {
                            const result = await vscode.window.showWarningMessage(`Are you sure you want to delete '${message.name}'?`, { modal: true }, 'Yes', 'No');
                            if (result === 'Yes') {
                                await this.connectionManager.deleteConnection(message.name);
                                vscode.window.showInformationMessage(`Connection '${message.name}' deleted.`);
                                this.sendConnectionsToWebview();
                            }
                        } catch (e: any) {
                            vscode.window.showErrorMessage(`Error deleting: ${e.message}`);
                        }
                        return;
                    case 'loadConnections':
                        this.sendConnectionsToWebview();
                        return;
                }
            },
            null,
            this._disposables
        );
    }

    private async sendConnectionsToWebview() {
        const connections = this.connectionManager.getConnections();
        const activeName = this.connectionManager.getActiveConnectionName();
        await this._panel.webview.postMessage({ command: 'updateConnections', connections, activeName });
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
                enableScripts: true,
                retainContextWhenHidden: true
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
                body { font-family: var(--vscode-font-family); padding: 0; margin: 0; color: var(--vscode-editor-foreground); background-color: var(--vscode-editor-background); display: flex; height: 100vh; }
                .sidebar { width: 250px; border-right: 1px solid var(--vscode-panel-border); background-color: var(--vscode-sideBar-background); padding: 10px; overflow-y: auto; }
                .main { flex: 1; padding: 20px; overflow-y: auto; }
                
                .connection-item { padding: 8px; cursor: pointer; display: flex; align-items: center; border-radius: 3px; margin-bottom: 2px; }
                .connection-item:hover { background-color: var(--vscode-list-hoverBackground); }
                .connection-item.active { background-color: var(--vscode-list-activeSelectionBackground); color: var(--vscode-list-activeSelectionForeground); }
                .connection-item .name { flex: 1; font-weight: 500; }
                .connection-item .status { font-size: 0.8em; margin-left: 5px; opacity: 0.7; }
                
                .form-group { margin-bottom: 15px; }
                label { display: block; margin-bottom: 5px; font-weight: bold; }
                input { width: 100%; padding: 8px; box-sizing: border-box; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); }
                input:focus { border-color: var(--vscode-focusBorder); outline: none; }
                
                .actions { margin-top: 20px; display: flex; gap: 10px; }
                button { background: var(--vscode-button-background); color: var(--vscode-button-foreground); padding: 8px 16px; border: none; cursor: pointer; border-radius: 2px; }
                button:hover { background: var(--vscode-button-hoverBackground); }
                button.secondary { background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); }
                button.secondary:hover { background: var(--vscode-button-secondaryHoverBackground); }
                button.danger { background: var(--vscode-errorForeground); color: white; }
                
                h2 { margin-top: 0; border-bottom: 1px solid var(--vscode-panel-border); padding-bottom: 10px; margin-bottom: 20px; }
            </style>
        </head>
        <body>
            <div class="sidebar">
                <div style="margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-weight: bold;">Connections</span>
                    <button id="btnNew" style="padding: 4px 8px; font-size: 0.9em;">+</button>
                </div>
                <div id="connectionList"></div>
            </div>
            
            <div class="main">
                <h2 id="formTitle">New Connection</h2>
                <div class="form-group">
                    <label for="name">Connection Name</label>
                    <input type="text" id="name" placeholder="e.g. Production, Dev">
                </div>
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
                
                <div class="actions">
                    <button id="btnSave" onclick="save()">Save & Connect</button>
                    <button id="btnDelete" class="danger" onclick="del()" style="display: none;">Delete</button>
                </div>
            </div>

            <script>
                const vscode = acquireVsCodeApi();
                let connections = [];
                let activeName = null;
                let currentEditName = null;

                // Load initial data
                window.addEventListener('message', event => {
                    const message = event.data;
                    switch (message.command) {
                        case 'updateConnections':
                            connections = message.connections;
                            activeName = message.activeName;
                            renderList();
                            break;
                    }
                });
                
                vscode.postMessage({ command: 'loadConnections' });

                document.getElementById('btnNew').addEventListener('click', () => {
                    clearForm();
                });

                function renderList() {
                    const list = document.getElementById('connectionList');
                    list.innerHTML = '';
                    
                    connections.forEach(conn => {
                        const div = document.createElement('div');
                        div.className = 'connection-item';
                        if (conn.name === currentEditName) { // Mark selected in list
                             // Maybe add style for 'editing'
                             div.style.border = '1px solid var(--vscode-focusBorder)';
                        }
                        
                        div.innerHTML = \`<span class="name">\${conn.name}</span>\`;
                        if (conn.name === activeName) {
                            div.innerHTML += \`<span class="status"> (Active)</span>\`;
                        }
                        
                        div.onclick = () => loadForm(conn);
                        list.appendChild(div);
                    });
                }

                function loadForm(conn) {
                    currentEditName = conn.name;
                    document.getElementById('formTitle').innerText = 'Edit Connection';
                    document.getElementById('name').value = conn.name;
                    document.getElementById('host').value = conn.host;
                    document.getElementById('port').value = conn.port;
                    document.getElementById('database').value = conn.database;
                    document.getElementById('user').value = conn.user;
                    document.getElementById('password').value = conn.password || ''; // Password might not be sent back for security? allow update
                    
                    document.getElementById('btnDelete').style.display = 'block';
                    renderList();
                }

                function clearForm() {
                    currentEditName = null;
                    document.getElementById('formTitle').innerText = 'New Connection';
                    document.getElementById('name').value = '';
                    document.getElementById('host').value = '';
                    document.getElementById('port').value = '5480';
                    document.getElementById('database').value = 'system';
                    document.getElementById('user').value = '';
                    document.getElementById('password').value = '';
                    
                    document.getElementById('btnDelete').style.display = 'none';
                    renderList();
                }

                function save() {
                    const name = document.getElementById('name').value;
                    if (!name) {
                        return; // Add validation UI?
                    }
                    
                    const data = {
                        name: name,
                        host: document.getElementById('host').value,
                        port: parseInt(document.getElementById('port').value),
                        database: document.getElementById('database').value,
                        user: document.getElementById('user').value,
                        password: document.getElementById('password').value
                    };
                    
                    vscode.postMessage({
                        command: 'save',
                        data: data
                    });
                }

                function del() {
                    if (currentEditName) {
                        vscode.postMessage({
                            command: 'delete',
                            name: currentEditName
                        });
                    }
                }
            </script>
        </body>
        </html>`;
    }
}
