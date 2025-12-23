import * as vscode from 'vscode';
import { ConnectionManager } from '../core/connectionManager';

export class LoginPanel {
    public static currentPanel: LoginPanel | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private _disposables: vscode.Disposable[] = [];

    private constructor(
        panel: vscode.WebviewPanel,
        private extensionUri: vscode.Uri,
        private connectionManager: ConnectionManager
    ) {
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
                            vscode.window.showInformationMessage(
                                `Connection '${message.data.name}' saved and activated!`
                            );
                            this.sendConnectionsToWebview(); // Refresh list
                        } catch (e: any) {
                            vscode.window.showErrorMessage(`Error saving: ${e.message}`);
                        }
                        return;
                    case 'delete':
                        try {
                            const result = await vscode.window.showWarningMessage(
                                `Are you sure you want to delete '${message.name}'?`,
                                { modal: true },
                                'Yes',
                                'No'
                            );
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
        const connections = await this.connectionManager.getConnections();
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
        const iconUri = webview.asWebviewUri(vscode.Uri.joinPath(this.extensionUri, 'netezza_icon64.png'));

        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Connect to Netezza</title>
            <style>
                :root {
                    --container-paddding: 20px;
                    --input-padding-vertical: 6px;
                    --input-padding-horizontal: 8px;
                    --input-margin-vertical: 4px;
                    --input-margin-horizontal: 0;
                }
                body {
                    font-family: var(--vscode-font-family);
                    padding: 0;
                    margin: 0;
                    color: var(--vscode-foreground);
                    background-color: var(--vscode-editor-background);
                    display: flex;
                    height: 100vh;
                    overflow: hidden;
                }
                
                /* Sidebar */
                .sidebar {
                    width: 260px;
                    background-color: var(--vscode-sideBar-background);
                    border-right: 1px solid var(--vscode-panel-border);
                    display: flex;
                    flex-direction: column;
                    flex-shrink: 0;
                    user-select: none;
                }
                .sidebar-header {
                    padding: 10px 15px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    border-bottom: 1px solid var(--vscode-panel-border);
                    background-color: var(--vscode-sideBarSectionHeader-background);
                }
                .sidebar-title {
                    font-weight: bold;
                    font-size: 11px;
                    text-transform: uppercase;
                    color: var(--vscode-sideBarTitle-foreground);
                }
                .connection-list {
                    flex: 1;
                    overflow-y: auto;
                    padding: 0;
                }
                .connection-item {
                    padding: 8px 15px;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    border-left: 3px solid transparent;
                    color: var(--vscode-sideBar-foreground);
                }
                .connection-item:hover {
                    background-color: var(--vscode-list-hoverBackground);
                }
                .connection-item.active {
                    background-color: var(--vscode-list-activeSelectionBackground);
                    color: var(--vscode-list-activeSelectionForeground);
                    border-left-color: var(--vscode-focusBorder);
                }
                .connection-item .name {
                    flex: 1;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                }
                .connection-item .status {
                    font-size: 0.8em;
                    margin-left: 5px;
                    opacity: 0.7;
                }

                /* Main Content */
                .main {
                    flex: 1;
                    padding: 40px;
                    overflow-y: auto;
                    display: flex;
                    justify-content: center;
                    align-items: flex-start;
                }
                .form-container {
                    width: 100%;
                    max-width: 500px;
                    background-color: var(--vscode-editorWidget-background);
                    border: 1px solid var(--vscode-widget-border);
                    padding: 30px;
                    border-radius: 4px;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                }
                
                h2 {
                    margin-top: 0;
                    margin-bottom: 25px;
                    font-size: 1.4em;
                    font-weight: 500;
                    padding-bottom: 10px;
                    border-bottom: 1px solid var(--vscode-panel-border);
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }

                .form-group {
                    margin-bottom: 18px;
                }
                .form-row {
                    display: flex;
                    gap: 15px;
                }
                .form-col {
                    flex: 1;
                }

                label {
                    display: block;
                    margin-bottom: 6px;
                    font-weight: 600;
                    font-size: 12px;
                    color: var(--vscode-input-placeholderForeground);
                    display: flex;
                    align-items: center;
                    gap: 6px;
                }

                input, select {
                    width: 100%;
                    padding: 8px 10px;
                    box-sizing: border-box;
                    background: var(--vscode-input-background);
                    color: var(--vscode-input-foreground);
                    border: 1px solid var(--vscode-input-border);
                    border-radius: 2px;
                    font-family: inherit;
                    font-size: 13px;
                }
                input:focus, select:focus {
                    border-color: var(--vscode-focusBorder);
                    outline: 1px solid var(--vscode-focusBorder);
                }
                
                /* Buttons */
                .actions {
                    margin-top: 30px;
                    display: flex;
                    gap: 12px;
                    padding-top: 20px;
                    border-top: 1px solid var(--vscode-panel-border);
                }
                button {
                    background: var(--vscode-button-background);
                    color: var(--vscode-button-foreground);
                    padding: 8px 16px;
                    border: none;
                    cursor: pointer;
                    border-radius: 2px;
                    font-size: 13px;
                    font-weight: 500;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }
                button:hover {
                    background: var(--vscode-button-hoverBackground);
                }
                button.secondary {
                    background: var(--vscode-button-secondaryBackground);
                    color: var(--vscode-button-secondaryForeground);
                }
                button.secondary:hover {
                    background: var(--vscode-button-secondaryHoverBackground);
                }
                button.danger {
                    background: var(--vscode-errorForeground);
                    color: white;
                }
                button.icon-btn {
                    padding: 4px;
                    background: transparent;
                    color: var(--vscode-icon-foreground);
                }
                button.icon-btn:hover {
                    background: var(--vscode-toolbar-hoverBackground);
                }

                .icon-img {
                    width: 16px;
                    height: 16px;
                    object-fit: contain;
                }
                .logo-header {
                    width: 32px;
                    height: 32px;
                    margin-right: 10px;
                }

            </style>
        </head>
        <body>
            <div class="sidebar">
                <div class="sidebar-header">
                    <span class="sidebar-title">Saved Connections</span>
                    <button class="icon-btn" id="btnNew" title="New Connection">
                        <svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M14 7v1H8v6H7V8H1V7h6V1h1v6h6z"/></svg>
                    </button>
                </div>
                <div id="connectionList" class="connection-list"></div>
            </div>
            
            <div class="main">
                <div class="form-container">
                    <h2 id="formTitle">
                        <img src="${iconUri}" class="logo-header" />
                        New Connection
                    </h2>
                    
                    <div class="form-group">
                        <label for="name">Connection Name</label>
                        <input type="text" id="name" placeholder="Friendly name (e.g. Production)">
                    </div>

                    <div class="form-group">
                        <label for="dbType">
                            Database Type 
                            <img src="${iconUri}" class="icon-img" />
                        </label>
                        <select id="dbType">
                            <option value="NetezzaSQL">NetezzaSQL</option>
                        </select>
                    </div>

                    <div class="form-row">
                        <div class="form-col">
                            <div class="form-group">
                                <label for="host">Host</label>
                                <input type="text" id="host" placeholder="Hostname or IP">
                            </div>
                        </div>
                        <div class="form-col" style="flex: 0 0 80px;">
                            <div class="form-group">
                                <label for="port">Port</label>
                                <input type="number" id="port" value="5480">
                            </div>
                        </div>
                    </div>

                    <div class="form-group">
                        <label for="database">
                            Database
                        </label>
                        <input type="text" id="database" placeholder="Database name" value="system">
                    </div>

                    <div class="form-row">
                         <div class="form-col">
                            <div class="form-group">
                                <label for="user">User</label>
                                <input type="text" id="user" placeholder="Username">
                            </div>
                         </div>
                         <div class="form-col">
                            <div class="form-group">
                                <label for="password">Password</label>
                                <input type="password" id="password" placeholder="Password">
                            </div>
                         </div>
                    </div>
                    
                    <div class="actions">
                        <button id="btnSave" onclick="save()">Save & Connect</button>
                        <button id="btnDelete" class="danger" onclick="del()" style="display: none;">Delete</button>
                    </div>
                </div>
            </div>

            <script>
                const vscode = acquireVsCodeApi();
                let connections = [];
                let activeName = null;
                let currentEditName = null;
                const iconSrc = "${iconUri}";

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
                        if (conn.name === currentEditName) {
                             div.classList.add('active');
                        }
                        
                        div.innerHTML = \`<span class="name"><img src="\${iconSrc}" class="icon-img"> \${conn.name}</span>\`;
                        if (conn.name === activeName) {
                            div.innerHTML += \`<span class="status">●</span>\`;
                            div.title = 'Active Connection';
                        }
                        
                        div.onclick = () => loadForm(conn);
                        list.appendChild(div);
                    });
                }

                function loadForm(conn) {
                    currentEditName = conn.name;
                    document.getElementById('formTitle').innerHTML = \`<img src="\${iconSrc}" class="logo-header" /> Edit Connection\`;
                    document.getElementById('name').value = conn.name;
                    document.getElementById('dbType').value = conn.dbType || 'NetezzaSQL';
                    document.getElementById('host').value = conn.host;
                    document.getElementById('port').value = conn.port;
                    document.getElementById('database').value = conn.database;
                    document.getElementById('user').value = conn.user;
                    document.getElementById('password').value = conn.password || ''; 
                    
                    document.getElementById('btnDelete').style.display = 'block';
                    renderList();
                }

                function clearForm() {
                    currentEditName = null;
                    document.getElementById('formTitle').innerHTML = \`<img src="\${iconSrc}" class="logo-header" /> New Connection\`;
                    document.getElementById('name').value = '';
                    document.getElementById('dbType').value = 'NetezzaSQL';
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
                        dbType: document.getElementById('dbType').value,
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
