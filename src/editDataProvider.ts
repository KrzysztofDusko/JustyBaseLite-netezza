import * as vscode from 'vscode';
import { runQuery, runQueriesSequentially } from './queryRunner';
import { ConnectionManager } from './connectionManager';

export class EditDataProvider {
    public static readonly viewType = 'netezza.editData';

    public static async createOrShow(extensionUri: vscode.Uri, item: any, context: vscode.ExtensionContext, connectionManager: ConnectionManager) {
        if (!item || !item.label || !item.dbName || !item.schema) {
            vscode.window.showErrorMessage('Invalid table selection');
            return;
        }

        const tableName = item.label;
        const schema = item.schema;
        const database = item.dbName;
        const fullTableName = `${database}.${schema}.${tableName}`;

        // Create Webview Panel
        const panel = vscode.window.createWebviewPanel(
            EditDataProvider.viewType,
            `Edit: ${tableName}`,
            vscode.ViewColumn.One,
            {
                enableScripts: true,
                localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'media')],
                retainContextWhenHidden: true
            }
        );

        // Set HTML
        panel.webview.html = this._getHtmlForWebview(panel.webview, extensionUri, fullTableName);

        // Load Data
        this._loadData(panel, fullTableName, item.connectionName, context, connectionManager);

        // Message Handling
        panel.webview.onDidReceiveMessage(async (message) => {
            switch (message.command) {
                case 'save':
                    await this._handleSave(panel, message.changes, fullTableName, item.connectionName, context, connectionManager);
                    break;
                case 'refresh':
                    this._loadData(panel, fullTableName, item.connectionName, context, connectionManager);
                    break;
                case 'error':
                    vscode.window.showErrorMessage(message.text);
                    break;
                case 'info':
                    vscode.window.showInformationMessage(message.text);
                    break;
            }
        });
    }

    private static async _loadData(panel: vscode.WebviewPanel, tableName: string, connectionName: string, context: vscode.ExtensionContext, connectionManager: ConnectionManager) {
        try {
            // Check if ROWID exists or just select * and ROWID
            // Netezza usually has ROWID virtual column.
            const sql = `SELECT ROWID, * FROM ${tableName} LIMIT 50000`;

            panel.webview.postMessage({ command: 'setLoading', loading: true });

            // We use runQueryRaw logic via runQuery wrapper which returns JSON string currently
            // We might need raw array for better performance or just parse the JSON
            const resultJson = await runQuery(context, sql, true, connectionName, connectionManager);

            if (!resultJson) {
                panel.webview.postMessage({ command: 'setData', data: [], columns: [] });
                return;
            }

            // Parse result if it's a string (runQuery returns string | undefined)
            let data = [];
            if (typeof resultJson === 'string') {
                try {
                    data = JSON.parse(resultJson);
                } catch (e) {
                    // It might be a message string
                    if (resultJson.startsWith('Query executed')) {
                        data = [];
                    } else {
                        throw new Error('Failed to parse result data');
                    }
                }
            }

            // Extract columns from first row if available
            let columns: string[] = [];
            if (data.length > 0) {
                columns = Object.keys(data[0]);
            } else {
                // Should fetch columns definition if empty? For now empty is fine.
            }

            panel.webview.postMessage({ command: 'setData', data, columns });

        } catch (err: any) {
            vscode.window.showErrorMessage(`Failed to load data: ${err.message}`);
            panel.webview.postMessage({ command: 'setError', text: err.message });
        } finally {
            panel.webview.postMessage({ command: 'setLoading', loading: false });
        }
    }

    private static async _handleSave(panel: vscode.WebviewPanel, changes: any, tableName: string, connectionName: string, context: vscode.ExtensionContext, connectionManager: ConnectionManager) {
        // changes: { updates: [], deletes: [], inserts: [] }
        // updates: { rowId, updates: { col: val, ... } }
        // deletes: [rowId, ...]
        // inserts: [{ col: val, ... }, ...]

        const queries: string[] = [];

        // 1. Process Deletes
        if (changes.deletes && changes.deletes.length > 0) {
            const ids = changes.deletes.join(',');
            queries.push(`DELETE FROM ${tableName} WHERE ROWID IN (${ids});`);
        }

        // 2. Process Updates
        if (changes.updates && changes.updates.length > 0) {
            for (const update of changes.updates) {
                const setClauses: string[] = [];
                for (const [col, val] of Object.entries(update.changes)) {
                    if (col === 'ROWID') continue; // Skip ROWID
                    setClauses.push(`${col} = ${this._formatValue(val)}`);
                }
                if (setClauses.length > 0) {
                    queries.push(`UPDATE ${tableName} SET ${setClauses.join(', ')} WHERE ROWID = ${update.rowId};`);
                }
            }
        }

        // 3. Process Inserts
        if (changes.inserts && changes.inserts.length > 0) {
            for (const insert of changes.inserts) {
                const cols = Object.keys(insert).filter(k => k !== 'ROWID'); // Exclude ROWID placeholder if any
                if (cols.length === 0) continue;

                const vals = cols.map(c => this._formatValue(insert[c]));
                queries.push(`INSERT INTO ${tableName} (${cols.join(', ')}) VALUES (${vals.join(', ')});`);
            }
        }

        if (queries.length === 0) {
            vscode.window.showInformationMessage('No changes to save.');
            return;
        }

        try {
            // Execute as batch/sequential
            await runQueriesSequentially(context, ['BEGIN;', ...queries, 'COMMIT;'], connectionManager); // Wrap in trans if possible, Netezza supports BEGIN/COMMIT? Yes usually.

            vscode.window.showInformationMessage(`Successfully executed ${queries.length} changes.`);

            // Reload data
            this._loadData(panel, tableName, connectionName, context, connectionManager);

        } catch (err: any) {
            vscode.window.showErrorMessage(`Failed to save changes: ${err.message}`);
        }
    }

    private static _formatValue(val: any): string {
        if (val === null || val === undefined || val === '') return 'NULL'; // Empty string as NULL? For editing usually yes
        if (typeof val === 'number') return val.toString();
        if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';
        // String escaping
        return `'${String(val).replace(/'/g, "''")}'`;
    }

    private static _getHtmlForWebview(webview: vscode.Webview, extensionUri: vscode.Uri, title: string) {
        const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'media', 'tanstack-table-core.js'));
        const virtualUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'media', 'tanstack-virtual-core.js'));
        const mainScriptUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'media', 'editDataPanel.js'));
        const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'media', 'editDataPanel.css'));
        const codiconsUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'media', 'codicon.css')); // If we have it, or use existing CSS from extension

        const nonce = getNonce();

        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}';">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <link href="${styleUri}" rel="stylesheet">
            <script nonce="${nonce}" src="${scriptUri}"></script>
            <script nonce="${nonce}" src="${virtualUri}"></script>
            <title>Edit Data: ${title}</title>
        </head>
        <body>
            <div class="toolbar">
                <span class="title">${title}</span>
                <span id="status" class="status"></span>
                <div class="actions">
                    <button id="refreshBtn">Refresh</button>
                    <button id="addRowBtn">Add Row</button>
                    <button id="saveBtn" class="primary">Save Changes</button>
                </div>
            </div>
            <div id="gridContainer" class="grid-container"></div>
            
            <script nonce="${nonce}" src="${mainScriptUri}"></script>
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
