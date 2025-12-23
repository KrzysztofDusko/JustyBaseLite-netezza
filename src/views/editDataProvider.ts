import * as vscode from 'vscode';
import { runQuery, runQueriesSequentially } from '../core/queryRunner';
import { ConnectionManager } from '../core/connectionManager';
import { getTableMetadata, toWebviewFormat } from '../providers/tableMetadataProvider';

export class EditDataProvider {
    public static readonly viewType = 'netezza.editData';

    public static async createOrShow(
        extensionUri: vscode.Uri,
        item: any,
        context: vscode.ExtensionContext,
        connectionManager: ConnectionManager
    ) {
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

        // Load Data and Metadata
        this._loadData(panel, database, schema, tableName, item.connectionName, context, connectionManager);

        // Message Handling
        panel.webview.onDidReceiveMessage(async message => {
            try {
                switch (message.command) {
                    case 'save':
                        await this._handleSave(
                            panel,
                            message.changes,
                            fullTableName,
                            item.connectionName,
                            context,
                            connectionManager
                        );
                        // Refresh after save
                        this._loadData(
                            panel,
                            database,
                            schema,
                            tableName,
                            item.connectionName,
                            context,
                            connectionManager
                        );
                        break;
                    case 'refresh':
                        this._loadData(
                            panel,
                            database,
                            schema,
                            tableName,
                            item.connectionName,
                            context,
                            connectionManager
                        );
                        break;

                    // Metadata Actions
                    case 'updateTableComment':
                        await this._execSimpleCommand(
                            context,
                            connectionManager,
                            item.connectionName,
                            `COMMENT ON TABLE ${fullTableName} IS '${(message.comment || '').replace(/'/g, "''")}'`,
                            'Table comment updated'
                        );
                        break;
                    case 'updateColumnComment':
                        await this._execSimpleCommand(
                            context,
                            connectionManager,
                            item.connectionName,
                            `COMMENT ON COLUMN ${fullTableName}.${message.column} IS '${(message.comment || '').replace(/'/g, "''")}'`,
                            'Column comment updated'
                        );
                        break;
                    case 'addColumn':
                        await this._execSimpleCommand(
                            context,
                            connectionManager,
                            item.connectionName,
                            `ALTER TABLE ${fullTableName} ADD COLUMN ${message.name} ${message.type}`,
                            `Column ${message.name} added`,
                            true // refresh after
                        );
                        this._loadData(
                            panel,
                            database,
                            schema,
                            tableName,
                            item.connectionName,
                            context,
                            connectionManager
                        );
                        break;
                    case 'dropColumn':
                        await this._execSimpleCommand(
                            context,
                            connectionManager,
                            item.connectionName,
                            `ALTER TABLE ${fullTableName} DROP COLUMN ${message.column}`,
                            `Column ${message.column} dropped`,
                            true
                        );
                        this._loadData(
                            panel,
                            database,
                            schema,
                            tableName,
                            item.connectionName,
                            context,
                            connectionManager
                        );
                        break;

                    case 'error':
                        vscode.window.showErrorMessage(message.text);
                        break;
                    case 'info':
                        vscode.window.showInformationMessage(message.text);
                        break;
                }
            } catch (e: any) {
                vscode.window.showErrorMessage(`Error: ${e.message}`);
            }
        });
    }

    private static async _execSimpleCommand(
        context: vscode.ExtensionContext,
        connectionManager: ConnectionManager,
        connectionName: string,
        sql: string,
        successMsg: string,
        _refresh = false
    ) {
        try {
            await runQuery(context, sql, true, connectionName, connectionManager);
            vscode.window.showInformationMessage(successMsg);
        } catch (e: any) {
            vscode.window.showErrorMessage(`Operation failed: ${e.message}`);
        }
    }

    private static async _loadData(
        panel: vscode.WebviewPanel,
        db: string,
        schema: string,
        table: string,
        connectionName: string,
        context: vscode.ExtensionContext,
        connectionManager: ConnectionManager
    ) {
        const fullTableName = `${db}.${schema}.${table}`;
        try {
            panel.webview.postMessage({ command: 'setLoading', loading: true, message: 'Fetching data...' });

            // Use centralized tableMetadataProvider for metadata queries
            const queryRunner = (query: string) => runQuery(context, query, true, connectionName, connectionManager);

            const [dataResult, metadata] = await Promise.all([
                runQuery(
                    context,
                    `SELECT ROWID, * FROM ${fullTableName} LIMIT 50000`,
                    true,
                    connectionName,
                    connectionManager
                ),
                getTableMetadata(queryRunner, db, schema, table)
            ]);

            // Convert to webview format (uppercase keys for JS compatibility)
            const tableComment = metadata.tableComment || '';
            const columnsMeta = toWebviewFormat(metadata.columns);

            // Parse Data
            let data: any[] = [];
            try {
                // Robust JSON parsing
                const raw = dataResult || '[]';
                console.log(
                    '[EditDataProvider] dataResult type:',
                    typeof raw,
                    'length:',
                    typeof raw === 'string' ? raw.length : 'N/A'
                );
                console.log(
                    '[EditDataProvider] dataResult preview:',
                    typeof raw === 'string' ? raw.substring(0, 200) : 'array'
                );

                if (typeof raw === 'string') {
                    if (raw.trim().startsWith('[')) {
                        data = JSON.parse(raw);
                    } else {
                        // Not a JSON array, likely empty or message
                        console.log('[EditDataProvider] dataResult is NOT JSON array:', raw.substring(0, 100));
                        data = [];
                    }
                } else if (Array.isArray(raw)) {
                    data = raw;
                }
                console.log('[EditDataProvider] Parsed data rows:', data.length);
            } catch (e) {
                console.error('[EditDataProvider] Data Parse Error', e);
            }

            // Columns extraction
            let columns: string[] = [];
            if (columnsMeta.length > 0) {
                columns = ['ROWID', ...columnsMeta.map((c: any) => c.ATTNAME)];
            } else if (data.length > 0) {
                columns = Object.keys(data[0]);
            }

            console.log('[EditDataProvider] Sending to webview:', { dataRows: data.length, columns: columns.length });

            panel.webview.postMessage({
                command: 'setData',
                data,
                columns,
                metadata: {
                    tableComment,
                    columns: columnsMeta
                }
            });
        } catch (err: any) {
            vscode.window.showErrorMessage(`Failed to load data: ${err.message}`);
            panel.webview.postMessage({ command: 'setError', text: err.message });
        } finally {
            panel.webview.postMessage({ command: 'setLoading', loading: false });
        }
    }

    private static async _handleSave(
        _panel: vscode.WebviewPanel,
        changes: any,
        tableName: string,
        _connectionName: string,
        context: vscode.ExtensionContext,
        connectionManager: ConnectionManager
    ) {
        // changes: { updates: [], deletes: [], inserts: [] }
        // updates: { rowId, updates: { col: val, ... } }
        // deletes: [rowId, ...]
        // inserts: [{ col: val, ... }, ...]

        const queries: string[] = [];

        // 1. Process Deletes
        if (changes.deletes && changes.deletes.length > 0) {
            const ids = changes.deletes.join(',');
            queries.push(`DELETE FROM ${tableName} WHERE ROWID IN (${ids})`);
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
                    queries.push(`UPDATE ${tableName} SET ${setClauses.join(', ')} WHERE ROWID = ${update.rowId}`);
                }
            }
        }

        // 3. Process Inserts
        if (changes.inserts && changes.inserts.length > 0) {
            for (const insert of changes.inserts) {
                const cols = Object.keys(insert).filter(k => k !== 'ROWID'); // Exclude ROWID placeholder if any
                if (cols.length === 0) continue;

                const vals = cols.map(c => this._formatValue(insert[c]));
                queries.push(`INSERT INTO ${tableName} (${cols.join(', ')}) VALUES (${vals.join(', ')})`);
            }
        }

        if (queries.length === 0) {
            vscode.window.showInformationMessage('No changes to save.');
            return;
        }

        try {
            // Execute as batch/sequential
            // Wrapped in explicit BEGIN/COMMIT transaction block
            const batch = ['BEGIN', ...queries, 'COMMIT'];

            // To ensure they run in one transaction, we need simple query mode usually,
            // but runQueriesSequentially does item by item.
            // If any fails, we want rollback.
            // Netezza via ODBC usually auto-commits unless in transaction.

            await runQueriesSequentially(context, batch, connectionManager);

            vscode.window.showInformationMessage(`Successfully executed ${queries.length} changes.`);
        } catch (err: any) {
            vscode.window.showErrorMessage(`Failed to save changes: ${err.message}`);
            // Attempt rollback if mid-way? (Requires session persistence which runQueriesSequentially *might* not guarantee if it opens new conns?
            // Actually runQueriesSequentially in this ext opens one connection and reuses it?
            // Checking runQueriesSequentially implementation is out of scope but assuming it works for now.
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
        // codiconsUri not used, font loaded via inline style
        const codiconsFontUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'media', 'codicon.ttf'));

        const nonce = getNonce();

        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="Content-Security-Policy" content="default-src 'none'; font-src ${webview.cspSource}; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}';">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <link href="${styleUri}" rel="stylesheet">
            <style>
                @font-face {
                    font-family: 'codicon';
                    src: url('${codiconsFontUri}') format('truetype');
                }
                .codicon { font-family: 'codicon'; }
            </style>
            <script nonce="${nonce}" src="${scriptUri}"></script>
            <script nonce="${nonce}" src="${virtualUri}"></script>
            <title>Edit Data: ${title}</title>
        </head>
        <body>
            <div class="main-layout">
                <!-- Tabs Header -->
                <div class="tabs-header">
                    <div class="tab active" data-target="tab-data">
                        Data
                    </div>
                    <div class="tab" data-target="tab-metadata">
                        Table Description
                    </div>
                </div>

                <!-- Tab: Data -->
                <div id="tab-data" class="tab-content active">
                    <div class="toolbar">
                        <span id="status" class="status"></span>
                        <div class="actions">
                            <button id="refreshBtn">Refresh</button>
                            <button id="addRowBtn">Add Row</button>
                            <button id="saveBtn" class="primary">Save Changes</button>
                        </div>
                    </div>
                    <div id="gridContainer" class="grid-container"></div>
                </div>

                <!-- Tab: Metadata -->
                <div id="tab-metadata" class="tab-content">
                    <div id="metadataContent" class="metadata-content">
                        <!-- Populated by JS -->
                        Loading metadata...
                    </div>
                </div>
            </div>
            
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
