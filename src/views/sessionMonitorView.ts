import * as vscode from 'vscode';
import { ConnectionManager } from '../core/connectionManager';
import { runQueryRaw, queryResultToRows } from '../core/queryRunner';

interface Session {
    ID: number;
    PID: number;
    USERNAME: string;
    DBNAME: string;
    TYPE: string;
    CONNTIME: string;
    STATUS: string;
    IPADDR: string;
    COMMAND: string;
    PRIORITY: number;
    CID: number;
    CLIENT_OS_USERNAME: string;
    [key: string]: unknown;
}

interface QueryInfo {
    QS_SESSIONID: number;
    QS_PLANID: number;
    QS_CLIENTID: number;
    QS_CLIIPADDR: string;
    QS_SQL: string;
    QS_STATE: string;
    QS_TSUBMIT: string;
    QS_TSTART: string;
    QS_PRIORITY: number;
    QS_PRITXT: string;
    QS_ESTCOST: number;
    QS_ESTDISK: number;
    QS_ESTMEM: number;
    QS_SNIPPETS: number;
    QS_CURSNIPT: number;
    QS_RESROWS: number;
    QS_RESBYTES: number;
    [key: string]: unknown;
}

interface StorageInfo {
    DATABASE: string;
    SCHEMA: string;
    ALLOC_MB: number;
    USED_MB: number;
    AVG_SKEW: number;
    TABLE_COUNT: number;
    [key: string]: unknown;
}

interface ResourceData {
    gra: unknown[];
    systemUtil: unknown[];
    sysUtilSummary: unknown;
}

export class SessionMonitorView {
    public static readonly viewType = 'netezza.sessionMonitor';
    private static currentPanel: SessionMonitorView | undefined;
    private _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private _disposables: vscode.Disposable[] = [];
    private _context: vscode.ExtensionContext;
    private _connectionManager: ConnectionManager;
    private _refreshInterval: NodeJS.Timeout | undefined;

    private constructor(
        panel: vscode.WebviewPanel,
        extensionUri: vscode.Uri,
        context: vscode.ExtensionContext,
        connectionManager: ConnectionManager
    ) {
        this._panel = panel;
        this._extensionUri = extensionUri;
        this._context = context;
        this._connectionManager = connectionManager;

        this._update();

        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);

        this._panel.webview.onDidReceiveMessage(
            async message => {
                switch (message.command) {
                    case 'refresh':
                        await this._fetchAndSendData();
                        return;
                    case 'killSession':
                        await this._killSession(message.sessionId);
                        return;
                    case 'toggleAutoRefresh':
                        this._toggleAutoRefresh(message.enabled);
                        return;
                }
            },
            null,
            this._disposables
        );

        // Initial data load
        this._fetchAndSendData();
    }

    public static createOrShow(
        extensionUri: vscode.Uri,
        context: vscode.ExtensionContext,
        connectionManager: ConnectionManager
    ) {
        const column = vscode.window.activeTextEditor ? vscode.ViewColumn.Beside : undefined;

        if (SessionMonitorView.currentPanel) {
            SessionMonitorView.currentPanel._panel.reveal(column);
            SessionMonitorView.currentPanel._fetchAndSendData();
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            SessionMonitorView.viewType,
            'Session Monitor',
            column || vscode.ViewColumn.One,
            {
                enableScripts: true,
                localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'media')],
                retainContextWhenHidden: true
            }
        );

        SessionMonitorView.currentPanel = new SessionMonitorView(panel, extensionUri, context, connectionManager);
    }

    public dispose() {
        SessionMonitorView.currentPanel = undefined;

        if (this._refreshInterval) {
            clearInterval(this._refreshInterval);
        }

        this._panel.dispose();

        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) {
                x.dispose();
            }
        }
    }

    private _toggleAutoRefresh(enabled: boolean) {
        if (enabled) {
            this._refreshInterval = setInterval(() => {
                this._fetchAndSendData();
            }, 30000); // 30 seconds
        } else if (this._refreshInterval) {
            clearInterval(this._refreshInterval);
            this._refreshInterval = undefined;
        }
    }

    private async _killSession(sessionId: number): Promise<void> {
        try {
            const confirmation = await vscode.window.showWarningMessage(
                `Are you sure you want to terminate session ${sessionId}?`,
                { modal: true },
                'Yes, Kill Session'
            );

            if (confirmation !== 'Yes, Kill Session') {
                return;
            }

            const sql = `DROP SESSION ${sessionId}`;
            await runQueryRaw(this._context, sql, true, this._connectionManager, undefined);

            vscode.window.showInformationMessage(`Session ${sessionId} terminated successfully.`);

            // Refresh data
            await this._fetchAndSendData();
        } catch (err: unknown) {
            vscode.window.showErrorMessage(`Failed to kill session: ${err instanceof Error ? err.message : String(err)}`);
        }
    }

    private async _fetchAndSendData(): Promise<void> {
        this._panel.webview.postMessage({ command: 'setLoading', loading: true });

        try {
            const [sessions, queries, storage, resources] = await Promise.all([
                this._fetchSessions(),
                this._fetchQueries(),
                this._fetchStorage(),
                this._fetchResources()
            ]);

            this._panel.webview.postMessage({
                command: 'updateData',
                data: { sessions, queries, storage, resources }
            });
        } catch (err: unknown) {
            this._panel.webview.postMessage({
                command: 'error',
                text: `Failed to fetch data: ${err instanceof Error ? err.message : String(err)}`
            });
        } finally {
            this._panel.webview.postMessage({ command: 'setLoading', loading: false });
        }
    }

    private async _fetchSessions(): Promise<Session[]> {
        const sql = `
            SELECT ID, PID, USERNAME, DBNAME, TYPE, CONNTIME, STATUS, 
                   SUBSTR(COMMAND, 1, 200) AS COMMAND, PRIORITY, CID, IPADDR, CLIENT_OS_USERNAME
            FROM _V_SESSION
            ORDER BY CONNTIME DESC
        `;
        try {
            const result = await runQueryRaw(this._context, sql, true, this._connectionManager, undefined);
            if (!result || !result.data) {
                return [];
            }
            return queryResultToRows<Session>(result);
        } catch (e) {
            console.error('Error fetching sessions:', e);
            return [];
        }
    }

    private async _fetchQueries(): Promise<QueryInfo[]> {
        const sql = `
            SELECT QS_SESSIONID, QS_PLANID, QS_CLIENTID, QS_CLIIPADDR,
                   SUBSTR(QS_SQL, 1, 300) AS QS_SQL, 
                   QS_STATE, QS_TSUBMIT, QS_TSTART, 
                   QS_PRIORITY, QS_PRITXT, QS_ESTCOST, 
                   QS_ESTDISK, QS_ESTMEM, QS_SNIPPETS, QS_CURSNIPT,
                   QS_RESROWS, QS_RESBYTES
            FROM _V_QRYSTAT
            ORDER BY QS_TSTART DESC
            LIMIT 100
        `;
        try {
            const result = await runQueryRaw(this._context, sql, true, this._connectionManager, undefined);
            if (!result || !result.data) {
                return [];
            }
            return queryResultToRows<QueryInfo>(result);
        } catch (e) {
            console.error('Error fetching queries:', e);
            return [];
        }
    }

    private async _fetchStorage(): Promise<StorageInfo[]> {
        const sql = `
            SELECT O.DBNAME AS DATABASE, TS.SCHEMA, 
                   ROUND(SUM(TS.ALLOCATED_BYTES) / 1024.0 / 1024.0, 2) AS ALLOC_MB,
                   ROUND(SUM(TS.USED_BYTES) / 1024.0 / 1024.0, 2) AS USED_MB,
                   ROUND(SUM(TS.SKEW * TS.USED_BYTES) / NULLIF(SUM(TS.USED_BYTES), 0), 2) AS AVG_SKEW,
                   COUNT(*) AS TABLE_COUNT
            FROM _V_TABLE_STORAGE_STAT TS
            JOIN _V_OBJECT_DATA O ON TS.OBJID = O.OBJID
            GROUP BY O.DBNAME, TS.SCHEMA
            ORDER BY USED_MB DESC
        `;
        try {
            const result = await runQueryRaw(this._context, sql, true, this._connectionManager, undefined);
            if (!result || !result.data) {
                return [];
            }
            return queryResultToRows<StorageInfo>(result);
        } catch (e) {
            console.error('Error fetching storage:', e);
            return [];
        }
    }

    private async _fetchResources(): Promise<ResourceData> {
        // Try multiple resource views
        let graData: unknown[] = [];
        let sysUtil: unknown[] = [];
        let sysUtilSummary: unknown = null;

        try {
            const graResult = await runQueryRaw(
                this._context,
                `SELECT * FROM _V_SCHED_GRA_EXT LIMIT 50`,
                true,
                this._connectionManager,
                undefined
            );
            if (graResult && graResult.data) {
                graData = queryResultToRows<Record<string, unknown>>(graResult);
            }
        } catch (e) {
            console.warn('_V_SCHED_GRA_EXT not available:', e);
        }

        try {
            const sysResult = await runQueryRaw(
                this._context,
                `SELECT * FROM _V_SYSTEM_UTIL ORDER BY 1 DESC LIMIT 50`,
                true,
                this._connectionManager,
                undefined
            );
            if (sysResult && sysResult.data) {
                sysUtil = queryResultToRows<Record<string, unknown>>(sysResult);
            }
        } catch (e) {
            console.warn('_V_SYSTEM_UTIL not available:', e);
        }

        // Try to get summary/averages for system utilization
        try {
            const summaryResult = await runQueryRaw(
                this._context,
                `SELECT 
                    ROUND(AVG(HOST_CPU) * 100, 1) AS AVG_HOST_CPU_PCT,
                    ROUND(AVG(SPU_CPU) * 100, 1) AS AVG_SPU_CPU_PCT,
                    ROUND(AVG(HOST_DISK) * 100, 1) AS AVG_DISK_PCT,
                    ROUND(AVG(HOST_MEMORY) * 100, 1) AS AVG_MEMORY_PCT,
                    ROUND(AVG(HOST_FABRIC) * 100, 1) AS AVG_FABRIC_PCT,
                    COUNT(*) AS SAMPLE_COUNT
                FROM _V_SYSTEM_UTIL`,
                true,
                this._connectionManager,
                undefined
            );
            if (summaryResult && summaryResult.data) {
                const parsed = queryResultToRows<Record<string, unknown>>(summaryResult);
                sysUtilSummary = parsed.length > 0 ? parsed[0] : null;
            }
        } catch (e) {
            console.warn('_V_SYSTEM_UTIL summary not available:', e);
        }

        return { gra: graData, systemUtil: sysUtil, sysUtilSummary };


    }

    private _update() {
        const webview = this._panel.webview;
        this._panel.title = 'Session Monitor';
        this._panel.webview.html = this._getHtmlForWebview(webview);
    }

    private _getHtmlForWebview(webview: vscode.Webview): string {
        const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'sessionMonitor.js'));
        const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'sessionMonitor.css'));

        const nonce = getNonce();

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}';">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="${styleUri}" rel="stylesheet">
    <title>Session Monitor</title>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>🖥️ Session Monitor Dashboard</h2>
            <div class="header-actions">
                <label class="auto-refresh">
                    <input type="checkbox" id="autoRefresh"> Auto-refresh (30s)
                </label>
                <button id="refreshBtn" class="btn btn-primary">🔄 Refresh</button>
            </div>
        </div>

        <div class="loading-overlay" id="loadingOverlay">
            <div class="spinner"></div>
            <span>Loading data...</span>
        </div>

        <div class="tabs">
            <button class="tab-btn active" data-tab="sessions">📋 Sessions</button>
            <button class="tab-btn" data-tab="queries">⏱️ Running Queries</button>
            <button class="tab-btn" data-tab="storage">💾 Storage</button>
            <button class="tab-btn" data-tab="resources">📊 Resources</button>
        </div>

        <div class="tab-content" id="sessions">
            <div class="section-header">
                <h3>Active Sessions</h3>
                <span class="count" id="sessionCount">0 sessions</span>
            </div>
            <div class="table-container">
                <table id="sessionsTable">
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>PID</th>
                            <th>User</th>
                            <th>Database</th>
                            <th>Type</th>
                            <th>Connected</th>
                            <th>Status</th>
                            <th>IP</th>
                            <th>Command</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>

        <div class="tab-content hidden" id="queries">
            <div class="section-header">
                <h3>Running Queries</h3>
                <span class="count" id="queryCount">0 queries</span>
            </div>
            <div class="table-container">
                <table id="queriesTable">
                    <thead>
                        <tr>
                            <th>Session</th>
                            <th>Plan ID</th>
                            <th>State</th>
                            <th>Priority</th>
                            <th>Submitted</th>
                            <th>Started</th>
                            <th>Est. Cost</th>
                            <th>Rows</th>
                            <th>SQL</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>

        <div class="tab-content hidden" id="storage">
            <div class="section-header">
                <h3>Storage by Schema</h3>
                <span class="count" id="storageCount">0 schemas</span>
            </div>
            <div class="table-container">
                <table id="storageTable">
                    <thead>
                        <tr>
                            <th>Database</th>
                            <th>Schema</th>
                            <th>Allocated (MB)</th>
                            <th>Used (MB)</th>
                            <th>Usage %</th>
                            <th>Avg Skew</th>
                            <th>Tables</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>

        <div class="tab-content hidden" id="resources">
            <div class="section-header">
                <h3>Resource Utilization</h3>
            </div>
            <div class="resources-grid">
                <div class="resource-section">
                    <h4>GRA Scheduler</h4>
                    <div id="graTable" class="table-container small"></div>
                </div>
                <div class="resource-section">
                    <h4>System Utilization</h4>
                    <div id="sysUtilSummary"></div>
                    <div id="sysUtilTable" class="table-container small"></div>
                </div>
            </div>
        </div>
    </div>

    <script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>`;
    }
}

function getNonce(): string {
    let text = '';
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    for (let i = 0; i < 32; i++) {
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
}
