import * as vscode from 'vscode';

export interface ConnectionDetails {
    name: string;
    host: string;
    port: number;
    database: string;
    user: string;
    password?: string;
}

export class ConnectionManager {
    private static readonly SERVICE_NAME = 'netezza-vscode-connections';
    private static readonly ACTIVE_CONN_KEY = 'netezza-active-connection';

    // Cache of connection details: { [name]: details }
    private _connections: Record<string, ConnectionDetails> = {};

    // Active connection name
    private _activeConnectionName: string | null = null;

    // Per-document connection selection: Map<documentUri, connectionName>
    private _documentConnections: Map<string, string> = new Map();

    // Persistent connections: { [name]: odbc_connection }
    private _persistentConnections: Map<string, any> = new Map();

    private _keepConnectionOpen: boolean = false;

    // Promise that resolves when connections are loaded
    private _loadingPromise: Promise<void>;

    // Event emitter for connection changes
    private _onDidChangeConnections = new vscode.EventEmitter<void>();
    readonly onDidChangeConnections = this._onDidChangeConnections.event;

    private _onDidChangeActiveConnection = new vscode.EventEmitter<string | null>();
    readonly onDidChangeActiveConnection = this._onDidChangeActiveConnection.event;

    private _onDidChangeDocumentConnection = new vscode.EventEmitter<string>();
    readonly onDidChangeDocumentConnection = this._onDidChangeDocumentConnection.event;

    constructor(private context: vscode.ExtensionContext) {
        this._loadingPromise = this.loadConnections();
    }

    private async loadConnections() {
        // Load active connection name
        this._activeConnectionName = this.context.globalState.get<string>(ConnectionManager.ACTIVE_CONN_KEY) || null;

        // Load connections from secrets
        const json = await this.context.secrets.get(ConnectionManager.SERVICE_NAME);
        if (json) {
            try {
                this._connections = JSON.parse(json);
            } catch (e) {
                console.error('Failed to parse connections:', e);
                this._connections = {};
            }
        } else {
            // Migration check: check for old single connection style
            const oldJson = await this.context.secrets.get('netezza-vscode');
            if (oldJson) {
                try {
                    const oldDetails = JSON.parse(oldJson);
                    if (oldDetails && oldDetails.host) {
                        const name = `Default (${oldDetails.host})`;
                        this._connections = {
                            [name]: { ...oldDetails, name }
                        };
                        this._activeConnectionName = name;
                        await this.saveConnectionsToStorage();
                        // Optional: Clear old secret? Maybe keep for safety.
                    }
                } catch (e) { /* ignore */ }
            }
        }
        this._onDidChangeConnections.fire();
    }

    private async ensureLoaded() {
        await this._loadingPromise;
    }

    private async saveConnectionsToStorage() {
        await this.context.secrets.store(ConnectionManager.SERVICE_NAME, JSON.stringify(this._connections));
        if (this._activeConnectionName) {
            await this.context.globalState.update(ConnectionManager.ACTIVE_CONN_KEY, this._activeConnectionName);
        } else {
            await this.context.globalState.update(ConnectionManager.ACTIVE_CONN_KEY, undefined);
        }
    }

    async saveConnection(details: ConnectionDetails) {
        await this.ensureLoaded();
        if (!details.name) {
            throw new Error('Connection name is required');
        }
        this._connections[details.name] = details;

        // If it's the first connection, make it active
        if (!this._activeConnectionName) {
            await this.setActiveConnection(details.name);
        }

        await this.saveConnectionsToStorage();
        this._onDidChangeConnections.fire();
    }

    async deleteConnection(name: string) {
        await this.ensureLoaded();
        if (this._connections[name]) {
            // Close persistent connection if exists
            await this.closePersistentConnection(name);

            delete this._connections[name];

            // If active connection was deleted, reset active
            if (this._activeConnectionName === name) {
                const names = Object.keys(this._connections);
                await this.setActiveConnection(names.length > 0 ? names[0] : null);
            }

            await this.saveConnectionsToStorage();
            this._onDidChangeConnections.fire();
        }
    }

    async getConnections(): Promise<ConnectionDetails[]> {
        await this.ensureLoaded();
        return Object.values(this._connections);
    }

    async getConnection(name: string): Promise<ConnectionDetails | undefined> {
        await this.ensureLoaded();
        return this._connections[name];
    }

    async setActiveConnection(name: string | null) {
        await this.ensureLoaded();
        this._activeConnectionName = name;
        await this.context.globalState.update(ConnectionManager.ACTIVE_CONN_KEY, name);
        this._onDidChangeActiveConnection.fire(name);
    }

    getActiveConnectionName(): string | null {
        return this._activeConnectionName;
    }

    async getConnectionString(name?: string): Promise<string | null> {
        await this.ensureLoaded();
        let targetName = name || this._activeConnectionName;
        // console.log(`[ConnectionManager] getConnectionString called with name='${name}', active='${this._activeConnectionName}', resolved='${targetName}'`);

        if (!targetName) {
            // console.log('[ConnectionManager] No target connection name resolved.');
            return null;
        }

        const details = this._connections[targetName];
        if (!details) {
            console.error(`[ConnectionManager] Connection '${targetName}' not found in registry. Available keys: ${Object.keys(this._connections).join(', ')}`);
            return null;
        }

        return `DRIVER={NetezzaSQL};SERVER=${details.host};PORT=${details.port};DATABASE=${details.database};UID=${details.user};PWD=${details.password};`;
    }

    async getCurrentDatabase(name?: string): Promise<string | null> {
        await this.ensureLoaded();
        let targetName = name || this._activeConnectionName;
        if (!targetName) return null;
        return this._connections[targetName]?.database || null;
    }

    setKeepConnectionOpen(keepOpen: boolean) {
        this._keepConnectionOpen = keepOpen;
        if (!keepOpen) {
            this.closeAllPersistentConnections();
        }
    }

    getKeepConnectionOpen(): boolean {
        return this._keepConnectionOpen;
    }

    async getPersistentConnection(name?: string): Promise<any> {
        const targetName = name || this._activeConnectionName;
        if (!targetName) {
            throw new Error('No connection selected');
        }

        const connString = await this.getConnectionString(targetName);
        if (!connString) {
            throw new Error(`Connection '${targetName}' not found or invalid`);
        }

        let existing = this._persistentConnections.get(targetName);

        // Check if existing connection matches current string (password change?) 
        // Note: For simplicity, we assume if it exists it's valid, unless we closed it.
        // But if user edited connection, we should have closed it. 
        // Let's add logic in saveConnection to close old one? 
        // For now, simpler: just check if exists. ODBS driver connection object doesn't show connection string easily?

        if (!existing) {
            const odbc = require('odbc');
            existing = await odbc.connect({ connectionString: connString, fetchArray: true });
            this._persistentConnections.set(targetName, existing);
        }

        return existing;
    }

    async closePersistentConnection(name: string) {
        const conn = this._persistentConnections.get(name);
        if (conn) {
            try {
                await conn.close();
            } catch (e) {
                console.error(`Error closing connection ${name}:`, e);
            }
            this._persistentConnections.delete(name);
        }
    }

    async closeAllPersistentConnections() {
        for (const name of this._persistentConnections.keys()) {
            await this.closePersistentConnection(name);
        }
    }

    // Per-document connection management
    getDocumentConnection(documentUri: string): string | undefined {
        return this._documentConnections.get(documentUri);
    }

    setDocumentConnection(documentUri: string, connectionName: string) {
        this._documentConnections.set(documentUri, connectionName);
        this._onDidChangeDocumentConnection.fire(documentUri);
    }

    clearDocumentConnection(documentUri: string) {
        this._documentConnections.delete(documentUri);
        this._onDidChangeDocumentConnection.fire(documentUri);
    }

    /**
     * Gets the connection to use for query execution.
     * If documentUri is provided and has a selected connection, use that.
     * Otherwise fall back to global active connection.
     */
    getConnectionForExecution(documentUri?: string): string | undefined {
        if (documentUri) {
            const docConnection = this._documentConnections.get(documentUri);
            if (docConnection) {
                return docConnection;
            }
        }
        return this._activeConnectionName || undefined;
    }
}
