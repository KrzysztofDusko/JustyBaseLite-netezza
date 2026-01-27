import * as vscode from 'vscode';
import { NzConnection } from '../types';

export interface ConnectionDetails {
    name: string;
    host: string;
    port: number;
    database: string;
    user: string;
    password?: string;
    dbType?: string;
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

    // Per-document persistent connections: Map<documentUri, NzConnection>
    private _documentPersistentConnections: Map<string, NzConnection> = new Map();

    // Per-document persistent connection metadata: Map<documentUri, { connectionName: string; database: string; lastSessionId?: string }>
    private _documentPersistentConnectionMeta: Map<string, { connectionName: string; database: string; lastSessionId?: string }> = new Map();

    // Per-document keep connection open setting: Map<documentUri, boolean>
    // Default is true for new documents
    private _documentKeepConnectionOpen: Map<string, boolean> = new Map();

    // Per-document database override: Map<documentUri, databaseName>
    // When set, overrides the default database from connection details
    private _documentDatabaseOverride: Map<string, string> = new Map();

    // Promise that resolves when connections are loaded
    private _loadingPromise: Promise<void>;

    // Event emitter for connection changes
    private _onDidChangeConnections = new vscode.EventEmitter<void>();
    readonly onDidChangeConnections = this._onDidChangeConnections.event;

    private _onDidChangeActiveConnection = new vscode.EventEmitter<string | null>();
    readonly onDidChangeActiveConnection = this._onDidChangeActiveConnection.event;

    private _onDidChangeDocumentConnection = new vscode.EventEmitter<string>();
    readonly onDidChangeDocumentConnection = this._onDidChangeDocumentConnection.event;

    private _onDidChangeDocumentDatabase = new vscode.EventEmitter<string>();
    readonly onDidChangeDocumentDatabase = this._onDidChangeDocumentDatabase.event;

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
                } catch {
                    /* ignore migration errors */
                }
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
            // Close any document persistent connections using this connection
            // (documents will need to reconnect with different connection)

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


    async getCurrentDatabase(name?: string): Promise<string | null> {
        await this.ensureLoaded();
        const targetName = name || this._activeConnectionName;
        if (!targetName) return null;
        return this._connections[targetName]?.database || null;
    }

    // ========== Per-Document Keep Connection Open ==========

    /**
     * Set keep connection open for a specific document (tab)
     */
    setDocumentKeepConnectionOpen(documentUri: string, keepOpen: boolean): void {
        this._documentKeepConnectionOpen.set(documentUri, keepOpen);
        if (!keepOpen) {
            // Close persistent connection for this document
            this.closeDocumentPersistentConnection(documentUri);
        }
    }

    /**
     * Get keep connection open setting for a specific document (tab)
     * Default is true for new documents
     */
    getDocumentKeepConnectionOpen(documentUri: string): boolean {
        const perDoc = this._documentKeepConnectionOpen.get(documentUri);
        if (perDoc !== undefined) {
            return perDoc;
        }
        // Default: keep connection open for new documents
        return true;
    }

    /**
     * Check if document has explicit keep connection setting
     */
    hasDocumentKeepConnectionOpen(documentUri: string): boolean {
        return this._documentKeepConnectionOpen.has(documentUri);
    }

    /**
     * Toggle keep connection open for a specific document
     */
    toggleDocumentKeepConnectionOpen(documentUri: string): boolean {
        const current = this.getDocumentKeepConnectionOpen(documentUri);
        const newValue = !current;
        this.setDocumentKeepConnectionOpen(documentUri, newValue);
        return newValue;
    }

    /**
     * Get persistent connection for a specific document (tab)
     * Uses document-specific database override if set
     */
    async getDocumentPersistentConnection(documentUri: string, connectionName?: string): Promise<NzConnection> {
        const targetName = connectionName || this.getConnectionForExecution(documentUri);
        if (!targetName) {
            throw new Error('No connection selected for this document');
        }

        const details = await this.getConnection(targetName);
        if (!details) {
            throw new Error(`Connection '${targetName}' not found or invalid`);
        }

        // Get effective database (override or default from connection)
        const effectiveDatabase = this._documentDatabaseOverride.get(documentUri) || details.database;

        const existing = this._documentPersistentConnections.get(documentUri);
        const existingMeta = this._documentPersistentConnectionMeta.get(documentUri);

        // If existing connection does not match current connection/database, close it
        if (existing && existingMeta) {
            const metaMatches =
                existingMeta.connectionName === targetName && existingMeta.database === effectiveDatabase;

            if (metaMatches) {
                return existing;
            }

            await this.closeDocumentPersistentConnection(documentUri);
        } else if (existing && !existingMeta) {
            // No metadata means we cannot safely verify; close and recreate
            await this.closeDocumentPersistentConnection(documentUri);
        }

        // Create new connection for this document with effective database
        const { createNzConnection } = require('./nzConnectionFactory');

        const conn = createNzConnection({
            host: details.host,
            port: details.port || 5480,
            database: effectiveDatabase,
            user: details.user,
            password: details.password
        }) as NzConnection;
        await conn.connect();

        this._documentPersistentConnections.set(documentUri, conn);
        this._documentPersistentConnectionMeta.set(documentUri, {
            connectionName: targetName,
            database: effectiveDatabase
        });
        return conn;
    }

    /**
     * Close persistent connection for a specific document
     */
    async closeDocumentPersistentConnection(documentUri: string): Promise<void> {
        const conn = this._documentPersistentConnections.get(documentUri);
        if (conn) {
            try {
                await conn.close();
            } catch (e) {
                console.error(`Error closing document connection for ${documentUri}:`, e);
            }
            this._documentPersistentConnections.delete(documentUri);
            this._documentPersistentConnectionMeta.delete(documentUri);
        }
    }

    /**
     * Close all document persistent connections
     */
    async closeAllDocumentPersistentConnections(): Promise<void> {
        for (const uri of this._documentPersistentConnections.keys()) {
            await this.closeDocumentPersistentConnection(uri);
        }
    }

    dispose() {
        this.closeAllDocumentPersistentConnections();
        this._onDidChangeConnections.dispose();
        this._onDidChangeActiveConnection.dispose();
        this._onDidChangeDocumentConnection.dispose();
        this._onDidChangeDocumentDatabase.dispose();
    }

    // ========== Per-Document Database Override ==========

    /**
     * Get database override for a specific document (tab)
     * Returns undefined if no override set (use connection's default database)
     */
    getDocumentDatabase(documentUri: string): string | undefined {
        return this._documentDatabaseOverride.get(documentUri);
    }

    /**
     * Set database override for a specific document (tab)
     * This will close the existing persistent connection to force reconnect with new database
     */
    setDocumentDatabase(documentUri: string, database: string): void {
        this._documentDatabaseOverride.set(documentUri, database);
        // Close persistent connection to force reconnect with new database
        this.closeDocumentPersistentConnection(documentUri);
        this._onDidChangeDocumentDatabase.fire(documentUri);
    }

    /**
     * Clear database override for a specific document (revert to connection's default)
     */
    clearDocumentDatabase(documentUri: string): void {
        this._documentDatabaseOverride.delete(documentUri);
        this.closeDocumentPersistentConnection(documentUri);
        this._onDidChangeDocumentDatabase.fire(documentUri);
    }

    /**
     * Get the effective database for a document
     * Returns override if set, otherwise falls back to connection's default database
     */
    async getEffectiveDatabase(documentUri: string): Promise<string | null> {
        const override = this._documentDatabaseOverride.get(documentUri);
        if (override) {
            return override;
        }
        const connectionName = this.getConnectionForExecution(documentUri);
        if (!connectionName) return null;
        const details = await this.getConnection(connectionName);
        return details?.database || null;
    }

    // Per-document connection management
    getDocumentConnection(documentUri: string): string | undefined {
        return this._documentConnections.get(documentUri);
    }

    setDocumentConnection(documentUri: string, connectionName: string) {
        this._documentConnections.set(documentUri, connectionName);
        // If connection changes, close existing persistent connection for this document
        this.closeDocumentPersistentConnection(documentUri);
        this._onDidChangeDocumentConnection.fire(documentUri);
    }

    clearDocumentConnection(documentUri: string) {
        this._documentConnections.delete(documentUri);
        this._documentDatabaseOverride.delete(documentUri);
        this.closeDocumentPersistentConnection(documentUri);
        this._documentKeepConnectionOpen.delete(documentUri);
        this._documentPersistentConnectionMeta.delete(documentUri);
        this._onDidChangeDocumentConnection.fire(documentUri);
    }

    // Per-document session ID tracking
    setDocumentLastSessionId(documentUri: string, sessionId: string) {
        const meta = this._documentPersistentConnectionMeta.get(documentUri);
        if (meta) {
            meta.lastSessionId = sessionId;
        } else {
            // Should usually strictly update existing meta since connection must exist
            // but we can be safe if it's missing (though weird flow)
        }
    }

    getDocumentLastSessionId(documentUri: string): string | undefined {
        return this._documentPersistentConnectionMeta.get(documentUri)?.lastSessionId;
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
