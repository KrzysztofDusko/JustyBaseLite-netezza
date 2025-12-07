import * as vscode from 'vscode';

export class ConnectionManager {
    private static readonly SERVICE_NAME = 'netezza-vscode';
    private _connectionDetails: any = null;
    private _persistentConnection: any = null;
    private _keepConnectionOpen: boolean = false;
    private _lastConnectionString: string | null = null;

    constructor(private context: vscode.ExtensionContext) { }

    async saveConnection(details: any) {
        this._connectionDetails = details;
        await this.context.secrets.store(ConnectionManager.SERVICE_NAME, JSON.stringify(details));
    }

    async getConnection(): Promise<any> {
        if (this._connectionDetails) {
            return this._connectionDetails;
        }
        const json = await this.context.secrets.get(ConnectionManager.SERVICE_NAME);
        if (json) {
            this._connectionDetails = JSON.parse(json);
            return this._connectionDetails;
        }
        return null;
    }

    async getConnectionString(): Promise<string | null> {
        const details = await this.getConnection();
        if (!details) {
            return null;
        }
        // Construct ODBC connection string
        // DRIVER={NetezzaSQL};SERVER=nzhost;PORT=5480;DATABASE=system;UID=admin;PWD=password;
        return `DRIVER={NetezzaSQL};SERVER=${details.host};PORT=${details.port};DATABASE=${details.database};UID=${details.user};PWD=${details.password};`;
    }

    setKeepConnectionOpen(keepOpen: boolean) {
        this._keepConnectionOpen = keepOpen;
        if (!keepOpen) {
            this.closePersistentConnection();
        }
    }

    getKeepConnectionOpen(): boolean {
        return this._keepConnectionOpen;
    }

    async getPersistentConnection(): Promise<any> {
        const connectionString = await this.getConnectionString();
        if (!connectionString) {
            throw new Error('Connection not configured. Please connect via Netezza: Connect...');
        }

        // If connection string changed or we don't have persistent connection, create new one
        if (this._lastConnectionString !== connectionString || !this._persistentConnection) {
            this.closePersistentConnection();
            
            try {
                // Import odbc here to avoid circular dependencies
                const odbc = require('odbc');
                this._persistentConnection = await odbc.connect({ connectionString, fetchArray: true });
                this._lastConnectionString = connectionString;
            } catch (error) {
                this._persistentConnection = null;
                this._lastConnectionString = null;
                throw error;
            }
        }

        return this._persistentConnection;
    }

    closePersistentConnection() {
        if (this._persistentConnection) {
            try {
                this._persistentConnection.close();
            } catch (error) {
                console.error('Error closing persistent connection:', error);
            }
            this._persistentConnection = null;
            this._lastConnectionString = null;
        }
    }
}
