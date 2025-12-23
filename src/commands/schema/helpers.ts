/**
 * Schema Commands - Helper Functions
 */

import * as vscode from 'vscode';
import { ConnectionManager } from '../../core/connectionManager';
import { SchemaItemData } from './types';

/**
 * Build fully qualified name from schema item
 */
export function getFullName(item: SchemaItemData): string {
    return `${item.dbName}.${item.schema}.${item.label}`;
}

/**
 * Validate connection exists and return connection string
 */
export async function requireConnection(
    connectionManager: ConnectionManager,
    connectionName?: string
): Promise<string | null> {
    const connectionString = await connectionManager.getConnectionString(connectionName);
    if (!connectionString) {
        vscode.window.showErrorMessage('No database connection');
        return null;
    }
    return connectionString;
}

/**
 * Execute an async task with VS Code progress notification
 */
export async function executeWithProgress<T>(
    title: string,
    task: () => Promise<T>
): Promise<T> {
    return vscode.window.withProgress(
        {
            location: vscode.ProgressLocation.Notification,
            title,
            cancellable: false
        },
        task
    );
}

/**
 * Escape single quotes in SQL strings
 */
export function escapeSqlString(value: string): string {
    return value.replace(/'/g, "''");
}

/**
 * Validate identifier name (e.g., constraint name, user name)
 */
export function isValidIdentifier(value: string): boolean {
    return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value.trim());
}
