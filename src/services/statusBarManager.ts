/**
 * Status Bar Manager - manages VS Code status bar items for Netezza extension
 */

import * as vscode from 'vscode';
import { ConnectionManager } from '../core/connectionManager';

/**
 * Update "Keep Connection Open" status bar item (per-document)
 */
export function updateKeepConnectionStatusBar(
    statusBarItem: vscode.StatusBarItem,
    connectionManager: ConnectionManager
): void {
    const editor = vscode.window.activeTextEditor;
    
    // Show only for SQL files
    if (editor && editor.document.languageId === 'sql') {
        const documentUri = editor.document.uri.toString();
        const isEnabled = connectionManager.getDocumentKeepConnectionOpen(documentUri);
        const isPerDocument = connectionManager.hasDocumentKeepConnectionOpen(documentUri);
        
        const prefix = isPerDocument ? '📌 ' : '';
        statusBarItem.text = isEnabled ? `${prefix}🔗 Keep ON` : `${prefix}⛓️‍💥 Keep OFF`;
        statusBarItem.tooltip = isEnabled
            ? `Keep Connection: ENABLED${isPerDocument ? ' (custom)' : ' (default)'} - Click to toggle`
            : `Keep Connection: DISABLED${isPerDocument ? ' (custom)' : ''} - Click to toggle`;
        statusBarItem.backgroundColor = isEnabled ? new vscode.ThemeColor('statusBarItem.prominentBackground') : undefined;
        statusBarItem.show();
    } else {
        statusBarItem.hide();
    }
}

/**
 * Create and configure the "Keep Connection Open" status bar item
 */
export function createKeepConnectionStatusBar(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager
): vscode.StatusBarItem {
    const statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
    statusBarItem.command = 'netezza.toggleKeepConnectionForTab';
    updateKeepConnectionStatusBar(statusBarItem, connectionManager);
    context.subscriptions.push(statusBarItem);
    return statusBarItem;
}

/**
 * Create and configure the "Active Connection" status bar item (per-tab)
 */
export function createActiveConnectionStatusBar(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager
): { statusBarItem: vscode.StatusBarItem; updateFn: () => void } {
    const statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
    statusBarItem.command = 'netezza.selectConnectionForTab';
    statusBarItem.tooltip = 'Click to select connection for this SQL tab';
    context.subscriptions.push(statusBarItem);

    const updateFn = () => {
        const editor = vscode.window.activeTextEditor;
        if (editor && editor.document.languageId === 'sql') {
            const documentUri = editor.document.uri.toString();
            const connectionName = connectionManager.getConnectionForExecution(documentUri);
            if (connectionName) {
                statusBarItem.text = `$(database) ${connectionName}`;
                statusBarItem.show();
            } else {
                statusBarItem.text = '$(database) Select Connection';
                statusBarItem.show();
            }
        } else {
            statusBarItem.hide();
        }
    };

    return { statusBarItem, updateFn };
}

/**
 * Create and configure the "Active Database" status bar item (per-tab)
 */
export function createActiveDatabaseStatusBar(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager
): { statusBarItem: vscode.StatusBarItem; updateFn: () => Promise<void> } {
    const statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 99);
    statusBarItem.command = 'netezza.selectDatabaseForTab';
    statusBarItem.tooltip = 'Click to select database for this SQL tab (will reconnect)';
    context.subscriptions.push(statusBarItem);

    const updateFn = async () => {
        const editor = vscode.window.activeTextEditor;
        if (editor && editor.document.languageId === 'sql') {
            const documentUri = editor.document.uri.toString();
            const connectionName = connectionManager.getConnectionForExecution(documentUri);
            
            if (connectionName) {
                const effectiveDb = await connectionManager.getEffectiveDatabase(documentUri);
                const hasOverride = connectionManager.getDocumentDatabase(documentUri) !== undefined;
                
                if (effectiveDb) {
                    const prefix = hasOverride ? '📌 ' : '';
                    statusBarItem.text = `${prefix}$(server) ${effectiveDb}`;
                    statusBarItem.tooltip = hasOverride 
                        ? `Database: ${effectiveDb} (custom for this tab) - Click to change`
                        : `Database: ${effectiveDb} (from connection) - Click to change`;
                    statusBarItem.show();
                } else {
                    statusBarItem.text = '$(server) Select Database';
                    statusBarItem.show();
                }
            } else {
                statusBarItem.hide();
            }
        } else {
            statusBarItem.hide();
        }
    };

    return { statusBarItem, updateFn };
}
