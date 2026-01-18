/**
 * Status Bar Manager - manages VS Code status bar items for Netezza extension
 */

import * as vscode from 'vscode';
import { ConnectionManager } from '../core/connectionManager';

/**
 * Update "Keep Connection Open" status bar item (per-document aware)
 */
export function updateKeepConnectionStatusBar(
    statusBarItem: vscode.StatusBarItem,
    connectionManager: ConnectionManager
): void {
    const editor = vscode.window.activeTextEditor;
    let isEnabled = false;
    let isPerDocument = false;
    
    if (editor && editor.document.languageId === 'sql') {
        const documentUri = editor.document.uri.toString();
        isEnabled = connectionManager.getDocumentKeepConnectionOpen(documentUri);
        isPerDocument = connectionManager.hasDocumentKeepConnectionOpen(documentUri);
    } else {
        isEnabled = connectionManager.getKeepConnectionOpen();
    }
    
    const prefix = isPerDocument ? '📌 ' : '';
    statusBarItem.text = isEnabled ? `${prefix}🔗 Keep ON` : `${prefix}⛓️‍💥 Keep OFF`;
    statusBarItem.tooltip = isEnabled
        ? `Keep Connection: ENABLED${isPerDocument ? ' (per-tab)' : ''} - Click to toggle for current tab`
        : `Keep Connection: DISABLED${isPerDocument ? ' (per-tab)' : ''} - Click to toggle for current tab`;
    statusBarItem.backgroundColor = isEnabled ? new vscode.ThemeColor('statusBarItem.prominentBackground') : undefined;
    
    // Show only for SQL files
    if (editor && editor.document.languageId === 'sql') {
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
