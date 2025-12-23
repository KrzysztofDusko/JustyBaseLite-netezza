/**
 * Status Bar Manager - manages VS Code status bar items for Netezza extension
 */

import * as vscode from 'vscode';
import { ConnectionManager } from '../core/connectionManager';

/**
 * Update "Keep Connection Open" status bar item
 */
export function updateKeepConnectionStatusBar(
    statusBarItem: vscode.StatusBarItem,
    connectionManager: ConnectionManager
): void {
    const isEnabled = connectionManager.getKeepConnectionOpen();
    statusBarItem.text = isEnabled ? '🔗 Keep Connection ON' : '⛓️‍💥 Keep Connection OFF';
    statusBarItem.tooltip = isEnabled
        ? 'Keep Connection Open: ENABLED - Click to disable'
        : 'Keep Connection Open: DISABLED - Click to enable';
    statusBarItem.backgroundColor = isEnabled ? new vscode.ThemeColor('statusBarItem.prominentBackground') : undefined;
}

/**
 * Create and configure the "Keep Connection Open" status bar item
 */
export function createKeepConnectionStatusBar(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager
): vscode.StatusBarItem {
    const statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
    statusBarItem.command = 'netezza.toggleKeepConnectionOpen';
    updateKeepConnectionStatusBar(statusBarItem, connectionManager);
    statusBarItem.show();
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
