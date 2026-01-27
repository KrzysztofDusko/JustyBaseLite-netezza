/**
 * Schema Commands - Utility Commands
 * Commands: revealInSchema, showQueryHistory, clearQueryHistory
 */

import * as vscode from 'vscode';
import { runQueryRaw, queryResultToRows } from '../../core/queryRunner';
import { SchemaItem } from '../../providers/schemaProvider';
import { SchemaCommandsDependencies } from './types';

interface RevealData {
    name: string;
    objType?: string;
    type?: string;
    parent?: string;
    database?: string;
    schema?: string;
    connectionName?: string;
}

/**
 * Register utility commands
 */
export function registerUtilityCommands(deps: SchemaCommandsDependencies): vscode.Disposable[] {
    const { context, connectionManager, metadataCache, schemaTreeView } = deps;

    return [
        // Reveal In Schema
        vscode.commands.registerCommand('netezza.revealInSchema', async (data: RevealData) => {
            const statusBarDisposable = vscode.window.setStatusBarMessage(
                `$(loading~spin) Revealing ${data.name} in schema...`
            );
            try {
                let targetConnectionName: string | undefined = data.connectionName;

                if (!targetConnectionName) {
                    const activeEditor = vscode.window.activeTextEditor;
                    if (activeEditor && activeEditor.document.languageId === 'sql') {
                        targetConnectionName = connectionManager.getConnectionForExecution(
                            activeEditor.document.uri.toString()
                        );
                    }
                }

                if (!targetConnectionName) {
                    targetConnectionName = connectionManager.getActiveConnectionName() || undefined;
                }

                if (!targetConnectionName) {
                    statusBarDisposable.dispose();
                    vscode.window.showWarningMessage('No active connection. Please select a connection first.');
                    return;
                }

                // Accept either `objType` (old callers) or `type` (webview payload)
                const searchType = (data.objType || data.type)?.trim().toUpperCase();
                let searchName = data.name;

                if (searchType === 'COLUMN') {
                    if (!data.parent) {
                        statusBarDisposable.dispose();
                        vscode.window.showWarningMessage('Cannot find column without parent table');
                        return;
                    }
                    searchName = data.parent;
                }

                // Try cache first
                if (data.database) {
                    const cachedObj = metadataCache.findObjectWithType(
                        targetConnectionName,
                        data.database,
                        data.schema,
                        searchName
                    );
                    if (cachedObj) {
                        const targetItem = new SchemaItem(
                            cachedObj.name,
                            vscode.TreeItemCollapsibleState.Collapsed,
                            `netezza:${cachedObj.objType}`,
                            data.database,
                            cachedObj.objType,
                            cachedObj.schema || data.schema,
                            cachedObj.objId,
                            undefined,
                            targetConnectionName
                        );
                        await schemaTreeView.reveal(targetItem, { select: true, focus: true, expand: true });
                        statusBarDisposable.dispose();
                        vscode.window.setStatusBarMessage(
                            `$(check) Found ${searchName} in ${data.database}.${cachedObj.schema || data.schema} (cached)`,
                            3000
                        );
                        return;
                    } else {
                        if (!metadataCache.hasConnectionPrefetchTriggered(targetConnectionName)) {
                            metadataCache.triggerConnectionPrefetch(targetConnectionName, async q =>
                                runQueryRaw(context, q, true, connectionManager, targetConnectionName)
                            );
                        }
                    }
                }

                const connectionDetails = await connectionManager.getConnection(targetConnectionName);
                if (!connectionDetails) {
                    statusBarDisposable.dispose();
                    vscode.window.showWarningMessage('Not connected to database and object not found in cache.');
                    return;
                }

                const targetDb = data.database || (await connectionManager.getCurrentDatabase(targetConnectionName));

                if (targetDb) {
                    let effectiveSearchType = searchType;
                    if (effectiveSearchType === 'EXTERNAL TABLE') {
                        effectiveSearchType = 'TABLE';
                    }
                    const typeFilter =
                        effectiveSearchType && effectiveSearchType !== 'COLUMN' ? `AND UPPER(OBJTYPE) = UPPER('${effectiveSearchType}')` : '';
                    const schemaFilter = data.schema
                        ? `AND UPPER(SCHEMA) = UPPER('${data.schema.replace(/'/g, "''").trim()}')`
                        : '';

                    const query = `
                        SELECT OBJNAME, OBJTYPE, SCHEMA, OBJID 
                        FROM ${targetDb}.._V_OBJECT_DATA 
                        WHERE UPPER(OBJNAME) = UPPER('${searchName.replace(/'/g, "''").trim()}') 
                        AND DBNAME = '${targetDb}'
                        ${typeFilter}
                        ${schemaFilter}
                        LIMIT 1
                    `;

                    try {
                        const objResult = await runQueryRaw(
                            context,
                            query,
                            true,
                            connectionManager,
                            targetConnectionName
                        );
                        if (objResult && objResult.data) {
                            const objects = queryResultToRows<{ OBJNAME: string; OBJTYPE: string; SCHEMA: string; OBJID: number }>(objResult);

                            if (objects.length > 0) {
                                const obj = objects[0];

                                if (obj.OBJTYPE === 'PROCEDURE') {
                                    try {
                                        const sigQuery = `SELECT PROCEDURESIGNATURE FROM ${targetDb}.._V_PROCEDURE WHERE OBJID = ${obj.OBJID}`;
                                        const sigResult = await runQueryRaw(
                                            context,
                                            sigQuery,
                                            true,
                                            connectionManager,
                                            targetConnectionName
                                        );
                                        if (sigResult && sigResult.data && sigResult.data.length > 0) {
                                            const sigObj = queryResultToRows<{ PROCEDURESIGNATURE: string }>(sigResult);
                                            if (sigObj.length > 0 && sigObj[0].PROCEDURESIGNATURE) {
                                                obj.OBJNAME = sigObj[0].PROCEDURESIGNATURE;
                                            }
                                        }
                                    } catch (sigErr) {
                                        console.warn('Failed to resolve procedure signature:', sigErr);
                                    }
                                }

                                const targetItem = new SchemaItem(
                                    obj.OBJNAME,
                                    vscode.TreeItemCollapsibleState.Collapsed,
                                    `netezza:${obj.OBJTYPE}`,
                                    targetDb,
                                    obj.OBJTYPE,
                                    obj.SCHEMA,
                                    obj.OBJID,
                                    undefined,
                                    targetConnectionName
                                );

                                await schemaTreeView.reveal(targetItem, { select: true, focus: true, expand: true });
                                statusBarDisposable.dispose();
                                vscode.window.setStatusBarMessage(
                                    `$(check) Found ${searchName} in ${targetDb}.${obj.SCHEMA}`,
                                    3000
                                );
                                return;
                            }
                        }
                    } catch (e) {
                        console.log(`Error searching in ${targetDb}:`, e);
                    }
                }

                // Fallback: search all databases
                if (!targetDb) {
                    const dbResultRaw = await runQueryRaw(
                        context,
                        'SELECT DATABASE FROM system.._v_database ORDER BY DATABASE',
                        true,
                        connectionManager,
                        targetConnectionName
                    );
                    if (dbResultRaw && dbResultRaw.data) {
                        const databases = queryResultToRows<{ DATABASE: string }>(dbResultRaw);
                        for (const db of databases) {
                            const dbName = db.DATABASE;
                            try {
                                let effectiveSearchType = searchType;
                                if (effectiveSearchType === 'EXTERNAL TABLE') {
                                    effectiveSearchType = 'TABLE';
                                }
                                const typeFilter =
                                    effectiveSearchType && effectiveSearchType !== 'COLUMN' ? `AND UPPER(OBJTYPE) = UPPER('${effectiveSearchType}')` : '';
                                const schemaFilter = data.schema
                                    ? `AND UPPER(SCHEMA) = UPPER('${data.schema.replace(/'/g, "''").trim()}')`
                                    : '';

                                const query = `
                                    SELECT OBJNAME, OBJTYPE, SCHEMA, OBJID 
                                    FROM ${dbName}.._V_OBJECT_DATA 
                                    WHERE UPPER(OBJNAME) = UPPER('${searchName.replace(/'/g, "''").trim()}') 
                                    AND DBNAME = '${dbName}'
                                    ${typeFilter}
                                    ${schemaFilter}
                                    LIMIT 1
                                `;

                                const objResultRaw = await runQueryRaw(
                                    context,
                                    query,
                                    true,
                                    connectionManager,
                                    targetConnectionName
                                );
                                if (objResultRaw && objResultRaw.data) {
                                    const objects = queryResultToRows<{ OBJNAME: string; OBJTYPE: string; SCHEMA: string; OBJID: number }>(objResultRaw);

                                    if (objects.length > 0) {
                                        const obj = objects[0];

                                        if (obj.OBJTYPE === 'PROCEDURE') {
                                            try {
                                                const sigQuery = `SELECT PROCEDURESIGNATURE FROM ${dbName}.._V_PROCEDURE WHERE OBJID = ${obj.OBJID}`;
                                                const sigResult = await runQueryRaw(
                                                    context,
                                                    sigQuery,
                                                    true,
                                                    connectionManager,
                                                    targetConnectionName
                                                );
                                                if (sigResult && sigResult.data && sigResult.data.length > 0) {
                                                    const sigObj = queryResultToRows<{ PROCEDURESIGNATURE: string }>(sigResult);
                                                    if (sigObj.length > 0 && sigObj[0].PROCEDURESIGNATURE) {
                                                        obj.OBJNAME = sigObj[0].PROCEDURESIGNATURE;
                                                    }
                                                }
                                            } catch (sigErr) {
                                                console.warn('Failed to resolve procedure signature:', sigErr);
                                            }
                                        }

                                        const targetItem = new SchemaItem(
                                            obj.OBJNAME,
                                            vscode.TreeItemCollapsibleState.Collapsed,
                                            `netezza:${obj.OBJTYPE}`,
                                            dbName,
                                            obj.OBJTYPE,
                                            obj.SCHEMA,
                                            obj.OBJID,
                                            undefined,
                                            targetConnectionName
                                        );

                                        await schemaTreeView.reveal(targetItem, {
                                            select: true,
                                            focus: true,
                                            expand: true
                                        });
                                        statusBarDisposable.dispose();
                                        vscode.window.setStatusBarMessage(
                                            `$(check) Found ${searchName} in ${dbName}.${obj.SCHEMA}`,
                                            3000
                                        );
                                        return;
                                    }
                                }
                            } catch (e) {
                                console.log(`Error searching in ${dbName}:`, e);
                            }
                        }
                    }
                }
                statusBarDisposable.dispose();
                vscode.window.showWarningMessage(`Could not find ${searchType || 'object'} ${searchName}`);
            } catch (err: unknown) {
                statusBarDisposable.dispose();
                const message = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error revealing item: ${message}`);
            }
        }),

        // Show Query History
        vscode.commands.registerCommand('netezza.showQueryHistory', () => {
            vscode.commands.executeCommand('netezza.queryHistory.focus');
        }),

        // Clear Query History
        vscode.commands.registerCommand('netezza.clearQueryHistory', async () => {
            const { QueryHistoryManager } = await import('../../core/queryHistoryManager');
            const historyManager = QueryHistoryManager.getInstance(context);

            const confirm = await vscode.window.showWarningMessage(
                'Are you sure you want to clear all query history?',
                { modal: true },
                'Clear All'
            );

            if (confirm === 'Clear All') {
                await historyManager.clearHistory();
                vscode.window.showInformationMessage('Query history cleared');
            }
        })
    ];
}
