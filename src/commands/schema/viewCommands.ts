/**
 * Schema Commands - Visualization Commands
 * Commands: showERD, showSessionMonitor
 */

import * as vscode from 'vscode';
import { runQuery } from '../../core/queryRunner';
import { SchemaCommandsDependencies, SchemaItemData } from './types';
import { requireConnection, executeWithProgress } from './helpers';

/**
 * Register view/visualization commands
 */
export function registerViewCommands(deps: SchemaCommandsDependencies): vscode.Disposable[] {
    const { context, connectionManager } = deps;

    return [
        // Show ERD
        vscode.commands.registerCommand('netezza.showERD', async (item: SchemaItemData) => {
            try {
                if (!item || !item.contextValue || !item.contextValue.startsWith('typeGroup:')) {
                    vscode.window.showErrorMessage('Please right-click on a TABLE type group to show ERD');
                    return;
                }

                const database = item.dbName || item.label;
                const connectionName = item.connectionName;

                if (!connectionName) {
                    vscode.window.showErrorMessage('No connection selected');
                    return;
                }

                const schemaQuery = `SELECT DISTINCT SCHEMA FROM ${database}.._V_TABLE ORDER BY SCHEMA`;
                const schemasJson = await runQuery(context, schemaQuery, true, connectionName, connectionManager);

                if (!schemasJson) {
                    vscode.window.showErrorMessage('Could not retrieve schemas');
                    return;
                }

                const schemas = JSON.parse(schemasJson);
                if (schemas.length === 0) {
                    vscode.window.showWarningMessage('No tables found in this database');
                    return;
                }

                let selectedSchema: string;
                if (schemas.length === 1) {
                    selectedSchema = schemas[0].SCHEMA;
                } else {
                    const schemaOptions: vscode.QuickPickItem[] = schemas.map((s: { SCHEMA: string }) => ({
                        label: s.SCHEMA as string,
                        description: `${database}.${s.SCHEMA}`
                    }));

                    const selected = await vscode.window.showQuickPick(schemaOptions, {
                        placeHolder: 'Select schema to show ERD for'
                    });

                    if (!selected) return;
                    selectedSchema = selected.label;
                }

                let tableCount = 0;
                let relCount = 0;
                await executeWithProgress(
                    `Building ERD for ${database}.${selectedSchema}...`,
                    async () => {
                        const { buildERDData } = await import('../../schema/erdProvider');
                        const { ERDView } = await import('../../views/erdView');

                        const erdData = await buildERDData(
                            context,
                            connectionManager,
                            connectionName,
                            database!,
                            selectedSchema
                        );

                        tableCount = erdData.tables.length;
                        relCount = erdData.relationships.length;

                        ERDView.createOrShow(context.extensionUri, erdData);
                    }
                );

                vscode.window.showInformationMessage(`ERD generated: ${tableCount} tables, ${relCount} relationships`);
            } catch (err: unknown) {
                const message = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error generating ERD: ${message}`);
            }
        }),

        // Show Session Monitor
        vscode.commands.registerCommand('netezza.showSessionMonitor', async () => {
            try {
                if (!await requireConnection(connectionManager)) {
                    vscode.window.showErrorMessage('Please connect to a Netezza database first.');
                    return;
                }

                const { SessionMonitorView } = await import('../../views/sessionMonitorView');
                SessionMonitorView.createOrShow(context.extensionUri, context, connectionManager);
            } catch (err: unknown) {
                const message = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error opening Session Monitor: ${message}`);
            }
        })
    ];
}
