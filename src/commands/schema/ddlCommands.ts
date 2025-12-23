/**
 * Schema Commands - DDL Generation and Comparison
 * Commands: createDDL, batchExportDDL, compareSchema
 */

import * as vscode from 'vscode';
import { runQuery } from '../../core/queryRunner';
import { SchemaCommandsDependencies, SchemaItemData } from './types';
import { requireConnection, executeWithProgress } from './helpers';

/**
 * Register DDL-related commands
 */
export function registerDDLCommands(deps: SchemaCommandsDependencies): vscode.Disposable[] {
    const { context, connectionManager } = deps;

    return [
        // Create DDL
        vscode.commands.registerCommand('netezza.createDDL', async (item: SchemaItemData) => {
            try {
                if (!item || !item.label || !item.dbName || !item.schema || !item.objType) {
                    vscode.window.showErrorMessage('Invalid object selected for DDL generation');
                    return;
                }

                const connectionString = await connectionManager.getConnectionString();
                if (!connectionString) {
                    vscode.window.showErrorMessage('Connection not configured. Please connect via Netezza: Connect...');
                    return;
                }

                await executeWithProgress(
                    `Generating DDL for ${item.objType} ${item.label}...`,
                    async () => {
                        const { generateDDL } = await import('../../ddlGenerator');

                        const result = await generateDDL(
                            connectionString,
                            item.dbName!,
                            item.schema!,
                            item.label!,
                            item.objType!
                        );

                        if (result.success && result.ddlCode) {
                            const action = await vscode.window.showQuickPick(
                                [
                                    {
                                        label: 'Open in Editor',
                                        description: 'Open DDL code in a new editor',
                                        value: 'editor'
                                    },
                                    {
                                        label: 'Copy to Clipboard',
                                        description: 'Copy DDL code to clipboard',
                                        value: 'clipboard'
                                    }
                                ],
                                {
                                    placeHolder: 'How would you like to access the DDL code?'
                                }
                            );

                            if (action) {
                                if (action.value === 'editor') {
                                    const doc = await vscode.workspace.openTextDocument({
                                        content: result.ddlCode,
                                        language: 'sql'
                                    });
                                    await vscode.window.showTextDocument(doc);
                                    vscode.window.showInformationMessage(
                                        `DDL code generated for ${item.objType} ${item.label}`
                                    );
                                } else if (action.value === 'clipboard') {
                                    await vscode.env.clipboard.writeText(result.ddlCode);
                                    vscode.window.showInformationMessage('DDL code copied to clipboard');
                                }
                            }
                        } else {
                            throw new Error(result.error || 'DDL generation failed');
                        }
                    }
                );
            } catch (err: unknown) {
                const message = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error generating DDL: ${message}`);
            }
        }),

        // Compare Schema
        vscode.commands.registerCommand('netezza.compareSchema', async (item: SchemaItemData) => {
            try {
                if (!item || !item.label || !item.dbName || !item.schema || !item.objType) {
                    vscode.window.showErrorMessage('Invalid object selected for comparison');
                    return;
                }

                const connectionString = await connectionManager.getConnectionString();
                if (!connectionString) {
                    vscode.window.showErrorMessage('Connection not configured. Please connect via Netezza: Connect...');
                    return;
                }

                const sourceType = item.objType;
                const sourceFullName = `${item.dbName}.${item.schema}.${item.label}`;

                const targetObjects: {
                    label: string;
                    description: string;
                    db: string;
                    schema: string;
                    name: string;
                }[] = [];

                await executeWithProgress(
                    `Loading ${sourceType}s for comparison...`,
                    async () => {
                        const typeFilter =
                            sourceType === 'PROCEDURE'
                                ? `OBJTYPE = 'PROCEDURE'`
                                : sourceType === 'VIEW'
                                    ? `OBJTYPE = 'VIEW'`
                                    : `OBJTYPE = 'TABLE'`;

                        const query =
                            sourceType === 'PROCEDURE'
                                ? `SELECT DISTINCT SCHEMA, PROCEDURESIGNATURE AS OBJNAME FROM ${item.dbName}.._V_PROCEDURE WHERE DATABASE = '${item.dbName!.toUpperCase()}' ORDER BY SCHEMA, PROCEDURESIGNATURE`
                                : `SELECT SCHEMA, OBJNAME FROM ${item.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${item.dbName!.toUpperCase()}' AND ${typeFilter} ORDER BY SCHEMA, OBJNAME`;

                        const result = await runQuery(context, query, true, item.connectionName, connectionManager);
                        if (result && !result.startsWith('Query executed successfully')) {
                            const objects = JSON.parse(result);
                            for (const obj of objects) {
                                const objName = obj.OBJNAME;
                                const objSchema = obj.SCHEMA;
                                const fullName = `${item.dbName}.${objSchema}.${objName}`;

                                if (fullName.toUpperCase() !== sourceFullName.toUpperCase()) {
                                    targetObjects.push({
                                        label: objName,
                                        description: `${item.dbName}.${objSchema}`,
                                        db: item.dbName!,
                                        schema: objSchema,
                                        name: objName
                                    });
                                }
                            }
                        }
                    }
                );

                if (targetObjects.length === 0) {
                    vscode.window.showWarningMessage(`No other ${sourceType}s found to compare with.`);
                    return;
                }

                const selected = await vscode.window.showQuickPick(targetObjects, {
                    placeHolder: `Select ${sourceType} to compare with ${item.label}`,
                    matchOnDescription: true
                });

                if (!selected) return;

                await executeWithProgress(
                    `Comparing ${item.label} with ${selected.label}...`,
                    async () => {
                        if (sourceType === 'PROCEDURE') {
                            const { compareProcedures } = await import('../../schema/schemaComparer');
                            const { SchemaCompareView } = await import('../../views/schemaCompareView');

                            const result = await compareProcedures(
                                connectionString,
                                item.dbName!,
                                item.schema!,
                                item.label!,
                                selected.db,
                                selected.schema,
                                selected.name
                            );

                            SchemaCompareView.createOrShow(context.extensionUri, result, 'procedure');
                        } else {
                            const { compareTableStructures } = await import('../../schema/schemaComparer');
                            const { SchemaCompareView } = await import('../../views/schemaCompareView');

                            const result = await compareTableStructures(
                                connectionString,
                                item.dbName!,
                                item.schema!,
                                item.label!,
                                selected.db,
                                selected.schema,
                                selected.name
                            );

                            SchemaCompareView.createOrShow(context.extensionUri, result, 'table');
                        }
                    }
                );

                vscode.window.showInformationMessage(`Comparison complete: ${item.label} ↔ ${selected.label}`);
            } catch (err: unknown) {
                const message = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error comparing objects: ${message}`);
            }
        }),

        // Batch Export DDL
        vscode.commands.registerCommand('netezza.batchExportDDL', async (item: SchemaItemData) => {
            try {
                if (!item || !item.contextValue) {
                    vscode.window.showErrorMessage('Invalid node selected for batch DDL export');
                    return;
                }

                const isDatabase = item.contextValue === 'database';
                const isTypeGroup = item.contextValue.startsWith('typeGroup:');

                if (!isDatabase && !isTypeGroup) {
                    vscode.window.showErrorMessage(
                        'Batch DDL export is only available on database or object type nodes'
                    );
                    return;
                }

                const connectionString = await requireConnection(connectionManager, item.connectionName);
                if (!connectionString) {
                    vscode.window.showErrorMessage('Connection not configured. Please connect via Netezza: Connect...');
                    return;
                }

                const database = item.dbName || item.label;
                const objectTypes = isTypeGroup
                    ? [item.objType || item.contextValue.replace('typeGroup:', '')]
                    : undefined;

                await executeWithProgress(
                    isDatabase
                        ? `Exporting all DDL for database ${database}...`
                        : `Exporting ${objectTypes?.[0]} DDL for ${database}...`,
                    async () => {
                        const { generateBatchDDL } = await import('../../ddlGenerator');

                        const result = await generateBatchDDL({
                            connectionString,
                            database: database!,
                            objectTypes
                        });

                        if (result.success && result.ddlCode) {
                            const action = await vscode.window.showQuickPick(
                                [
                                    {
                                        label: 'Open in Editor',
                                        description: 'Open DDL code in a new editor',
                                        value: 'editor'
                                    },
                                    {
                                        label: 'Save to File',
                                        description: 'Save DDL code to a .sql file',
                                        value: 'file'
                                    },
                                    {
                                        label: 'Copy to Clipboard',
                                        description: 'Copy DDL code to clipboard',
                                        value: 'clipboard'
                                    }
                                ],
                                {
                                    placeHolder: `${result.objectCount} objects found. How would you like to access the DDL code?`
                                }
                            );

                            if (action) {
                                if (action.value === 'editor') {
                                    const doc = await vscode.workspace.openTextDocument({
                                        content: result.ddlCode,
                                        language: 'sql'
                                    });
                                    await vscode.window.showTextDocument(doc);
                                    vscode.window.showInformationMessage(`DDL exported: ${result.objectCount} objects`);
                                } else if (action.value === 'file') {
                                    const fileName = isDatabase
                                        ? `${database}_all_ddl.sql`
                                        : `${database}_${objectTypes?.[0]?.toLowerCase() || 'objects'}_ddl.sql`;

                                    const uri = await vscode.window.showSaveDialog({
                                        defaultUri: vscode.Uri.file(fileName),
                                        filters: { 'SQL Files': ['sql'] }
                                    });

                                    if (uri) {
                                        await vscode.workspace.fs.writeFile(uri, Buffer.from(result.ddlCode, 'utf8'));
                                        vscode.window.showInformationMessage(`DDL saved to ${uri.fsPath}`);
                                    }
                                } else if (action.value === 'clipboard') {
                                    await vscode.env.clipboard.writeText(result.ddlCode);
                                    vscode.window.showInformationMessage(`DDL copied: ${result.objectCount} objects`);
                                }
                            }

                            if (result.errors.length > 0) {
                                vscode.window.showWarningMessage(
                                    `Batch DDL completed with ${result.errors.length} error(s). Check the generated file for details.`
                                );
                            }
                        } else {
                            throw new Error(result.errors.join(', ') || 'Batch DDL generation failed');
                        }
                    }
                );
            } catch (err: unknown) {
                const message = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error exporting DDL: ${message}`);
            }
        })
    ];
}
