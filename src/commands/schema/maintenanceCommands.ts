/**
 * Schema Commands - Maintenance Operations
 * Commands: groomTable, generateStatistics, checkSkew, recreateTable
 */

import * as vscode from 'vscode';
import { runQuery } from '../../core/queryRunner';
import { SchemaCommandsDependencies, SchemaItemData } from './types';
import { getFullName, executeWithProgress } from './helpers';

/**
 * Register maintenance commands
 */
export function registerMaintenanceCommands(deps: SchemaCommandsDependencies): vscode.Disposable[] {
    const { context, connectionManager } = deps;

    return [
        // Groom Table
        vscode.commands.registerCommand('netezza.groomTable', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = getFullName(item);

                const mode = await vscode.window.showQuickPick(
                    [
                        { label: 'RECORDS ALL', description: 'Groom all records (reclaim space from deleted rows)' },
                        { label: 'RECORDS READY', description: 'Groom only ready records' },
                        { label: 'PAGES ALL', description: 'Groom all pages (reorganize data pages)' },
                        { label: 'PAGES START', description: 'Groom pages from start' },
                        { label: 'VERSIONS', description: 'Groom versions (clean up old row versions)' }
                    ],
                    {
                        placeHolder: 'Select GROOM mode'
                    }
                );

                if (!mode) return;

                const backupsetOption = await vscode.window.showQuickPick(
                    [
                        { label: 'DEFAULT', description: 'Use default backupset', value: 'DEFAULT' },
                        { label: 'NONE', description: 'No backupset', value: 'NONE' },
                        { label: 'Custom', description: 'Specify custom backupset ID', value: 'CUSTOM' }
                    ],
                    {
                        placeHolder: 'Select RECLAIM BACKUPSET option'
                    }
                );

                if (!backupsetOption) return;

                let backupsetValue = backupsetOption.value;

                if (backupsetOption.value === 'CUSTOM') {
                    const customId = await vscode.window.showInputBox({
                        prompt: 'Enter backupset ID',
                        placeHolder: 'e.g. 12345',
                        validateInput: value => {
                            if (!value || value.trim().length === 0) {
                                return 'Backupset ID cannot be empty';
                            }
                            if (!/^\d+$/.test(value.trim())) {
                                return 'Backupset ID must be a number';
                            }
                            return null;
                        }
                    });

                    if (!customId) return;
                    backupsetValue = customId.trim();
                }

                const sql = `GROOM TABLE ${fullName} ${mode.label} RECLAIM BACKUPSET ${backupsetValue};`;

                const confirmation = await vscode.window.showWarningMessage(
                    `Execute GROOM on table "${fullName}"?\n\n${sql}\n\nWarning: This operation may be time-consuming for large tables.`,
                    { modal: true },
                    'Yes, execute',
                    'Cancel'
                );

                if (confirmation === 'Yes, execute') {
                    // Note: runQuery uses connectionManager internally

                    try {
                        const startTime = Date.now();
                        await executeWithProgress(
                            `GROOM TABLE ${fullName} (${mode.label})...`,
                            async () => {
                                await runQuery(context, sql, true, item.connectionName, connectionManager);
                            }
                        );

                        const duration = ((Date.now() - startTime) / 1000).toFixed(1);
                        vscode.window.showInformationMessage(
                            `GROOM completed successfully (${duration}s): ${fullName}`
                        );
                    } catch (err: unknown) {
                        const message = err instanceof Error ? err.message : String(err);
                        vscode.window.showErrorMessage(`Error during GROOM: ${message}`);
                    }
                }
            }
        }),

        // Generate Statistics
        vscode.commands.registerCommand('netezza.generateStatistics', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = getFullName(item);
                const sql = `GENERATE EXPRESS STATISTICS ON ${fullName};`;

                const confirmation = await vscode.window.showInformationMessage(
                    `Generate statistics for table "${fullName}"?\n\n${sql}`,
                    { modal: true },
                    'Yes, generate',
                    'Cancel'
                );

                if (confirmation === 'Yes, generate') {
                    // Note: runQuery uses connectionManager internally

                    try {
                        const startTime = Date.now();
                        await executeWithProgress(
                            `Generating statistics for ${fullName}...`,
                            async () => {
                                await runQuery(context, sql, true, item.connectionName, connectionManager);
                            }
                        );

                        const duration = ((Date.now() - startTime) / 1000).toFixed(1);
                        vscode.window.showInformationMessage(
                            `Statistics generated successfully (${duration}s): ${fullName}`
                        );
                    } catch (err: unknown) {
                        const message = err instanceof Error ? err.message : String(err);
                        vscode.window.showErrorMessage(`Error generating statistics: ${message}`);
                    }
                }
            }
        }),

        // Check Skew
        vscode.commands.registerCommand('netezza.checkSkew', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = getFullName(item);
                const sql = `SELECT datasliceid, count(*) as row_count FROM ${fullName} GROUP BY 1 ORDER BY 1;`;

                const confirm = await vscode.window.showInformationMessage(
                    `Check skew for "${fullName}"?\n\nThis will run: ${sql}\n\nNote: This may be slow on very large tables.`,
                    { modal: true },
                    'Yes, check skew',
                    'Cancel'
                );

                if (confirm === 'Yes, check skew') {
                    const doc = await vscode.workspace.openTextDocument({
                        content: `-- Check Skew for ${fullName}\n${sql}`,
                        language: 'sql'
                    });
                    await vscode.window.showTextDocument(doc);
                    vscode.commands.executeCommand('netezza.runQuery');
                }
            }
        }),

        // Recreate Table
        vscode.commands.registerCommand('netezza.recreateTable', async (item: SchemaItemData) => {
            if (!item || !item.label || !item.dbName || !item.schema || !item.objType || item.objType !== 'TABLE') {
                vscode.window.showErrorMessage('Invalid object selected for Recreate Table');
                return;
            }

            const connectionDetails = await connectionManager.getConnection(connectionManager.getActiveConnectionName() || '');
            if (!connectionDetails) {
                vscode.window.showErrorMessage('Connection not configured. Please connect via Netezza: Connect...');
                return;
            }

            const newNameInput = await vscode.window.showInputBox({
                prompt: 'Enter temporary table name (Optional)',
                placeHolder: 'Leave empty to auto-generate timestamped name',
                value: ''
            });

            if (newNameInput === undefined) return;

            try {
                await executeWithProgress(
                    `Generating Recreate Script for ${item.label}...`,
                    async () => {
                        const { generateRecreateTableScript } = await import('../../schema/tableRecreator');

                        const result = await generateRecreateTableScript(
                            connectionDetails,
                            item.dbName!,
                            item.schema!,
                            item.label!,
                            newNameInput || undefined
                        );

                        if (result.success && result.sqlScript) {
                            const doc = await vscode.workspace.openTextDocument({
                                content: result.sqlScript,
                                language: 'sql'
                            });
                            await vscode.window.showTextDocument(doc);
                            vscode.window.showInformationMessage(`Recreate script generated for ${item.label}`);
                        } else {
                            throw new Error(result.error || 'Script generation failed');
                        }
                    }
                );
            } catch (err: unknown) {
                const message = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Error generating recreate script: ${message}`);
            }
        })
    ];
}
