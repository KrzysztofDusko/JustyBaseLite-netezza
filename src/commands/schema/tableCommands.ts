/**
 * Schema Commands - Table Modification Commands
 * Commands: addPrimaryKey, addTableComment, addColumnComment, truncateTable, 
 *           renameTable, changeOwner, grantPermissions
 */

import * as vscode from 'vscode';
import { runQuery } from '../../core/queryRunner';
import { SchemaCommandsDependencies, SchemaItemData } from './types';
import { getFullName, requireConnection, executeWithProgress, escapeSqlString, isValidIdentifier } from './helpers';

/**
 * Register table modification commands
 */
export function registerTableCommands(deps: SchemaCommandsDependencies): vscode.Disposable[] {
    const { context, connectionManager, metadataCache, schemaProvider } = deps;

    return [
        // Grant Permissions
        vscode.commands.registerCommand('netezza.grantPermissions', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema && item.objType) {
                const fullName = getFullName(item);

                const privilege = await vscode.window.showQuickPick(
                    [
                        { label: 'SELECT', description: 'Privileges to read data' },
                        { label: 'INSERT', description: 'Privileges to insert data' },
                        { label: 'UPDATE', description: 'Privileges to update data' },
                        { label: 'DELETE', description: 'Privileges to delete data' },
                        { label: 'ALL', description: 'All privileges (SELECT, INSERT, UPDATE, DELETE)' },
                        { label: 'LIST', description: 'Privileges to list objects' }
                    ],
                    {
                        placeHolder: 'Select privilege type'
                    }
                );

                if (!privilege) return;

                const grantee = await vscode.window.showInputBox({
                    prompt: 'Enter user or group name',
                    placeHolder: 'e.g. SOME_USER or GROUP_NAME',
                    validateInput: value => {
                        if (!value || value.trim().length === 0) {
                            return 'User/group name cannot be empty';
                        }
                        if (!isValidIdentifier(value)) {
                            return 'Invalid user/group name';
                        }
                        return null;
                    }
                });

                if (!grantee) return;

                const sql = `GRANT ${privilege.label} ON ${fullName} TO ${grantee.trim().toUpperCase()};`;

                const confirmation = await vscode.window.showInformationMessage(
                    `Execute: ${sql}`,
                    { modal: true },
                    'Yes, execute',
                    'Cancel'
                );

                if (confirmation === 'Yes, execute') {
                    const connectionString = await requireConnection(connectionManager);
                    if (!connectionString) return;

                    try {
                        await executeWithProgress(
                            `Granting ${privilege.label} on ${fullName}...`,
                            async () => {
                                await runQuery(context, sql, true, item.connectionName, connectionManager);
                            }
                        );

                        vscode.window.showInformationMessage(
                            `Granted ${privilege.label} on ${fullName} to ${grantee.trim().toUpperCase()}`
                        );
                    } catch (err: unknown) {
                        const message = err instanceof Error ? err.message : String(err);
                        vscode.window.showErrorMessage(`Error granting privileges: ${message}`);
                    }
                }
            }
        }),

        // Add Table Comment
        vscode.commands.registerCommand('netezza.addTableComment', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = getFullName(item);

                const comment = await vscode.window.showInputBox({
                    prompt: 'Enter comment for table',
                    placeHolder: 'e.g. Table contains customer data',
                    value: item.objectDescription || ''
                });

                if (comment === undefined) return;

                const sql = `COMMENT ON TABLE ${fullName} IS '${escapeSqlString(comment)}';`;

                try {
                    const connectionString = await requireConnection(connectionManager);
                    if (!connectionString) return;

                    await runQuery(context, sql, true, item.connectionName, connectionManager);
                    vscode.window.showInformationMessage(`Comment added to table: ${fullName}`);
                    schemaProvider.refresh();
                } catch (err: unknown) {
                    const message = err instanceof Error ? err.message : String(err);
                    vscode.window.showErrorMessage(`Error adding comment: ${message}`);
                }
            }
        }),

        // Add Column Comment
        vscode.commands.registerCommand('netezza.addColumnComment', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.parentName) {
                const colName = item.label.split(' ')[0];
                const tableName = item.parentName;
                const schemaName = item.schema || 'ADMIN';
                const fullColumnRef = `${item.dbName}.${schemaName}.${tableName}.${colName}`;

                const comment = await vscode.window.showInputBox({
                    prompt: `Enter comment for column "${colName}"`,
                    placeHolder: 'e.g. Customer ID from CRM',
                    value: item.objectDescription || ''
                });

                if (comment === undefined) return;

                const sql = `COMMENT ON COLUMN ${fullColumnRef} IS '${escapeSqlString(comment)}';`;

                try {
                    const connectionString = await requireConnection(connectionManager);
                    if (!connectionString) return;

                    await runQuery(context, sql, true, item.connectionName, connectionManager);
                    vscode.window.showInformationMessage(`Comment added to column: ${colName}`);
                    schemaProvider.refresh();
                } catch (err: unknown) {
                    const message = err instanceof Error ? err.message : String(err);
                    vscode.window.showErrorMessage(`Error adding comment: ${message}`);
                }
            }
        }),

        // Truncate Table
        vscode.commands.registerCommand('netezza.truncateTable', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = getFullName(item);
                const sql = `TRUNCATE TABLE ${fullName};`;

                const confirmation = await vscode.window.showWarningMessage(
                    `⚠️ WARNING: Are you sure you want to delete ALL data from the table "${fullName}"?\n\n${sql}\n\nThis operation is IRREVERSIBLE!`,
                    { modal: true },
                    'Yes, delete all data',
                    'Cancel'
                );

                if (confirmation === 'Yes, delete all data') {
                    const connectionString = await requireConnection(connectionManager);
                    if (!connectionString) return;

                    try {
                        await executeWithProgress(
                            `Clearing table ${fullName}...`,
                            async () => {
                                await runQuery(context, sql, true, item.connectionName, connectionManager);
                            }
                        );

                        vscode.window.showInformationMessage(`Table cleared: ${fullName}`);
                    } catch (err: unknown) {
                        const message = err instanceof Error ? err.message : String(err);
                        vscode.window.showErrorMessage(`Error clearing table: ${message}`);
                    }
                }
            }
        }),

        // Add Primary Key
        vscode.commands.registerCommand('netezza.addPrimaryKey', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = getFullName(item);

                const constraintName = await vscode.window.showInputBox({
                    prompt: 'Enter primary key constraint name',
                    placeHolder: `e.g. PK_${item.label}`,
                    value: `PK_${item.label}`,
                    validateInput: value => {
                        if (!value || value.trim().length === 0) {
                            return 'Constraint name cannot be empty';
                        }
                        if (!isValidIdentifier(value)) {
                            return 'Invalid constraint name';
                        }
                        return null;
                    }
                });

                if (!constraintName) return;

                const columns = await vscode.window.showInputBox({
                    prompt: 'Enter primary key column names (comma separated)',
                    placeHolder: 'e.g. COL1, COL2 or ID',
                    validateInput: value => {
                        if (!value || value.trim().length === 0) {
                            return 'You must provide at least one column';
                        }
                        return null;
                    }
                });

                if (!columns) return;

                const columnList = columns
                    .split(',')
                    .map(c => c.trim().toUpperCase())
                    .join(', ');
                const sql = `ALTER TABLE ${fullName} ADD CONSTRAINT ${constraintName.trim().toUpperCase()} PRIMARY KEY (${columnList});`;

                const confirmation = await vscode.window.showInformationMessage(
                    `Add primary key to table "${fullName}"?\n\n${sql}`,
                    { modal: true },
                    'Yes, add',
                    'Cancel'
                );

                if (confirmation === 'Yes, add') {
                    const connectionString = await requireConnection(connectionManager);
                    if (!connectionString) return;

                    try {
                        await executeWithProgress(
                            `Adding primary key to ${fullName}...`,
                            async () => {
                                await runQuery(context, sql, true, item.connectionName, connectionManager);
                            }
                        );

                        vscode.window.showInformationMessage(
                            `Primary key added: ${constraintName.trim().toUpperCase()}`
                        );
                        schemaProvider.refresh();
                    } catch (err: unknown) {
                        const message = err instanceof Error ? err.message : String(err);
                        vscode.window.showErrorMessage(`Error adding primary key: ${message}`);
                    }
                }
            }
        }),

        // Change Owner
        vscode.commands.registerCommand('netezza.changeOwner', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = getFullName(item);

                const newOwner = await vscode.window.showInputBox({
                    prompt: 'Enter new owner name',
                    placeHolder: 'e.g. USER_NAME or GROUP_NAME'
                });

                if (!newOwner) return;

                const sql = `ALTER TABLE ${fullName} OWNER TO ${newOwner.trim()};`;

                try {
                    const connectionString = await requireConnection(connectionManager);
                    if (!connectionString) return;

                    await runQuery(context, sql, true, item.connectionName, connectionManager);
                    vscode.window.showInformationMessage(`Owner changed to ${newOwner} for ${fullName}`);

                    metadataCache.invalidateSchema(item.connectionName!, item.dbName, item.schema);
                    schemaProvider.refresh();
                } catch (err: unknown) {
                    const message = err instanceof Error ? err.message : String(err);
                    vscode.window.showErrorMessage(`Error changing owner: ${message}`);
                }
            }
        }),

        // Rename Table
        vscode.commands.registerCommand('netezza.renameTable', async (item: SchemaItemData) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = getFullName(item);

                const newName = await vscode.window.showInputBox({
                    prompt: 'Enter new table name',
                    placeHolder: 'NewTableName',
                    value: item.label
                });

                if (!newName || newName === item.label) return;

                const sql = `ALTER TABLE ${fullName} RENAME TO ${newName.trim()};`;

                try {
                    const connectionString = await requireConnection(connectionManager);
                    if (!connectionString) return;

                    await runQuery(context, sql, true, item.connectionName, connectionManager);
                    vscode.window.showInformationMessage(`Table renamed to ${newName}`);

                    metadataCache.invalidateSchema(item.connectionName!, item.dbName, item.schema);
                    schemaProvider.refresh();
                } catch (err: unknown) {
                    const message = err instanceof Error ? err.message : String(err);
                    vscode.window.showErrorMessage(`Error renaming table: ${message}`);
                }
            }
        })
    ];
}
