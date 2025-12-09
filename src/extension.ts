import * as vscode from 'vscode';
import { runQuery, runQueriesSequentially } from './queryRunner';
import { ConnectionManager } from './connectionManager';
import { LoginPanel } from './loginPanel';
import { SchemaProvider } from './schemaProvider';
import { ResultPanelView } from './resultPanelView';
import { SqlCompletionItemProvider } from './completionProvider';
import { SchemaSearchProvider } from './schemaSearchProvider';
import { SqlParser } from './sqlParser';
import { NetezzaDocumentLinkProvider } from './documentLinkProvider';
import { NetezzaFoldingRangeProvider } from './foldingProvider';
import { QueryHistoryView } from './queryHistoryView';
import { MetadataCache } from './metadataCache';
import * as path from 'path';


// Helper function to update status bar item
function updateKeepConnectionStatusBar(statusBarItem: vscode.StatusBarItem, connectionManager: ConnectionManager) {
    const isEnabled = connectionManager.getKeepConnectionOpen();
    statusBarItem.text = isEnabled ? '🔗 Keep Connection ON' : '⛓️‍💥 Keep Connection OFF';
    statusBarItem.tooltip = isEnabled
        ? 'Keep Connection Open: ENABLED - Click to disable'
        : 'Keep Connection Open: DISABLED - Click to enable';
    statusBarItem.backgroundColor = isEnabled ? new vscode.ThemeColor('statusBarItem.prominentBackground') : undefined;
}

export function activate(context: vscode.ExtensionContext) {
    console.log('Netezza extension: Activating...');

    // Ensure persistent connection is closed when extension is deactivated
    context.subscriptions.push({
        dispose: () => {
            connectionManager.closeAllPersistentConnections();
        }
    });

    const connectionManager = new ConnectionManager(context);
    const metadataCache = new MetadataCache(context);
    const schemaProvider = new SchemaProvider(context, connectionManager, metadataCache);
    const resultPanelProvider = new ResultPanelView(context.extensionUri);

    // Create status bar item for "Active Connection" (per-tab)
    const activeConnectionStatusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
    activeConnectionStatusBarItem.command = 'netezza.selectConnectionForTab';
    activeConnectionStatusBarItem.tooltip = 'Click to select connection for this SQL tab';
    context.subscriptions.push(activeConnectionStatusBarItem);

    // Function to update active connection status bar based on active editor
    const updateActiveConnectionStatusBar = () => {
        const editor = vscode.window.activeTextEditor;
        if (editor && editor.document.languageId === 'sql') {
            const documentUri = editor.document.uri.toString();
            const connectionName = connectionManager.getConnectionForExecution(documentUri);
            if (connectionName) {
                activeConnectionStatusBarItem.text = `$(database) ${connectionName}`;
                activeConnectionStatusBarItem.show();
            } else {
                activeConnectionStatusBarItem.text = '$(database) Select Connection';
                activeConnectionStatusBarItem.show();
            }
        } else {
            activeConnectionStatusBarItem.hide();
        }
    };

    // Initial update and listen for changes
    updateActiveConnectionStatusBar();
    connectionManager.onDidChangeActiveConnection(updateActiveConnectionStatusBar);
    connectionManager.onDidChangeConnections(updateActiveConnectionStatusBar);
    connectionManager.onDidChangeDocumentConnection(updateActiveConnectionStatusBar);
    // Update status bar when switching editors
    vscode.window.onDidChangeActiveTextEditor(updateActiveConnectionStatusBar);

    // Create status bar item for "Keep Connection Open"
    const keepConnectionStatusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
    keepConnectionStatusBarItem.command = 'netezza.toggleKeepConnectionOpen';
    updateKeepConnectionStatusBar(keepConnectionStatusBarItem, connectionManager);
    keepConnectionStatusBarItem.show();
    context.subscriptions.push(keepConnectionStatusBarItem);

    console.log('Netezza extension: Registering SchemaSearchProvider...');
    // metadataCache already instantiated above
    const schemaSearchProvider = new SchemaSearchProvider(context.extensionUri, context, metadataCache);

    console.log('Netezza extension: Registering QueryHistoryView...');
    const queryHistoryProvider = new QueryHistoryView(context.extensionUri, context);

    const schemaTreeView = vscode.window.createTreeView('netezza.schema', {
        treeDataProvider: schemaProvider,
        showCollapseAll: true
    });

    context.subscriptions.push(
        vscode.window.registerWebviewViewProvider(
            ResultPanelView.viewType,
            resultPanelProvider
        ),
        vscode.window.registerWebviewViewProvider(
            SchemaSearchProvider.viewType,
            schemaSearchProvider
        ),
        vscode.window.registerWebviewViewProvider(
            QueryHistoryView.viewType,
            queryHistoryProvider
        )
    );

    // --- CodeLens + decoration for python script invocations ---
    const scriptRegex = /(^|\s)(?:[A-Za-z]:\\|\\|\/)?[\w.\-\\\/]+\.py\b|(^|\s)python(?:\.exe)?\s+[^\n]*\.py\b/i;

    // Helper: quote an argument for the shell
    function quoteArg(a: string) {
        if (!a) return a;
        if (a.includes(' ')) {
            return `"${a.replace(/"/g, '\\"')}"`;
        }
        return a;
    }

    // Helper: build a PowerShell-friendly exec command. If executable path contains spaces or a path separator,
    // prefix with & and quote it. Otherwise leave unquoted so 'python' resolves normally.
    function buildExecCommand(execPath: string, scriptPath: string, args: string[]) {
        const needsAmp = /[ \\/]/.test(execPath);
        const execPart = needsAmp ? `& ${quoteArg(execPath)}` : execPath;
        const scriptPart = quoteArg(scriptPath);
        const argsPart = args.map(a => quoteArg(a)).join(' ');
        return `${execPart} ${scriptPart}${argsPart ? ' ' + argsPart : ''}`.trim();
    }

    class ScriptCodeLensProvider implements vscode.CodeLensProvider {
        private _onDidChange = new vscode.EventEmitter<void>();
        readonly onDidChangeCodeLenses = this._onDidChange.event;

        public provideCodeLenses(document: vscode.TextDocument): vscode.CodeLens[] {
            const lenses: vscode.CodeLens[] = [];
            for (let i = 0; i < document.lineCount; i++) {
                const line = document.lineAt(i);
                if (scriptRegex.test(line.text)) {
                    const range = line.range;
                    const cmd: vscode.Command = {
                        title: 'Run as script',
                        command: 'netezza.runScriptFromLens',
                        arguments: [document.uri, range]
                    };
                    lenses.push(new vscode.CodeLens(range, cmd));
                }
            }
            return lenses;
        }

        public refresh() { this._onDidChange.fire(); }
    }

    const scriptLensProvider = new ScriptCodeLensProvider();
    context.subscriptions.push(
        vscode.languages.registerCodeLensProvider({ scheme: 'file' }, scriptLensProvider)
    );

    // Decoration for script lines
    const scriptDecoration = vscode.window.createTextEditorDecorationType({
        backgroundColor: new vscode.ThemeColor('editor.rangeHighlightBackground'),
        borderRadius: '3px'
    });

    function updateScriptDecorations(editor?: vscode.TextEditor) {
        const active = editor || vscode.window.activeTextEditor;
        if (!active) return;
        const doc = active.document;
        const ranges: vscode.DecorationOptions[] = [];
        for (let i = 0; i < doc.lineCount; i++) {
            const line = doc.lineAt(i);
            if (scriptRegex.test(line.text)) {
                ranges.push({ range: line.range, hoverMessage: 'Python script invocation' });
            }
        }
        active.setDecorations(scriptDecoration, ranges);
    }

    context.subscriptions.push(
        vscode.window.onDidChangeActiveTextEditor(() => updateScriptDecorations()),
        vscode.workspace.onDidChangeTextDocument(e => {
            if (vscode.window.activeTextEditor && e.document === vscode.window.activeTextEditor.document) {
                updateScriptDecorations(vscode.window.activeTextEditor);
            }
        })
    );

    // Command to run from CodeLens
    context.subscriptions.push(
        vscode.commands.registerCommand('netezza.runScriptFromLens', async (uri: vscode.Uri, range: vscode.Range) => {
            try {
                const doc = await vscode.workspace.openTextDocument(uri);
                const text = doc.getText(range).trim() || doc.lineAt(range.start.line).text.trim();
                if (!text) { vscode.window.showWarningMessage('No script command found'); return; }

                // Reuse logic from runQuery: build command and run in terminal
                const tokens = text.split(/\s+/);
                const first = tokens[0] || '';
                const isPythonExec = /python(\\.exe)?$/i.test(first) && tokens.length >= 2 && tokens[1].toLowerCase().endsWith('.py');
                const isScriptDirect = first.toLowerCase().endsWith('.py');
                const config = vscode.workspace.getConfiguration('netezza');
                const pythonPath = config.get<string>('pythonPath') || 'python';

                let cmd = '';
                if (isPythonExec) {
                    const py = tokens[0];
                    const script = tokens[1];
                    const args = tokens.slice(2);
                    cmd = buildExecCommand(py, script, args);
                } else if (isScriptDirect) {
                    const script = first;
                    const args = tokens.slice(1);
                    cmd = buildExecCommand(pythonPath, script, args);
                } else {
                    // fallback - run whole text using python
                    const args = tokens;
                    cmd = buildExecCommand(pythonPath, '', args);
                }

                const term = vscode.window.createTerminal({ name: 'Netezza: Script' });
                term.show(true);
                term.sendText(cmd, true);
                vscode.window.showInformationMessage(`Running script: ${cmd}`);
            } catch (e: any) {
                vscode.window.showErrorMessage(`Error running script: ${e.message}`);
            }
        })
    );

    // initialize decorations for the active editor
    updateScriptDecorations(vscode.window.activeTextEditor);

    // Sync result view with active editor
    context.subscriptions.push(
        vscode.window.onDidChangeActiveTextEditor(editor => {
            if (editor && editor.document) {
                const sourceUri = editor.document.uri.toString();
                resultPanelProvider.setActiveSource(sourceUri);
            }
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('netezza.toggleKeepConnectionOpen', () => {
            const currentState = connectionManager.getKeepConnectionOpen();
            connectionManager.setKeepConnectionOpen(!currentState);
            updateKeepConnectionStatusBar(keepConnectionStatusBarItem, connectionManager);

            const newState = !currentState;
            vscode.window.showInformationMessage(
                newState
                    ? 'Keep connection open: ENABLED - connection will remain open after queries'
                    : 'Keep connection open: DISABLED - connection will be closed after each query'
            );
        }),
        vscode.commands.registerCommand('netezza.selectActiveConnection', async () => {
            const connections = await connectionManager.getConnections();
            if (connections.length === 0) {
                vscode.window.showWarningMessage('No connections configured. Please connect first.');
                return;
            }

            const selected = await vscode.window.showQuickPick(connections.map(c => c.name), {
                placeHolder: 'Select Active Connection'
            });

            if (selected) {
                await connectionManager.setActiveConnection(selected);
                vscode.window.showInformationMessage(`Active connection set to: ${selected}`);
            }
        }),
        vscode.commands.registerCommand('netezza.selectConnectionForTab', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor || editor.document.languageId !== 'sql') {
                vscode.window.showWarningMessage('This command is only available for SQL files');
                return;
            }

            const connections = await connectionManager.getConnections();
            if (connections.length === 0) {
                vscode.window.showWarningMessage('No connections configured. Please connect first.');
                return;
            }

            const documentUri = editor.document.uri.toString();
            const currentConnection = connectionManager.getDocumentConnection(documentUri) || connectionManager.getActiveConnectionName();

            const items = connections.map(c => ({
                label: c.name,
                description: currentConnection === c.name ? '$(check) Currently selected' : `${c.host}:${c.port}/${c.database}`,
                detail: currentConnection === c.name ? undefined : undefined,
                name: c.name
            }));

            const selected = await vscode.window.showQuickPick(items, {
                placeHolder: 'Select connection for this SQL tab'
            });

            if (selected) {
                connectionManager.setDocumentConnection(documentUri, selected.name);
                vscode.window.showInformationMessage(`Connection for this tab set to: ${selected.name}`);
            }
        }),
        vscode.commands.registerCommand('netezza.openLogin', () => {
            LoginPanel.createOrShow(context.extensionUri, connectionManager);
        }),
        vscode.commands.registerCommand('netezza.refreshSchema', () => {
            schemaProvider.refresh();
            vscode.window.showInformationMessage('Schema refreshed');
        }),
        vscode.commands.registerCommand('netezza.copySelectAll', (item: any) => {
            if (item && item.label && item.dbName && item.schema) {
                // If Item has connectionName, ideally we should prefix SQL or use it?
                // But this command just copies SQL. The user executes it.
                // The new Query Runner will use the Active Connection by default.
                // If they want to run it against a specific one, they should select it.
                // However, if we copy SQL to clipboard, we can't force the connection unless we use a comment or directive ?
                // For now, simple SQL copy is fine.
                const sql = `SELECT * FROM ${item.dbName}.${item.schema}.${item.label} LIMIT 100;`;
                vscode.env.clipboard.writeText(sql);
                vscode.window.showInformationMessage('Copied to clipboard');
            }
        }),
        vscode.commands.registerCommand('netezza.copyDrop', async (item: any) => {
            if (item && item.label && item.dbName && item.schema && item.objType) {
                const fullName = `${item.dbName}.${item.schema}.${item.label}`;
                const sql = `DROP ${item.objType} ${fullName};`;

                // Show confirmation dialog
                const confirmation = await vscode.window.showWarningMessage(
                    `Are you sure you want to delete ${item.objType.toLowerCase()} "${fullName}"?`,
                    { modal: true },
                    'Yes, delete',
                    'Cancel'
                );

                if (confirmation === 'Yes, delete') {
                    try {
                        // Get connection string
                        const connectionString = await connectionManager.getConnectionString();
                        if (!connectionString) {
                            vscode.window.showErrorMessage('No database connection');
                            return;
                        }

                        // Execute DROP statement
                        await vscode.window.withProgress({
                            location: vscode.ProgressLocation.Notification,
                            title: `Deleting ${item.objType.toLowerCase()} ${fullName}...`,
                            cancellable: false
                        }, async (progress) => {
                            // Use connection from item if available
                            await runQuery(context, sql, true, item.connectionName, connectionManager);
                        });

                        vscode.window.showInformationMessage(`Deleted ${item.objType.toLowerCase()}: ${fullName}`);

                        // Refresh schema view
                        schemaProvider.refresh();
                    } catch (err: any) {
                        vscode.window.showErrorMessage(`Error during deletion: ${err.message}`);
                    }
                }
            }
        }),
        vscode.commands.registerCommand('netezza.copyName', (item: any) => {
            if (item && item.label && item.dbName && item.schema) {
                const name = `${item.dbName}.${item.schema}.${item.label}`;
                vscode.env.clipboard.writeText(name);
                vscode.window.showInformationMessage('Copied to clipboard');
            }
        }),
        vscode.commands.registerCommand('netezza.grantPermissions', async (item: any) => {
            if (item && item.label && item.dbName && item.schema && item.objType) {
                const fullName = `${item.dbName}.${item.schema}.${item.label}`;

                // Step 1: Select privilege type
                const privilege = await vscode.window.showQuickPick([
                    { label: 'SELECT', description: 'Privileges to read data' },
                    { label: 'INSERT', description: 'Privileges to insert data' },
                    { label: 'UPDATE', description: 'Privileges to update data' },
                    { label: 'DELETE', description: 'Privileges to delete data' },
                    { label: 'ALL', description: 'All privileges (SELECT, INSERT, UPDATE, DELETE)' },
                    { label: 'LIST', description: 'Privileges to list objects' }
                ], {
                    placeHolder: 'Select privilege type'
                });

                if (!privilege) {
                    return; // User cancelled
                }

                // Step 2: Enter user/group name
                const grantee = await vscode.window.showInputBox({
                    prompt: 'Enter user or group name',
                    placeHolder: 'e.g. SOME_USER or GROUP_NAME',
                    validateInput: (value) => {
                        if (!value || value.trim().length === 0) {
                            return 'User/group name cannot be empty';
                        }
                        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value.trim())) {
                            return 'Invalid user/group name';
                        }
                        return null;
                    }
                });

                if (!grantee) {
                    return; // User cancelled
                }

                const sql = `GRANT ${privilege.label} ON ${fullName} TO ${grantee.trim().toUpperCase()};`;

                // Step 3: Confirm and execute
                const confirmation = await vscode.window.showInformationMessage(
                    `Execute: ${sql}`,
                    { modal: true },
                    'Yes, execute',
                    'Cancel'
                );

                if (confirmation === 'Yes, execute') {
                    try {
                        // Get connection string
                        const connectionString = await connectionManager.getConnectionString();
                        if (!connectionString) {
                            vscode.window.showErrorMessage('No database connection');
                            return;
                        }

                        // Execute GRANT statement
                        await vscode.window.withProgress({
                            location: vscode.ProgressLocation.Notification,
                            title: `Granting ${privilege.label} on ${fullName}...`,
                            cancellable: false
                        }, async (progress) => {
                            await runQuery(context, sql, true, item.connectionName, connectionManager);
                        });

                        vscode.window.showInformationMessage(`Granted ${privilege.label} on ${fullName} to ${grantee.trim().toUpperCase()}`);
                    } catch (err: any) {
                        vscode.window.showErrorMessage(`Error granting privileges: ${err.message}`);
                    }
                }
            }
        }),
        vscode.commands.registerCommand('netezza.groomTable', async (item: any) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = `${item.dbName}.${item.schema}.${item.label}`;

                // Step 1: Select GROOM mode
                const mode = await vscode.window.showQuickPick([
                    { label: 'RECORDS ALL', description: 'Groom all records (reclaim space from deleted rows)' },
                    { label: 'RECORDS READY', description: 'Groom only ready records' },
                    { label: 'PAGES ALL', description: 'Groom all pages (reorganize data pages)' },
                    { label: 'PAGES START', description: 'Groom pages from start' },
                    { label: 'VERSIONS', description: 'Groom versions (clean up old row versions)' }
                ], {
                    placeHolder: 'Select GROOM mode'
                });

                if (!mode) {
                    return; // User cancelled
                }

                // Step 2: Select RECLAIM BACKUPSET option
                const backupsetOption = await vscode.window.showQuickPick([
                    { label: 'DEFAULT', description: 'Use default backupset', value: 'DEFAULT' },
                    { label: 'NONE', description: 'No backupset', value: 'NONE' },
                    { label: 'Custom', description: 'Specify custom backupset ID', value: 'CUSTOM' }
                ], {
                    placeHolder: 'Select RECLAIM BACKUPSET option'
                });

                if (!backupsetOption) {
                    return; // User cancelled
                }

                let backupsetValue = backupsetOption.value;

                // If custom, ask for backupset ID
                if (backupsetOption.value === 'CUSTOM') {
                    const customId = await vscode.window.showInputBox({
                        prompt: 'Enter backupset ID',
                        placeHolder: 'np. 12345',
                        validateInput: (value) => {
                            if (!value || value.trim().length === 0) {
                                return 'Backupset ID cannot be empty';
                            }
                            if (!/^\d+$/.test(value.trim())) {
                                return 'Backupset ID must be a number';
                            }
                            return null;
                        }
                    });

                    if (!customId) {
                        return; // User cancelled
                    }

                    backupsetValue = customId.trim();
                }

                const sql = `GROOM TABLE ${fullName} ${mode.label} RECLAIM BACKUPSET ${backupsetValue};`;

                // Step 3: Confirm and execute
                const confirmation = await vscode.window.showWarningMessage(
                    `Execute GROOM on table "${fullName}"?\n\n${sql}\n\nWarning: This operation may be time-consuming for large tables.`,
                    { modal: true },
                    'Yes, execute',
                    'Cancel'
                );

                if (confirmation === 'Yes, execute') {
                    try {
                        // Get connection string
                        const connectionString = await connectionManager.getConnectionString();
                        if (!connectionString) {
                            vscode.window.showErrorMessage('No database connection');
                            return;
                        }

                        // Execute GROOM statement
                        const startTime = Date.now();
                        await vscode.window.withProgress({
                            location: vscode.ProgressLocation.Notification,
                            title: `GROOM TABLE ${fullName} (${mode.label})...`,
                            cancellable: false
                        }, async (progress) => {
                            await runQuery(context, sql, true, item.connectionName, connectionManager);
                        });

                        const duration = ((Date.now() - startTime) / 1000).toFixed(1);
                        vscode.window.showInformationMessage(`GROOM completed successfully (${duration}s): ${fullName}`);
                    } catch (err: any) {
                        vscode.window.showErrorMessage(`Error during GROOM: ${err.message}`);
                    }
                }
            }
        }),
        vscode.commands.registerCommand('netezza.addTableComment', async (item: any) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = `${item.dbName}.${item.schema}.${item.label}`;

                const comment = await vscode.window.showInputBox({
                    prompt: 'Enter comment for table',
                    placeHolder: 'e.g. Table contains customer data',
                    value: item.objectDescription || ''
                });

                if (comment === undefined) {
                    return;
                }

                const sql = `COMMENT ON TABLE ${fullName} IS '${comment.replace(/'/g, "''")}';`;

                try {
                    const connectionString = await connectionManager.getConnectionString();
                    if (!connectionString) {
                        vscode.window.showErrorMessage('No database connection');
                        return;
                    }

                    await runQuery(context, sql, true, item.connectionName, connectionManager);
                    vscode.window.showInformationMessage(`Comment added to table: ${fullName}`);
                    schemaProvider.refresh();
                } catch (err: any) {
                    vscode.window.showErrorMessage(`Error adding comment: ${err.message}`);
                }
            }
        }),
        vscode.commands.registerCommand('netezza.generateStatistics', async (item: any) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = `${item.dbName}.${item.schema}.${item.label}`;
                const sql = `GENERATE EXPRESS STATISTICS ON ${fullName};`;

                const confirmation = await vscode.window.showInformationMessage(
                    `Generate statistics for table "${fullName}"?\n\n${sql}`,
                    { modal: true },
                    'Yes, generate',
                    'Cancel'
                );

                if (confirmation === 'Yes, generate') {
                    try {
                        const connectionString = await connectionManager.getConnectionString();
                        if (!connectionString) {
                            vscode.window.showErrorMessage('No database connection');
                            return;
                        }

                        const startTime = Date.now();
                        await vscode.window.withProgress({
                            location: vscode.ProgressLocation.Notification,
                            title: `Generating statistics for ${fullName}...`,
                            cancellable: false
                        }, async (progress) => {
                            await runQuery(context, sql, true, item.connectionName, connectionManager);
                        });

                        const duration = ((Date.now() - startTime) / 1000).toFixed(1);
                        vscode.window.showInformationMessage(`Statistics generated successfully (${duration}s): ${fullName}`);
                    } catch (err: any) {
                        vscode.window.showErrorMessage(`Error generating statistics: ${err.message}`);
                    }
                }
            }
        }),
        vscode.commands.registerCommand('netezza.truncateTable', async (item: any) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = `${item.dbName}.${item.schema}.${item.label}`;
                const sql = `TRUNCATE TABLE ${fullName};`;

                const confirmation = await vscode.window.showWarningMessage(
                    `⚠️ WARNING: Are you sure you want to delete ALL data from the table "${fullName}"?\n\n${sql}\n\nThis operation is IRREVERSIBLE!`,
                    { modal: true },
                    'Yes, delete all data',
                    'Cancel'
                );

                if (confirmation === 'Yes, delete all data') {
                    try {
                        const connectionString = await connectionManager.getConnectionString();
                        if (!connectionString) {
                            vscode.window.showErrorMessage('No database connection');
                            return;
                        }

                        await vscode.window.withProgress({
                            location: vscode.ProgressLocation.Notification,
                            title: `Clearing table ${fullName}...`,
                            cancellable: false
                        }, async (progress) => {
                            await runQuery(context, sql, true, item.connectionName, connectionManager);
                        });

                        vscode.window.showInformationMessage(`Table cleared: ${fullName}`);
                    } catch (err: any) {
                        vscode.window.showErrorMessage(`Error clearing table: ${err.message}`);
                    }
                }
            }
        }),
        vscode.commands.registerCommand('netezza.addPrimaryKey', async (item: any) => {
            if (item && item.label && item.dbName && item.schema && item.objType === 'TABLE') {
                const fullName = `${item.dbName}.${item.schema}.${item.label}`;

                const constraintName = await vscode.window.showInputBox({
                    prompt: 'Enter primary key constraint name',
                    placeHolder: `e.g. PK_${item.label}`,
                    value: `PK_${item.label}`,
                    validateInput: (value) => {
                        if (!value || value.trim().length === 0) {
                            return 'Constraint name cannot be empty';
                        }
                        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value.trim())) {
                            return 'Invalid constraint name';
                        }
                        return null;
                    }
                });

                if (!constraintName) {
                    return;
                }

                const columns = await vscode.window.showInputBox({
                    prompt: 'Enter primary key column names (comma separated)',
                    placeHolder: 'e.g. COL1, COL2 or ID',
                    validateInput: (value) => {
                        if (!value || value.trim().length === 0) {
                            return 'You must provide at least one column';
                        }
                        return null;
                    }
                });

                if (!columns) {
                    return;
                }

                const columnList = columns.split(',').map(c => c.trim().toUpperCase()).join(', ');
                const sql = `ALTER TABLE ${fullName} ADD CONSTRAINT ${constraintName.trim().toUpperCase()} PRIMARY KEY (${columnList});`;

                const confirmation = await vscode.window.showInformationMessage(
                    `Add primary key to table "${fullName}"?\n\n${sql}`,
                    { modal: true },
                    'Yes, add',
                    'Cancel'
                );

                if (confirmation === 'Yes, add') {
                    try {
                        const connectionString = await connectionManager.getConnectionString();
                        if (!connectionString) {
                            vscode.window.showErrorMessage('No database connection');
                            return;
                        }

                        await vscode.window.withProgress({
                            location: vscode.ProgressLocation.Notification,
                            title: `Adding primary key to ${fullName}...`,
                            cancellable: false
                        }, async (progress) => {
                            await runQuery(context, sql, true, item.connectionName, connectionManager);
                        });

                        vscode.window.showInformationMessage(`Primary key added: ${constraintName.trim().toUpperCase()}`);
                        schemaProvider.refresh();
                    } catch (err: any) {
                        vscode.window.showErrorMessage(`Error adding primary key: ${err.message}`);
                    }
                }
            }
        }),
        vscode.commands.registerCommand('netezza.createDDL', async (item: any) => {
            try {
                // Validate item has required properties
                if (!item || !item.label || !item.dbName || !item.schema || !item.objType) {
                    vscode.window.showErrorMessage('Invalid object selected for DDL generation');
                    return;
                }

                // Get connection string
                const connectionString = await connectionManager.getConnectionString();
                if (!connectionString) {
                    vscode.window.showErrorMessage('Connection not configured. Please connect via Netezza: Connect...');
                    return;
                }

                // Show progress while generating DDL
                await vscode.window.withProgress({
                    location: vscode.ProgressLocation.Notification,
                    title: `Generating DDL for ${item.objType} ${item.label}...`,
                    cancellable: false
                }, async () => {
                    const { generateDDL } = await import('./ddlGenerator');

                    const result = await generateDDL(
                        connectionString,
                        item.dbName,
                        item.schema,
                        item.label,
                        item.objType
                    );

                    if (result.success && result.ddlCode) {
                        // Ask user what to do with the DDL code
                        const action = await vscode.window.showQuickPick([
                            { label: 'Open in Editor', description: 'Open DDL code in a new editor', value: 'editor' },
                            { label: 'Copy to Clipboard', description: 'Copy DDL code to clipboard', value: 'clipboard' }
                        ], {
                            placeHolder: 'How would you like to access the DDL code?'
                        });

                        if (action) {
                            if (action.value === 'editor') {
                                // Open in new editor
                                const doc = await vscode.workspace.openTextDocument({
                                    content: result.ddlCode,
                                    language: 'sql'
                                });
                                await vscode.window.showTextDocument(doc);
                                vscode.window.showInformationMessage(`DDL code generated for ${item.objType} ${item.label}`);
                            } else if (action.value === 'clipboard') {
                                // Copy to clipboard
                                await vscode.env.clipboard.writeText(result.ddlCode);
                                vscode.window.showInformationMessage('DDL code copied to clipboard');
                            }
                        }
                    } else {
                        throw new Error(result.error || 'DDL generation failed');
                    }
                });

            } catch (err: any) {
                vscode.window.showErrorMessage(`Error generating DDL: ${err.message}`);
            }
        }),
        vscode.commands.registerCommand('netezza.revealInSchema', async (data: any) => {
            const statusBarDisposable = vscode.window.setStatusBarMessage(`$(loading~spin) Revealing ${data.name} in schema...`);
            try {
                // Determine which connection to use
                let targetConnectionName = data.connectionName;

                // If no connection specified, try to use active connection
                if (!targetConnectionName) {
                    targetConnectionName = connectionManager.getActiveConnectionName();
                }

                if (!targetConnectionName) {
                    statusBarDisposable.dispose();
                    vscode.window.showWarningMessage('No active connection. Please select a connection first.');
                    return;
                }

                const connectionString = await connectionManager.getConnectionString(targetConnectionName);
                if (!connectionString) {
                    statusBarDisposable.dispose();
                    vscode.window.showWarningMessage('Not connected to database');
                    return;
                }

                let searchName = data.name;
                let searchType = data.objType;

                // If objType is not specified, search for common object types
                const searchTypes = searchType ? [searchType] : ['TABLE', 'VIEW', 'EXTERNAL TABLE', 'PROCEDURE', 'FUNCTION', 'SEQUENCE', 'SYNONYM'];

                if (searchType === 'COLUMN') {
                    if (!data.parent) {
                        statusBarDisposable.dispose();
                        vscode.window.showWarningMessage('Cannot find column without parent table');
                        return;
                    }
                    searchName = data.parent;
                    // When looking for the parent table of a column, we look for tables/views
                    // We don't change searchTypes here because we want to find the parent object
                }

                // Determine which databases to search
                let databasesToSearch: string[] = [];
                if (data.database) {
                    // Use the specific database from the data
                    databasesToSearch = [data.database];
                } else {
                    // If no database specified, try to get current database from schema or fetch all
                    // First, check if there's a schema-based hint
                    const currentDb = await connectionManager.getCurrentDatabase(targetConnectionName);
                    if (currentDb) {
                        databasesToSearch = [currentDb];
                    } else {
                        // Fall back to searching all databases (slower)
                        const dbResults = await runQuery(context, "SELECT DATABASE FROM system.._v_database ORDER BY DATABASE", true, targetConnectionName, connectionManager);
                        if (!dbResults) {
                            statusBarDisposable.dispose();
                            return;
                        }
                        const databases = JSON.parse(dbResults);
                        databasesToSearch = databases.map((db: any) => db.DATABASE);
                    }
                }

                for (const dbName of databasesToSearch) {
                    try {
                        const types = searchType === 'COLUMN' ? ['TABLE', 'VIEW', 'EXTERNAL TABLE'] : searchTypes;

                        for (const type of types) {
                            let query = `SELECT OBJNAME, OBJTYPE, SCHEMA, OBJID FROM ${dbName}.._V_OBJECT_DATA WHERE UPPER(OBJNAME) = UPPER('${searchName.replace(/'/g, "''")}') AND UPPER(OBJTYPE) = UPPER('${type}') AND DBNAME = '${dbName}'`;

                            if (data.schema) {
                                query += ` AND UPPER(SCHEMA) = UPPER('${data.schema.replace(/'/g, "''")}')`;
                            }

                            // Pass connection name provided in data or determined above
                            const objResults = await runQuery(context, query, true, targetConnectionName, connectionManager);
                            if (objResults) {
                                const objects = JSON.parse(objResults);
                                if (objects.length > 0) {
                                    const obj = objects[0];
                                    const { SchemaItem } = await import('./schemaProvider');
                                    // IMPORTANT: Pass the connectionName so the SchemaItem can be found in the tree
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

                                    // If we were looking for a column, we found the parent table. Now we might want to expand it?
                                    // The original code just revealed the table.

                                    await schemaTreeView.reveal(targetItem, { select: true, focus: true, expand: true });
                                    statusBarDisposable.dispose();
                                    vscode.window.setStatusBarMessage(`$(check) Found ${searchName} in ${dbName}.${obj.SCHEMA}`, 3000);
                                    return;
                                }
                            }
                        }
                    } catch (e) { console.log(`Error searching in ${dbName}:`, e); }
                }
                statusBarDisposable.dispose();
                vscode.window.showWarningMessage(`Could not find ${searchType || 'object'} ${searchName}`);
            } catch (err: any) {
                statusBarDisposable.dispose();
                vscode.window.showErrorMessage(`Error revealing item: ${err.message}`);
            }
        }),
        vscode.commands.registerCommand('netezza.showQueryHistory', () => {
            vscode.commands.executeCommand('netezza.queryHistory.focus');
        }),
        vscode.commands.registerCommand('netezza.clearQueryHistory', async () => {
            const { QueryHistoryManager } = await import('./queryHistoryManager');
            const historyManager = new QueryHistoryManager(context);

            const confirm = await vscode.window.showWarningMessage(
                'Are you sure you want to clear all query history?',
                { modal: true },
                'Clear All'
            );

            if (confirm === 'Clear All') {
                await historyManager.clearHistory();
                queryHistoryProvider.refresh();
                vscode.window.showInformationMessage('Query history cleared');
            }
        })
    );

    // Register DocumentLinkProvider
    context.subscriptions.push(
        vscode.languages.registerDocumentLinkProvider(
            { language: 'sql' },
            new NetezzaDocumentLinkProvider()
        )
    );

    // Register FoldingRangeProvider
    context.subscriptions.push(
        vscode.languages.registerFoldingRangeProvider(
            { language: 'sql' },
            new NetezzaFoldingRangeProvider()
        )
    );

    // Register Jump to Schema command
    context.subscriptions.push(
        vscode.commands.registerCommand('netezza.jumpToSchema', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor) return;

            const document = editor.document;
            const selection = editor.selection;
            const offset = document.offsetAt(selection.active);

            const objectInfo = SqlParser.getObjectAtPosition(document.getText(), offset);

            if (objectInfo) {
                vscode.commands.executeCommand('netezza.revealInSchema', objectInfo);
            } else {
                vscode.window.showWarningMessage('No object found at cursor');
            }
        })
    );

    // F5/Ctrl+Enter - Smart/Sequential Execution
    let disposable = vscode.commands.registerCommand('netezza.runQuery', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage('No active editor found');
            return;
        }

        const document = editor.document;
        const selection = editor.selection;
        const text = document.getText();
        const sourceUri = document.uri.toString();

        let queries: string[] = [];

        if (!selection.isEmpty) {
            // Execute selection
            const selectedText = document.getText(selection);
            if (!selectedText.trim()) {
                vscode.window.showWarningMessage('No SQL query selected');
                return;
            }
            queries = SqlParser.splitStatements(selectedText);
        } else {
            // Execute statement at cursor
            const offset = document.offsetAt(selection.active);
            const statement = SqlParser.getStatementAtPosition(text, offset);

            if (statement) {
                queries = [statement.sql];
                // Optional: Select the statement in editor to show what's running
                const startPos = document.positionAt(statement.start);
                const endPos = document.positionAt(statement.end);
                editor.selection = new vscode.Selection(startPos, endPos);
            } else {
                vscode.window.showWarningMessage('No SQL statement found at cursor');
                return;
            }
        }

        if (queries.length === 0) {
            return;
        }

        // If the content looks like a python script invocation or a direct .py script,
        // run it in an integrated terminal instead of sending to the DB.
        const single = queries.length === 1 ? queries[0].trim() : null;
        if (single) {
            const tokens = single.split(/\s+/);
            const first = tokens[0] || '';

            const isPythonExec = /python(\.exe)?$/i.test(first) && tokens.length >= 2 && tokens[1].toLowerCase().endsWith('.py');
            const isScriptDirect = first.toLowerCase().endsWith('.py');

            if (isPythonExec || isScriptDirect) {
                // Build command to run in terminal
                const config = vscode.workspace.getConfiguration('netezza');
                const pythonPath = config.get<string>('pythonPath') || 'python';

                let cmd = '';
                if (isPythonExec) {
                    const py = tokens[0];
                    const script = tokens[1];
                    const args = tokens.slice(2);
                    cmd = buildExecCommand(py, script, args);
                } else {
                    const script = first;
                    const args = tokens.slice(1);
                    cmd = buildExecCommand(pythonPath, script, args);
                }

                const term = vscode.window.createTerminal({ name: 'Netezza: Script' });
                term.show(true);
                term.sendText(cmd, true);
                vscode.window.showInformationMessage(`Running script: ${cmd}`);
                return;
            }
        }

        try {
            const results = await runQueriesSequentially(context, queries, connectionManager, sourceUri);

            // Transform results to match what ResultPanelView expects
            // We need to pass columns and data separately or as a structured object
            // Let's update ResultPanelView.updateResults to accept QueryResult[]

            // For now, let's assume we'll update ResultPanelView to accept QueryResult[]
            resultPanelProvider.updateResults(results, sourceUri, false);
            vscode.commands.executeCommand('netezza.results.focus');

        } catch (err: any) {
            vscode.window.showErrorMessage(`Error executing query: ${err.message}`);
        }
    });

    // Ctrl+F5 / Ctrl+Shift+F5 - Run Batch (executes all statements at once)
    let disposableBatch = vscode.commands.registerCommand('netezza.runQueryBatch', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage('No active editor found');
            return;
        }

        const document = editor.document;
        const selection = editor.selection;
        const sourceUri = document.uri.toString();

        // Get text to execute: selected text if something is selected, otherwise entire document
        let text: string;
        if (!selection.isEmpty) {
            text = document.getText(selection);
        } else {
            text = document.getText();
        }

        if (!text.trim()) {
            vscode.window.showWarningMessage('No SQL query to execute');
            return;
        }

        // If the content looks like a script invocation, run it in a terminal
        const full = text.trim();
        const tokens = full.split(/\s+/);
        const first = tokens[0] || '';
        const isPythonExec = /python(\.exe)?$/i.test(first) && tokens.length >= 2 && tokens[1].toLowerCase().endsWith('.py');
        const isScriptDirect = first.toLowerCase().endsWith('.py');

        if (isPythonExec || isScriptDirect) {
            const config = vscode.workspace.getConfiguration('netezza');
            const pythonPath = config.get<string>('pythonPath') || 'python';

            let cmd = '';
            if (isPythonExec) {
                const py = tokens[0];
                const script = tokens[1];
                const args = tokens.slice(2);
                cmd = buildExecCommand(py, script, args);
            } else {
                const script = first;
                const args = tokens.slice(1);
                cmd = buildExecCommand(pythonPath, script, args);
            }

            const term = vscode.window.createTerminal({ name: 'Netezza: Script' });
            term.show(true);
            term.sendText(cmd, true);
            vscode.window.showInformationMessage(`Running script: ${cmd}`);
            return;
        }

        try {
            // Use runQueryRaw to get proper structure - sends everything as one batch (no splitting)
            const { runQueryRaw } = await import('./queryRunner');
            const result = await runQueryRaw(context, text, false, connectionManager, undefined, sourceUri);

            if (result) {
                // Wrap in array to match QueryResult[] (one result set)
                resultPanelProvider.updateResults([result], sourceUri, false);
                vscode.commands.executeCommand('netezza.results.focus');
            }
        } catch (err: any) {
            vscode.window.showErrorMessage(`Error executing query: ${err.message}`);
        }
    });

    context.subscriptions.push(disposableBatch);

    // Output channel for logging
    const outputChannel = vscode.window.createOutputChannel('Netezza');

    // Helper to log execution time
    const logExecutionTime = (operation: string, startTime: number) => {
        const duration = Date.now() - startTime;
        outputChannel.appendLine(`[${new Date().toLocaleTimeString()}] ${operation} completed in ${duration}ms`);
        outputChannel.show(true); // Show output channel without taking focus
    };

    // Export to Excel
    let disposableExportXlsb = vscode.commands.registerCommand('netezza.exportToXlsb', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage('No active editor found');
            return;
        }

        const selection = editor.selection;
        const text = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);

        if (!text.trim()) {
            vscode.window.showWarningMessage('No SQL query to export');
            return;
        }

        // Show save dialog
        const uri = await vscode.window.showSaveDialog({
            filters: {
                'Excel Workbook': ['xlsx']
            },
            saveLabel: 'Export to XLSX'
        });

        if (!uri) {
            return; // User cancelled
        }

        const startTime = Date.now(); // Start timing after user interaction

        try {
            // Get connection string for this document
            const documentUri = editor.document.uri.toString();
            const connectionName = connectionManager.getConnectionForExecution(documentUri);
            const connectionString = await connectionManager.getConnectionString(connectionName);
            if (!connectionString) {
                throw new Error('Connection not configured. Please connect via Netezza: Connect...');
            }

            // Show progress
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: 'Exporting to XLSX...',
                cancellable: false
            }, async (progress) => {
                const { exportQueryToXlsb } = await import('./xlsbExporter');

                const result = await exportQueryToXlsb(
                    connectionString,
                    text,
                    uri.fsPath,
                    false, // Don't copy to clipboard
                    (message: string) => {
                        progress.report({ message: message });
                        outputChannel.appendLine(`[XLSX Export] ${message}`);
                    }
                );

                if (!result.success) {
                    throw new Error(result.message);
                }
            });

            logExecutionTime('Export to XLSX', startTime);
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath}`);

        } catch (err: any) {
            vscode.window.showErrorMessage(`Error exporting to XLSX: ${err.message}`);
        }
    });

    // Export to CSV
    let disposableExportCsv = vscode.commands.registerCommand('netezza.exportToCsv', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage('No active editor found');
            return;
        }

        const selection = editor.selection;
        const text = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);

        if (!text.trim()) {
            vscode.window.showWarningMessage('No SQL query to export');
            return;
        }

        // Show save dialog
        const uri = await vscode.window.showSaveDialog({
            filters: {
                'CSV Files': ['csv']
            },
            saveLabel: 'Export to CSV'
        });

        if (!uri) {
            return; // User cancelled
        }

        const startTime = Date.now(); // Start timing after user interaction

        try {
            // Get connection string for this document
            const documentUri = editor.document.uri.toString();
            const connectionName = connectionManager.getConnectionForExecution(documentUri);
            const connectionString = await connectionManager.getConnectionString(connectionName);
            if (!connectionString) {
                throw new Error('Connection not configured. Please connect via Netezza: Connect...');
            }

            // Show progress
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: 'Exporting to CSV...',
                cancellable: false
            }, async (progress) => {
                const { exportToCsv } = await import('./csvExporter');
                await exportToCsv(context, connectionString, text, uri.fsPath, progress);
            });

            logExecutionTime('Export to CSV', startTime);
            vscode.window.showInformationMessage(`Results exported to ${uri.fsPath}`);

        } catch (err: any) {
            vscode.window.showErrorMessage(`Error exporting to CSV: ${err.message}`);
        }
    });

    // Copy XLSB to Clipboard
    let disposableCopyXlsb = vscode.commands.registerCommand('netezza.copyXlsbToClipboard', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage('No active editor found');
            return;
        }

        const selection = editor.selection;
        const text = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);

        if (!text.trim()) {
            vscode.window.showWarningMessage('No SQL query to export');
            return;
        }

        try {
            // Get connection string for this document
            const documentUri = editor.document.uri.toString();
            const connectionName = connectionManager.getConnectionForExecution(documentUri);
            const connectionString = await connectionManager.getConnectionString(connectionName);
            if (!connectionString) {
                throw new Error('Connection not configured. Please connect via Netezza: Connect...');
            }

            const startTime = Date.now();

            // Show progress
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: 'Exporting to XLSX and copying to clipboard...',
                cancellable: false
            }, async (progress) => {
                const { exportQueryToXlsb, getTempFilePath } = await import('./xlsbExporter');

                // Generate temporary file path
                const tempPath = getTempFilePath();

                const result = await exportQueryToXlsb(
                    connectionString,
                    text,
                    tempPath,
                    true, // Copy to clipboard
                    (message: string) => {
                        progress.report({ message: message });
                        outputChannel.appendLine(`[XLSX Clipboard] ${message}`);
                    }
                );

                if (!result.success) {
                    throw new Error(result.message);
                }

                if (!result.details?.clipboard_success) {
                    throw new Error('Failed to copy file to clipboard');
                }
            });

            logExecutionTime('Copy XLSX to Clipboard', startTime);

            // Show success message with details
            const action = await vscode.window.showInformationMessage(
                'Excel file copied to clipboard! You can now paste it into Excel or Windows Explorer.',
                'Show Temp Folder',
                'OK'
            );

            if (action === 'Show Temp Folder') {
                // Open temp directory
                const tempDir = require('os').tmpdir();
                await vscode.env.openExternal(vscode.Uri.file(tempDir));
            }

        } catch (err: any) {
            vscode.window.showErrorMessage(`Error copying XLSX to clipboard: ${err.message}`);
        }
    });

    // Export to XLSB and Open
    let disposableExportXlsbAndOpen = vscode.commands.registerCommand('netezza.exportToXlsbAndOpen', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage('No active editor found');
            return;
        }

        const selection = editor.selection;
        const text = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);

        if (!text.trim()) {
            vscode.window.showWarningMessage('No SQL query to export');
            return;
        }

        // Show save dialog
        const uri = await vscode.window.showSaveDialog({
            filters: {
                'Excel Workbook': ['xlsx']
            },
            saveLabel: 'Export to XLSX and Open'
        });

        if (!uri) {
            return; // User cancelled
        }

        const startTime = Date.now(); // Start timing after user interaction

        try {
            // Get connection string for this document
            const documentUri = editor.document.uri.toString();
            const connectionName = connectionManager.getConnectionForExecution(documentUri);
            const connectionString = await connectionManager.getConnectionString(connectionName);
            if (!connectionString) {
                throw new Error('Connection not configured. Please connect via Netezza: Connect...');
            }

            // Show progress
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: 'Exporting to XLSX and opening...',
                cancellable: false
            }, async (progress) => {
                const { exportQueryToXlsb } = await import('./xlsbExporter');

                const result = await exportQueryToXlsb(
                    connectionString,
                    text,
                    uri.fsPath,
                    false, // Don't copy to clipboard
                    (message: string) => {
                        progress.report({ message: message });
                        outputChannel.appendLine(`[XLSX Export] ${message}`);
                    }
                );

                if (!result.success) {
                    throw new Error(result.message);
                }
            });

            logExecutionTime('Export to XLSX and Open', startTime);

            // Open the file
            await vscode.env.openExternal(uri);
            vscode.window.showInformationMessage(`Results exported and opened: ${uri.fsPath}`);

        } catch (err: any) {
            vscode.window.showErrorMessage(`Error exporting to XLSX: ${err.message}`);
        }
    });

    // Import Data from Clipboard
    let disposableImportClipboard = vscode.commands.registerCommand('netezza.importClipboard', async () => {
        try {
            // Get connection string for active document
            const editor = vscode.window.activeTextEditor;
            const documentUri = editor?.document?.uri?.toString();
            const connectionName = connectionManager.getConnectionForExecution(documentUri);
            const connectionString = await connectionManager.getConnectionString(connectionName);
            if (!connectionString) {
                throw new Error('Connection not configured. Please connect via Netezza: Connect...');
            }

            // Ask for target table name (optional)
            const targetTable = await vscode.window.showInputBox({
                prompt: 'Enter target table name (leave empty for auto-generated name)',
                placeHolder: 'e.g. my_schema.my_table or leave empty',
                validateInput: (value) => {
                    if (!value || value.trim().length === 0) {
                        return null; // Empty is allowed - will generate auto name
                    }
                    // Basic validation for table name
                    if (!/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(value.trim())) {
                        return 'Invalid table name format. Use: [database.]schema.table';
                    }
                    return null;
                }
            });

            if (targetTable === undefined) {
                return; // User cancelled (Escape)
            }

            // Generate table name if empty
            let finalTableName: string;
            if (!targetTable || targetTable.trim().length === 0) {
                // Generate auto table name: DATABASE.SCHEMA.[IMPORT_YYYYMMDD_RANDOM]
                // First, get current database and schema from connection
                try {
                    const currentDbQuery = "SELECT CURRENT_CATALOG, CURRENT_SCHEMA";
                    const currentDbResult = await runQuery(context, currentDbQuery, true);

                    if (currentDbResult) {
                        const dbInfo = JSON.parse(currentDbResult);
                        if (dbInfo && dbInfo.length > 0) {
                            const database = dbInfo[0].CURRENT_CATALOG || 'SYSTEM';
                            const schema = dbInfo[0].CURRENT_SCHEMA || 'ADMIN';

                            // Generate random suffix
                            const now = new Date();
                            const dateStr = now.toISOString().slice(0, 10).replace(/-/g, ''); // YYYYMMDD
                            const random = Math.floor(Math.random() * 10000).toString().padStart(4, '0');

                            finalTableName = `${database}.${schema}.IMPORT_${dateStr}_${random}`;

                            vscode.window.showInformationMessage(`Auto-generated table name: ${finalTableName}`);
                        } else {
                            throw new Error('Could not determine current database/schema');
                        }
                    } else {
                        throw new Error('Could not determine current database/schema');
                    }
                } catch (err: any) {
                    vscode.window.showErrorMessage(`Error getting current database/schema: ${err.message}`);
                    return;
                }
            } else {
                finalTableName = targetTable.trim();
            }

            // Show format options
            const formatOptions = await vscode.window.showQuickPick([
                { label: 'Auto-detect', description: 'Automatically detect clipboard format (text or Excel XML)', value: null },
                { label: 'Excel XML Spreadsheet', description: 'Force Excel XML format processing', value: 'XML Spreadsheet' },
                { label: 'Plain Text', description: 'Force plain text processing with delimiter detection', value: 'TEXT' }
            ], {
                placeHolder: 'Select clipboard data format'
            });

            if (!formatOptions) {
                return; // User cancelled
            }

            const startTime = Date.now(); // Start timing after user interaction

            // Execute import using native TypeScript module
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: 'Importing clipboard data...',
                cancellable: false
            }, async (progress) => {
                const { importClipboardDataToNetezza } = await import('./clipboardImporter');

                const result = await importClipboardDataToNetezza(
                    finalTableName,
                    connectionString,
                    formatOptions.value,
                    {},
                    (message: string) => {
                        progress.report({ message: message });
                        outputChannel.appendLine(`[Clipboard Import] ${message}`);
                    }
                );

                if (!result.success) {
                    throw new Error(result.message);
                }

                // Log details
                if (result.details) {
                    outputChannel.appendLine(`[Clipboard Import] Rows processed: ${result.details.rowsProcessed}`);
                    outputChannel.appendLine(`[Clipboard Import] Columns: ${result.details.columns}`);
                    outputChannel.appendLine(`[Clipboard Import] Format: ${result.details.format}`);
                }
            });

            logExecutionTime('Import Clipboard Data', startTime);
            vscode.window.showInformationMessage(`Clipboard data imported successfully to table: ${finalTableName}`);

        } catch (err: any) {
            vscode.window.showErrorMessage(`Error importing clipboard data: ${err.message}`);
        }
    });

    // Import Data
    let disposableImportData = vscode.commands.registerCommand('netezza.importData', async () => {
        try {
            // Get connection string first
            const connectionString = await connectionManager.getConnectionString();
            if (!connectionString) {
                throw new Error('Connection not configured. Please connect via Netezza: Connect...');
            }

            // Show file selection dialog
            const fileUris = await vscode.window.showOpenDialog({
                canSelectFiles: true,
                canSelectFolders: false,
                canSelectMany: false,
                filters: {
                    'Data Files': ['csv', 'txt', 'xlsx', 'xlsb', 'json'],
                    'CSV Files': ['csv'],
                    'Excel Files': ['xlsx', 'xlsb'],
                    'Text Files': ['txt'],
                    'JSON Files': ['json'],
                    'All Files': ['*']
                },
                openLabel: 'Select file to import'
            });

            if (!fileUris || fileUris.length === 0) {
                return; // User cancelled
            }

            const sourceFile = fileUris[0].fsPath;

            // Ask for target table name (optional - like clipboard import)
            const targetTable = await vscode.window.showInputBox({
                prompt: 'Enter target table name (leave empty for auto-generated name)',
                placeHolder: 'e.g. my_schema.my_table or leave empty',
                validateInput: (value) => {
                    if (!value || value.trim().length === 0) {
                        return null; // Empty is allowed - will generate auto name
                    }
                    // Basic validation for table name
                    if (!/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(value.trim())) {
                        return 'Invalid table name format. Use: [database.]schema.table';
                    }
                    return null;
                }
            });

            if (targetTable === undefined) {
                return; // User cancelled (Escape)
            }

            // Generate table name if empty
            let finalTableName: string;
            if (!targetTable || targetTable.trim().length === 0) {
                // Generate auto table name: DATABASE.SCHEMA.[IMPORT_YYYYMMDD_RANDOM]
                // First, get current database and schema from connection
                try {
                    const currentDbQuery = "SELECT CURRENT_CATALOG, CURRENT_SCHEMA";
                    const currentDbResult = await runQuery(context, currentDbQuery, true);

                    if (currentDbResult) {
                        const dbInfo = JSON.parse(currentDbResult);
                        if (dbInfo && dbInfo.length > 0) {
                            const database = dbInfo[0].CURRENT_CATALOG || 'SYSTEM';
                            const schema = dbInfo[0].CURRENT_SCHEMA || 'ADMIN';

                            // Generate random suffix
                            const now = new Date();
                            const dateStr = now.toISOString().slice(0, 10).replace(/-/g, ''); // YYYYMMDD
                            const random = Math.floor(Math.random() * 10000).toString().padStart(4, '0');

                            finalTableName = `${database}.${schema}.IMPORT_${dateStr}_${random}`;

                            vscode.window.showInformationMessage(`Auto-generated table name: ${finalTableName}`);
                        } else {
                            throw new Error('Could not determine current database/schema');
                        }
                    } else {
                        throw new Error('Could not determine current database/schema');
                    }
                } catch (err: any) {
                    vscode.window.showErrorMessage(`Error getting current database/schema: ${err.message}`);
                    return;
                }
            } else {
                finalTableName = targetTable.trim();
            }

            // Show import options (for future use)
            const importOptions = await vscode.window.showQuickPick([
                { label: 'Default Import', description: 'Use default settings', value: {} },
                { label: 'Custom Options', description: 'Configure import settings (coming soon)', value: null }
            ], {
                placeHolder: 'Select import options'
            });

            if (!importOptions) {
                return; // User cancelled
            }

            if (importOptions.value === null) {
                vscode.window.showInformationMessage('Custom options will be available in future version');
                return;
            }

            const startTime = Date.now(); // Start timing after user interaction

            // Execute import using native TypeScript module
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: 'Importing data...',
                cancellable: false
            }, async (progress) => {
                const { importDataToNetezza } = await import('./dataImporter');

                const result = await importDataToNetezza(
                    sourceFile,
                    finalTableName,
                    connectionString,
                    importOptions.value || {},
                    (message: string) => {
                        progress.report({ message: message });
                        outputChannel.appendLine(`[Import] ${message}`);
                    }
                );

                if (!result.success) {
                    throw new Error(result.message);
                }

                // Log details
                if (result.details) {
                    outputChannel.appendLine(`[Import] Rows processed: ${result.details.rowsProcessed}`);
                    outputChannel.appendLine(`[Import] Columns: ${result.details.columns}`);
                    outputChannel.appendLine(`[Import] Delimiter: ${result.details.detectedDelimiter}`);
                }
            });

            logExecutionTime('Import Data', startTime);
            vscode.window.showInformationMessage(`Data imported successfully to table: ${finalTableName}`);

        } catch (err: any) {
            vscode.window.showErrorMessage(`Error importing data: ${err.message}`);
        }
    });

    // Export Current Result to XLSB and Open (for datagrid)
    let disposableExportCurrentResultToXlsbAndOpen = vscode.commands.registerCommand('netezza.exportCurrentResultToXlsbAndOpen', async (csvContent: string, sql?: string) => {
        try {
            if (!csvContent) {
                vscode.window.showErrorMessage('No data to export');
                return;
            }

            // Generate temporary file path
            const os = require('os');
            const path = require('path');
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const tempPath = path.join(os.tmpdir(), `netezza_results_${timestamp}.xlsx`);

            const startTime = Date.now();

            // Show progress
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: 'Creating Excel file...',
                cancellable: false
            }, async (progress) => {
                const { exportCsvToXlsb } = await import('./xlsbExporter');

                const result = await exportCsvToXlsb(
                    csvContent,
                    tempPath,
                    false, // Don't copy to clipboard
                    { source: 'Query Results Panel', sql: sql },
                    (message: string) => {
                        progress.report({ message: message });
                        outputChannel.appendLine(`[CSV to XLSX] ${message}`);
                    }
                );

                if (!result.success) {
                    throw new Error(result.message);
                }
            });

            const duration = Date.now() - startTime;
            outputChannel.appendLine(`[${new Date().toLocaleTimeString()}] Export Current Result to Excel completed in ${duration}ms`);

            // Open the file
            await vscode.env.openExternal(vscode.Uri.file(tempPath));
            vscode.window.showInformationMessage(`Results exported and opened: ${tempPath}`);

        } catch (err: any) {
            vscode.window.showErrorMessage(`Error exporting to Excel: ${err.message}`);
        }
    });

    context.subscriptions.push(disposable);
    // context.subscriptions.push(disposableSelection); // Removed
    context.subscriptions.push(disposableExportXlsb);
    context.subscriptions.push(disposableExportCsv);
    context.subscriptions.push(disposableCopyXlsb);
    context.subscriptions.push(disposableExportXlsbAndOpen);
    context.subscriptions.push(disposableExportCurrentResultToXlsbAndOpen);
    context.subscriptions.push(disposableImportClipboard);
    context.subscriptions.push(disposableImportData);

    // Auto-detect XML Spreadsheet paste operations
    let disposablePasteDetection = vscode.workspace.onWillSaveTextDocument(async (event) => {
        // This is a workaround since VS Code doesn't have direct paste event
        // We'll use a different approach with clipboard monitoring
    });

    // Alternative approach: Register paste command override
    let disposablePasteOverride = vscode.commands.registerCommand('netezza.smartPaste', async () => {
        try {
            const activeEditor = vscode.window.activeTextEditor;
            if (!activeEditor) {
                return;
            }

            // Check if clipboard contains XML Spreadsheet format (native Windows format check)
            const config = vscode.workspace.getConfiguration('netezza');
            const pythonPath = config.get<string>('pythonPath') || 'python';
            const checkScriptPath = path.join(context.extensionPath, 'python', 'check_clipboard_format.py');

            const cp = require('child_process');

            // Check clipboard format synchronously
            const hasXmlSpreadsheet = await new Promise<boolean>((resolve) => {
                const checkProcess = cp.spawn(pythonPath, [checkScriptPath]);
                checkProcess.on('close', (code: number) => {
                    resolve(code === 1); // Exit code 1 means XML Spreadsheet format is available
                });
                checkProcess.on('error', () => {
                    resolve(false); // If error, assume format not available
                });
            });

            if (hasXmlSpreadsheet) {
                const action = await vscode.window.showQuickPick([
                    {
                        label: '📊 import to Netezza table',
                        description: 'Detected "XML Spreadsheet" format - import data to database',
                        value: 'import'
                    },
                    {
                        label: '📝 Paste as text',
                        description: 'Paste clipboard content as plain text',
                        value: 'paste'
                    }
                ], {
                    placeHolder: 'Detected "XML Spreadsheet" format in clipboard - choose an action'
                });

                if (action?.value === 'import') {
                    // Execute clipboard import with XML Spreadsheet format
                    vscode.commands.executeCommand('netezza.importClipboard');
                } else if (action?.value === 'paste') {
                    // Get text content and insert at cursor position
                    const clipboardContent = await vscode.env.clipboard.readText();
                    const selection = activeEditor.selection;
                    await activeEditor.edit(editBuilder => {
                        editBuilder.replace(selection, clipboardContent);
                    });
                }
            } else {
                // Normal paste for non-XML Spreadsheet content
                const clipboardContent = await vscode.env.clipboard.readText();
                const selection = activeEditor.selection;
                await activeEditor.edit(editBuilder => {
                    editBuilder.replace(selection, clipboardContent);
                });
            }

        } catch (error: any) {
            vscode.window.showErrorMessage(`Error during paste: ${error.message}`);
        }
    });


    // SQL Shortcuts - Auto-expand shortcuts like SX -> SELECT
    let disposableSqlShortcuts = vscode.workspace.onDidChangeTextDocument(async (event) => {
        // Only process SQL files
        if (event.document.languageId !== 'sql' && event.document.languageId !== 'mssql') {
            return;
        }

        // Only process single character additions (typing)
        if (event.contentChanges.length !== 1) {
            return;
        }

        const change = event.contentChanges[0];

        // Check if user typed a space (trigger for shortcuts)
        if (change.text !== ' ') {
            return;
        }

        // Get the active editor
        const editor = vscode.window.activeTextEditor;
        if (!editor || editor.document !== event.document) {
            return;
        }

        // Get the line where the change occurred
        const line = event.document.lineAt(change.range.start.line);
        const lineText = line.text;

        // Define SQL shortcuts
        const shortcuts = new Map([
            ['SX', 'SELECT'],
            ['WX', 'WHERE'],
            ['GX', 'GROUP BY'],
            ['HX', 'HAVING'],
            ['OX', 'ORDER BY'],
            ['FX', 'FROM'],
            ['JX', 'JOIN'],
            ['LX', 'LIMIT'],
            ['IX', 'INSERT INTO'],
            ['UX', 'UPDATE'],
            ['DX', 'DELETE FROM'],
            ['CX', 'CREATE TABLE']
        ]);

        // Check if any shortcut should be expanded
        for (const [trigger, replacement] of shortcuts) {
            const pattern = new RegExp(`\\b${trigger}\\s$`, 'i');

            if (pattern.test(lineText)) {
                // Found a shortcut to expand
                const triggerStart = lineText.toUpperCase().lastIndexOf(trigger.toUpperCase());
                if (triggerStart >= 0) {
                    // Calculate positions
                    const startPos = new vscode.Position(change.range.start.line, triggerStart);
                    const endPos = new vscode.Position(change.range.start.line, triggerStart + trigger.length + 1); // +1 for space

                    // Replace the shortcut + space with the full text + space
                    await editor.edit(editBuilder => {
                        editBuilder.replace(new vscode.Range(startPos, endPos), replacement + ' ');
                    });

                    // Trigger IntelliSense for SELECT, FROM, JOIN
                    if (['SELECT', 'FROM', 'JOIN'].includes(replacement)) {
                        setTimeout(() => {
                            vscode.commands.executeCommand('editor.action.triggerSuggest');
                        }, 100);
                    }

                    break; // Only process one shortcut at a time
                }
            }
        }
    });

    context.subscriptions.push(disposablePasteOverride);
    context.subscriptions.push(disposableSqlShortcuts);

    // Register SQL Completion Provider
    const completionProvider = new SqlCompletionItemProvider(context, metadataCache);
    context.subscriptions.push(
        vscode.languages.registerCompletionItemProvider(['sql', 'mssql'], completionProvider, '.', ' ')
    );

    // Register command to clear autocomplete cache
    context.subscriptions.push(
        vscode.commands.registerCommand('netezza.clearAutocompleteCache', async () => {
            const confirm = await vscode.window.showWarningMessage(
                'Are you sure you want to clear the autocomplete cache? This will remove all cached databases, schemas, tables, and columns.',
                { modal: true },
                'Clear Cache'
            );

            if (confirm === 'Clear Cache') {
                await metadataCache.clearCache();
                vscode.window.showInformationMessage('Autocomplete cache cleared successfully. Cache will be rebuilt on next use.');
            }
        })
    );
}

export function deactivate() { }
