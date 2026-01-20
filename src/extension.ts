/**
 * Netezza VS Code Extension - Main Entry Point
 *
 * This file has been refactored to use modular command registration.
 * All commands are now organized in separate modules under src/commands/
 */

import * as vscode from 'vscode';
import { runQueryRaw } from './core/queryRunner';
import { ConnectionManager } from './core/connectionManager';
import { LoginPanel } from './views/loginPanel';
import { SchemaProvider } from './providers/schemaProvider';
import { ResultPanelView } from './views/resultPanelView';
import { SqlCompletionItemProvider } from './providers/completionProvider';
import { SchemaSearchProvider } from './providers/schemaSearchProvider';
import { SqlParser } from './sql/sqlParser';
import { NetezzaDocumentLinkProvider } from './providers/documentLinkProvider';
import { NetezzaFoldingRangeProvider } from './providers/foldingProvider';
import { QueryHistoryView } from './views/queryHistoryView';
import { EditDataProvider, EditDataItem } from './views/editDataProvider';
import { MetadataCache } from './metadataCache';
import { activateSqlLinter } from './providers/sqlLinterProvider';
import { NetezzaLinterCodeActionProvider } from './providers/linterCodeActions';
import { EtlDesignerView } from './views/etlDesignerView';
import { EtlProjectManager } from './etl/etlProjectManager';
import { 
    CopilotService, 
    SchemaTool, 
    ColumnsTool, 
    TablesTool,
    ExecuteQueryTool,
    SampleDataTool,
    ExplainPlanTool,
    SearchSchemaTool,
    TableStatsTool,
    DependenciesTool,
    ValidateSqlTool,
    DatabasesTool,
    SchemasTool,
    ProceduresTool,
    ViewsTool,
    ExternalTablesTool,
    GetObjectDefinitionTool
} from './services/copilotService';

// Import modular command registrations
import { registerSchemaCommands } from './commands/schemaCommands';
import { registerExportCommands } from './commands/exportCommands';
import { registerImportCommands } from './commands/importCommands';
import { registerQueryCommands } from './commands/queryCommands';

// Import services and utilities
import {
    createKeepConnectionStatusBar,
    createActiveConnectionStatusBar,
    createActiveDatabaseStatusBar,
    updateKeepConnectionStatusBar
} from './services/statusBarManager';
import {
    ScriptCodeLensProvider,
    createScriptDecoration,
    createSqlStatementDecoration,
    registerDecorationSubscriptions
} from './editors/decorationManager';
import { registerSqlShortcuts } from './editors/sqlShortcuts';
import { buildExecCommand } from './utils/shellUtils';

// Known SQL extensions that may conflict with Netezza
const KNOWN_SQL_EXTENSIONS = [
    { id: 'mtxr.sqltools', name: 'SQLTools' },
    { id: 'ms-mssql.mssql', name: 'Microsoft SQL Server' },
    { id: 'oracle.oracledevtools', name: 'Oracle Developer Tools' },
    { id: 'cweijan.vscode-mysql-client2', name: 'MySQL' },
    { id: 'ckolkman.vscode-postgres', name: 'PostgreSQL' }
];

/**
 * Check for conflicting SQL extensions and warn user
 */
async function checkForConflictingExtensions(_context: vscode.ExtensionContext): Promise<void> {
    const config = vscode.workspace.getConfiguration('netezza');
    const showWarnings = config.get<boolean>('showConflictWarnings', true);

    if (!showWarnings) {
        return;
    }

    const foundKnown: string[] = [];
    for (const ext of KNOWN_SQL_EXTENSIONS) {
        if (vscode.extensions.getExtension(ext.id)) {
            foundKnown.push(ext.name);
        }
    }

    const otherSqlExtensions = vscode.extensions.all.filter(ext => {
        const pkg = ext.packageJSON;
        if (!pkg || ext.id === 'krzysztof-d.justybaselite-netezza') {
            return false;
        }

        if (KNOWN_SQL_EXTENSIONS.some(k => k.id === ext.id)) {
            return false;
        }

        const activatesOnSql = pkg.activationEvents?.some(
            (e: string) => e.includes('onLanguage:sql') || e.includes('onLanguage:mssql')
        );

        const contributesSql = pkg.contributes?.languages?.some(
            (lang: { id: string; extensions?: string[] }) => lang.id === 'sql' || lang.extensions?.includes('.sql')
        );

        const displayName = pkg.displayName || '';
        if (displayName === 'SQL Language Basics') {
            return false;
        }

        return activatesOnSql || contributesSql;
    });

    const foundOther = otherSqlExtensions.map(ext => ext.packageJSON.displayName || ext.id);
    const allConflicts = [...foundKnown, ...foundOther];

    if (allConflicts.length > 0) {
        const message =
            allConflicts.length === 1
                ? `SQL extension detected "${allConflicts[0]}" which may cause conflicts (e.g. duplicate keybindings F5, Ctrl+Enter).`
                : `SQL extensions detected which may cause conflicts: ${allConflicts.join(', ')}. Some functions (e.g. F5, Ctrl+Enter) may be duplicated.`;

        const result = await vscode.window.showWarningMessage(message, 'OK', 'Do not show again');

        if (result === 'Do not show again') {
            await config.update('showConflictWarnings', false, vscode.ConfigurationTarget.Global);
        }
    }
}

/**
 * Get list of databases from the Netezza server
 */
async function getDatabaseList(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionName: string
): Promise<string[]> {
    const query = `SELECT DATABASE FROM _V_DATABASE ORDER BY DATABASE`;
    
    const result = await runQueryRaw(
        context,
        query,
        true, // silent
        connectionManager,
        connectionName,
        undefined, // no documentUri - this is a utility query
        undefined, // no logCallback
        undefined  // no extensionUri
    );

    if (result.data && result.data.length > 0) {
        return result.data.map(row => String(row[0]));
    }
    return [];
}

export async function activate(context: vscode.ExtensionContext) {
    console.log('Netezza extension: Activating...');

    // Check for other SQL extensions that may conflict
    checkForConflictingExtensions(context);

    // Initialize core components
    const connectionManager = new ConnectionManager(context);
    const metadataCache = new MetadataCache(context);
    await metadataCache.initialize();
    const schemaProvider = new SchemaProvider(context, connectionManager, metadataCache);
    const resultPanelProvider = new ResultPanelView(context.extensionUri);

    // Ensure persistent connections are closed when extension is deactivated
    context.subscriptions.push({
        dispose: () => {
            connectionManager.closeAllDocumentPersistentConnections();
        }
    });

    // Output channel for logging
    const outputChannel = vscode.window.createOutputChannel('Netezza');

    // ========== Status Bar ==========
    const keepConnectionStatusBar = createKeepConnectionStatusBar(context, connectionManager);
    const { updateFn: updateActiveConnectionStatusBar } =
        createActiveConnectionStatusBar(context, connectionManager);
    const { updateFn: updateActiveDatabaseStatusBar } =
        createActiveDatabaseStatusBar(context, connectionManager);

    // Function to update keep connection status bar
    const updateKeepConnectionStatusBarFn = () => {
        updateKeepConnectionStatusBar(keepConnectionStatusBar, connectionManager);
    };

    // Initial update and listen for changes
    updateActiveConnectionStatusBar();
    updateActiveDatabaseStatusBar();
    updateKeepConnectionStatusBarFn();
    connectionManager.onDidChangeActiveConnection(connectionName => {
        updateActiveConnectionStatusBar();
        updateActiveDatabaseStatusBar();
        updateKeepConnectionStatusBarFn();
        if (connectionName && !metadataCache.hasConnectionPrefetchTriggered(connectionName)) {
            metadataCache.triggerConnectionPrefetch(connectionName, q =>
                runQueryRaw(context, q, true, connectionManager, connectionName!)
            );
        }
    });
    connectionManager.onDidChangeConnections(updateActiveConnectionStatusBar);
    connectionManager.onDidChangeDocumentConnection((documentUri: string) => {
        updateActiveConnectionStatusBar();
        updateActiveDatabaseStatusBar();
        updateKeepConnectionStatusBarFn();
        const connectionName = connectionManager.getDocumentConnection(documentUri);
        if (connectionName && !metadataCache.hasConnectionPrefetchTriggered(connectionName)) {
            metadataCache.triggerConnectionPrefetch(connectionName, q =>
                runQueryRaw(context, q, true, connectionManager, connectionName!)
            );
        }
    });
    connectionManager.onDidChangeDocumentDatabase(() => {
        updateActiveDatabaseStatusBar();
    });
    vscode.window.onDidChangeActiveTextEditor(() => {
        updateActiveConnectionStatusBar();
        updateActiveDatabaseStatusBar();
        updateKeepConnectionStatusBarFn();
    });

    // ========== Schema Explorer ==========
    console.log('Netezza extension: Registering SchemaSearchProvider...');
    const schemaSearchProvider = new SchemaSearchProvider(
        context.extensionUri,
        context,
        metadataCache,
        connectionManager
    );

    console.log('Netezza extension: Registering QueryHistoryView...');
    const queryHistoryProvider = new QueryHistoryView(context.extensionUri, context);

    const schemaTreeView = vscode.window.createTreeView('netezza.schema', {
        treeDataProvider: schemaProvider,
        showCollapseAll: true
    });

    context.subscriptions.push(
        vscode.window.registerWebviewViewProvider(ResultPanelView.viewType, resultPanelProvider),
        vscode.window.registerWebviewViewProvider(SchemaSearchProvider.viewType, schemaSearchProvider),
        vscode.window.registerWebviewViewProvider(QueryHistoryView.viewType, queryHistoryProvider)
    );

    // ========== Decorations ==========
    const scriptDecoration = createScriptDecoration();
    const sqlStatementDecoration = createSqlStatementDecoration();
    registerDecorationSubscriptions(context, scriptDecoration, sqlStatementDecoration);

    // CodeLens for scripts
    const scriptLensProvider = new ScriptCodeLensProvider();
    context.subscriptions.push(vscode.languages.registerCodeLensProvider({ scheme: 'file' }, scriptLensProvider));

    // Sync result view with active editor
    context.subscriptions.push(
        vscode.window.onDidChangeActiveTextEditor(editor => {
            // Only switch result context if the new editor is a SQL file with allowed URI scheme
            // Ignore code blocks from Copilot chat, output panels, and other non-editable sources
            if (editor && editor.document && editor.document.languageId === 'sql') {
                const scheme = editor.document.uri.scheme;
                // Only allow 'file' (saved files) and 'untitled' (new unsaved files)
                if (scheme === 'file' || scheme === 'untitled') {
                    const sourceUri = editor.document.uri.toString();
                    resultPanelProvider.setActiveSource(sourceUri);
                }
            }
        })
    );

    // ========== Register All Commands ==========

    // Schema commands
    const schemaDisposables = registerSchemaCommands({
        context,
        connectionManager,
        metadataCache,
        schemaProvider,
        schemaTreeView
    });
    context.subscriptions.push(...schemaDisposables);

    // Export commands
    const exportDisposables = registerExportCommands({
        context,
        connectionManager,
        outputChannel
    });
    context.subscriptions.push(...exportDisposables);

    // Import commands
    const importDisposables = registerImportCommands({
        context,
        connectionManager,
        outputChannel
    });
    context.subscriptions.push(...importDisposables);

    // Query commands
    const queryDisposables = registerQueryCommands({
        context,
        connectionManager,
        resultPanelProvider
    });
    context.subscriptions.push(...queryDisposables);

    // ========== Copilot Commands ==========
    const copilotService = new CopilotService(connectionManager, context);

    const copilotDisposables = [
        vscode.commands.registerCommand('netezza.copilotFixSql', async () => {
            try {
                await copilotService.fixSql();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.copilotFixSqlInteractive', async () => {
            try {
                await copilotService.fixSqlInteractive();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.copilotOptimizeSql', async () => {
            try {
                await copilotService.optimizeSql();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.copilotOptimizeSqlInteractive', async () => {
            try {
                await copilotService.optimizeSqlInteractive();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.copilotExplainSql', async () => {
            try {
                await copilotService.explainSql();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.copilotExplainSqlInteractive', async () => {
            try {
                await copilotService.explainSqlInteractive();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.copilotCustomQuestion', async () => {
            try {
                await copilotService.askCustomQuestion();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.copilotCustomQuestionInteractive', async () => {
            try {
                await copilotService.askCustomQuestionInteractive();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.copilotGenerateSql', async () => {
            try {
                await copilotService.generateSqlInteractive();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('justyBaseLite.copilot.changeModel', async () => {
            try {
                await copilotService.changeModel();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('justyBaseLite.copilot.clearModel', async () => {
            try {
                await copilotService.clearPersistedModel();
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.describeDataWithCopilot', async (data: Record<string, unknown>[], sql?: string) => {
            try {
                await copilotService.describeDataWithCopilot(data, sql);
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.fixSqlError', async (errorMessage: string, sql: string) => {
            try {
                await copilotService.fixSqlError(errorMessage, sql);
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Copilot Error: ${msg}`);
            }
        })
    ];

    context.subscriptions.push(...copilotDisposables);

    // ========== Chat Participant Registration ==========
    // Register the @sql-copilot chat participant with handlers for /schema, /optimize, /fix, /explain
    const sqlCopilotParticipant = copilotService.registerChatParticipant(context);
    if (sqlCopilotParticipant) {
        context.subscriptions.push(sqlCopilotParticipant);
    }

    // ========== Language Model Tool Registration ==========
    // Register the #schema tool that Copilot can automatically invoke in agent mode
    const schemaTool = new SchemaTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_get_sql_schema', schemaTool)
    );

    // Register the #getColumns tool for getting column metadata
    const columnsTool = new ColumnsTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_get_columns', columnsTool)
    );

    // Register the #getTables tool for listing tables in a database
    const tablesTool = new TablesTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_get_tables', tablesTool)
    );

    // Register the #executeQuery tool for executing SELECT queries
    const executeQueryTool = new ExecuteQueryTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_execute_query', executeQueryTool)
    );

    // Register the #sampleData tool for getting sample data from tables
    const sampleDataTool = new SampleDataTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_sample_data', sampleDataTool)
    );

    // Register the #explainPlan tool for getting query execution plans
    const explainPlanTool = new ExplainPlanTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_explain_plan', explainPlanTool)
    );

    // Register the #searchSchema tool for searching tables/columns by pattern
    const searchSchemaTool = new SearchSchemaTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_search_schema', searchSchemaTool)
    );

    // Register the #tableStats tool for getting table statistics and skew
    const tableStatsTool = new TableStatsTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_table_stats', tableStatsTool)
    );

    // Register the #dependencies tool for finding object dependencies
    const dependenciesTool = new DependenciesTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_dependencies', dependenciesTool)
    );

    // Register the #validateSql tool for validating SQL syntax
    const validateSqlTool = new ValidateSqlTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_validate_sql', validateSqlTool)
    );

    // Register the #getDatabases tool for listing databases
    const databasesTool = new DatabasesTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_get_databases', databasesTool)
    );

    // Register the #getSchemas tool for listing schemas
    const schemasTool = new SchemasTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_get_schemas', schemasTool)
    );

    // Register the #getProcedures tool for listing procedures
    const proceduresTool = new ProceduresTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_get_procedures', proceduresTool)
    );

    // Register the #getViews tool for listing views
    const viewsTool = new ViewsTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_get_views', viewsTool)
    );

    // Register the #getExternalTables tool for listing external tables
    const externalTablesTool = new ExternalTablesTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_get_external_tables', externalTablesTool)
    );

    // Register the #getObjectDefinition tool for getting view/procedure source code
    const getObjectDefinitionTool = new GetObjectDefinitionTool(copilotService);
    context.subscriptions.push(
        vscode.lm.registerTool('netezza_get_object_definition', getObjectDefinitionTool)
    );

    // ========== Core Commands (kept in extension.ts) ==========
    context.subscriptions.push(
        vscode.commands.registerCommand('netezza.viewEditData', (item: EditDataItem) => {
            // EditDataProvider.createOrShow expects EditDataItem which is compatible with Record<string, unknown>
            // or we delegate type checking to it.
            // Assumption: item is what we expect from the view. 
            // We use 'any' cast here to satisfy TS for now or if createOrShow expects specific type.
            // Looking at editDataProvider it likely expects EditDataItem.
            // Let's use 'as any' just to satisfy the call if types don't align perfectly yet, 
            // but the goal is to remove 'any' from the parameter list to suppress lint.
            EditDataProvider.createOrShow(context.extensionUri, item, context, connectionManager);
        }),

        vscode.commands.registerCommand('netezza.createProcedure', async (item: { dbName?: string }) => {
            const itemObj = item;
            if (!itemObj || !itemObj.dbName) {
                vscode.window.showErrorMessage('Invalid selection. Select a Procedure folder.');
                return;
            }

            const procName = await vscode.window.showInputBox({
                prompt: 'Enter new procedure name',
                placeHolder: 'NEW_PROCEDURE',
                value: 'NEW_PROCEDURE'
            });

            if (procName === undefined) return;

            const finalName = procName.trim() || 'NEW_PROCEDURE';
            const database = itemObj.dbName;

            const codetemplate = `CREATE OR REPLACE PROCEDURE ${database}.SCHEMA.${finalName}(INTEGER)
RETURNS INTEGER
EXECUTE AS CALLER
LANGUAGE NZPLSQL AS
BEGIN_PROC
DECLARE
    arg1 ALIAS FOR $1;
BEGIN
    -- YOUR CODE GOES HERE
    
    EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE EXCEPTION  'Procedure failed: %', sqlerrm;
        --RAISE NOTICE 'Caught error, continuing %', sqlerrm;

END;
END_PROC;`;

            const doc = await vscode.workspace.openTextDocument({ content: codetemplate, language: 'sql' });
            await vscode.window.showTextDocument(doc);
        }),

        // Toggle Keep Connection for current tab (per-document)
        vscode.commands.registerCommand('netezza.toggleKeepConnectionForTab', () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor || editor.document.languageId !== 'sql') {
                vscode.window.showWarningMessage('Please open a SQL file first.');
                return;
            }
            
            const documentUri = editor.document.uri.toString();
            const newState = connectionManager.toggleDocumentKeepConnectionOpen(documentUri);
            updateKeepConnectionStatusBar(keepConnectionStatusBar, connectionManager);

            vscode.window.showInformationMessage(
                newState
                    ? `Keep connection: ENABLED for this tab - connection will remain open after queries`
                    : `Keep connection: DISABLED for this tab - connection will be closed after each query`
            );
        }),


        vscode.commands.registerCommand('netezza.selectActiveConnection', async () => {
            const connections = await connectionManager.getConnections();
            if (connections.length === 0) {
                vscode.window.showWarningMessage('No connections configured. Please connect first.');
                return;
            }

            const selected = await vscode.window.showQuickPick(
                connections.map(c => c.name),
                {
                    placeHolder: 'Select Active Connection'
                }
            );

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
            const currentConnection =
                connectionManager.getDocumentConnection(documentUri) || connectionManager.getActiveConnectionName();

            const items = connections.map(c => ({
                label: c.name,
                description:
                    currentConnection === c.name ? '$(check) Currently selected' : `${c.host}:${c.port}/${c.database}`,
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

        // Select database for current tab (changes database with reconnect)
        vscode.commands.registerCommand('netezza.selectDatabaseForTab', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor || editor.document.languageId !== 'sql') {
                vscode.window.showWarningMessage('This command is only available for SQL files');
                return;
            }

            const documentUri = editor.document.uri.toString();
            const connectionName = connectionManager.getConnectionForExecution(documentUri);
            
            if (!connectionName) {
                vscode.window.showWarningMessage('No connection selected. Please select a connection first.');
                return;
            }

            // Get list of databases from the server
            try {
                const databases = await getDatabaseList(context, connectionManager, connectionName);
                
                if (databases.length === 0) {
                    vscode.window.showWarningMessage('No databases found on server.');
                    return;
                }

                const currentDatabase = await connectionManager.getEffectiveDatabase(documentUri);
                
                const items = databases.map(db => ({
                    label: db,
                    description: db === currentDatabase ? '$(check) Currently selected' : '',
                    database: db
                }));

                const selected = await vscode.window.showQuickPick(items, {
                    placeHolder: `Select database for this SQL tab (current: ${currentDatabase || 'default'})`
                });

                if (selected) {
                    connectionManager.setDocumentDatabase(documentUri, selected.database);
                    vscode.window.showInformationMessage(`Database for this tab set to: ${selected.database} (reconnecting...)`);
                }
            } catch (error) {
                const msg = error instanceof Error ? error.message : String(error);
                vscode.window.showErrorMessage(`Failed to get database list: ${msg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.openLogin', () => {
            LoginPanel.createOrShow(context.extensionUri, connectionManager);
        }),

        vscode.commands.registerCommand('netezza.refreshSchema', async () => {
            await metadataCache.clearCache();
            schemaProvider.refresh();
            vscode.window.showInformationMessage('Schema refreshed (Cache cleared)');
        }),

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
        }),

        vscode.commands.registerCommand('netezza.runScriptFromLens', async (uri: vscode.Uri, range: vscode.Range) => {
            try {
                const doc = await vscode.workspace.openTextDocument(uri);
                const text = doc.getText(range).trim() || doc.lineAt(range.start.line).text.trim();
                if (!text) {
                    vscode.window.showWarningMessage('No script command found');
                    return;
                }

                const tokens = text.split(/\s+/);
                const first = tokens[0] || '';
                const isPythonExec =
                    /python(\.exe)?$/i.test(first) && tokens.length >= 2 && tokens[1].toLowerCase().endsWith('.py');
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
                    const args = tokens;
                    cmd = buildExecCommand(pythonPath, '', args);
                }

                const term = vscode.window.createTerminal({ name: 'Netezza: Script' });
                term.show(true);
                term.sendText(cmd, true);
                vscode.window.showInformationMessage(`Running script: ${cmd}`);
            } catch (e: unknown) {
                const errorMsg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Error running script: ${errorMsg}`);
            }
        }),

        vscode.commands.registerCommand('netezza.clearAutocompleteCache', async () => {
            const confirm = await vscode.window.showWarningMessage(
                'Are you sure you want to clear the autocomplete cache? This will remove all cached databases, schemas, tables, and columns.',
                { modal: true },
                'Clear Cache'
            );

            if (confirm === 'Clear Cache') {
                await metadataCache.clearCache();
                vscode.window.showInformationMessage(
                    'Autocomplete cache cleared successfully. Cache will be rebuilt on next use.'
                );
            }
        }),

        vscode.commands.registerCommand('netezza.copySelection', () => {
            resultPanelProvider.triggerCopySelection();
        }),

        // ETL Designer Commands
        vscode.commands.registerCommand('netezza.openEtlDesigner', () => {
            EtlDesignerView.setConnectionManager(connectionManager);
            EtlDesignerView.createOrShow(context);
        }),

        vscode.commands.registerCommand('netezza.newEtlProject', async () => {
            const name = await vscode.window.showInputBox({
                prompt: 'Enter ETL project name',
                value: 'New ETL Project'
            });
            if (name) {
                const projectManager = EtlProjectManager.getInstance();
                projectManager.createProject(name);
                EtlDesignerView.setConnectionManager(connectionManager);
                EtlDesignerView.createOrShow(context);
            }
        }),

        vscode.commands.registerCommand('netezza.openEtlProject', async () => {
            const files = await vscode.window.showOpenDialog({
                filters: { 'ETL Project': ['etl.json'] },
                canSelectMany: false
            });
            if (files && files[0]) {
                try {
                    const projectManager = EtlProjectManager.getInstance();
                    const project = await projectManager.loadProject(files[0].fsPath);
                    EtlDesignerView.setConnectionManager(connectionManager);
                    EtlDesignerView.createOrShow(context, project);
                    vscode.window.showInformationMessage(`ETL project loaded: ${project.name}`);
                } catch (error) {
                    vscode.window.showErrorMessage(`Failed to load ETL project: ${error}`);
                }
            }
        }),

        vscode.commands.registerCommand('netezza.runEtlProject', async () => {
            const projectManager = EtlProjectManager.getInstance();
            const project = projectManager.getCurrentProject();
            if (!project) {
                vscode.window.showWarningMessage('No ETL project is currently open. Please open or create a project first.');
                return;
            }
            // Open the designer and trigger run
            EtlDesignerView.setConnectionManager(connectionManager);
            EtlDesignerView.createOrShow(context, project);
            // The designer will handle the run command via its webview
            vscode.window.showInformationMessage('ETL project opened. Use the Run button in the designer to execute.');
        })
    );

    // ========== Register Providers ==========
    context.subscriptions.push(
        vscode.languages.registerDocumentLinkProvider({ language: 'sql' }, new NetezzaDocumentLinkProvider()),
        vscode.languages.registerFoldingRangeProvider({ language: 'sql' }, new NetezzaFoldingRangeProvider())
    );

    // SQL Completion Provider
    const completionProvider = new SqlCompletionItemProvider(context, metadataCache, connectionManager);
    context.subscriptions.push(
        vscode.languages.registerCompletionItemProvider(['sql', 'mssql'], completionProvider, '.', ' ')
    );

    // SQL Shortcuts (SX -> SELECT etc.)
    registerSqlShortcuts(context);

    // SQL Linter
    activateSqlLinter(context);

    // SQL Linter Quick Fixes
    context.subscriptions.push(
        vscode.languages.registerCodeActionsProvider(
            ['sql', 'netezza-sql'],
            new NetezzaLinterCodeActionProvider(),
            { providedCodeActionKinds: NetezzaLinterCodeActionProvider.providedCodeActionKinds }
        )
    );

    console.log('Netezza extension: Activation complete.');
}

export function deactivate() { }
