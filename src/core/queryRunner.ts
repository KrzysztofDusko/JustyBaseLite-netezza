import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';
import { QueryHistoryManager } from './queryHistoryManager';
import { extractVariables, parseSetVariables, replaceVariablesInSql } from './variableUtils';
import { NzConnection, NzCommand, QueryResult, ColumnDefinition, NzDataReader } from '../types';

// Re-export QueryResult for backward compatibility
export { QueryResult };

// Session tracking for DROP SESSION functionality
interface ExecutingQueryState {
    command: NzCommand;
    isCancelled: boolean;
    sessionId?: string;
}
const executingCommands = new Map<string, ExecutingQueryState>(); // Map documentUri -> State

function normalizeUriKey(uri: string): string {
    // Basic normalization: handles Windows drive letter casing differences
    if (uri.startsWith('file:///')) {
        const driveMatch = uri.match(/^file:\/\/\/([A-Z]):\//i);
        if (driveMatch) {
            const drive = driveMatch[1].toLowerCase();
            return `file:///${drive}:${uri.substring(10)}`;
        }
    }
    return uri;
}

export async function cancelCurrentQuery(): Promise<void> {
    const editor = vscode.window.activeTextEditor;
    if (!editor) {
        vscode.window.showWarningMessage('No active editor to cancel query for.');
        return;
    }

    // Check if we have a command for this specific document
    const docUri = editor.document.uri.toString();
    const uriStr = normalizeUriKey(docUri);
    const state = executingCommands.get(uriStr);

    if (state) {
        state.isCancelled = true; // Signal fetch loops to stop
        try {
            await state.command.cancel();
            vscode.window.showInformationMessage('Cancellation request sent.');
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to cancel query: ${msg}`);
        }
    } else {
        vscode.window.showInformationMessage('No active query found for this tab.');
    }
}

export async function cancelQueryByUri(docUri: string | vscode.Uri): Promise<void> {
    const uriStr = normalizeUriKey(typeof docUri === 'string' ? docUri : docUri.toString());
    const state = executingCommands.get(uriStr);

    console.log(`[cancelQueryByUri] Found state for ${uriStr}: ${!!state}`);

    if (state) {
        state.isCancelled = true;
        // Notify immediately to give feedback
        vscode.window.showInformationMessage('Cancellation request sent.');
        
        try {
            console.log(`[cancelQueryByUri] Calling cmd.cancel() for ${uriStr}`);
            await state.command.cancel();
            console.log(`[cancelQueryByUri] cmd.cancel() completed for ${uriStr}`);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            console.error(`[cancelQueryByUri] Failed to cancel: ${msg}`);
            vscode.window.showErrorMessage(`Failed to cancel query: ${msg}`);
        }
    } else {
        console.warn(`[cancelQueryByUri] No active command found for ${uriStr}`);
        // Log all current keys for debugging
        console.log(`[cancelQueryByUri] Active keys: ${Array.from(executingCommands.keys()).join(', ')}`);
    }
}

/**
 * Prompt user for values for each variable. If `silent` is true and variables exist,
 * throw an error because we cannot prompt in silent mode.
 */
async function promptForVariableValues(
    variables: Set<string>,
    silent: boolean,
    defaults?: Record<string, string>,
    extensionUri?: vscode.Uri
): Promise<Record<string, string>> {
    const values: Record<string, string> = {};
    if (variables.size === 0) return values;

    if (silent) {
        // If silent but defaults present for all variables, use them. Otherwise error.
        const missing = Array.from(variables).filter(v => !(defaults && defaults[v] !== undefined));
        if (missing.length > 0) {
            throw new Error(
                'Query contains variables but silent mode is enabled; cannot prompt for values. Missing: ' +
                missing.join(', ')
            );
        }
        for (const v of variables) {
            values[v] = defaults![v];
        }
        return values;
    }

    // Use webview panel for better UX
    const { VariableInputPanel } = require('../views/variableInputPanel');
    const result = await VariableInputPanel.show(
        Array.from(variables),
        defaults,
        extensionUri
    );

    if (!result) {
        throw new Error('Variable input cancelled by user');
    }

    return result;
}

// QueryResult is now imported from '../types' and re-exported above

// --- Helper functions for runQueryRaw ---

interface OutputLogger {
    outputChannel?: vscode.OutputChannel;
    logCallback?: (msg: string) => void;
}

function createLogger(silent: boolean, logCallback?: (msg: string) => void): OutputLogger {
    let outputChannel: vscode.OutputChannel | undefined;
    if (!silent && !logCallback) {
        outputChannel = vscode.window.createOutputChannel('Netezza SQL');
        outputChannel.show(true);
    }
    return { outputChannel, logCallback };
}

/**
 * Check if error indicates a broken/closed connection that should trigger retry
 */
function isConnectionBrokenError(error: unknown): boolean {
    if (!(error instanceof Error)) return false;
    const msg = error.message.toLowerCase();
    return (
        msg.includes('socket closed') ||
        msg.includes('socket destroyed') ||
        msg.includes('connection reset') ||
        msg.includes('connection closed') ||
        msg.includes('econnreset') ||
        msg.includes('epipe') ||
        msg.includes('broken pipe')
    );
}

function log(logger: OutputLogger, message: string): void {
    if (logger.outputChannel) {
        logger.outputChannel.appendLine(message);
    }
    if (logger.logCallback) {
        logger.logCallback(message);
    }
}

async function resolveQueryVariables(
    query: string,
    silent: boolean,
    extensionUri?: vscode.Uri
): Promise<string> {
    const parsed = parseSetVariables(query);
    let queryToExecute = parsed.sql;
    const setDefaults = parsed.setValues;

    const vars = extractVariables(queryToExecute);
    if (vars.size > 0) {
        // Only prompt for variables that do NOT have a value set via @SET
        const missingVars = new Set<string>();
        for (const v of vars) {
            // DEBUG LOG
            console.log(`[VariableDebug] Checking variable '${v}'. Value in defaults: '${setDefaults[v]}'`);
            if (setDefaults[v] === undefined) {
                missingVars.add(v);
            }
        }

        let promptedValues: Record<string, string> = {};
        if (missingVars.size > 0) {
            console.log(`[VariableDebug] Prompting for missing vars: ${Array.from(missingVars).join(', ')}`);
            // We only prompt for missing variables. defined ones are used automatically.
            promptedValues = await promptForVariableValues(missingVars, silent, undefined, extensionUri);
        } else {
            console.log(`[VariableDebug] All variables defined in defaults. Skipping prompt.`);
        }

        const finalValues = { ...setDefaults, ...promptedValues };
        queryToExecute = replaceVariablesInSql(queryToExecute, finalValues);
    }

    return queryToExecute;
}

function resolveConnectionName(
    connManager: ConnectionManager,
    connectionName?: string,
    documentUri?: string
): string {
    let resolvedConnectionName = connectionName;

    if (!resolvedConnectionName && documentUri) {
        resolvedConnectionName = connManager.getConnectionForExecution(documentUri);
    }
    if (!resolvedConnectionName) {
        resolvedConnectionName = connManager.getActiveConnectionName() || undefined;
    }

    if (!resolvedConnectionName) {
        throw new Error('No connection selected');
    }

    return resolvedConnectionName;
}

/**
 * Get connection for a specific document (per-tab connection)
 * Uses document-specific persistent connection if keepConnectionOpen is true
 * 
 * IMPORTANT: When documentUri is not provided, ALWAYS creates a new connection
 * to avoid conflicts with document connections (e.g., Object Search vs SQL execution)
 */
async function getConnectionForDocument(
    connManager: ConnectionManager,
    resolvedConnectionName: string,
    keepConnectionOpen: boolean,
    documentUri?: string
): Promise<{ connection: NzConnection; shouldCloseConnection: boolean }> {
    if (keepConnectionOpen && documentUri) {
        // Use per-document persistent connection
        const connection = await connManager.getDocumentPersistentConnection(documentUri, resolvedConnectionName);
        return { connection, shouldCloseConnection: false };
    } else {
        // No documentUri means this is a background/utility query (e.g., Object Search, metadata refresh)
        // ALWAYS create a new connection to avoid conflicts with document connections
        const { createNzConnection } = require('./nzConnectionFactory');
        const details = await connManager.getConnection(resolvedConnectionName);
        if (!details) {
            throw new Error(`Connection '${resolvedConnectionName}' not found`);
        }
        const connection = createNzConnection({
            host: details.host,
            port: details.port || 5480,
            database: details.database,
            user: details.user,
            password: details.password
        }) as NzConnection;
        await connection.connect();
        return { connection, shouldCloseConnection: true };
    }
}

/**
 * Execute DROP SESSION on a new connection.
 * If keepConnectionOpen is enabled, close the old persistent connection
 * and establish a new one (per-document if documentUri is provided).
 */
async function executeDropSession(
    sessionId: string,
    connectionManager: ConnectionManager,
    documentUri?: string
): Promise<void> {
    try {
        const connName = connectionManager.getActiveConnectionName();
        if (connName) {
            const details = await connectionManager.getConnection(connName);
            if (details) {
                const { createNzConnection } = require('./nzConnectionFactory');
                const connection = createNzConnection({
                    host: details.host,
                    port: details.port || 5480,
                    database: details.database,
                    user: details.user,
                    password: details.password
                }) as NzConnection;
                await connection.connect();
                try {
                    const dropCmd = connection.createCommand(`DROP SESSION ${sessionId}`);
                    const r = await dropCmd.executeReader();
                    await r.close();
                    vscode.window.showInformationMessage(`Session ${sessionId} dropped successfully.`);
                    
                    // If keepConnectionOpen is enabled, we need to close the old persistent
                    // connection (which had the dropped session) and establish a new one
                    if (documentUri && connectionManager.getDocumentKeepConnectionOpen(documentUri)) {
                        // Per-document persistent connection
                        await connectionManager.closeDocumentPersistentConnection(documentUri);
                        // Establish new persistent connection immediately
                        await connectionManager.getDocumentPersistentConnection(documentUri, connName);
                        console.log(`[executeDropSession] Re-established per-document persistent connection for ${documentUri}`);
                    }
                } finally {
                    await connection.close();
                }
            }
        }
    } catch (dropErr) {
        const dropMsg = dropErr instanceof Error ? dropErr.message : String(dropErr);
        vscode.window.showErrorMessage(`Failed to drop session: ${dropMsg}`);
    }
}


async function consumeRestAndCancel(
    reader: NzDataReader,
    cmd: NzCommand,
    documentUri?: string,
    sessionId?: string,
    connectionManager?: ConnectionManager
): Promise<void> {
    const startTime = Date.now();
    const timeoutMs = 5000; // 5 seconds

    try {
        let timedOut = false;
        do {
            while (await reader.read()) {
                // Respect cancellation even during draining
                if (documentUri) {
                    const state = executingCommands.get(normalizeUriKey(documentUri));
                    if (state && state.isCancelled) {
                        break;
                    }
                }

                if (Date.now() - startTime > timeoutMs) {
                    timedOut = true;
                    break;
                }
            }
            if (timedOut) break;

            if (Date.now() - startTime > timeoutMs) {
                timedOut = true;
                break;
            }
        } while (await reader.nextResult());

        if (timedOut) {
            console.warn(`consumeRestAndCancel timed out after ${timeoutMs}ms, forcing cancel`);

            // If we timed out and have a session ID, offer to Drop Session
            if (sessionId && connectionManager) {
                const msg = `Cancellation is taking longer than expected. Do you want to force DROP SESSION ${sessionId}?`;
                const selection = await vscode.window.showWarningMessage(msg, `Drop Session ${sessionId}`, 'Keep Waiting');

                if (selection === `Drop Session ${sessionId}`) {
                    // Execute DROP SESSION immediately
                    await executeDropSession(sessionId, connectionManager, documentUri);
                } else if (selection === 'Keep Waiting') {
                    // Continue consuming for another 15 seconds
                    const extendedTimeoutMs = 15000;
                    const extendedStartTime = Date.now();
                    let extendedTimedOut = false;

                    try {
                        do {
                            while (await reader.read()) {
                                // Respect cancellation even during extended draining
                                if (documentUri) {
                                    const state = executingCommands.get(normalizeUriKey(documentUri));
                                    if (state && state.isCancelled) {
                                        break;
                                    }
                                }

                                if (Date.now() - extendedStartTime > extendedTimeoutMs) {
                                    extendedTimedOut = true;
                                    break;
                                }
                            }
                            if (extendedTimedOut) break;

                            if (Date.now() - extendedStartTime > extendedTimeoutMs) {
                                extendedTimedOut = true;
                                break;
                            }
                        } while (await reader.nextResult());
                    } catch (extendedErr) {
                        console.warn('Error during extended consume:', extendedErr);
                        extendedTimedOut = true;
                    }

                    if (extendedTimedOut) {
                        console.warn(`Extended consume timed out after ${extendedTimeoutMs}ms, forcing DROP SESSION`);
                        await executeDropSession(sessionId, connectionManager, documentUri);
                    }
                }
            }
        }

        await cmd.cancel();
    } catch (e) {
        console.warn('Failed to cancel command after limit reached:', e);
    }
}

async function executeQueryAndFetch(
    connection: NzConnection,
    queryToExecute: string,
    queryTimeout: number,
    documentUri?: string,
    sessionId?: string
): Promise<{ columns: { name: string; type?: string }[]; data: unknown[][]; limitReached: boolean }> {
    const cmd = connection.createCommand(queryToExecute);
    cmd.commandTimeout = queryTimeout;

    if (documentUri) {
        executingCommands.set(normalizeUriKey(documentUri), { command: cmd, isCancelled: false, sessionId });
    }

    try {
        const reader = await cmd.executeReader();
        const columns: { name: string; type?: string }[] = [];
        const data: unknown[][] = [];

        const limit = 200000;
        let fetchedCount = 0;
        let limitReached = false;

        // Read column metadata BEFORE the fetch loop (even if there are 0 rows)
        for (let i = 0; i < reader.fieldCount; i++) {
            columns.push({ name: reader.getName(i), type: reader.getTypeName(i) });
        }

        while (await reader.read()) {
            // Check for cancellation
            if (documentUri) {
                const state = executingCommands.get(normalizeUriKey(documentUri));
                if (state && state.isCancelled) {
                    break; // Exit loop if cancelled
                }
            }

            const row: unknown[] = [];
            for (let i = 0; i < reader.fieldCount; i++) {
                row.push(reader.getValue(i));
            }
            data.push(row);
            fetchedCount++;

            if (fetchedCount >= limit) {
                limitReached = true;
                // Cancel the command on the server side since we don't need more data
                // Actually, executeQueryAndFetch doesn't have ConnectionManager.
                break;
            }
        }

        return { columns, data, limitReached };
    } finally {
        if (documentUri) {
            executingCommands.delete(normalizeUriKey(documentUri));
        }
    }
}

async function logQueryToHistory(
    context: vscode.ExtensionContext,
    connManager: ConnectionManager,
    resolvedConnectionName: string,
    query: string
): Promise<void> {
    const currentSchema = 'unknown';
    const details = await connManager.getConnection(resolvedConnectionName);
    if (details) {
        const historyManager = QueryHistoryManager.getInstance(context);
        historyManager
            .addEntry(details.host, details.database, currentSchema, query, resolvedConnectionName)
            .catch(err => console.error('History log error:', err));
    }
}

// --- Main runQueryRaw function (refactored) ---

export async function runQueryRaw(
    context: vscode.ExtensionContext,
    query: string,
    silent: boolean = false,
    connectionManager?: ConnectionManager,
    connectionName?: string,
    documentUri?: string,
    logCallback?: (msg: string) => void,
    extensionUri?: vscode.Uri
): Promise<QueryResult> {
    const connManager = connectionManager || new ConnectionManager(context);
    // Use per-document keep connection setting if documentUri is provided
    // Default to true for background queries without documentUri
    const keepConnectionOpen = documentUri 
        ? connManager.getDocumentKeepConnectionOpen(documentUri)
        : true;
    const logger = createLogger(silent, logCallback);

    log(logger, 'Executing query...');

    if (connectionName) {
        log(logger, `Target Connection: ${connectionName}`);
    }

    // Resolve variables and connection name BEFORE try block so they're available in catch for retry
    let queryToExecute: string;
    let resolvedConnectionName: string;
    
    try {
        queryToExecute = await resolveQueryVariables(query, silent, extensionUri || context.extensionUri);
        resolvedConnectionName = resolveConnectionName(connManager, connectionName, documentUri);
    } catch (resolveError: unknown) {
        const errObj = resolveError as { message?: string };
        const errorMessage = `Error: ${errObj.message || String(resolveError)}`;
        log(logger, errorMessage);
        throw new Error(errorMessage);
    }

    log(logger, `Using connection: ${resolvedConnectionName}`);
    log(logger, 'Connecting to database...');

    try {
        // 1. Variables and connection name already resolved above

        // 2. Get connection (use per-document connection if keepConnectionOpen)
        const { connection, shouldCloseConnection } = await getConnectionForDocument(
            connManager,
            resolvedConnectionName,
            keepConnectionOpen,
            documentUri
        );
        log(logger, 'Connected.');

        // Attach listener for notices
        if (logger.outputChannel) {
            const noticeHandler = (msg: unknown) => {
                const notification = msg as { message: string };
                logger.outputChannel!.appendLine(`NOTICE: ${notification.message}`);
            };
            connection.on('notice', noticeHandler);
        }

        let sessionId: string | undefined;
        try {
            const sidCmd = connection.createCommand('SELECT CURRENT_SID');
            const sidReader = await sidCmd.executeReader();
            if (await sidReader.read()) {
                sessionId = String(sidReader.getValue(0));
            }
            await sidReader.close();
        } catch {
            // Ignore if we can't get SID
        }

        try {
            // 4. Get timeout configuration
            const config = vscode.workspace.getConfiguration('netezza');
            const queryTimeout = config.get<number>('queryTimeout', 1800);
            log(logger, 'Executing SQL on server...');

            // 5. Execute query and fetch results
            const { columns, data, limitReached } = await executeQueryAndFetch(
                connection,
                queryToExecute,
                queryTimeout,
                documentUri,
                sessionId
            );

            // 6. Log to history (async, don't wait)
            await logQueryToHistory(context, connManager, resolvedConnectionName, query);

            // 7. Return result
            if (columns.length > 0) {
                log(logger, 'Query completed.');
                return {
                    columns,
                    data,
                    rowsAffected: undefined,
                    limitReached,
                    sql: queryToExecute
                };
            } else {
                log(logger, 'Query executed successfully (no results).');
                return {
                    columns: [],
                    data: [],
                    rowsAffected: undefined,
                    message: 'Query executed successfully (no results).',
                    sql: queryToExecute
                };
            }
        } finally {
            if (shouldCloseConnection && connection) {
                await connection.close();
            }
        }
    } catch (error: unknown) {
        // Check if this is a broken connection error and we have a persistent connection
        if (isConnectionBrokenError(error) && documentUri && keepConnectionOpen) {
            log(logger, 'Connection was closed by server. Reconnecting and retrying...');
            
            // Close the broken persistent connection
            await connManager.closeDocumentPersistentConnection(documentUri);
            
            // Retry once with a fresh connection
            try {
                const { connection: retryConnection, shouldCloseConnection: retryClose } = await getConnectionForDocument(
                    connManager,
                    resolvedConnectionName,
                    keepConnectionOpen,
                    documentUri
                );
                log(logger, 'Reconnected. Retrying query...');

                let retrySessionId: string | undefined;
                try {
                    const sidCmd = retryConnection.createCommand('SELECT CURRENT_SID');
                    const sidReader = await sidCmd.executeReader();
                    if (await sidReader.read()) {
                        retrySessionId = String(sidReader.getValue(0));
                    }
                    await sidReader.close();
                } catch {
                    // Ignore if we can't get SID
                }

                try {
                    const config = vscode.workspace.getConfiguration('netezza');
                    const queryTimeout = config.get<number>('queryTimeout', 1800);
                    
                    const { columns, data, limitReached } = await executeQueryAndFetch(
                        retryConnection,
                        queryToExecute,
                        queryTimeout,
                        documentUri,
                        retrySessionId
                    );

                    await logQueryToHistory(context, connManager, resolvedConnectionName, query);

                    if (columns.length > 0) {
                        log(logger, 'Query completed (after reconnect).');
                        return {
                            columns,
                            data,
                            rowsAffected: undefined,
                            limitReached,
                            sql: queryToExecute
                        };
                    } else {
                        log(logger, 'Query executed successfully after reconnect (no results).');
                        return {
                            columns: [],
                            data: [],
                            rowsAffected: undefined,
                            message: 'Query executed successfully (no results).',
                            sql: queryToExecute
                        };
                    }
                } finally {
                    if (retryClose && retryConnection) {
                        await retryConnection.close();
                    }
                }
            } catch (retryError: unknown) {
                const retryErrObj = retryError as { message?: string };
                const retryErrorMessage = `Error (after reconnect attempt): ${retryErrObj.message || String(retryError)}`;
                log(logger, retryErrorMessage);
                throw new Error(retryErrorMessage);
            }
        }

        const errObj = error as { message?: string };
        const errorMessage = `Error: ${errObj.message || String(error)}`;
        log(logger, errorMessage);
        throw new Error(errorMessage);
    }
}

export async function runQuery(
    context: vscode.ExtensionContext,
    query: string,
    silent: boolean = false,
    connectionName?: string,
    connectionManager?: ConnectionManager,
    documentUri?: string
): Promise<string | undefined> {
    // Wrapper for backward compatibility - returns JSON string of objects
    const result = await runQueryRaw(context, query, silent, connectionManager, connectionName, documentUri);

    if (result.data && result.data.length > 0) {
        // Convert array of arrays back to array of objects
        // WARNING: This will lose duplicate columns, but that's expected for legacy callers
        const mapped = result.data.map(row => {
            const obj: Record<string, unknown> = {};
            result.columns.forEach((col, index) => {
                obj[col.name] = row[index];
            });
            return obj;
        });

        // Custom replacer to handle BigInt serialization
        const jsonOutput = JSON.stringify(
            mapped,
            (_key, value) => {
                if (typeof value === 'bigint') {
                    if (value >= Number.MIN_SAFE_INTEGER && value <= Number.MAX_SAFE_INTEGER) {
                        return Number(value);
                    }
                    return value.toString();
                }
                return value;
            },
            2
        );
        return jsonOutput;
    } else if (result.message) {
        return result.message;
    }
    return undefined;
}

/**
 * Convert QueryResult (columns[] + data[][]) to array of typed objects.
 * This avoids JSON serialization/deserialization overhead.
 */
export function queryResultToRows<T extends Record<string, unknown>>(result: QueryResult): T[] {
    if (!result.columns || !result.data || result.data.length === 0) {
        return [];
    }

    return result.data.map(row => {
        const obj: Record<string, unknown> = {};
        result.columns.forEach((col, index) => {
            let value = row[index];
            // Handle BigInt like JSON.stringify does
            if (typeof value === 'bigint') {
                if (value >= Number.MIN_SAFE_INTEGER && value <= Number.MAX_SAFE_INTEGER) {
                    value = Number(value);
                } else {
                    value = value.toString();
                }
            }
            obj[col.name] = value;
        });
        return obj as T;
    });
}

/**
 * Run a query with a temporary catalog (database) change.
 * This is needed for queries like _V_VIEW.DEFINITION which require
 * an active connection to the specific database.
 * 
 * The function:
 * 1. Executes SET CATALOG to the target database
 * 2. Runs the query
 * 3. Restores the original catalog (if known)
 * 
 * @param targetDatabase - Database to switch to before running the query
 * @param query - The query to execute
 * @param connectionManager - Connection manager
 * @param connectionName - Connection name to use
 * @returns QueryResult
 */
export async function runQueryWithCatalog(
    targetDatabase: string,
    query: string,
    connectionManager: ConnectionManager,
    connectionName: string
): Promise<QueryResult> {
    const connManager = connectionManager;
    
    // Get or create a connection
    const { connection, shouldCloseConnection } = await getConnectionForDocument(
        connManager,
        connectionName,
        true, // keep connection open
        undefined // no document URI
    );

    try {
        // Get current catalog to restore later
        let originalCatalog: string | undefined;
        try {
            const catalogCmd = connection.createCommand('SELECT CURRENT_CATALOG');
            const catalogReader = await catalogCmd.executeReader();
            if (await catalogReader.read()) {
                originalCatalog = String(catalogReader.getValue(0));
            }
            await catalogReader.close();
        } catch {
            // Ignore if we can't get current catalog
        }

        // Set target catalog
        // Note: This may fail for read-only databases or permission issues
        try {
            const setCatalogCmd = connection.createCommand(`SET CATALOG ${targetDatabase}`);
            const setCatalogReader = await setCatalogCmd.executeReader();
            try {
                await setCatalogReader.close();
            } catch {
                // Ignore close errors
            }
        } catch (catalogError) {
            // SET CATALOG failed (e.g., read-only database, permission denied)
            // Return empty result set - caller should handle gracefully
            console.debug(`[runQueryWithCatalog] Failed to SET CATALOG ${targetDatabase}:`, catalogError);
            return {
                columns: [],
                data: [],
                rowsAffected: undefined,
                limitReached: false,
                sql: query
            };
        }

        try {
            // Execute the actual query
            const config = vscode.workspace.getConfiguration('netezza');
            const queryTimeout = config.get<number>('queryTimeout', 1800);

            const { columns, data, limitReached } = await executeQueryAndFetch(
                connection,
                query,
                queryTimeout,
                undefined, // no document URI
                undefined  // no session ID
            );

            return {
                columns,
                data,
                rowsAffected: undefined,
                limitReached,
                sql: query
            };
        } finally {
            // Restore original catalog if we had one
            if (originalCatalog && originalCatalog !== targetDatabase) {
                try {
                    const restoreCmd = connection.createCommand(`SET CATALOG ${originalCatalog}`);
                    const restoreReader = await restoreCmd.executeReader();
                    try {
                        await restoreReader.close();
                    } catch {
                        // Ignore close errors
                    }
                } catch {
                    // Ignore restore errors
                }
            }
        }
    } finally {
        if (shouldCloseConnection && connection) {
            await connection.close();
        }
    }
}

/**
 * Parse JSON result from runQuery() safely.
 * Handles empty results and "Query executed successfully" messages.
 * This is a transitional helper for legacy code using runQuery + JSON.parse.
 * New code should use runQueryRaw + queryResultToRows instead.
 */
export function parseQueryJsonResult<T>(resultJson: string | undefined): T[] {
    if (!resultJson) {
        return [];
    }
    if (resultJson.startsWith('Query executed successfully') ||
        resultJson === 'Query executed successfully (no results).') {
        return [];
    }
    try {
        return JSON.parse(resultJson) as T[];
    } catch {
        return [];
    }
}

/**
 * Run EXPLAIN query and capture NOTICE messages as the result.
 * Netezza returns EXPLAIN output via NOTICE messages, not as regular query results.
 */
export async function runExplainQuery(
    context: vscode.ExtensionContext,
    query: string,
    connectionName?: string,
    connectionManager?: ConnectionManager,
    documentUri?: string
): Promise<string> {
    const connManager = connectionManager || new ConnectionManager(context);
    // Use per-document keep connection setting if documentUri is provided
    // Default to true for background queries without documentUri
    const keepConnectionOpen = documentUri 
        ? connManager.getDocumentKeepConnectionOpen(documentUri)
        : true;

    // Collect all notices
    const notices: string[] = [];

    // Resolve connection name
    const resolvedConnectionName = resolveConnectionName(connManager, connectionName, documentUri);

    const { connection, shouldCloseConnection } = await getConnectionForDocument(
        connManager,
        resolvedConnectionName,
        keepConnectionOpen,
        documentUri
    );

    try {
        // Attach listener to capture notices
        const noticeHandler = (msg: unknown) => {
            const notification = msg as { message: string };
            notices.push(notification.message);
        };
        connection.on('notice', noticeHandler);

        try {
            const config = vscode.workspace.getConfiguration('netezza');
            const queryTimeout = config.get<number>('queryTimeout', 1800);

            const cmd = connection.createCommand(query);
            cmd.commandTimeout = queryTimeout;

            if (documentUri) {
                executingCommands.set(documentUri, { command: cmd, isCancelled: false });
            }

            try {
                const reader = await cmd.executeReader();
                // Consume any results (EXPLAIN usually doesn't return rows, just notices)
                // Consume any results (EXPLAIN usually doesn't return rows, just notices)
                while (await reader.read()) {
                    if (documentUri) {
                        const state = executingCommands.get(documentUri);
                        if (state && state.isCancelled) {
                            break;
                        }
                    }
                    // Ignore actual rows, we only care about notices
                }
            } finally {
                if (documentUri) {
                    executingCommands.delete(documentUri);
                }
            }
        } finally {
            // Remove notice listener
            connection.removeListener('notice', noticeHandler);
        }
    } finally {
        if (shouldCloseConnection && connection) {
            await connection.close();
        }
    }

    // Return all captured notices as a single string
    return notices.join('\n');
}

export async function runQueriesSequentially(
    context: vscode.ExtensionContext,
    queries: string[],
    connectionManager?: ConnectionManager,
    documentUri?: string,
    logCallback?: (msg: string) => void,
    resultCallback?: (results: QueryResult[]) => void,
    extensionUri?: vscode.Uri,
    _isRetry: boolean = false
): Promise<QueryResult[]> {
    const connManager = connectionManager || new ConnectionManager(context);
    // Use per-document keep connection setting if documentUri is provided
    // Default to true for background queries without documentUri
    const keepConnectionOpen = documentUri 
        ? connManager.getDocumentKeepConnectionOpen(documentUri)
        : true;

    let outputChannel: vscode.OutputChannel | undefined;
    if (!logCallback) {
        outputChannel = vscode.window.createOutputChannel('Netezza SQL');
        outputChannel.show(true);
        outputChannel.appendLine(`Executing ${queries.length} queries sequentially...`);
    }

    const allResults: QueryResult[] = [];

    // Resolve connection name from document or use active
    let resolvedConnectionName = connectionManager
        ? connectionManager.getConnectionForExecution(documentUri)
        : undefined;
    if (!resolvedConnectionName) {
        resolvedConnectionName = connManager.getActiveConnectionName() || undefined;
    }

    if (!resolvedConnectionName) {
        if (outputChannel) outputChannel.appendLine('Error: No connection selected');
        if (logCallback) logCallback('Error: No connection selected');
        throw new Error('No connection selected');
    }

    try {
        // Get details (needed for history logging)
        const details = await connManager.getConnection(resolvedConnectionName);
        if (!details) {
            throw new Error(`Connection '${resolvedConnectionName}' not found`);
        }

        // Log connection info before connecting
        if (outputChannel) outputChannel.appendLine(`Using connection: ${resolvedConnectionName}`);
        if (logCallback) logCallback(`Using connection: ${resolvedConnectionName}`);
        if (outputChannel) outputChannel.appendLine('Connecting to database...');
        if (logCallback) logCallback('Connecting to database...');

        // Use shared getConnectionForDocument helper to eliminate code duplication
        const { connection, shouldCloseConnection } = await getConnectionForDocument(
            connManager,
            resolvedConnectionName,
            keepConnectionOpen,
            documentUri
        );

        // Attach listener for notices
        // Attach listener for notices
        connection.on('notice', (msg: unknown) => {
            const notification = msg as { message: string };
            if (outputChannel) outputChannel.appendLine(`NOTICE: ${notification.message}`);
            if (logCallback) logCallback(`NOTICE: ${notification.message}`);
        });

        try {
            // Get current schema for history logging
            const currentSchema = 'unknown';
            try {
                // Reuse valid connection to get schema?
                // const schemaCmd = connection.createCommand('SELECT CURRENT_SCHEMA');
                // ...
            } catch {
                // Schema retrieval failed - not critical, continue with 'unknown'
            }

            // Capture Netezza Session ID
            let sessionId: string | undefined;
            try {
                // Execute scalar query to get Session ID
                const sidCmd = connection.createCommand('SELECT CURRENT_SID');
                const sidReader = await sidCmd.executeReader();
                if (await sidReader.read()) {
                    const sid = sidReader.getValue(0);
                    if (sid !== undefined) {
                        sessionId = String(sid);
                        if (logCallback) {
                            logCallback(`Connected. Session ID: ${sessionId}`);
                        }
                    }
                }
                // Important: Close reader to release connection lock (_executing flag)
                await sidReader.close();
            } catch (sidErr) {
                console.debug('Could not retrieve session ID:', sidErr);
                if (logCallback) logCallback(`Connected.`);
            }


            const historyManager = QueryHistoryManager.getInstance(context);
            // Use resolved connection name for history
            const activeConnectionName = resolvedConnectionName;

            // --- BATCH VARIABLE HANDLING START ---
            // 1. Scan all queries for variables and @SET defaults
            const allVariables = new Set<string>();
            const allDefaults: Record<string, string> = {};

            for (const q of queries) {
                const parsed = parseSetVariables(q);
                // defaults from later queries override earlier ones (standard script behavior)
                Object.assign(allDefaults, parsed.setValues);
                const vars = extractVariables(parsed.sql);
                vars.forEach(v => allVariables.add(v));
            }

            // 2. Prompt for values ONCE for the entire batch
            // Only prompt for variables that are NOT in allDefaults
            const missingVars = new Set<string>();
            for (const v of allVariables) {
                if (allDefaults[v] === undefined) {
                    missingVars.add(v);
                }
            }

            const resolvedVars: Record<string, string> = { ...allDefaults };

            if (missingVars.size > 0) {
                const prompted = await promptForVariableValues(
                    missingVars,
                    false, // silent=false means we want to prompt
                    undefined,
                    extensionUri || context.extensionUri
                );
                Object.assign(resolvedVars, prompted);
            }
            // --- BATCH VARIABLE HANDLING END ---

            for (let i = 0; i < queries.length; i++) {
                const query = queries[i];
                const msg = `Executing query ${i + 1}/${queries.length}...`;
                if (outputChannel) outputChannel.appendLine(msg);
                if (logCallback) logCallback(msg);

                try {
                    // Parse @SET locally to strip them from SQL
                    // We use the globally resolved variables here
                    const parsed = parseSetVariables(query);
                    const queryToExecute = replaceVariablesInSql(parsed.sql, resolvedVars);

                    // Execute query

                    // Execute query
                    const config = vscode.workspace.getConfiguration('netezza');
                    const queryTimeout = config.get<number>('queryTimeout', 1800);

                    const startTime = Date.now();
                    const { results: batchResults, error: batchError } = await executeAndFetchWithLimit(
                        connection,
                        queryToExecute,
                        200000,
                        queryTimeout,
                        documentUri,
                        sessionId ? String(sessionId) : undefined,
                        connManager
                    );
                    const durationMs = Date.now() - startTime;
                    if (logCallback) logCallback(`Executed query ${i + 1}/${queries.length} in ${durationMs}ms`);

                    // Log to history (async, don't wait)
                    historyManager
                        .addEntry(
                            details.host,
                            details.database,
                            currentSchema,
                            query,
                            activeConnectionName // Pass the connection name to history
                        )
                        .catch(err => {
                            console.error('Failed to log query to history:', err);
                        });

                    if (batchResults && batchResults.length > 0) {
                        // Process all results (partial or full)
                        for (const rs of batchResults) {
                            if (rs.columns.length > 0) {
                                allResults.push({
                                    columns: rs.columns,
                                    data: rs.rows,
                                    rowsAffected: undefined, // Not captured here
                                    limitReached: rs.limitReached,
                                    sql: queryToExecute
                                });
                            } else {
                                allResults.push({
                                    columns: [],
                                    data: [],
                                    rowsAffected: undefined, // Not captured here
                                    message: 'Query executed successfully',
                                    sql: queryToExecute
                                });
                            }
                        }
                    }

                    // Progressive callback: send results immediately after each query
                    if (resultCallback && batchResults && batchResults.length > 0) {
                        const queryResults: QueryResult[] = [];
                        for (const rs of batchResults) {
                            if (rs.columns.length > 0) {
                                queryResults.push({
                                    columns: rs.columns,
                                    data: rs.rows,
                                    rowsAffected: undefined, // Not captured here
                                    limitReached: rs.limitReached,
                                    sql: queryToExecute
                                });
                            } else {
                                queryResults.push({
                                    columns: [],
                                    data: [],
                                    rowsAffected: undefined,
                                    message: 'Query executed successfully',
                                    sql: queryToExecute
                                });
                            }
                        }
                        resultCallback(queryResults);
                    }

                    if (batchError) {
                        // Re-throw valid error to be caught by outer catch
                        throw batchError;
                    }
                } catch (err: unknown) {
                    const errorMsg = err instanceof Error ? err.message : String(err);
                    if (outputChannel) outputChannel.appendLine(`Error in query ${i + 1}: ${errorMsg}`);
                    throw new Error(errorMsg);
                }
            }
            if (outputChannel) outputChannel.appendLine('All queries completed.');
        } finally {
            // Close connection only if not using persistent connection
            if (shouldCloseConnection) {
                await connection.close();
            }
        }
    } catch (error: unknown) {
        // Check if this is a broken connection error and we have a persistent connection
        // Only retry once (check _isRetry flag to prevent infinite loop)
        if (!_isRetry && isConnectionBrokenError(error) && documentUri && keepConnectionOpen) {
            if (outputChannel) outputChannel.appendLine('Connection was closed by server. Reconnecting and retrying...');
            if (logCallback) logCallback('Connection was closed by server. Reconnecting and retrying...');
            
            // Close the broken persistent connection
            await connManager.closeDocumentPersistentConnection(documentUri);
            
            // Retry once by recursively calling the function with _isRetry=true
            // This will create a new connection and re-execute all queries
            try {
                return await runQueriesSequentially(
                    context,
                    queries,
                    connManager,
                    documentUri,
                    logCallback,
                    resultCallback,
                    extensionUri,
                    true // _isRetry = true to prevent further retries
                );
            } catch (retryError: unknown) {
                const retryErrObj = retryError as { message?: string };
                const retryErrorMessage = `Error (after reconnect attempt): ${retryErrObj.message || String(retryError)}`;
                if (outputChannel) outputChannel.appendLine(retryErrorMessage);
                if (logCallback) logCallback(retryErrorMessage);
                throw new Error(retryErrorMessage);
            }
        }

        const errObj = error as { message?: string };
        const errorMessage = `Error: ${errObj.message || String(error)}`;
        if (outputChannel) outputChannel.appendLine(errorMessage);
        if (logCallback) logCallback(errorMessage);
        throw new Error(errorMessage);
    }

    return allResults;
}

// function formatOdbcError ... removed/not used

/**
 * Execute a query and fetch results up to a limit using NzConnection.
 */
/**
 * Execute a query and fetch results up to a limit using NzConnection.
 */
// Internal result set interface for executeAndFetchWithLimit
// Uses 'rows' internally, converted to 'data' when creating QueryResult
interface InternalResultSet {
    columns: ColumnDefinition[];
    rows: unknown[][];
    limitReached: boolean;
}

async function executeAndFetchWithLimit(
    connection: NzConnection,
    query: string,
    limit: number,
    timeoutSeconds?: number,
    documentUri?: string,
    sessionId?: string,
    connectionManager?: ConnectionManager
): Promise<{ results: InternalResultSet[]; error?: Error }> {
    const cmd = connection.createCommand(query);
    if (timeoutSeconds && timeoutSeconds > 0) {
        cmd.commandTimeout = timeoutSeconds;
    }

    // Track command
    if (documentUri) {
        const normalizedKey = normalizeUriKey(documentUri);
        console.log(`[executeWithStreaming] Registering command with key: ${normalizedKey}`);
        executingCommands.set(normalizedKey, { command: cmd, isCancelled: false, sessionId });
    }

    try {
        const reader = await cmd.executeReader();
        const results: InternalResultSet[] = [];
        let hasMore = true;
        let caughtError: Error | undefined;

        try {
            do {
                const columns: { name: string; type?: string }[] = [];
                const rows: unknown[][] = [];
                let fetchedCount = 0;
                let limitReached = false;

                // Read column metadata BEFORE the fetch loop (even if there are 0 rows)
                for (let i = 0; i < reader.fieldCount; i++) {
                    columns.push({ name: reader.getName(i), type: reader.getTypeName(i) });
                }

                // Fetch loop
                while (await reader.read()) {
                    // Check for cancellation
                    if (documentUri) {
                        const state = executingCommands.get(normalizeUriKey(documentUri));
                        if (state && state.isCancelled) {
                            throw new Error('Query cancelled by user');
                        }
                    }

                    if (fetchedCount < limit) {
                        const row: unknown[] = [];
                        for (let i = 0; i < reader.fieldCount; i++) {
                            row.push(reader.getValue(i));
                        }
                        rows.push(row);
                        fetchedCount++;
                    }

                    if (fetchedCount >= limit) {
                        limitReached = true;
                        // Cancel the command on the server side since we don't need more data
                        await consumeRestAndCancel(reader, cmd, documentUri, sessionId, connectionManager);
                        break;
                    }
                }

                results.push({
                    columns,
                    rows,
                    limitReached
                });

                hasMore = await reader.nextResult();
            } while (hasMore);

        } catch (readErr: unknown) {
            caughtError = readErr instanceof Error ? readErr : new Error(String(readErr));
            // Don't throw loop error immediately, return what we have so far
        }

        return { results, error: caughtError };
    } finally {
        if (documentUri) {
            executingCommands.delete(normalizeUriKey(documentUri));
        }
    }
}

/**
 * Streaming chunk callback
 */
export interface StreamingChunk {
    columns: { name: string; type?: string }[];
    rows: unknown[][];
    isFirstChunk: boolean;
    isLastChunk: boolean;
    totalRowsSoFar: number;
    limitReached: boolean;
}

/**
 * Execute a query and stream results in chunks to avoid memory pressure.
 * Uses chunk-based streaming for better memory management and faster first-data-display.
 */
async function executeAndFetchStreaming(
    connection: NzConnection,
    query: string,
    limit: number,
    chunkSize: number,
    timeoutSeconds: number | undefined,
    documentUri: string | undefined,
    onChunk: (chunk: StreamingChunk) => void,
    sessionId?: string,
    connectionManager?: ConnectionManager
): Promise<{ totalRows: number; limitReached: boolean; error?: Error }> {
    const cmd = connection.createCommand(query);
    if (timeoutSeconds && timeoutSeconds > 0) {
        cmd.commandTimeout = timeoutSeconds;
    }

    // Track command
    if (documentUri) {
        const normalizedKey = normalizeUriKey(documentUri);
        console.log(`[executeAndFetchStreaming] Registering command with key: ${normalizedKey}`);
        executingCommands.set(normalizedKey, { command: cmd, isCancelled: false, sessionId });
    }

    try {
        const reader = await cmd.executeReader();
        let caughtError: Error | undefined;
        let totalRows = 0;
        let limitReached = false;

        try {
            // We only handle the first result set for streaming (common case)
            const columns: { name: string; type?: string }[] = [];
            let chunk: unknown[][] = [];
            let isFirstChunk = true;

            // Read column metadata BEFORE the fetch loop (even if there are 0 rows)
            for (let i = 0; i < reader.fieldCount; i++) {
                columns.push({ name: reader.getName(i), type: reader.getTypeName(i) });
            }

            let userCancelled = false;
            while (await reader.read()) {
                // Check for cancellation
                if (documentUri) {
                    const state = executingCommands.get(normalizeUriKey(documentUri));
                    if (state && state.isCancelled) {
                        userCancelled = true;
                        // User cancelled during fetch - consume remaining data and cancel properly
                        await consumeRestAndCancel(reader, cmd, documentUri, sessionId, connectionManager);
                        break;
                    }
                }

                // Add row to chunk
                const row: unknown[] = [];
                for (let i = 0; i < reader.fieldCount; i++) {
                    row.push(reader.getValue(i));
                }
                chunk.push(row);
                totalRows++;

                // Send chunk when it reaches chunk size
                if (chunk.length >= chunkSize) {
                    onChunk({
                        columns: isFirstChunk ? columns : [],
                        rows: chunk,
                        isFirstChunk,
                        isLastChunk: false,
                        totalRowsSoFar: totalRows,
                        limitReached: false
                    });
                    chunk = [];
                    isFirstChunk = false;

                    // Yield to event loop periodically to allow Cancel messages to be processed
                    await new Promise(resolve => setImmediate(resolve));
                }

                // Check limit
                if (totalRows >= limit) {
                    limitReached = true;
                    await consumeRestAndCancel(reader, cmd, documentUri, sessionId, connectionManager);
                    break;
                }
            }

            // If user cancelled, return early with an error
            if (userCancelled) {
                // Return clear error type
                return { totalRows, limitReached, error: new Error('Query cancelled') };
            }

            // Send final chunk (even if empty, to signal completion)
            // But skip it if we were cancelled in the meantime
            if (documentUri) {
                const state = executingCommands.get(normalizeUriKey(documentUri));
                if (state && state.isCancelled) {
                    return { totalRows, limitReached, error: caughtError || new Error('Query cancelled') };
                }
            }

            onChunk({
                columns: isFirstChunk ? columns : [],
                rows: chunk,
                isFirstChunk,
                isLastChunk: true,
                totalRowsSoFar: totalRows,
                limitReached
            });

        } catch (readErr: unknown) {
            caughtError = readErr instanceof Error ? readErr : new Error(String(readErr));
        }

        return { totalRows, limitReached, error: caughtError };
    } finally {
        if (documentUri) {
            executingCommands.delete(normalizeUriKey(documentUri));
        }
    }
}

/**
 * Run queries sequentially with streaming support.
 * Sends results in chunks for better memory efficiency and responsiveness.
 */
export async function runQueriesWithStreaming(
    context: vscode.ExtensionContext,
    queries: string[],
    connectionManager?: ConnectionManager,
    documentUri?: string,
    logCallback?: (msg: string) => void,
    chunkCallback?: (queryIndex: number, chunk: StreamingChunk, sql: string) => void,
    chunkSize: number = 5000,
    extensionUri?: vscode.Uri,
    _isRetry: boolean = false
): Promise<void> {
    const connManager = connectionManager || new ConnectionManager(context);
    // Use per-document keep connection setting if documentUri is provided
    // Default to true for background queries without documentUri
    const keepConnectionOpen = documentUri 
        ? connManager.getDocumentKeepConnectionOpen(documentUri)
        : true;

    let outputChannel: vscode.OutputChannel | undefined;
    if (!logCallback) {
        outputChannel = vscode.window.createOutputChannel('Netezza SQL');
        outputChannel.show(true);
        outputChannel.appendLine(`Executing ${queries.length} queries with streaming...`);
    }

    // Resolve connection name from document or use active
    let resolvedConnectionName = connectionManager
        ? connectionManager.getConnectionForExecution(documentUri)
        : undefined;
    if (!resolvedConnectionName) {
        resolvedConnectionName = connManager.getActiveConnectionName() || undefined;
    }

    if (!resolvedConnectionName) {
        const msg = 'Error: No connection selected';
        if (outputChannel) outputChannel.appendLine(msg);
        if (logCallback) logCallback(msg);
        throw new Error('No connection selected');
    }

    try {
        // Get connection details
        const details = await connManager.getConnection(resolvedConnectionName);
        if (!details) {
            throw new Error(`Connection '${resolvedConnectionName}' not found`);
        }

        // Use shared getConnectionForDocument helper to eliminate code duplication
        const { connection, shouldCloseConnection } = await getConnectionForDocument(
            connManager,
            resolvedConnectionName,
            keepConnectionOpen,
            documentUri
        );

        // Attach listener for notices
        connection.on('notice', (msg: unknown) => {
            const notification = msg as { message: string };
            if (outputChannel) outputChannel.appendLine(`NOTICE: ${notification.message}`);
            if (logCallback) logCallback(`NOTICE: ${notification.message}`);
        });

        try {
            // Get Session ID
            let sessionId: string | undefined;
            try {
                const sidCmd = connection.createCommand('SELECT CURRENT_SID');
                const sidReader = await sidCmd.executeReader();
                if (await sidReader.read()) {
                    sessionId = String(sidReader.getValue(0));
                    if (logCallback) {
                        logCallback(`Connected. Session ID: ${sessionId}`);
                    }
                }
                await sidReader.close();
            } catch (sidErr) {
                console.debug('Could not retrieve session ID:', sidErr);
                if (logCallback) logCallback('Connected.');
            }


            const historyManager = QueryHistoryManager.getInstance(context);
            const currentSchema = 'unknown';

            // --- BATCH VARIABLE HANDLING START ---
            // 1. Scan all queries for variables and @SET defaults
            const allVariables = new Set<string>();
            const allDefaults: Record<string, string> = {};

            for (const q of queries) {
                const parsed = parseSetVariables(q);
                // defaults from later queries override earlier ones (standard script behavior)
                Object.assign(allDefaults, parsed.setValues);
                const vars = extractVariables(parsed.sql);
                vars.forEach(v => allVariables.add(v));
            }

            // 2. Prompt for values ONCE for the entire batch
            // Only prompt for variables that are NOT in allDefaults
            const missingVars = new Set<string>();
            for (const v of allVariables) {
                if (allDefaults[v] === undefined) {
                    missingVars.add(v);
                }
            }

            const resolvedVars: Record<string, string> = { ...allDefaults };

            if (missingVars.size > 0) {
                const prompted = await promptForVariableValues(
                    missingVars,
                    false, // silent=false means we want to prompt
                    undefined,
                    extensionUri || context.extensionUri
                );
                Object.assign(resolvedVars, prompted);
            }
            // --- BATCH VARIABLE HANDLING END ---

            for (let i = 0; i < queries.length; i++) {
                const query = queries[i];
                const msg = `Executing query ${i + 1}/${queries.length}...`;
                if (outputChannel) outputChannel.appendLine(msg);
                if (logCallback) logCallback(msg);

                try {
                    // Parse @SET locally to strip them from SQL
                    // We use the globally resolved variables here
                    const parsed = parseSetVariables(query);
                    const queryToExecute = replaceVariablesInSql(parsed.sql, resolvedVars);

                    const config = vscode.workspace.getConfiguration('netezza');
                    const queryTimeout = config.get<number>('queryTimeout', 1800);

                    const startTime = Date.now();

                    // Use streaming fetch
                    const { totalRows, limitReached, error } = await executeAndFetchStreaming(
                        connection,
                        queryToExecute,
                        200000, // limit
                        chunkSize,
                        queryTimeout,
                        documentUri,
                        (chunk) => {
                            if (chunkCallback) {
                                chunkCallback(i, chunk, queryToExecute);
                            }
                        },
                        sessionId,
                        connManager
                    );

                    const durationMs = Date.now() - startTime;
                    if (logCallback) {
                        logCallback(`Query ${i + 1}/${queries.length}: ${totalRows} rows in ${durationMs}ms${limitReached ? ' (limit reached)' : ''}`);
                    }

                    // Log to history
                    historyManager
                        .addEntry(details.host, details.database, currentSchema, query, resolvedConnectionName)
                        .catch(err => console.error('Failed to log query to history:', err));

                    if (error) {
                        throw error;
                    }
                } catch (err: unknown) {
                    const errorMsg = err instanceof Error ? err.message : String(err);
                    if (outputChannel) outputChannel.appendLine(`Error in query ${i + 1}: ${errorMsg}`);
                    throw new Error(errorMsg);
                }
            }

            if (outputChannel) outputChannel.appendLine('All queries completed.');
        } finally {
            if (shouldCloseConnection) {
                await connection.close();
            }
        }
    } catch (error: unknown) {
        // Check if this is a broken connection error and we have a persistent connection
        // Only retry once (check _isRetry flag to prevent infinite loop)
        if (!_isRetry && isConnectionBrokenError(error) && documentUri && keepConnectionOpen) {
            if (outputChannel) outputChannel.appendLine('Connection was closed by server. Reconnecting and retrying...');
            if (logCallback) logCallback('Connection was closed by server. Reconnecting and retrying...');
            
            // Close the broken persistent connection
            await connManager.closeDocumentPersistentConnection(documentUri);
            
            // Retry once by recursively calling the function with _isRetry=true
            // This will create a new connection and re-execute all queries
            try {
                return await runQueriesWithStreaming(
                    context,
                    queries,
                    connManager,
                    documentUri,
                    logCallback,
                    chunkCallback,
                    chunkSize,
                    extensionUri,
                    true // _isRetry = true to prevent further retries
                );
            } catch (retryError: unknown) {
                const retryErrObj = retryError as { message?: string };
                const retryErrorMessage = `Error (after reconnect attempt): ${retryErrObj.message || String(retryError)}`;
                if (outputChannel) outputChannel.appendLine(retryErrorMessage);
                if (logCallback) logCallback(retryErrorMessage);
                throw new Error(retryErrorMessage);
            }
        }

        const errObj = error as { message?: string };
        const errorMessage = `Error: ${errObj.message || String(error)}`;
        if (outputChannel) outputChannel.appendLine(errorMessage);
        if (logCallback) logCallback(errorMessage);
        throw new Error(errorMessage);
    }
}


