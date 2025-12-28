import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';
import { QueryHistoryManager } from './queryHistoryManager';
import { extractVariables, parseSetVariables, replaceVariablesInSql } from './variableUtils';
import { NzConnection, NzCommand, QueryResult, ColumnDefinition, NzDataReader } from '../types';

// Re-export QueryResult for backward compatibility
export { QueryResult };

// Session tracking for DROP SESSION functionality
const executingCommands = new Map<string, NzCommand>(); // Map documentUri -> NzCommand

export async function cancelCurrentQuery(): Promise<void> {
    const editor = vscode.window.activeTextEditor;
    if (!editor) {
        vscode.window.showWarningMessage('No active editor to cancel query for.');
        return;
    }

    // Check if we have a command for this specific document
    const docUri = editor.document.uri.toString();
    const cmd = executingCommands.get(docUri);

    if (cmd) {
        try {
            await cmd.cancel();
            vscode.window.showInformationMessage('Cancellation request sent.');
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to cancel query: ${msg}`);
        }
    } else {
        vscode.window.showInformationMessage('No active query found for this tab.');
    }
}

/**
 * Prompt user for values for each variable. If `silent` is true and variables exist,
 * throw an error because we cannot prompt in silent mode.
 */
async function promptForVariableValues(
    variables: Set<string>,
    silent: boolean,
    defaults?: Record<string, string>
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

    // First use defaults when provided
    const toPrompt: string[] = [];
    for (const name of variables) {
        if (defaults && defaults[name] !== undefined) {
            values[name] = defaults[name];
        } else {
            toPrompt.push(name);
        }
    }

    for (const name of toPrompt) {
        const input = await vscode.window.showInputBox({
            prompt: `Enter value for ${name}`,
            placeHolder: '',
            value: defaults && defaults[name] ? defaults[name] : undefined,
            ignoreFocusOut: true
        });
        if (input === undefined) {
            throw new Error('Variable input cancelled by user');
        }
        values[name] = input;
    }

    return values;
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
    silent: boolean
): Promise<string> {
    const parsed = parseSetVariables(query);
    let queryToExecute = parsed.sql;
    const setDefaults = parsed.setValues;

    const vars = extractVariables(queryToExecute);
    if (vars.size > 0) {
        const resolved = await promptForVariableValues(vars, silent, setDefaults);
        queryToExecute = replaceVariablesInSql(queryToExecute, resolved);
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

async function getConnection(
    connManager: ConnectionManager,
    resolvedConnectionName: string,
    keepConnectionOpen: boolean
): Promise<{ connection: NzConnection; shouldCloseConnection: boolean }> {
    if (keepConnectionOpen) {
        const connection = await connManager.getPersistentConnection(resolvedConnectionName);
        return { connection, shouldCloseConnection: false };
    } else {
        const NzConnection = require('../../libs/driver/src/NzConnection');
        const details = await connManager.getConnection(resolvedConnectionName);
        if (!details) {
            throw new Error(`Connection '${resolvedConnectionName}' not found`);
        }
        const config = {
            host: details.host,
            port: details.port || 5480,
            database: details.database,
            user: details.user,
            password: details.password
        };
        const connection = new NzConnection(config) as NzConnection;
        await connection.connect();
        return { connection, shouldCloseConnection: true };
    }
}

async function consumeRestAndCancel(reader: NzDataReader, cmd: NzCommand): Promise<void> {
    const startTime = Date.now();
    const timeoutMs = 10000; // 10 seconds

    try {
        let timedOut = false;
        do {
            while (await reader.read()) {
                if (Date.now() - startTime > timeoutMs) {
                    timedOut = true;
                    break;
                }
            }
            if (timedOut) break;

            // Check timeout before waiting for nextResult 
            if (Date.now() - startTime > timeoutMs) {
                timedOut = true;
                break;
            }
        } while (await reader.nextResult());

        if (timedOut) {
            console.warn(`consumeRestAndCancel timed out after ${timeoutMs}ms, forcing cancel`);
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
    documentUri?: string
): Promise<{ columns: { name: string; type?: string }[]; data: unknown[][]; limitReached: boolean }> {
    const cmd = connection.createCommand(queryToExecute);
    cmd.commandTimeout = queryTimeout;

    if (documentUri) {
        executingCommands.set(documentUri, cmd);
    }

    try {
        const reader = await cmd.executeReader();
        const columns: { name: string; type?: string }[] = [];
        const data: unknown[][] = [];

        const limit = 200000;
        let fetchedCount = 0;
        let limitReached = false;

        while (await reader.read()) {
            if (columns.length === 0) {
                for (let i = 0; i < reader.fieldCount; i++) {
                    columns.push({ name: reader.getName(i), type: reader.getTypeName(i) });
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
                await consumeRestAndCancel(reader, cmd);
                break;
            }
        }

        return { columns, data, limitReached };
    } finally {
        if (documentUri) {
            executingCommands.delete(documentUri);
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
        const historyManager = new QueryHistoryManager(context);
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
    logCallback?: (msg: string) => void
): Promise<QueryResult> {
    const connManager = connectionManager || new ConnectionManager(context);
    const keepConnectionOpen = connManager.getKeepConnectionOpen();
    const logger = createLogger(silent, logCallback);

    log(logger, 'Executing query...');

    if (connectionName) {
        log(logger, `Target Connection: ${connectionName}`);
    }

    try {
        // 1. Resolve variables in query
        const queryToExecute = await resolveQueryVariables(query, silent);

        // 2. Resolve connection name
        const resolvedConnectionName = resolveConnectionName(connManager, connectionName, documentUri);

        // 3. Get connection
        const { connection, shouldCloseConnection } = await getConnection(
            connManager,
            resolvedConnectionName,
            keepConnectionOpen
        );

        // Attach listener for notices
        if (logger.outputChannel) {
            const noticeHandler = (msg: unknown) => {
                const notification = msg as { message: string };
                logger.outputChannel!.appendLine(`NOTICE: ${notification.message}`);
            };
            connection.on('notice', noticeHandler);
        }

        try {
            // 4. Get timeout configuration
            const config = vscode.workspace.getConfiguration('netezza');
            const queryTimeout = config.get<number>('queryTimeout', 1800);

            // 5. Execute query and fetch results
            const { columns, data, limitReached } = await executeQueryAndFetch(
                connection,
                queryToExecute,
                queryTimeout,
                documentUri
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
    const keepConnectionOpen = connManager.getKeepConnectionOpen();

    // Collect all notices
    const notices: string[] = [];

    // Resolve connection name
    const resolvedConnectionName = resolveConnectionName(connManager, connectionName, documentUri);

    const { connection, shouldCloseConnection } = await getConnection(
        connManager,
        resolvedConnectionName,
        keepConnectionOpen
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
                executingCommands.set(documentUri, cmd);
            }

            try {
                const reader = await cmd.executeReader();
                // Consume any results (EXPLAIN usually doesn't return rows, just notices)
                while (await reader.read()) {
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
    resultCallback?: (results: QueryResult[]) => void
): Promise<QueryResult[]> {
    const connManager = connectionManager || new ConnectionManager(context);
    const keepConnectionOpen = connManager.getKeepConnectionOpen();

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
        // Connect with fetchArray option to get results as arrays
        let connection;
        let shouldCloseConnection = true;

        // Get details (needed for creating connection and history)
        const details = await connManager.getConnection(resolvedConnectionName);
        if (!details) {
            throw new Error(`Connection '${resolvedConnectionName}' not found`);
        }

        if (keepConnectionOpen) {
            connection = await connManager.getPersistentConnection(resolvedConnectionName);
            shouldCloseConnection = false; // Don't close persistent connection
        } else {
            const NzConnection = require('../../libs/driver/src/NzConnection');
            const config = {
                host: details.host,
                port: details.port || 5480,
                database: details.database,
                user: details.user,
                password: details.password
            };
            connection = new NzConnection(config);
            await connection.connect();
        }

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
            try {
                // Execute scalar query to get Session ID
                const sidCmd = connection.createCommand('SELECT CURRENT_SID');
                const sidReader = await sidCmd.executeReader();
                if (await sidReader.read()) {
                    const sid = sidReader.getValue(0);
                    if (sid !== undefined && logCallback) {
                        logCallback(`Connected. Session ID: ${sid}`);
                    }
                }
                // Important: Close reader to release connection lock (_executing flag)
                await sidReader.close();
            } catch (sidErr) {
                console.debug('Could not retrieve session ID:', sidErr);
                if (logCallback) logCallback(`Connected.`);
            }

            const historyManager = new QueryHistoryManager(context);
            // Use resolved connection name for history
            const activeConnectionName = resolvedConnectionName;

            for (let i = 0; i < queries.length; i++) {
                const query = queries[i];
                const msg = `Executing query ${i + 1}/${queries.length}...`;
                if (outputChannel) outputChannel.appendLine(msg);
                if (logCallback) logCallback(msg);

                try {
                    // Parse @SET defaults and detect/resolve ${VAR} placeholders for this query
                    const parsed = parseSetVariables(query);
                    let queryToExecute = parsed.sql;
                    const setDefaults = parsed.setValues;
                    const vars = extractVariables(queryToExecute);
                    if (vars.size > 0) {
                        const resolved = await promptForVariableValues(vars, false, setDefaults);
                        queryToExecute = replaceVariablesInSql(queryToExecute, resolved);
                    }

                    // Execute query
                    const config = vscode.workspace.getConfiguration('netezza');
                    const queryTimeout = config.get<number>('queryTimeout', 1800);

                    const startTime = Date.now();
                    const { results: batchResults, error: batchError } = await executeAndFetchWithLimit(
                        connection,
                        queryToExecute,
                        200000,
                        queryTimeout,
                        documentUri
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
    documentUri?: string
): Promise<{ results: InternalResultSet[]; error?: Error }> {
    const cmd = connection.createCommand(query);
    if (timeoutSeconds && timeoutSeconds > 0) {
        cmd.commandTimeout = timeoutSeconds;
    }

    // Track command
    if (documentUri) {
        executingCommands.set(documentUri, cmd);
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

                // Fetch loop
                while (await reader.read()) {
                    // Initialize columns on first row
                    if (columns.length === 0) {
                        for (let i = 0; i < reader.fieldCount; i++) {
                            columns.push({ name: reader.getName(i), type: reader.getTypeName(i) });
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
                        await consumeRestAndCancel(reader, cmd);
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
            executingCommands.delete(documentUri);
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
    onChunk: (chunk: StreamingChunk) => void
): Promise<{ totalRows: number; limitReached: boolean; error?: Error }> {
    const cmd = connection.createCommand(query);
    if (timeoutSeconds && timeoutSeconds > 0) {
        cmd.commandTimeout = timeoutSeconds;
    }

    // Track command
    if (documentUri) {
        executingCommands.set(documentUri, cmd);
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

            while (await reader.read()) {
                // Initialize columns on first row
                if (columns.length === 0) {
                    for (let i = 0; i < reader.fieldCount; i++) {
                        columns.push({ name: reader.getName(i), type: reader.getTypeName(i) });
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
                }

                // Check limit
                if (totalRows >= limit) {
                    limitReached = true;
                    await consumeRestAndCancel(reader, cmd);
                    break;
                }
            }

            // Send final chunk (even if empty, to signal completion)
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
            executingCommands.delete(documentUri);
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
    chunkSize: number = 5000
): Promise<void> {
    const connManager = connectionManager || new ConnectionManager(context);
    const keepConnectionOpen = connManager.getKeepConnectionOpen();

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

        let connection;
        let shouldCloseConnection = true;

        if (keepConnectionOpen) {
            connection = await connManager.getPersistentConnection(resolvedConnectionName);
            shouldCloseConnection = false;
        } else {
            const NzConnection = require('../../libs/driver/src/NzConnection');
            const config = {
                host: details.host,
                port: details.port || 5480,
                database: details.database,
                user: details.user,
                password: details.password
            };
            connection = new NzConnection(config);
            await connection.connect();
        }

        // Attach listener for notices
        connection.on('notice', (msg: unknown) => {
            const notification = msg as { message: string };
            if (outputChannel) outputChannel.appendLine(`NOTICE: ${notification.message}`);
            if (logCallback) logCallback(`NOTICE: ${notification.message}`);
        });

        try {
            // Get Session ID
            try {
                const sidCmd = connection.createCommand('SELECT CURRENT_SID');
                const sidReader = await sidCmd.executeReader();
                if (await sidReader.read()) {
                    const sid = sidReader.getValue(0);
                    if (sid !== undefined && logCallback) {
                        logCallback(`Connected. Session ID: ${sid}`);
                    }
                }
                await sidReader.close();
            } catch (sidErr) {
                console.debug('Could not retrieve session ID:', sidErr);
                if (logCallback) logCallback('Connected.');
            }

            const historyManager = new QueryHistoryManager(context);
            const currentSchema = 'unknown';

            for (let i = 0; i < queries.length; i++) {
                const query = queries[i];
                const msg = `Executing query ${i + 1}/${queries.length}...`;
                if (outputChannel) outputChannel.appendLine(msg);
                if (logCallback) logCallback(msg);

                try {
                    // Parse variables
                    const parsed = parseSetVariables(query);
                    let queryToExecute = parsed.sql;
                    const setDefaults = parsed.setValues;
                    const vars = extractVariables(queryToExecute);
                    if (vars.size > 0) {
                        const resolved = await promptForVariableValues(vars, false, setDefaults);
                        queryToExecute = replaceVariablesInSql(queryToExecute, resolved);
                    }

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
                        }
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
        const errObj = error as { message?: string };
        const errorMessage = `Error: ${errObj.message || String(error)}`;
        if (outputChannel) outputChannel.appendLine(errorMessage);
        if (logCallback) logCallback(errorMessage);
        throw new Error(errorMessage);
    }
}
