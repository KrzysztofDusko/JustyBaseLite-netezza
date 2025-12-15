import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';
import { QueryHistoryManager } from './queryHistoryManager';



// Session tracking for DROP SESSION functionality
const executingCommands = new Map<string, any>(); // Map documentUri -> NzCommand

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
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to cancel query: ${e.message}`);
        }
    } else {
        vscode.window.showInformationMessage('No active query found for this tab.');
    }
}

/**
 * Helper to extract connection details from connection string
 */
function parseConnectionString(connectionString: string): { host: string; database: string } {
    const hostMatch = connectionString.match(/SERVER=([^;]+)/i);
    const dbMatch = connectionString.match(/DATABASE=([^;]+)/i);

    return {
        host: hostMatch ? hostMatch[1] : 'unknown',
        database: dbMatch ? dbMatch[1] : 'unknown'
    };
}

/**
 * Extract placeholder variable names from SQL of the form ${VAR_NAME}
 */
function extractVariables(sql: string): Set<string> {
    const vars = new Set<string>();
    if (!sql) return vars;
    for (const m of sql.matchAll(/\$\{([A-Za-z0-9_]+)\}/g)) {
        if (m[1]) vars.add(m[1]);
    }
    return vars;
}

/**
 * Parse lines like `@SET NAME = value` (case-insensitive) at the top/anywhere in SQL.
 * Returns the SQL with those lines removed and a map of defaults.
 */
function parseSetVariables(sql: string): { sql: string; setValues: Record<string, string> } {
    if (!sql) return { sql: '', setValues: {} };
    const lines = sql.split(/\r?\n/);
    const remaining: string[] = [];
    const setValues: Record<string, string> = {};

    for (let line of lines) {
        const m = line.match(/^\s*@SET\s+([A-Za-z0-9_]+)\s*=\s*(.+)$/i);
        if (m) {
            let val = m[2].trim();
            if (val.endsWith(';')) val = val.slice(0, -1).trim();
            const qm = val.match(/^'(.*)'$/s) || val.match(/^"(.*)"$/s);
            if (qm) val = qm[1];
            setValues[m[1]] = val;
        } else {
            remaining.push(line);
        }
    }

    return { sql: remaining.join('\n'), setValues };
}

/**
 * Prompt user for values for each variable. If `silent` is true and variables exist,
 * throw an error because we cannot prompt in silent mode.
 */
async function promptForVariableValues(variables: Set<string>, silent: boolean, defaults?: Record<string, string>): Promise<Record<string, string>> {
    const values: Record<string, string> = {};
    if (variables.size === 0) return values;
    if (silent) {
        // If silent but defaults present for all variables, use them. Otherwise error.
        const missing = Array.from(variables).filter(v => !(defaults && defaults[v] !== undefined));
        if (missing.length > 0) {
            throw new Error('Query contains variables but silent mode is enabled; cannot prompt for values. Missing: ' + missing.join(', '));
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

function replaceVariablesInSql(sql: string, values: Record<string, string>): string {
    return sql.replace(/\$\{([A-Za-z0-9_]+)\}/g, (_: string, name: string) => {
        return values[name] ?? '';
    });
}

export interface QueryResult {
    columns: { name: string; type?: string }[];
    data: any[][];
    rowsAffected?: number;
    message?: string;
    limitReached?: boolean;
    sql?: string;
}

export async function runQueryRaw(context: vscode.ExtensionContext, query: string, silent: boolean = false, connectionManager?: ConnectionManager, connectionName?: string, documentUri?: string, logCallback?: (msg: string) => void): Promise<QueryResult> {
    const connManager = connectionManager || new ConnectionManager(context);
    const keepConnectionOpen = connManager.getKeepConnectionOpen();

    // Create and show output channel only if not silent AND no callback
    // If callback is provided, we assume the caller handles UI/Logging (e.g. Logs Tab)
    // The user requested: "Skoro zapisujesz do 'Log' to już nie zapisuj do 'Output'"
    let outputChannel: vscode.OutputChannel | undefined;
    if (!silent && !logCallback) {
        outputChannel = vscode.window.createOutputChannel('Netezza SQL');
        outputChannel.show(true);
        outputChannel.appendLine('Executing query...');
    }

    if (logCallback) logCallback('Executing query...');

    if (connectionName) {
        const msg = `Target Connection: ${connectionName}`;
        if (outputChannel) outputChannel.appendLine(msg);
        if (logCallback) logCallback(msg);
    }

    try {
        // Parse @SET definitions (they are removed from SQL and used as defaults)
        const parsed = parseSetVariables(query);
        let queryToExecute = parsed.sql;
        const setDefaults = parsed.setValues;

        // Detect and resolve ${VAR} placeholders before executing
        const vars = extractVariables(queryToExecute);
        if (vars.size > 0) {
            const resolved = await promptForVariableValues(vars, silent, setDefaults);
            queryToExecute = replaceVariablesInSql(queryToExecute, resolved);
        }

        // Connect to database using JsNzDriver
        let connection;
        let shouldCloseConnection = true;
        let connectionString: string = ""; // Still used for history logging? maybe we can refactor history too but for now keep it or use dummy
        let resolvedConnectionName: string | undefined = connectionName;

        // If no explicit connectionName, try to resolve from document or fall back to active
        if (!resolvedConnectionName && documentUri) {
            resolvedConnectionName = connManager.getConnectionForExecution(documentUri);
        }
        if (!resolvedConnectionName) {
            resolvedConnectionName = connManager.getActiveConnectionName() || undefined;
        }

        if (!resolvedConnectionName) {
            throw new Error('No connection selected');
        }

        // We can get details to log history properly later
        // const details = await connManager.getConnection(resolvedConnectionName!);

        if (keepConnectionOpen) {
            // Use persistent connection
            connection = await connManager.getPersistentConnection(resolvedConnectionName);
            shouldCloseConnection = false; // Don't close persistent connection
        } else {
            // Create new connection
            const NzConnection = require('../driver/src/NzConnection');
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
            connection = new NzConnection(config);
            await connection.connect();
        }

        // Attach listener for notices (e.g. RAISE NOTICE)
        if (outputChannel) {
            connection.on('notice', (msg: any) => {
                outputChannel.appendLine(`NOTICE: ${msg.message}`);
            });
        }

        try {
            // Get timeout configuration
            const config = vscode.workspace.getConfiguration('netezza');
            const queryTimeout = config.get<number>('queryTimeout', 1800);

            // Execute query
            const cmd = connection.createCommand(queryToExecute);
            cmd.commandTimeout = queryTimeout;

            // Track command for cancellation if documentUri is available
            if (documentUri) {
                executingCommands.set(documentUri, cmd);
            }

            try {
                // We use executeReader which works for SELECT and others (returns reader logic)
                // But we need to handle "non-query" commands too? 
                // In JsNzDriver, executeReader returns a reader even if empty?
                // Or we can use execute() for non-queries? 
                // Let's assume most are SELECT-like or we want to allow reading result if any.
                // If it's pure INSERT/UPDATE without RETURNING, reader might be empty.

                const reader = await cmd.executeReader();
                const columns: { name: string; type?: string }[] = [];
                const data: any[][] = [];
                let rowsAffected: number | undefined;

                // Check if we have columns
                // JsNzDriver reader API: reader.read() returns boolean, reader.getName(i), reader.getValue(i) 

                // Fetch loop with limit
                const limit = 200000;
                let fetchedCount = 0;
                let limitReached = false;

                while (await reader.read()) {
                    // Initialize columns on first row if not done
                    if (columns.length === 0) {
                        for (let i = 0; i < reader.fieldCount; i++) {
                            columns.push({ name: reader.getName(i), type: reader.getTypeName(i) });
                        }
                    }

                    const row: any[] = [];
                    for (let i = 0; i < reader.fieldCount; i++) {
                        row.push(reader.getValue(i));
                    }
                    data.push(row);
                    fetchedCount++;

                    if (fetchedCount >= limit) {
                        limitReached = true;
                        break;
                    }
                }

                // Only close reader (it doesn't have close method? JsNzDriver NzDataReader usually just completes)
                // But we can check rowsAffected if available in reader or command?
                // JsNzDriver doesn't seem to expose rowsAffected directly on reader easily unless extended.
                // But 'CommandComplete' message has it. NzConnections usually stores it?
                // Let's assume undefined for now or check if Reader has it.

                // Get current schema for history logging
                let currentSchema = 'unknown';
                try {
                    // Reuse valid connection to get schema? Or skip to avoid overhead?
                    // Let's skip automatic schema check for performance unless crucial.
                } catch (schemaErr) { }

                // Log to history (async, don't wait)
                // We need valid details
                const details = await connManager.getConnection(resolvedConnectionName!);
                if (details) {
                    const historyManager = new QueryHistoryManager(context);
                    historyManager.addEntry(
                        details.host,
                        details.database,
                        currentSchema,
                        query,
                        resolvedConnectionName
                    ).catch(err => console.error('History log error:', err));
                }

                if (columns.length > 0) {
                    if (outputChannel) outputChannel.appendLine('Query completed.');
                    return {
                        columns: columns,
                        data: data,
                        rowsAffected: rowsAffected,
                        limitReached: limitReached,
                        sql: queryToExecute
                    };
                } else {
                    if (outputChannel) outputChannel.appendLine('Query executed successfully (no results).');
                    return {
                        columns: [],
                        data: [],
                        rowsAffected: rowsAffected,
                        message: 'Query executed successfully (no results).',
                        sql: queryToExecute
                    };
                }
            } finally {
                // Cleanup cancellation tracking
                if (documentUri) {
                    executingCommands.delete(documentUri);
                }
            }
        } finally {
            // Close connection only if not using persistent connection
            if (shouldCloseConnection) {
                await connection.close();
            }
        }
    } catch (error: any) {
        // const errorMessage = formatOdbcError(error);
        const errorMessage = `Error: ${error.message || error}`;
        if (outputChannel) {
            outputChannel.appendLine(errorMessage);
        }
        if (logCallback) logCallback(errorMessage);
        throw new Error(errorMessage);
    }
}

export async function runQuery(context: vscode.ExtensionContext, query: string, silent: boolean = false, connectionName?: string, connectionManager?: ConnectionManager, documentUri?: string): Promise<string | undefined> {
    // Wrapper for backward compatibility - returns JSON string of objects
    try {
        const result = await runQueryRaw(context, query, silent, connectionManager, connectionName, documentUri);

        if (result.data && result.data.length > 0) {
            // Convert array of arrays back to array of objects
            // WARNING: This will lose duplicate columns, but that's expected for legacy callers
            const mapped = result.data.map(row => {
                const obj: any = {};
                result.columns.forEach((col, index) => {
                    obj[col.name] = row[index];
                });
                return obj;
            });

            // Custom replacer to handle BigInt serialization
            const jsonOutput = JSON.stringify(mapped, (key, value) => {
                if (typeof value === 'bigint') {
                    if (value >= Number.MIN_SAFE_INTEGER && value <= Number.MAX_SAFE_INTEGER) {
                        return Number(value);
                    }
                    return value.toString();
                }
                return value;
            }, 2);
            return jsonOutput;
        } else if (result.message) {
            return result.message;
        }
        return undefined;
    } catch (error: any) {
        throw error;
    }
}

export async function runQueriesSequentially(context: vscode.ExtensionContext, queries: string[], connectionManager?: ConnectionManager, documentUri?: string, logCallback?: (msg: string) => void, resultCallback?: (results: QueryResult[]) => void): Promise<QueryResult[]> {
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
    let resolvedConnectionName = connectionManager ? connectionManager.getConnectionForExecution(documentUri) : undefined;
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
        // let connectionString: string;

        // Get details (needed for creating connection and history)
        const details = await connManager.getConnection(resolvedConnectionName);
        if (!details) {
            throw new Error(`Connection '${resolvedConnectionName}' not found`);
        }

        if (keepConnectionOpen) {
            connection = await connManager.getPersistentConnection(resolvedConnectionName);
            shouldCloseConnection = false; // Don't close persistent connection
        } else {
            const NzConnection = require('../driver/src/NzConnection');
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
        connection.on('notice', (msg: any) => {
            if (outputChannel) outputChannel.appendLine(`NOTICE: ${msg.message}`);
            if (logCallback) logCallback(`NOTICE: ${msg.message}`);
        });


        try {
            // Get current schema for history logging
            let currentSchema = 'unknown';
            try {
                // Reuse valid connection to get schema?
                // const schemaCmd = connection.createCommand('SELECT CURRENT_SCHEMA');
                // ...
            } catch (schemaErr) { }

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
                    const { results: batchResults, error: batchError } = await executeAndFetchWithLimit(connection, queryToExecute, 200000, queryTimeout, documentUri);
                    const durationMs = Date.now() - startTime;
                    if (logCallback) logCallback(`Executed query ${i + 1}/${queries.length} in ${durationMs}ms`);

                    // Log to history (async, don't wait)
                    historyManager.addEntry(
                        details.host,
                        details.database,
                        currentSchema,
                        query,
                        activeConnectionName // Pass the connection name to history
                    ).catch(err => {
                        console.error('Failed to log query to history:', err);
                    });

                    if (batchResults && batchResults.length > 0) {
                        // Process all results (partial or full)
                        for (const rs of batchResults) {
                            if (rs && (rs as any).columns) {
                                const columns = (rs as any).columns;
                                allResults.push({
                                    columns: columns,
                                    data: rs,
                                    rowsAffected: (rs as any).count,
                                    limitReached: (rs as any).limitReached,
                                    sql: queryToExecute
                                });
                            } else {
                                allResults.push({
                                    columns: [],
                                    data: [],
                                    rowsAffected: (rs as any)?.count,
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
                            if (rs && (rs as any).columns) {
                                queryResults.push({
                                    columns: (rs as any).columns,
                                    data: rs,
                                    rowsAffected: (rs as any).count,
                                    limitReached: (rs as any).limitReached,
                                    sql: queryToExecute
                                });
                            } else {
                                queryResults.push({
                                    columns: [],
                                    data: [],
                                    rowsAffected: (rs as any)?.count,
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
                } catch (err: any) {
                    const errorMsg = `Error: ${err.message}`;
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
    } catch (error: any) {
        const errorMessage = `Error: ${error.message}`;
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
async function executeAndFetchWithLimit(connection: any, query: string, limit: number, timeoutSeconds?: number, documentUri?: string): Promise<{ results: any[], error?: Error }> {
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
        const results: any[] = [];
        let hasMore = true;
        let caughtError: Error | undefined;

        try {
            do {
                const columns: { name: string; type?: string }[] = [];
                const allRows: any[] = [];
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
                        const row: any[] = [];
                        for (let i = 0; i < reader.fieldCount; i++) {
                            row.push(reader.getValue(i));
                        }
                        allRows.push(row);
                        fetchedCount++;
                    }

                    if (fetchedCount >= limit) {
                        limitReached = true;
                        break;
                    }
                }

                // Attach columns metadata
                if (columns.length > 0) {
                    (allRows as any).columns = columns;
                }
                if (limitReached) {
                    (allRows as any).limitReached = true;
                }

                results.push(allRows);

                hasMore = await reader.nextResult();

            } while (hasMore);
        } catch (readErr: any) {
            caughtError = readErr;
            // Don't throw loop error immediately, return what we have so far
        }

        return { results, error: caughtError };
    } finally {
        if (documentUri) {
            executingCommands.delete(documentUri);
        }
    }
}