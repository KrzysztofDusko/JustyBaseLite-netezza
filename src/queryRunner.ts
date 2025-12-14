import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';
import { QueryHistoryManager } from './queryHistoryManager';



// Session tracking for DROP SESSION functionality
let currentExecutingSessionId: number | null = null;
let currentExecutingConnectionName: string | undefined = undefined;

export function getCurrentSessionId(): number | null {
    return currentExecutingSessionId;
}

export function getCurrentSessionConnectionName(): string | undefined {
    return currentExecutingConnectionName;
}

export function clearCurrentSession(): void {
    currentExecutingSessionId = null;
    currentExecutingConnectionName = undefined;
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

export async function runQueryRaw(context: vscode.ExtensionContext, query: string, silent: boolean = false, connectionManager?: ConnectionManager, connectionName?: string, documentUri?: string): Promise<QueryResult> {
    const connManager = connectionManager || new ConnectionManager(context);
    const keepConnectionOpen = connManager.getKeepConnectionOpen();

    // Create and show output channel only if not silent
    let outputChannel: vscode.OutputChannel | undefined;
    if (!silent) {
        outputChannel = vscode.window.createOutputChannel('Netezza SQL');
        outputChannel.show(true);
        outputChannel.appendLine('Executing query...');
        if (connectionName) {
            outputChannel.appendLine(`Target Connection: ${connectionName}`);
        }
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
            const details = await connManager.getConnection(resolvedConnectionName);
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

export async function runQueriesSequentially(context: vscode.ExtensionContext, queries: string[], connectionManager?: ConnectionManager, documentUri?: string): Promise<QueryResult[]> {
    const connManager = connectionManager || new ConnectionManager(context);
    const keepConnectionOpen = connManager.getKeepConnectionOpen();

    const outputChannel = vscode.window.createOutputChannel('Netezza SQL');
    outputChannel.show(true);
    outputChannel.appendLine(`Executing ${queries.length} queries sequentially...`);

    const allResults: QueryResult[] = [];

    // Resolve connection name from document or use active
    let resolvedConnectionName = connectionManager ? connectionManager.getConnectionForExecution(documentUri) : undefined;
    if (!resolvedConnectionName) {
        resolvedConnectionName = connManager.getActiveConnectionName() || undefined;
    }

    if (!resolvedConnectionName) {
        outputChannel.appendLine('Error: No connection selected');
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
        connection.on('notice', (msg: any) => {
            outputChannel.appendLine(`NOTICE: ${msg.message}`);
        });


        try {
            // Get current schema for history logging
            let currentSchema = 'unknown';
            try {
                // Reuse valid connection to get schema?
                // const schemaCmd = connection.createCommand('SELECT CURRENT_SCHEMA');
                // ...
            } catch (schemaErr) { }

            // Capture current session ID for DROP SESSION functionality
            try {
                // TODO: Update to usage of NzConnection if needed
                // const sidCmd = connection.createCommand('SELECT CURRENT_SID');
                // ...
            } catch (sidErr) {
                console.debug('Could not retrieve session ID:', sidErr);
            }

            const historyManager = new QueryHistoryManager(context);
            // Use resolved connection name for history
            const activeConnectionName = resolvedConnectionName;

            for (let i = 0; i < queries.length; i++) {
                const query = queries[i];
                outputChannel.appendLine(`Executing query ${i + 1}/${queries.length}...`);

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

                    const result = await executeAndFetchWithLimit(connection, queryToExecute, 200000, queryTimeout);

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

                    if (result && Array.isArray(result) && (result as any).columns) {
                        const columns = (result as any).columns;
                        allResults.push({
                            columns: columns,
                            data: result,
                            rowsAffected: (result as any).count,
                            limitReached: (result as any).limitReached,
                            sql: queryToExecute
                        });
                    } else {
                        allResults.push({
                            columns: [],
                            data: [],
                            rowsAffected: (result as any)?.count,
                            message: 'Query executed successfully',
                            sql: queryToExecute
                        });
                    }
                } catch (err: any) {
                    const errorMsg = `Error: ${err.message}`;
                    outputChannel.appendLine(`Error in query ${i + 1}: ${errorMsg}`);
                    throw new Error(errorMsg);
                }
            }
            outputChannel.appendLine('All queries completed.');
        } finally {
            // Clear session tracking
            clearCurrentSession();
            // Close connection only if not using persistent connection
            if (shouldCloseConnection) {
                await connection.close();
            }
        }
    } catch (error: any) {
        const errorMessage = `Error: ${error.message}`;
        outputChannel.appendLine(errorMessage);
        throw new Error(errorMessage);
    }

    return allResults;
}

// function formatOdbcError ... removed/not used

/**
 * Execute a query and fetch results up to a limit using NzConnection.
 */
async function executeAndFetchWithLimit(connection: any, query: string, limit: number, timeoutSeconds?: number): Promise<any> {
    const cmd = connection.createCommand(query);
    if (timeoutSeconds && timeoutSeconds > 0) {
        cmd.commandTimeout = timeoutSeconds;
    }

    const reader = await cmd.executeReader();
    const columns: { name: string; type?: string }[] = [];
    const allRows: any[] = [];
    let fetchedCount = 0;

    // Fetch loop
    while (await reader.read()) {
        // Initialize columns on first row
        if (columns.length === 0) {
            for (let i = 0; i < reader.fieldCount; i++) {
                columns.push({ name: reader.getName(i), type: reader.getTypeName(i) });
            }
        }

        const row: any[] = [];
        for (let i = 0; i < reader.fieldCount; i++) {
            row.push(reader.getValue(i));
        }
        allRows.push(row);
        fetchedCount++;

        if (fetchedCount >= limit) {
            (allRows as any).limitReached = true;
            break;
        }
    }

    // Attach columns metadata
    if (columns.length > 0) {
        (allRows as any).columns = columns;
    }
    // (allRows as any).count = ...; // Not easily available unless from CommandComplete

    return allRows;
}