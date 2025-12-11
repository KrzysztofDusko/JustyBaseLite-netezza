import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';
import { QueryHistoryManager } from './queryHistoryManager';

// Import odbc package for native ODBC connectivity
let odbc: any;
try {
    odbc = require('odbc');
} catch (err) {
    console.error('odbc package not installed. Run: npm install odbc');
}

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
    if (!odbc) {
        throw new Error('odbc package not installed. Please run: npm install odbc');
    }

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

        // Connect to database using native ODBC with fetchArray option
        // This tells the driver to return results as arrays instead of objects
        let connection;
        let shouldCloseConnection = true;
        let connectionString: string;
        let resolvedConnectionName: string | undefined = connectionName;

        // If no explicit connectionName, try to resolve from document or fall back to active
        if (!resolvedConnectionName && documentUri) {
            resolvedConnectionName = connManager.getConnectionForExecution(documentUri);
        }
        if (!resolvedConnectionName) {
            resolvedConnectionName = connManager.getActiveConnectionName() || undefined;
        }

        if (keepConnectionOpen) {
            // Use persistent connection for specific name if provided, or active if not
            connection = await connManager.getPersistentConnection(resolvedConnectionName);
            shouldCloseConnection = false; // Don't close persistent connection
            // Still need connection string for history logging
            const connStr = await connManager.getConnectionString(resolvedConnectionName);
            if (!connStr) {
                throw new Error('Connection not configured. Please connect via Netezza: Connect...');
            }
            connectionString = connStr;
        } else {
            const connStr = await connManager.getConnectionString(resolvedConnectionName);
            if (!connStr) {
                throw new Error('Connection not configured. Please connect via Netezza: Connect...');
            }
            connectionString = connStr;
            connection = await odbc.connect({ connectionString, fetchArray: true });
        }

        try {
            // Get timeout configuration
            const config = vscode.workspace.getConfiguration('netezza');
            const queryTimeout = config.get<number>('queryTimeout', 1800);

            // Execute query - results will be arrays since we set fetchArray: true on connection
            // Limit to 200,000 rows to prevent UI lockup
            const result = await executeAndFetchWithLimit(connection, queryToExecute, 200000, queryTimeout);

            // Get current schema for history logging
            let currentSchema = 'unknown';
            try {
                const schemaResult = await connection.query('SELECT CURRENT_SCHEMA');
                if (schemaResult && schemaResult.length > 0) {
                    if (Array.isArray(schemaResult[0])) {
                        currentSchema = schemaResult[0][0] || 'unknown';
                    } else {
                        currentSchema = schemaResult[0].CURRENT_SCHEMA || 'unknown';
                    }
                }
            } catch (schemaErr) {
                // Fallback if schema query fails
                console.debug('Could not retrieve current schema:', schemaErr);
            }

            // Log to history (async, don't wait)
            const connectionDetails = parseConnectionString(connectionString);
            const historyManager = new QueryHistoryManager(context);
            historyManager.addEntry(
                connectionDetails.host,
                connectionDetails.database,
                currentSchema,
                query,
                resolvedConnectionName // Pass the RESOLVED connection name to history
            ).catch(err => {
                console.error('Failed to log query to history:', err);
            });

            if (result && Array.isArray(result)) {
                // SELECT query - return results as columns + data
                const columns = (result as any).columns ? (result as any).columns.map((c: any) => ({ name: c.name, type: c.dataType })) : [];

                if (outputChannel) {
                    outputChannel.appendLine('Query completed.');
                }

                return {
                    columns: columns,
                    data: result,
                    rowsAffected: (result as any).count,
                    limitReached: (result as any).limitReached,
                    sql: queryToExecute
                };
            } else {
                // Non-SELECT query (INSERT, UPDATE, DELETE, etc.)
                if (outputChannel) {
                    outputChannel.appendLine('Query executed successfully (no results).');
                }
                return {
                    columns: [],
                    data: [],
                    rowsAffected: (result as any)?.count,
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
        const errorMessage = formatOdbcError(error);
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
    if (!odbc) {
        throw new Error('odbc package not installed. Please run: npm install odbc');
    }

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

    try {
        // Connect with fetchArray option to get results as arrays
        let connection;
        let shouldCloseConnection = true;
        let connectionString: string;

        if (keepConnectionOpen) {
            connection = await connManager.getPersistentConnection(resolvedConnectionName);
            shouldCloseConnection = false; // Don't close persistent connection
            // Still need connection string for history logging
            const connStr = await connManager.getConnectionString(resolvedConnectionName);
            if (!connStr) {
                throw new Error('Connection not configured. Please connect via Netezza: Connect...');
            }
            connectionString = connStr;
        } else {
            const connStr = await connManager.getConnectionString(resolvedConnectionName);
            if (!connStr) {
                throw new Error('Connection not configured. Please connect via Netezza: Connect...');
            }
            connectionString = connStr;
            connection = await odbc.connect({ connectionString, fetchArray: true });
        }

        try {
            // Get current schema for history logging
            let currentSchema = 'unknown';
            try {
                const schemaResult = await connection.query('SELECT CURRENT_SCHEMA');
                if (schemaResult && schemaResult.length > 0) {
                    if (Array.isArray(schemaResult[0])) {
                        currentSchema = schemaResult[0][0] || 'unknown';
                    } else {
                        currentSchema = schemaResult[0].CURRENT_SCHEMA || 'unknown';
                    }
                }
            } catch (schemaErr) {
                console.debug('Could not retrieve current schema:', schemaErr);
            }

            // Capture current session ID for DROP SESSION functionality
            try {
                const sidResult = await connection.query('SELECT CURRENT_SID');
                if (sidResult && sidResult.length > 0) {
                    currentExecutingSessionId = Array.isArray(sidResult[0])
                        ? sidResult[0][0]
                        : sidResult[0].CURRENT_SID;
                    currentExecutingConnectionName = resolvedConnectionName;
                    outputChannel.appendLine(`Session ID: ${currentExecutingSessionId}`);
                }
            } catch (sidErr) {
                console.debug('Could not retrieve session ID:', sidErr);
            }

            const connectionDetails = parseConnectionString(connectionString);
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

                    // Execute query - results will be arrays since we set fetchArray: true on connection
                    // Limit to 200,000 rows to prevent UI lockup
                    const config = vscode.workspace.getConfiguration('netezza');
                    const queryTimeout = config.get<number>('queryTimeout', 1800);
                    const result = await executeAndFetchWithLimit(connection, queryToExecute, 200000, queryTimeout);

                    // Log to history (async, don't wait)
                    historyManager.addEntry(
                        connectionDetails.host,
                        connectionDetails.database,
                        currentSchema,
                        query,
                        activeConnectionName // Pass the connection name to history
                    ).catch(err => {
                        console.error('Failed to log query to history:', err);
                    });

                    if (result && Array.isArray(result)) {
                        const columns = (result as any).columns ? (result as any).columns.map((c: any) => ({ name: c.name, type: c.dataType })) : [];
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
                    const errorMsg = formatOdbcError(err);
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
        const errorMessage = formatOdbcError(error);
        outputChannel.appendLine(errorMessage);
        throw new Error(errorMessage);
    }

    return allResults;
}

function formatOdbcError(error: any): string {
    if (error.odbcErrors && Array.isArray(error.odbcErrors) && error.odbcErrors.length > 0) {
        return error.odbcErrors.map((e: any) => {
            return `[ODBC Error] State: ${e.state}, Native Code: ${e.code}\nMessage: ${e.message}`;
        }).join('\n\n');
    }
    return `Error: ${error.message || error}`;
}

/**
 * Execute a query and fetch results up to a limit.
 * This replaces connection.query() to allow capping the number of rows returned.
 */
async function executeAndFetchWithLimit(connection: any, query: string, limit: number, timeoutSeconds?: number): Promise<any> {
    const statement = await connection.createStatement();
    try {
        await statement.prepare(query);
        // Pass cursor: true to get a cursor instead of full results immediately
        const options: any = { cursor: true, fetchSize: 5000 };
        if (timeoutSeconds && timeoutSeconds > 0) {
            options.timeout = timeoutSeconds;
        }
        const cursor = await statement.execute(options);

        const allRows: any[] = [];
        let fetchedCount = 0;

        // Fetch first batch
        let result = await cursor.fetch();

        // If result has columns, preserve them on the final array
        let columns = result ? (result as any).columns : undefined;
        // Also capture 'count' (affected rows) if present, though likely only on last or first?
        let affectedRows = (result as any).count;

        while (result && result.length > 0) {
            // Update columns if not yet set (should be on first result)
            if (!columns && (result as any).columns) {
                columns = (result as any).columns;
            }

            // Append rows
            for (const row of result) {
                allRows.push(row);
                fetchedCount++;
                if (fetchedCount >= limit) {
                    (allRows as any).limitReached = true;
                    break;
                }
            }

            if (fetchedCount >= limit) {
                (allRows as any).limitReached = true;
                break;
            }

            // Fetch next batch
            result = await cursor.fetch();
        }

        // Clean up cursor explicitly if needed, though statement.close() handles it usually.
        await cursor.close();

        // Attach columns metadata to the array to match connection.query behavior
        if (columns) {
            (allRows as any).columns = columns;
        }
        (allRows as any).count = affectedRows; // Approximate or from first batch
        return allRows;

    } catch (err: any) {
        // If it's a non-SELECT query that doesn't return a cursor/result set in a standard way?
        // But statement.execute({cursor:true}) should still work.
        // Falls through to finally
        throw err;
    } finally {
        await statement.close();
    }
}