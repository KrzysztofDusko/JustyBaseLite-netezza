import * as vscode from 'vscode';
import { runQueryRaw, queryResultToRows, QueryResult } from '../core/queryRunner';
import { SchemaSearchHtmlGenerator } from '../views/schemaSearchHtmlGenerator';
import { MetadataCache } from '../metadataCache';
import { ConnectionManager, ConnectionDetails } from '../core/connectionManager';
import { searchInCodeWithMode, SourceSearchMode } from '../sql/sqlTextUtils';
import { NZ_QUERIES, NZ_SYSTEM_VIEWS } from '../metadata/systemQueries';
import { createNzConnection } from '../core/nzConnectionFactory';

interface SearchResultItem {
    NAME: string;
    SCHEMA: string;
    DATABASE: string;
    TYPE: string;
    PARENT: string;
    DESCRIPTION: string;
    MATCH_TYPE: string;
    connectionName: string;
}

export class SchemaSearchProvider implements vscode.WebviewViewProvider {
    public static readonly viewType = 'netezza.search';
    private _view?: vscode.WebviewView;

    constructor(
        private readonly _extensionUri: vscode.Uri,
        private context: vscode.ExtensionContext,
        private metadataCache: MetadataCache,
        private connectionManager: ConnectionManager
    ) { }

    public resolveWebviewView(
        webviewView: vscode.WebviewView,
        _context: vscode.WebviewViewResolveContext,
        _token: vscode.CancellationToken
    ) {
        this._view = webviewView;

        webviewView.webview.options = {
            enableScripts: true,
            localResourceRoots: [this._extensionUri]
        };

        webviewView.webview.html = new SchemaSearchHtmlGenerator().generateHtml();

        webviewView.webview.onDidReceiveMessage(async data => {
            switch (data.type) {
                case 'search':
                    await this.search(data.value);
                    break;
                case 'searchSource':
                    await this.searchSourceCode(data.value, data.mode || 'noCommentsNoLiterals');
                    break;
                case 'searchCombined':
                    await this.doCombinedSearch(data.value, data.mode || 'raw');
                    break;
                case 'navigate':
                    // Execute command to reveal item in schema tree
                    vscode.commands.executeCommand('netezza.revealInSchema', data);
                    break;
                case 'cancel':
                    // Cancel current search by incrementing searchId
                    this.currentSearchId++;
                    this._view?.webview.postMessage({ type: 'cancelled' });
                    break;
                case 'reset':
                    // Reset search - cancel and clear results
                    this.currentSearchId++;
                    this._view?.webview.postMessage({ type: 'reset' });
                    break;
            }
        });
    }

    private currentSearchId = 0;

    private async getDatabases(connectionName: string, details?: ConnectionDetails): Promise<string[]> {
        let databases: string[] = [];
        try {
            const dbResult = await runQueryRaw(
                this.context,
                NZ_QUERIES.LIST_DATABASES,
                true,
                this.connectionManager,
                connectionName
            );
            if (dbResult && dbResult.data) {
                const dbRows = queryResultToRows<{ DATABASE: string }>(dbResult);
                databases = dbRows.map(d => d.DATABASE);
            }
        } catch (e) {
            console.error('Error fetching databases for search:', e);
        }

        if (databases.length === 0 && details && details.database) {
            databases = [details.database];
        }
        return databases;
    }

    private async search(term: string, searchId?: number, combined?: boolean, preloadedDatabases?: string[]) {
        if (!term || term.length < 2) {
            return;
        }

        // Determine active connection
        // Priority: Active Tab (if SQL) -> Global Active Connection
        let connectionName: string | undefined;

        const activeEditor = vscode.window.activeTextEditor;
        if (activeEditor && activeEditor.document.languageId === 'sql') {
            connectionName = this.connectionManager.getConnectionForExecution(activeEditor.document.uri.toString());
        }

        if (!connectionName) {
            connectionName = this.connectionManager.getActiveConnectionName() || undefined;
        }

        if (!connectionName) {
            this._view?.webview.postMessage({ type: 'results', data: [], append: combined ? true : false });
            // Optionally could notify user: vscode.window.showWarningMessage('No active connection for search');
            return;
        }

        if (searchId === undefined) {
            searchId = ++this.currentSearchId;
        }

        const sentIds = new Set<string>();

        // 1. Search in Cache first (Immediate results) - CONNECTION SCOPED
        if (this._view) {
            const cachedResults = this.metadataCache.search(term, connectionName);
            if (cachedResults.length > 0) {
                const mappedResults: SearchResultItem[] = [];

                cachedResults.forEach(item => {
                    // Generate ID to deduplicate later - normalized
                    const key = `${item.name.toUpperCase().trim()}|${item.type.toUpperCase().trim()}|${(item.parent || '').toUpperCase().trim()}`;

                    if (!sentIds.has(key)) {
                        sentIds.add(key);
                        mappedResults.push({
                            NAME: item.name,
                            SCHEMA: item.schema || '',
                            DATABASE: item.database || '',
                            TYPE: (item.type || '').toString().trim().toUpperCase(),
                            PARENT: item.parent || '',
                            DESCRIPTION: 'Result from Cache',
                            MATCH_TYPE: 'NAME', // Cache mostly matches by name
                            connectionName: connectionName! // Pass connection name for Reveal
                        });
                    }
                });

                // Sort cached results by priority: Tables/Views (1) -> Columns (2)
                mappedResults.sort((a, b) => {
                    const getPrio = (t: string) => (t === 'COLUMN' ? 2 : 1);
                    return getPrio(b.TYPE) - getPrio(a.TYPE)
                        ? getPrio(a.TYPE) - getPrio(b.TYPE)
                        : a.NAME.localeCompare(b.NAME);
                });

                // Send cached results immediately
                if (mappedResults.length > 0 && searchId === this.currentSearchId) {
                    this._view.webview.postMessage({ type: 'results', data: mappedResults, append: false });
                }
            } else {
                // Cache is empty, show searching indicator while waiting for DB results
                if (searchId === this.currentSearchId) {
                    this._view.webview.postMessage({ type: 'searching', message: 'Searching in database...' });
                }
            }

            if (!this.metadataCache.hasAllObjectsPrefetchTriggered(connectionName)) {
                this.metadataCache.prefetchAllObjects(connectionName, async q =>
                    runQueryRaw(this.context, q, true, this.connectionManager, connectionName)
                );
            }
        }

        // 2. Search in Database (Comprehensive results) using parallel connections
        // Only if connection is available
        const safeTerm = term.replace(/'/g, "''").toUpperCase();
        const likeTerm = `%${safeTerm}%`;

        // Get connection details for parallel connections
        const details = await this.connectionManager.getConnection(connectionName);
        if (!details) {
            this._view?.webview.postMessage({ type: 'results', data: [], append: combined ? true : false });
            return;
        }

        // Get list of databases to search
        let databases: string[] = [];

        if (preloadedDatabases) {
            databases = preloadedDatabases;
        } else {
            databases = await this.getDatabases(connectionName, details);
        }

        if (databases.length === 0) {
            this._view?.webview.postMessage({ type: 'results', data: [], append: combined ? true : false });
            return;
        }

        // Build search tasks for each database - use parallel execution with max 8 connections
        const MAX_CONCURRENCY = 8;

        const searchTasks = databases.map(db => async () => {
            const cleanDb = db.toUpperCase();

            // Build query for this database with all search types
            const query = `
                SELECT * FROM (
                    -- Part 1: Objects (Tables, Views, etc) matching NAME
                    SELECT 1 AS PRIORITY, OBJNAME AS NAME, SCHEMA, DBNAME AS DATABASE, OBJTYPE AS TYPE, '' AS PARENT, 
                           COALESCE(DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
                    FROM ${cleanDb}..${NZ_SYSTEM_VIEWS.OBJECT_DATA}
                    WHERE DBNAME = '${cleanDb}' AND UPPER(OBJNAME) LIKE '${likeTerm}'
                    UNION ALL
                    -- Part 2: Objects matching DESCRIPTION  
                    SELECT 1 AS PRIORITY, OBJNAME AS NAME, SCHEMA, DBNAME AS DATABASE, OBJTYPE AS TYPE, '' AS PARENT, 
                           COALESCE(DESCRIPTION, '') AS DESCRIPTION, 'OBJ_DESC' AS MATCH_TYPE
                    FROM ${cleanDb}..${NZ_SYSTEM_VIEWS.OBJECT_DATA}
                    WHERE DBNAME = '${cleanDb}' AND UPPER(DESCRIPTION) LIKE '${likeTerm}' AND UPPER(OBJNAME) NOT LIKE '${likeTerm}'
                    UNION ALL
                    -- Part 3: Columns matching NAME
                    SELECT 2 AS PRIORITY, C.ATTNAME AS NAME, O.SCHEMA, O.DBNAME AS DATABASE, 'COLUMN' AS TYPE, O.OBJNAME AS PARENT,
                           COALESCE(C.DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
                    FROM ${cleanDb}..${NZ_SYSTEM_VIEWS.RELATION_COLUMN} C
                    JOIN ${cleanDb}..${NZ_SYSTEM_VIEWS.OBJECT_DATA} O ON C.OBJID = O.OBJID
                    WHERE O.DBNAME = '${cleanDb}' AND UPPER(C.ATTNAME) LIKE '${likeTerm}'
                    UNION ALL
                    -- Part 4: Columns matching DESCRIPTION (NEW!)
                    SELECT 2 AS PRIORITY, C.ATTNAME AS NAME, O.SCHEMA, O.DBNAME AS DATABASE, 'COLUMN' AS TYPE, O.OBJNAME AS PARENT,
                           COALESCE(C.DESCRIPTION, '') AS DESCRIPTION, 'COL_DESC' AS MATCH_TYPE
                    FROM ${cleanDb}..${NZ_SYSTEM_VIEWS.RELATION_COLUMN} C
                    JOIN ${cleanDb}..${NZ_SYSTEM_VIEWS.OBJECT_DATA} O ON C.OBJID = O.OBJID
                    WHERE O.DBNAME = '${cleanDb}' AND UPPER(C.DESCRIPTION) LIKE '${likeTerm}' AND UPPER(C.ATTNAME) NOT LIKE '${likeTerm}'
                    UNION ALL
                    -- Part 5: External Tables matching DATAOBJECT
                    SELECT 3 AS PRIORITY, E1.TABLENAME AS NAME, E1.SCHEMA, E1.DATABASE, 'EXTERNAL TABLE' AS TYPE, '' AS PARENT,
                           COALESCE(E2.EXTOBJNAME, '') AS DESCRIPTION, 'DATAOBJECT' AS MATCH_TYPE
                    FROM ${cleanDb}..${NZ_SYSTEM_VIEWS.EXTERNAL} E1
                    JOIN ${cleanDb}..${NZ_SYSTEM_VIEWS.EXTOBJECT} E2 ON E1.DATABASE = E2.DATABASE AND E1.SCHEMA = E2.SCHEMA AND E1.TABLENAME = E2.TABLENAME
                    WHERE E1.DATABASE = '${cleanDb}' AND UPPER(E2.EXTOBJNAME) LIKE '${likeTerm}'
                ) AS R
                ORDER BY PRIORITY, NAME
                LIMIT 200
            `;

            try {
                const result = await this.runSearchOnDatabase(details, db, query);
                if (result && result.data && searchId === this.currentSearchId) {
                    return queryResultToRows<SearchResultItem & { [key: string]: unknown }>(result);
                }
            } catch (e) {
                console.debug(`Error searching in database ${db}:`, e);
            }
            return [];
        });

        try {
            // Execute all database searches in parallel with concurrency limit
            const resultBatches = await this.runWithConcurrencyLimit(searchTasks, MAX_CONCURRENCY);

            if (searchId !== this.currentSearchId) {
                return; // Old search, ignore
            }

            const mappedResults: SearchResultItem[] = [];

            for (const batch of resultBatches) {
                for (const i of batch) {
                    const item = i as SearchResultItem;
                    const key = `${item.NAME.toUpperCase().trim()}|${item.TYPE.toUpperCase().trim()}|${(item.PARENT || '').toUpperCase().trim()}|${item.DATABASE}`;

                    if (!sentIds.has(key)) {
                        mappedResults.push({
                            NAME: item.NAME,
                            SCHEMA: item.SCHEMA,
                            DATABASE: item.DATABASE,
                            TYPE: (item.TYPE || '').toString().trim().toUpperCase(),
                            PARENT: item.PARENT,
                            DESCRIPTION: item.DESCRIPTION,
                            MATCH_TYPE: item.MATCH_TYPE,
                            connectionName: connectionName!
                        });
                        sentIds.add(key);
                    }
                }
            }

            // Sort results by priority and name
            mappedResults.sort((a, b) => {
                const getPriority = (type: string) => {
                    if (type === 'COLUMN') return 2;
                    if (type === 'EXTERNAL TABLE') return 3;
                    return 1;
                };
                const pA = getPriority(a.TYPE);
                const pB = getPriority(b.TYPE);
                if (pA !== pB) return pA - pB;
                return a.NAME.localeCompare(b.NAME);
            });

            // Send DB results
            if (mappedResults.length > 0 && this._view) {
                this._view.webview.postMessage({ type: 'results', data: mappedResults, append: true });
            } else if (sentIds.size === 0 && this._view) {
                this._view.webview.postMessage({ type: 'results', data: [], append: combined ? true : false });
            }
        } catch (e: unknown) {
            console.error('Search error:', e);
            if (this._view && searchId === this.currentSearchId) {
                this._view.webview.postMessage({ type: 'error', message: e instanceof Error ? e.message : String(e) });
            }
        }
    }

    private async runSearchOnDatabase(details: ConnectionDetails, db: string, sql: string): Promise<QueryResult> {
        const conn = createNzConnection({
            host: details.host,
            port: details.port || 5480,
            database: db || details.database,
            user: details.user,
            password: details.password
        });

        try {
            await conn.connect();
            const cmd = conn.createCommand(sql);
            // Default timeout for search 30s
            cmd.commandTimeout = 30;
            const reader = await cmd.executeReader();

            const columns: Array<{ name: string }> = [];
            for (let i = 0; i < reader.fieldCount; i++) {
                columns.push({ name: reader.getName(i) }); // minimal for queryResultToRows
            }

            const data: Array<Array<unknown>> = [];
            while (await reader.read()) {
                const row: Array<unknown> = [];
                for (let i = 0; i < reader.fieldCount; i++) {
                    row.push(reader.getValue(i));
                }
                data.push(row);
            }
            await reader.close();

            return {
                columns,
                data,
                rowsAffected: undefined,
                limitReached: false,
                sql
            };
        } finally {
            try {
                await conn.close();
            } catch {
                // Ignore close errors
            }
        }
    }

    /**
     * Helper: Run up to N promises with max concurrency limit
     * Useful for limiting database connections to prevent overload
     * 
     * Uses a worker pool pattern to avoid race conditions
     */
    private async runWithConcurrencyLimit<T>(
        tasks: Array<() => Promise<T>>,
        maxConcurrency: number
    ): Promise<T[]> {
        if (tasks.length === 0) {
            return [];
        }

        const results: T[] = new Array(tasks.length);
        let nextIndex = 0;

        // Worker function that processes tasks from the queue
        const worker = async (): Promise<void> => {
            while (true) {
                // Atomically get next task index
                const currentIndex = nextIndex;
                if (currentIndex >= tasks.length) {
                    break; // No more tasks
                }
                nextIndex = currentIndex + 1;

                try {
                    const result = await tasks[currentIndex]();
                    results[currentIndex] = result;
                } catch (e) {
                    // Log error but continue processing other tasks
                    console.debug(`Task ${currentIndex} failed:`, e);
                    // Store empty array for failed tasks to avoid undefined
                    results[currentIndex] = [] as unknown as T;
                }
            }
        };

        // Start worker pool - each worker processes tasks sequentially
        const workerCount = Math.min(maxConcurrency, tasks.length);
        const workers: Promise<void>[] = [];
        for (let i = 0; i < workerCount; i++) {
            workers.push(worker());
        }

        // Wait for all workers to complete
        await Promise.all(workers);

        // Filter out any undefined values (shouldn't happen now, but safety check)
        return results.filter((r): r is T => r !== undefined);
    }

    /**
     * Search in VIEW/PROCEDURE source code with configurable mode
     * @param mode Search mode: 'raw', 'noComments', 'noCommentsNoLiterals'
     */
    private async searchSourceCode(term: string, mode: SourceSearchMode, searchId?: number, combined?: boolean, preloadedDatabases?: string[]) {
        if (!term || term.length < 2) {
            return;
        }

        // Determine active connection
        let connectionName: string | undefined;
        const activeEditor = vscode.window.activeTextEditor;
        if (activeEditor && activeEditor.document.languageId === 'sql') {
            connectionName = this.connectionManager.getConnectionForExecution(activeEditor.document.uri.toString());
        }
        if (!connectionName) {
            connectionName = this.connectionManager.getActiveConnectionName() || undefined;
        }
        if (!connectionName) {
            this._view?.webview.postMessage({ type: 'results', data: [], append: combined ? true : false });
            return;
        }

        const details = await this.connectionManager.getConnection(connectionName);
        if (!details) {
            this._view?.webview.postMessage({ type: 'results', data: [], append: combined ? true : false });
            return;
        }

        if (searchId === undefined) {
            searchId = ++this.currentSearchId;
        }

        // Human readable mode description
        const modeDesc = mode === 'raw' ? 'raw source' :
            mode === 'noComments' ? 'source (excl. comments)' :
                'source (excl. comments/strings)';

        try {
            // Send searching status to panel
            if (this._view && searchId === this.currentSearchId) {
                this._view.webview.postMessage({ type: 'searching', message: `Searching in ${modeDesc}...` });
            }

            const safeTerm = term.replace(/'/g, "''").toUpperCase();
            const likeTerm = `%${safeTerm}%`;
            const results: SearchResultItem[] = [];

            // First, get list of all databases to search across (for procedures)
            let databases: string[] = [];

            if (preloadedDatabases) {
                databases = preloadedDatabases.filter(db => db !== 'SYSTEM');
            } else {
                const allDbs = await this.getDatabases(connectionName, details);
                databases = allDbs.filter(db => db !== 'SYSTEM');
            }

            // RAW mode: Use WHERE LIKE to filter at database level (no need to download source)
            // Other modes: Download source and filter in-memory
            const useServerSideFilter = mode === 'raw';
            const MAX_CONCURRENCY = 8;

            // 1. Search in VIEW definitions across all databases
            // Create all view search tasks upfront
            const viewTasks = databases.map(db => async () => {
                const viewQuery = useServerSideFilter
                    ? `SELECT VIEWNAME AS NAME, SCHEMA, DATABASE 
                       FROM ${db}.._V_VIEW 
                       WHERE DATABASE != 'SYSTEM' AND UPPER(DEFINITION) LIKE '${likeTerm}'`
                    : `SELECT VIEWNAME AS NAME, SCHEMA, DATABASE, DEFINITION AS SOURCE 
                       FROM ${db}.._V_VIEW 
                       WHERE DATABASE != 'SYSTEM'`;

                try {
                    const viewResult = await this.runSearchOnDatabase(details, db, viewQuery);

                    if (viewResult && viewResult.data && searchId === this.currentSearchId) {
                        const views = queryResultToRows<{ NAME: string; SCHEMA: string; DATABASE: string; SOURCE?: string } & { [key: string]: unknown }>(viewResult);
                        const batchResults: SearchResultItem[] = [];
                        for (const view of views) {
                            // For RAW mode with server-side filter, all results match
                            // For other modes, check in-memory
                            if (useServerSideFilter || (view.SOURCE && searchInCodeWithMode(view.SOURCE, safeTerm, mode))) {
                                batchResults.push({
                                    NAME: view.NAME,
                                    SCHEMA: view.SCHEMA,
                                    DATABASE: view.DATABASE,
                                    TYPE: 'VIEW',
                                    PARENT: '',
                                    DESCRIPTION: `Found in view ${modeDesc}`,
                                    MATCH_TYPE: 'SOURCE_CODE',
                                    connectionName: connectionName!
                                });
                            }
                        }
                        return batchResults;
                    }
                } catch (e) {
                    console.debug(`Error searching views in database ${db}:`, e);
                }
                return [];
            });

            // Run all view searches with concurrency limit
            const viewResultBatches = await this.runWithConcurrencyLimit(viewTasks, MAX_CONCURRENCY);
            for (const batchResults of viewResultBatches) {
                results.push(...batchResults);
            }

            // 2. Search in PROCEDURE sources across all databases
            // Create all procedure search tasks upfront
            const procTasks = databases.map(db => async () => {
                const procQuery = useServerSideFilter
                    ? `SELECT PROCEDURE AS NAME, SCHEMA, DATABASE FROM ${db}.._V_PROCEDURE WHERE DATABASE != 'SYSTEM' AND UPPER(PROCEDURESOURCE) LIKE '${likeTerm}'`
                    : `SELECT PROCEDURE AS NAME, SCHEMA, DATABASE, PROCEDURESOURCE AS SOURCE FROM ${db}.._V_PROCEDURE WHERE DATABASE != 'SYSTEM'`;

                try {
                    const procResult = await this.runSearchOnDatabase(details, db, procQuery);
                    if (procResult && procResult.data && searchId === this.currentSearchId) {
                        const procs = queryResultToRows<{ NAME: string; SCHEMA: string; DATABASE: string; SOURCE?: string } & { [key: string]: unknown }>(procResult);
                        const batchResults: SearchResultItem[] = [];
                        for (const proc of procs) {
                            // For RAW mode with server-side filter, all results match
                            // For other modes, check in-memory
                            if (useServerSideFilter || (proc.SOURCE && searchInCodeWithMode(proc.SOURCE, safeTerm, mode))) {
                                batchResults.push({
                                    NAME: proc.NAME,
                                    SCHEMA: proc.SCHEMA,
                                    DATABASE: proc.DATABASE,
                                    TYPE: 'PROCEDURE',
                                    PARENT: '',
                                    DESCRIPTION: `Found in procedure ${modeDesc}`,
                                    MATCH_TYPE: 'SOURCE_CODE',
                                    connectionName: connectionName!
                                });
                            }
                        }
                        return batchResults;
                    }
                } catch (e) {
                    console.debug(`Error searching procedures in database ${db}:`, e);
                }
                return [];
            });

            // Run all procedure searches with concurrency limit
            const procResultBatches = await this.runWithConcurrencyLimit(procTasks, MAX_CONCURRENCY);
            for (const batchResults of procResultBatches) {
                results.push(...batchResults);
            }

            // Send results (append so combined "objects + source raw" mode sums both sets)
            if (this._view && searchId === this.currentSearchId) {
                this._view.webview.postMessage({ type: 'results', data: results, append: true });
            }
        } catch (e: unknown) {
            console.error('Source code search error:', e);
            if (this._view && searchId === this.currentSearchId) {
                this._view.webview.postMessage({ type: 'error', message: e instanceof Error ? e.message : String(e) });
            }
        }
    }

    private async doCombinedSearch(term: string, mode: SourceSearchMode) {
        const searchId = ++this.currentSearchId;

        // Optimization: Fetch databases once and share
        let connectionName: string | undefined;
        const activeEditor = vscode.window.activeTextEditor;
        if (activeEditor && activeEditor.document.languageId === 'sql') {
            connectionName = this.connectionManager.getConnectionForExecution(activeEditor.document.uri.toString());
        }
        if (!connectionName) {
            connectionName = this.connectionManager.getActiveConnectionName() || undefined;
        }

        let databases: string[] | undefined;
        if (connectionName) {
            const details = await this.connectionManager.getConnection(connectionName);
            if (details) {
                databases = await this.getDatabases(connectionName, details);
            }
        }

        // Run both with the same searchId
        this.search(term, searchId, true, databases);
        this.searchSourceCode(term, mode, searchId, true, databases);
    } // End of doCombinedSearch
}
