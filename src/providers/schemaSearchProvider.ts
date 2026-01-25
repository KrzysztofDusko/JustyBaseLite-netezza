import * as vscode from 'vscode';
import { runQueryRaw, queryResultToRows, QueryResult } from '../core/queryRunner';
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

        webviewView.webview.html = this._getHtmlForWebview(webviewView.webview);

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

    private async search(term: string, searchId?: number, combined?: boolean) {
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
                            TYPE: item.type,
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

        // If generic listing fails, try to get current database from connection details
        if (databases.length === 0) {
            if (details.database) {
                databases = [details.database];
            }
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
                            TYPE: item.TYPE,
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
    private async searchSourceCode(term: string, mode: SourceSearchMode, searchId?: number, combined?: boolean) {
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
                    databases = dbRows.map(d => d.DATABASE).filter(db => db !== 'SYSTEM');
                }
            } catch (e) {
                console.error('Error fetching databases:', e);
            }

            // If no databases found, fallback to current database only
            if (databases.length === 0) {
                databases = [details.database];
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
        // Run both with the same searchId so they don't cancel each other
        this.search(term, searchId, true);
        this.searchSourceCode(term, mode, searchId, true);
    } // End of doCombinedSearch

    private _getHtmlForWebview(_webview: vscode.Webview) {
        return `<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Schema Search</title>
        <style>
            body { 
                font-family: var(--vscode-font-family); 
                padding: 0; 
                margin: 0;
                color: var(--vscode-foreground); 
                display: flex;
                flex-direction: column;
                height: 100vh;
                overflow: hidden;
            }
            .search-box { 
                display: flex; 
                gap: 5px; 
                padding: 10px;
                flex-shrink: 0;
                border-bottom: 1px solid var(--vscode-panel-border);
            }
            input { flex-grow: 1; padding: 5px; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); }
            button {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 6px;
                background-color: var(--vscode-button-secondaryBackground);
                color: var(--vscode-button-secondaryForeground);
                border: 1px solid var(--vscode-contrastBorder, transparent);
                padding: 4px 10px;
                cursor: pointer;
                border-radius: 2px;
                font-family: var(--vscode-font-family);
                font-size: 12px;
                line-height: 18px;
            }
            button:hover { background-color: var(--vscode-button-secondaryHoverBackground); }
            button.primary { background-color: var(--vscode-button-background); color: var(--vscode-button-foreground); }
            button.primary:hover { background-color: var(--vscode-button-hoverBackground); }
            #status { padding: 5px 10px; flex-shrink: 0; }
            .results { 
                list-style: none; 
                padding: 0; 
                margin: 0; 
                flex-grow: 1; 
                overflow-y: auto; 
            }
            .result-item { padding: 8px 10px; border-bottom: 1px solid var(--vscode-panel-border); display: flex; flex-direction: column; cursor: pointer; position: relative; }
            .result-item:hover { background: var(--vscode-list-hoverBackground); }
            .group-header {
                padding: 10px 10px 5px 10px;
                font-weight: bold;
                background: var(--vscode-editor-background);
                border-bottom: 1px solid var(--vscode-panel-border);
                display: flex;
                justify-content: space-between;
                align-items: center;
                cursor: pointer;
                user-select: none;
                position: sticky;
                top: 0;
                z-index: 10;
            }
            .group-header:hover {
                background: var(--vscode-list-hoverBackground);
            }
            .group-count {
                background: var(--vscode-badge-background);
                color: var(--vscode-badge-foreground);
                padding: 2px 8px;
                border-radius: 12px;
                font-size: 0.85em;
                font-weight: normal;
            }
            .group-toggle {
                display: inline-block;
                width: 12px;
                height: 12px;
                margin-right: 6px;
                transition: transform 0.2s;
            }
            .group-toggle.collapsed {
                transform: rotate(-90deg);
            }
            .group-items {
                display: contents;
            }
            .group-items.collapsed {
                display: none;
            }
            .item-header { display: flex; justify-content: space-between; font-weight: bold; }
            .item-details { font-size: 0.9em; opacity: 0.8; display: flex; gap: 10px; }
            .type-badge { font-size: 0.8em; background: var(--vscode-badge-background); color: var(--vscode-badge-foreground); padding: 2px 5px; border-radius: 3px; }
            .tooltip { position: absolute; background: var(--vscode-editorHoverWidget-background); color: var(--vscode-editorHoverWidget-foreground); border: 1px solid var(--vscode-editorHoverWidget-border); padding: 8px; border-radius: 4px; font-size: 0.9em; max-width: 300px; word-wrap: break-word; z-index: 1000; opacity: 0; visibility: hidden; transition: opacity 0.2s, visibility 0.2s; pointer-events: none; }
            .result-item:hover .tooltip { opacity: 1; visibility: visible; }
            .tooltip.top { bottom: 100%; left: 0; margin-bottom: 5px; }
            .tooltip.bottom { top: 100%; left: 0; margin-top: 5px; }
            .cache-badge { background-color: var(--vscode-charts-green); color: white; padding: 1px 4px; border-radius: 2px; font-size: 0.7em; margin-left: 5px; }
            .spinner {
                border: 2px solid transparent;
                border-top: 2px solid var(--vscode-progressBar-background);
                border-radius: 50%;
                width: 14px;
                height: 14px;
                animation: spin 1s linear infinite;
                display: inline-block;
                vertical-align: middle;
                margin-right: 8px;
            }
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
            .options-row {
                display: flex;
                align-items: center;
                gap: 8px;
                padding: 5px 10px;
                font-size: 12px;
                border-bottom: 1px solid var(--vscode-panel-border);
            }
            .options-row label {
                display: flex;
                align-items: center;
                gap: 4px;
                cursor: pointer;
            }
            .options-row select {
                background: var(--vscode-dropdown-background);
                color: var(--vscode-dropdown-foreground);
                border: 1px solid var(--vscode-dropdown-border);
                padding: 3px 6px;
                border-radius: 2px;
                cursor: pointer;
                font-size: 12px;
            }
            .searching-indicator {
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 20px;
                color: var(--vscode-descriptionForeground);
            }
        </style>
    </head>
    <body>
        <div class="search-box">
            <input type="text" id="searchInput" placeholder="Search tables, columns, view definitions, procedure source..." />
            <button id="searchBtn" class="primary">Search</button>
            <button id="cancelBtn" style="display: none;" title="Cancel search">✕</button>
            <button id="resetBtn" title="Reset search">↺</button>
        </div>
        <div class="options-row">
            <label>
                Source search mode:
                <select id="sourceModeSelect">
                    <option value="">Objects Only</option>
                    <option value="raw">Source: Raw</option>
                    <option value="objectsRaw">Objects + Source Raw</option>
                    <option value="noComments">Source: No Comments</option>
                    <option value="noCommentsNoLiterals">Source: No Comments/Strings</option>
                </select>
            </label>
        </div>
        <div id="status"></div>
        <ul class="results" id="resultsList"></ul>

        <script>
            try {
            const vscode = acquireVsCodeApi();
            const searchInput = document.getElementById('searchInput');
            const searchBtn = document.getElementById('searchBtn');
            const cancelBtn = document.getElementById('cancelBtn');
            const resetBtn = document.getElementById('resetBtn');
            const sourceModeSelect = document.getElementById('sourceModeSelect');
            const resultsList = document.getElementById('resultsList');
            const status = document.getElementById('status');
            
            let isSearching = false;
            let allResults = [];
            
            function setSearchingState(searching) {
                isSearching = searching;
                cancelBtn.style.display = searching ? 'inline-flex' : 'none';
                searchBtn.disabled = searching;
            }

            searchBtn.addEventListener('click', () => {
                const term = searchInput.value;
                if (term) {
                    // Clear any previous results and show searching indicator in results panel
                    allResults = [];
                    resultsList.innerHTML = '<li class="searching-indicator"><span class="spinner"></span> Searching...</li>';
                    status.textContent = '';
                    setSearchingState(true);
                    
                    const sourceMode = sourceModeSelect.value;
                    if (sourceMode === 'objectsRaw') {
                        // Combined mode: objects + source raw
                        vscode.postMessage({ type: 'searchCombined', value: term, mode: 'raw' });
                    } else if (sourceMode) {
                        // Source code search with mode
                        vscode.postMessage({ type: 'searchSource', value: term, mode: sourceMode });
                    } else {
                        // Object search (cache + database)
                        vscode.postMessage({ type: 'search', value: term });
                    }
                }
            });
            
            cancelBtn.addEventListener('click', () => {
                vscode.postMessage({ type: 'cancel' });
            });
            
            resetBtn.addEventListener('click', () => {
                vscode.postMessage({ type: 'reset' });
            });

            searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    searchBtn.click();
                }
            });

            window.addEventListener('message', event => {
                const message = event.data;
                switch (message.type) {
                    case 'searching':
                        // Show searching indicator in results panel
                        resultsList.innerHTML = '<li class="searching-indicator"><span class="spinner"></span> ' + (message.message || 'Searching...') + '</li>';
                        status.textContent = '';
                        break;
                    case 'results':
                        status.textContent = '';
                        setSearchingState(false);
                        renderResults(message.data, message.append);
                        break;
                    case 'error':
                        resultsList.innerHTML = '';
                        status.textContent = 'Error: ' + message.message;
                        allResults = [];
                        setSearchingState(false);
                        break;
                    case 'cancelled':
                        resultsList.innerHTML = '';
                        status.textContent = 'Search cancelled.';
                        allResults = [];
                        setSearchingState(false);
                        break;
                    case 'reset':
                        searchInput.value = '';
                        resultsList.innerHTML = '';
                        status.textContent = '';
                        allResults = [];
                        setSearchingState(false);
                        break;
                }
            });

            function renderResults(data, append) {
                if (!append) {
                    allResults = data || [];
                } else if (data && data.length > 0) {
                    allResults = allResults.concat(data);
                }

                resultsList.innerHTML = '';

                if (!allResults || allResults.length === 0) {
                    status.textContent = 'No results found.';
                    return;
                }

                // Deduplicate items (by NAME + DATABASE + SCHEMA + TYPE combination)
                const seen = new Set();
                const uniqueData = [];
                allResults.forEach(item => {
                    const normalizedType = (item.TYPE || 'OTHER').trim();
                    const key = (item.NAME || '') + '|' + (item.DATABASE || '') + '|' + (item.SCHEMA || '') + '|' + normalizedType;
                    if (!seen.has(key)) {
                        seen.add(key);
                        uniqueData.push(item);
                    }
                });

                // Group results by TYPE (normalized to uppercase and trimmed)
                const groups = {};
                uniqueData.forEach(item => {
                    const type = (item.TYPE || 'OTHER').trim().toUpperCase();
                    if (!groups[type]) {
                        groups[type] = [];
                    }
                    groups[type].push(item);
                });

                // Sort items within each group by DATABASE, then by NAME
                Object.keys(groups).forEach(type => {
                    groups[type].sort((a, b) => {
                        const dbA = (a.DATABASE || '').toUpperCase();
                        const dbB = (b.DATABASE || '').toUpperCase();
                        if (dbA !== dbB) {
                            return dbA.localeCompare(dbB);
                        }
                        const nameA = (a.NAME || '').toUpperCase();
                        const nameB = (b.NAME || '').toUpperCase();
                        return nameA.localeCompare(nameB);
                    });
                });

                // Render groups in order: VIEW, PROCEDURE, TABLE, COLUMN, FUNCTION, then others
                const groupOrder = ['VIEW', 'PROCEDURE', 'TABLE', 'COLUMN', 'FUNCTION', 'INDEX', 'SYSTEM TABLE', 'SYSTEM VIEW', 'SYSTEM SEQ'];
                const sortedTypes = Object.keys(groups).sort((a, b) => {
                    const orderA = groupOrder.indexOf(a);
                    const orderB = groupOrder.indexOf(b);
                    if (orderA === -1 && orderB === -1) return a.localeCompare(b);
                    if (orderA === -1) return 1;
                    if (orderB === -1) return -1;
                    return orderA - orderB;
                });

                sortedTypes.forEach(type => {
                    const items = groups[type];
                    const groupId = 'group-' + type;
                    
                    // Create group header
                    const groupHeader = document.createElement('li');
                    groupHeader.className = 'group-header';
                    groupHeader.setAttribute('data-group', type);
                    groupHeader.innerHTML = \`
                        <div>
                            <span class="group-toggle">▼</span>
                            <span>\${type}</span>
                        </div>
                        <span class="group-count">\${items.length} item\${items.length !== 1 ? 's' : ''}</span>
                    \`;
                    resultsList.appendChild(groupHeader);

                    // Create container for group items
                    const groupContainer = document.createElement('div');
                    groupContainer.className = 'group-items';
                    groupContainer.setAttribute('data-group', type);

                    // Add items to group (already sorted by DATABASE, NAME)
                    items.forEach(item => {
                        const li = document.createElement('li');
                        li.className = 'result-item';

                        const parentInfo = item.PARENT ? \`Parent: \${item.PARENT}\` : '';
                        const schemaInfo = item.SCHEMA ? \`Schema: \${item.SCHEMA}\` : '';
                        const databaseInfo = item.DATABASE ? \`Database: \${item.DATABASE}\` : '';
                        const description = item.DESCRIPTION && item.DESCRIPTION.trim() ? item.DESCRIPTION : '';
                        
                        // Add match type indicator
                        const matchTypeInfo = item.MATCH_TYPE === 'DEFINITION' ? 'Match in view definition' :
                                            item.MATCH_TYPE === 'SOURCE' ? 'Match in procedure source' :
                                            item.MATCH_TYPE === 'SOURCE_CODE' ? 'Match in source code' :
                                            item.MATCH_TYPE === 'NAME' ? 'Match in name' : '';
                        
                        li.innerHTML = \`
                            <div class="item-header">
                                <span>\${item.NAME}</span>
                                <span class="type-badge">\${item.TYPE}</span>
                            </div>
                            <div class="item-details">
                                <span>\${databaseInfo}</span>
                                <span>\${schemaInfo}</span>
                                <span>\${parentInfo}</span>
                                \${matchTypeInfo ? \`<span style="font-style: italic; color: var(--vscode-descriptionForeground);">\${matchTypeInfo}</span>\` : ''}
                            </div>
                            \${description ? \`<div class="tooltip bottom">\${description}</div>\` : ''}
                        \`;
                        
                        // Add double-click handler to navigate to schema tree
                        li.addEventListener('dblclick', () => {
                            vscode.postMessage({ 
                                type: 'navigate', 
                                name: item.NAME,
                                schema: item.SCHEMA,
                                database: item.DATABASE,
                                objType: item.TYPE,
                                parent: item.PARENT,
                                connectionName: item.connectionName // Pass back connection name
                            });
                        });
                        
                        groupContainer.appendChild(li);
                    });

                    resultsList.appendChild(groupContainer);

                    // Add toggle handler
                    groupHeader.addEventListener('click', () => {
                        const toggle = groupHeader.querySelector('.group-toggle');
                        groupContainer.classList.toggle('collapsed');
                        toggle.classList.toggle('collapsed');
                    });
                });
            }
            } catch (e) {
                document.body.innerHTML = '<pre style="color:red;">Error loading Schema Search: ' + e.message + '\\n' + e.stack + '</pre>';
            }
        </script>
    </body>
    </html>`;
    }
}
