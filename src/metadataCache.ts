
import * as vscode from 'vscode';

// Per-key cache entry with individual timestamps
export interface PerKeyEntry<T> {
    data: T;
    timestamp: number;
}

// Cache type identifiers for selective saving
export type CacheType = 'db' | 'schema' | 'table' | 'column';

export class MetadataCache {
    // In-memory caches with per-key timestamps
    private dbCache: Map<string, { data: any[], timestamp: number }> = new Map();
    public schemaCache: Map<string, PerKeyEntry<any[]>> = new Map();
    public tableCache: Map<string, PerKeyEntry<any[]>> = new Map(); // Key: "DB.SCHEMA" or "DB.."
    public columnCache: Map<string, PerKeyEntry<any[]>> = new Map(); // Key: "DB.SCHEMA.TABLE" or "DB..TABLE"
    // tableIdMap is synced with tableCache - stored per tableCache key
    public tableIdMap: Map<string, PerKeyEntry<Map<string, number>>> = new Map(); // Key: tableCache key -> {tableName -> OBJID}

    private readonly CACHE_TTL = 4 * 60 * 60 * 1000; // 4 hours in milliseconds

    // Debounce support for saves
    private savePending: boolean = false;
    private pendingCacheTypes: Set<CacheType> = new Set();
    private saveTimeoutId: ReturnType<typeof setTimeout> | undefined;

    // Background prefetch tracking
    private columnPrefetchInProgress: Set<string> = new Set(); // Track prefetch by "DB.SCHEMA" or "DB.."


    constructor(private context: vscode.ExtensionContext) {
        this.loadCacheFromWorkspaceState();
    }

    private loadCacheFromWorkspaceState(): void {
        try {
            const now = Date.now();

            // Load databases cache (map of connection -> entry)
            // Previous version was single entry. We try to load as Record<string, ...>
            // If it was the old format, it will fail the loop or need check. 
            // Better to use new key or just check type.
            const dbCacheEntry = this.context.workspaceState.get<any>('sqlCompletion.dbCache');
            if (dbCacheEntry) {
                // Check if it's the old single entry format
                if (dbCacheEntry.data && Array.isArray(dbCacheEntry.data)) {
                    // specific case: ignore old cache or map to 'default'?
                    // Let's ignore old cache to avoid confusion.
                } else {
                    // Assume it's Record<string, Entry>
                    for (const [key, entry] of Object.entries(dbCacheEntry as Record<string, { data: any[], timestamp: number }>)) {
                        if ((now - entry.timestamp) < this.CACHE_TTL) {
                            this.dbCache.set(key, entry);
                        }
                    }
                }
            }

            // Load schemas cache (per-key timestamps)
            const schemaCacheEntry = this.context.workspaceState.get<Record<string, { data: any[], timestamp: number }>>('sqlCompletion.schemaCache');
            if (schemaCacheEntry) {
                for (const [key, entry] of Object.entries(schemaCacheEntry)) {
                    if ((now - entry.timestamp) < this.CACHE_TTL) {
                        this.schemaCache.set(key, entry);
                    }
                }
            }

            // Load tables cache (per-key timestamps)
            const tableCacheEntry = this.context.workspaceState.get<Record<string, { data: any[], timestamp: number }>>('sqlCompletion.tableCache');
            if (tableCacheEntry) {
                for (const [key, entry] of Object.entries(tableCacheEntry)) {
                    if ((now - entry.timestamp) < this.CACHE_TTL) {
                        this.tableCache.set(key, entry);
                    }
                }
            }

            // Load table ID map (synced with tableCache - per-key timestamps)
            const tableIdMapEntry = this.context.workspaceState.get<Record<string, { data: Record<string, number>, timestamp: number }>>('sqlCompletion.tableIdMap');
            if (tableIdMapEntry) {
                for (const [key, entry] of Object.entries(tableIdMapEntry)) {
                    // Only load if corresponding tableCache entry is still valid or not expired
                    if (this.tableCache.has(key) || (now - entry.timestamp) < this.CACHE_TTL) {
                        this.tableIdMap.set(key, {
                            data: new Map(Object.entries(entry.data)),
                            timestamp: entry.timestamp
                        });
                    }
                }
            }

            // Load columns cache (per-key timestamps)
            const columnCacheEntry = this.context.workspaceState.get<Record<string, { data: any[], timestamp: number }>>('sqlCompletion.columnCache');
            if (columnCacheEntry) {
                for (const [key, entry] of Object.entries(columnCacheEntry)) {
                    if ((now - entry.timestamp) < this.CACHE_TTL) {
                        this.columnCache.set(key, entry);
                    }
                }
            }
        } catch (e) {
            console.error('[MetadataCache] Error loading cache from workspace state:', e);
        }
    }

    // Debounced save scheduler - only saves specified cache types
    public scheduleSave(cacheType: CacheType): void {
        this.pendingCacheTypes.add(cacheType);
        if (!this.savePending) {
            this.savePending = true;
            this.saveTimeoutId = setTimeout(() => this.flushSave(), 1000); // 1s debounce
        }
    }

    private async flushSave(): Promise<void> {
        this.savePending = false;
        const typesToSave = new Set(this.pendingCacheTypes);
        this.pendingCacheTypes.clear();

        try {
            // Save only the cache types that were modified
            if (typesToSave.has('db') && this.dbCache.size > 0) {
                const serialized: Record<string, { data: any[], timestamp: number }> = {};
                this.dbCache.forEach((entry, key) => {
                    serialized[key] = entry;
                });
                await this.context.workspaceState.update('sqlCompletion.dbCache', serialized);
            }

            if (typesToSave.has('schema') && this.schemaCache.size > 0) {
                const serialized: Record<string, { data: any[], timestamp: number }> = {};
                this.schemaCache.forEach((entry, key) => {
                    serialized[key] = entry;
                });
                await this.context.workspaceState.update('sqlCompletion.schemaCache', serialized);
            }

            if (typesToSave.has('table') && this.tableCache.size > 0) {
                const serialized: Record<string, { data: any[], timestamp: number }> = {};
                this.tableCache.forEach((entry, key) => {
                    serialized[key] = entry;
                });
                await this.context.workspaceState.update('sqlCompletion.tableCache', serialized);

                // Also save tableIdMap synced with tableCache
                const tableIdSerialized: Record<string, { data: Record<string, number>, timestamp: number }> = {};
                this.tableIdMap.forEach((entry, key) => {
                    const dataObj: Record<string, number> = {};
                    entry.data.forEach((value, k) => { dataObj[k] = value; });
                    tableIdSerialized[key] = {
                        data: dataObj,
                        timestamp: entry.timestamp
                    };
                });
                await this.context.workspaceState.update('sqlCompletion.tableIdMap', tableIdSerialized);
            }

            if (typesToSave.has('column') && this.columnCache.size > 0) {
                const serialized: Record<string, { data: any[], timestamp: number }> = {};
                this.columnCache.forEach((entry, key) => {
                    serialized[key] = entry;
                });
                await this.context.workspaceState.update('sqlCompletion.columnCache', serialized);
            }
        } catch (e) {
            console.error('[MetadataCache] Error saving cache to workspace state:', e);
        }
    }

    public async clearCache(): Promise<void> {
        // Cancel pending saves
        if (this.saveTimeoutId) {
            clearTimeout(this.saveTimeoutId);
            this.saveTimeoutId = undefined;
        }
        this.savePending = false;
        this.pendingCacheTypes.clear();

        this.dbCache.clear();
        this.schemaCache.clear();
        this.tableCache.clear();
        this.columnCache.clear();
        this.tableIdMap.clear();

        await this.context.workspaceState.update('sqlCompletion.dbCache', undefined);
        await this.context.workspaceState.update('sqlCompletion.schemaCache', undefined);
        await this.context.workspaceState.update('sqlCompletion.tableCache', undefined);
        await this.context.workspaceState.update('sqlCompletion.columnCache', undefined);
        await this.context.workspaceState.update('sqlCompletion.tableIdMap', undefined);
    }

    // Modifying cache methods to accept connectionName
    public getDatabases(connectionName: string): any[] | undefined {
        return this.dbCache.get(connectionName)?.data;
    }

    public setDatabases(connectionName: string, data: any[]) {
        this.dbCache.set(connectionName, { data, timestamp: Date.now() });
        this.scheduleSave('db');
    }

    public getSchemas(connectionName: string, dbName: string): any[] | undefined {
        const key = `${connectionName}|${dbName}`;
        return this.schemaCache.get(key)?.data;
    }

    public setSchemas(connectionName: string, dbName: string, data: any[]) {
        const key = `${connectionName}|${dbName}`;
        this.schemaCache.set(key, { data, timestamp: Date.now() });
        this.scheduleSave('schema');
    }

    public getTables(connectionName: string, key: string): any[] | undefined {
        // incoming key is DB.SCHEMA or DB..
        const fullKey = `${connectionName}|${key}`;
        return this.tableCache.get(fullKey)?.data;
    }

    public setTables(connectionName: string, key: string, data: any[], idMap: Map<string, number>) {
        const now = Date.now();
        const fullKey = `${connectionName}|${key}`;
        this.tableCache.set(fullKey, { data, timestamp: now });
        this.tableIdMap.set(fullKey, { data: idMap, timestamp: now });
        this.scheduleSave('table');
    }

    public getColumns(connectionName: string, key: string): any[] | undefined {
        // incoming key is DB.SCHEMA.TABLE
        const fullKey = `${connectionName}|${key}`;
        return this.columnCache.get(fullKey)?.data;
    }

    public setColumns(connectionName: string, key: string, data: any[]) {
        const fullKey = `${connectionName}|${key}`;
        this.columnCache.set(fullKey, { data, timestamp: Date.now() });
        this.scheduleSave('column');
    }

    public findTableId(connectionName: string, lookupKey: string): number | undefined {
        // Search through tableIdMap entries that match the connection
        const prefix = `${connectionName}|`;
        for (const [key, entry] of this.tableIdMap) {
            if (key.startsWith(prefix)) {
                const found = entry.data.get(lookupKey);
                if (found !== undefined) {
                    return found;
                }
            }
        }
        return undefined;
    }

    // Search Method
    public search(term: string, connectionName?: string): { name: string, type: string, database?: string, schema?: string, parent?: string }[] {
        const results: { name: string, type: string, database?: string, schema?: string, parent?: string }[] = [];
        const lowerTerm = term.toLowerCase();

        // Helper to check if entry belongs to connection
        const matchesConnection = (key: string) => {
            if (!connectionName) return true; // if no connection specified, search all (or maybe should restrict?)
            return key.startsWith(`${connectionName}|`);
        };

        // Search Tables (in tableCache) 
        for (const [key, entry] of this.tableCache) {
            if (!matchesConnection(key)) continue;

            // Key format: "CONN|DBNAME.SCHEMA" or "CONN|DBNAME.."
            // We need to parse strictly.
            const parts = key.split('|');
            if (parts.length < 2) continue;

            const dbKey = parts[1]; // DBNAME.SCHEMA
            const dbParts = dbKey.split('.');
            const dbName = dbParts[0];
            const schemaName = dbParts.length > 1 ? dbParts[1] : undefined;

            for (const item of entry.data) {
                const name = typeof item.label === 'string' ? item.label : item.label.label;

                if (name && name.toLowerCase().includes(lowerTerm)) {
                    results.push({
                        name: name,
                        type: 'TABLE',
                        database: dbName,
                        schema: schemaName || (item.detail && item.detail.includes('(') ? item.detail.match(/\((.*?)\)/)?.[1] : undefined)
                    });
                }
            }
        }

        // Search Columns (in columnCache)
        for (const [key, entry] of this.columnCache) {
            if (!matchesConnection(key)) continue;

            // Key: CONN|DB.SCHEMA.TABLE
            const parts = key.split('|');
            if (parts.length < 2) continue;

            const dbKey = parts[1];
            const dbParts = dbKey.split('.');
            const dbName = dbParts[0];
            const schemaName = dbParts[1];
            const tableName = dbParts[2];

            for (const item of entry.data) {
                const name = typeof item.label === 'string' ? item.label : item.label.label;
                if (name && name.toLowerCase().includes(lowerTerm)) {
                    results.push({
                        name: name,
                        type: 'COLUMN',
                        database: dbName,
                        schema: schemaName,
                        parent: tableName
                    });
                }
            }
        }

        return results;
    }

    public async prefetchColumnsForSchema(
        connectionName: string,
        dbName: string,
        schemaName: string | undefined,
        runQueryFn: (query: string) => Promise<string | undefined>
    ): Promise<void> {
        const prefetchKey = schemaName ? `${dbName}.${schemaName}` : `${dbName}..`;
        const fullPrefetchKey = `${connectionName}|${prefetchKey}`;

        // Check if already prefetching
        if (this.columnPrefetchInProgress.has(fullPrefetchKey)) {
            return;
        }

        // Get tables from cache
        const tables = this.getTables(connectionName, prefetchKey);
        if (!tables || tables.length === 0) {
            return;
        }

        this.columnPrefetchInProgress.add(fullPrefetchKey);
        // console.log(`[MetadataCache] Starting background column prefetch for ${fullPrefetchKey}`);

        try {
            // Get list of tables that need columns fetched
            const tablesToFetch: string[] = [];
            for (const table of tables) {
                const tableName = typeof table.label === 'string' ? table.label : table.label?.label;
                if (!tableName) continue;

                const columnKey = `${dbName}.${schemaName || ''}.${tableName}`;
                if (!this.getColumns(connectionName, columnKey)) {
                    tablesToFetch.push(tableName);
                }
            }

            if (tablesToFetch.length === 0) {
                return;
            }

            // console.log(`[MetadataCache] Fetching columns for ${tablesToFetch.length} tables in ${fullPrefetchKey}`);

            // Fetch columns in batches
            const BATCH_SIZE = 10;
            for (let i = 0; i < tablesToFetch.length; i += BATCH_SIZE) {
                const batch = tablesToFetch.slice(i, i + BATCH_SIZE);
                const tableList = batch.map(t => `'${t}'`).join(',');

                const dbPrefix = `${dbName}..`;
                const schemaClause = schemaName ? `AND UPPER(O.SCHEMA) = UPPER('${schemaName}')` : '';

                const query = `
                    SELECT O.OBJNAME AS TABLENAME, C.ATTNAME, C.FORMAT_TYPE, C.ATTNUM
                    FROM ${dbPrefix}_V_RELATION_COLUMN C
                    JOIN ${dbPrefix}_V_OBJECT_DATA O ON C.OBJID = O.OBJID
                    WHERE UPPER(O.OBJNAME) IN (${tableList.toUpperCase()}) 
                    ${schemaClause}
                    AND UPPER(O.DBNAME) = UPPER('${dbName}')
                    ORDER BY O.OBJNAME, C.ATTNUM
                `;

                try {
                    const resultJson = await runQueryFn(query);
                    if (resultJson) {
                        const results = JSON.parse(resultJson);
                        const columnsByTable = new Map<string, any[]>();
                        for (const row of results) {
                            const tableName = row.TABLENAME;
                            if (!columnsByTable.has(tableName)) {
                                columnsByTable.set(tableName, []);
                            }
                            columnsByTable.get(tableName)!.push({
                                label: row.ATTNAME,
                                kind: 5,
                                detail: row.FORMAT_TYPE
                            });
                        }

                        for (const [tableName, columns] of columnsByTable) {
                            const columnKey = `${dbName}.${schemaName || ''}.${tableName}`;
                            this.setColumns(connectionName, columnKey, columns);
                        }
                    }
                } catch (e) {
                    console.error(`[MetadataCache] Error fetching batch columns:`, e);
                }
            }

        } finally {
            this.columnPrefetchInProgress.delete(fullPrefetchKey);
        }
    }

    public async prefetchAllObjects(
        connectionName: string,
        runQueryFn: (query: string) => Promise<string | undefined>
    ): Promise<void> {
        // We track prefetch trigger per connection?
        // Let's use a Set instead of boolean
        // But the previous API was boolean. Let's make it scoped.
        const key = `ALL_OBJECTS|${connectionName}`;
        if (this.allObjectsPrefetchTriggeredSet.has(key)) {
            return;
        }
        this.allObjectsPrefetchTriggeredSet.add(key);

        console.log(`[MetadataCache] Starting background prefetch of all objects for search (Connection: ${connectionName})`);

        try {
            const tablesQuery = `
                SELECT OBJNAME, OBJID, SCHEMA, DBNAME 
                FROM _V_OBJECT_DATA 
                WHERE OBJTYPE = 'TABLE' 
                ORDER BY DBNAME, SCHEMA, OBJNAME
            `;

            const resultJson = await runQueryFn(tablesQuery);
            if (!resultJson) return;

            const results = JSON.parse(resultJson);
            const tablesByKey = new Map<string, { tables: any[], idMap: Map<string, number> }>();

            for (const row of results) {
                const key = row.SCHEMA ? `${row.DBNAME}.${row.SCHEMA}` : `${row.DBNAME}..`;
                if (!tablesByKey.has(key)) {
                    tablesByKey.set(key, { tables: [], idMap: new Map() });
                }
                const entry = tablesByKey.get(key)!;
                entry.tables.push({
                    label: row.OBJNAME,
                    kind: 7,
                    detail: row.SCHEMA ? 'Table' : `Table (${row.SCHEMA})`,
                    sortText: row.OBJNAME
                });

                const fullKey = row.SCHEMA
                    ? `${row.DBNAME}.${row.SCHEMA}.${row.OBJNAME}`
                    : `${row.DBNAME}..${row.OBJNAME}`;
                entry.idMap.set(fullKey, row.OBJID);
            }

            for (const [key, entry] of tablesByKey) {
                // Set only if not already cached? Or update?
                // setTables will overwrite.
                this.setTables(connectionName, key, entry.tables, entry.idMap);
            }

            console.log(`[MetadataCache] Prefetched tables for ${tablesByKey.size} schema(s) on ${connectionName}`);

        } catch (e) {
            console.error(`[MetadataCache] Error in prefetchAllObjects:`, e);
        }
    }

    private allObjectsPrefetchTriggeredSet: Set<string> = new Set();

    public hasAllObjectsPrefetchTriggered(connectionName: string): boolean {
        return this.allObjectsPrefetchTriggeredSet.has(`ALL_OBJECTS|${connectionName}`);
    }

    // ========== Eager Connection Prefetch ==========

    // Track which connections have already triggered full prefetch
    private connectionPrefetchTriggered: Set<string> = new Set();
    private connectionPrefetchInProgress: Set<string> = new Set();

    /**
     * Check if connection prefetch has been triggered
     */
    public hasConnectionPrefetchTriggered(connectionName: string): boolean {
        return this.connectionPrefetchTriggered.has(connectionName);
    }

    /**
     * Trigger background prefetch for a connection.
     * This is non-blocking - returns immediately and fetches data in background.
     * Fetches: databases -> schemas -> tables (with views) -> columns
     */
    public triggerConnectionPrefetch(
        connectionName: string,
        runQueryFn: (query: string) => Promise<string | undefined>
    ): void {
        // Skip if already triggered or in progress
        if (this.connectionPrefetchTriggered.has(connectionName) ||
            this.connectionPrefetchInProgress.has(connectionName)) {
            return;
        }

        this.connectionPrefetchInProgress.add(connectionName);
        console.log(`[MetadataCache] Starting eager prefetch for connection: ${connectionName}`);

        // Run prefetch in background (non-blocking)
        this.executeConnectionPrefetch(connectionName, runQueryFn)
            .catch(e => console.error(`[MetadataCache] Connection prefetch error:`, e))
            .finally(() => {
                this.connectionPrefetchInProgress.delete(connectionName);
                this.connectionPrefetchTriggered.add(connectionName);
                console.log(`[MetadataCache] Completed eager prefetch for connection: ${connectionName}`);
            });
    }

    /**
     * Execute the actual prefetch logic
     */
    private async executeConnectionPrefetch(
        connectionName: string,
        runQueryFn: (query: string) => Promise<string | undefined>
    ): Promise<void> {
        // 1. Fetch all databases
        const databases = await this.prefetchDatabases(connectionName, runQueryFn);
        if (!databases || databases.length === 0) {
            return;
        }

        // 2. For each database, fetch schemas and tables/views
        for (const dbName of databases) {
            // Fetch schemas for this database
            await this.prefetchSchemasForDb(connectionName, dbName, runQueryFn);

            // Small delay between databases to not overload the server
            await new Promise(resolve => setTimeout(resolve, 50));
        }

        // 3. Fetch all tables and views for connection (bulk query)
        await this.prefetchAllTablesAndViews(connectionName, runQueryFn);

        // 4. Fetch columns in batches (for all tables)
        await this.prefetchAllColumnsForConnection(connectionName, runQueryFn);
    }

    private async prefetchDatabases(
        connectionName: string,
        runQueryFn: (query: string) => Promise<string | undefined>
    ): Promise<string[]> {
        // Skip if already cached
        if (this.getDatabases(connectionName)) {
            const cached = this.getDatabases(connectionName);
            return cached?.map((item: any) =>
                typeof item.label === 'string' ? item.label : item.label?.label
            ).filter(Boolean) || [];
        }

        try {
            const query = "SELECT DATABASE FROM system.._v_database ORDER BY DATABASE";
            const resultJson = await runQueryFn(query);
            if (!resultJson) return [];

            const results = JSON.parse(resultJson);
            const items = results.map((row: any) => ({
                label: row.DATABASE,
                kind: 9, // Module
                detail: 'Database'
            }));

            this.setDatabases(connectionName, items);
            return results.map((row: any) => row.DATABASE);
        } catch (e) {
            console.error('[MetadataCache] prefetchDatabases error:', e);
            return [];
        }
    }

    private async prefetchSchemasForDb(
        connectionName: string,
        dbName: string,
        runQueryFn: (query: string) => Promise<string | undefined>
    ): Promise<void> {
        // Skip if already cached
        if (this.getSchemas(connectionName, dbName)) {
            return;
        }

        try {
            const query = `SELECT SCHEMA FROM ${dbName}.._V_SCHEMA ORDER BY SCHEMA`;
            const resultJson = await runQueryFn(query);
            if (!resultJson) return;

            const results = JSON.parse(resultJson);
            const items = results
                .filter((row: any) => row.SCHEMA != null && row.SCHEMA !== '')
                .map((row: any) => ({
                    label: row.SCHEMA,
                    kind: 19, // Folder
                    detail: `Schema in ${dbName}`,
                    insertText: row.SCHEMA,
                    sortText: row.SCHEMA,
                    filterText: row.SCHEMA
                }));

            this.setSchemas(connectionName, dbName, items);
        } catch (e) {
            console.error(`[MetadataCache] prefetchSchemasForDb error for ${dbName}:`, e);
        }
    }

    private async prefetchAllTablesAndViews(
        connectionName: string,
        runQueryFn: (query: string) => Promise<string | undefined>
    ): Promise<void> {
        try {
            // Fetch all tables AND views in one query
            const query = `
                SELECT OBJNAME, OBJID, SCHEMA, DBNAME, OBJTYPE
                FROM _V_OBJECT_DATA 
                WHERE OBJTYPE IN ('TABLE', 'VIEW')
                ORDER BY DBNAME, SCHEMA, OBJNAME
            `;

            const resultJson = await runQueryFn(query);
            if (!resultJson) return;

            const results = JSON.parse(resultJson);
            const tablesByKey = new Map<string, { tables: any[], idMap: Map<string, number> }>();

            for (const row of results) {
                const key = row.SCHEMA ? `${row.DBNAME}.${row.SCHEMA}` : `${row.DBNAME}..`;
                if (!tablesByKey.has(key)) {
                    tablesByKey.set(key, { tables: [], idMap: new Map() });
                }
                const entry = tablesByKey.get(key)!;
                entry.tables.push({
                    label: row.OBJNAME,
                    kind: row.OBJTYPE === 'VIEW' ? 18 : 7, // Interface for VIEW, Class for TABLE
                    detail: row.SCHEMA ? row.OBJTYPE : `${row.OBJTYPE} (${row.SCHEMA})`,
                    sortText: row.OBJNAME
                });

                const fullKey = row.SCHEMA
                    ? `${row.DBNAME}.${row.SCHEMA}.${row.OBJNAME}`
                    : `${row.DBNAME}..${row.OBJNAME}`;
                entry.idMap.set(fullKey, row.OBJID);
            }

            for (const [key, entry] of tablesByKey) {
                // Only set if not already cached
                if (!this.getTables(connectionName, key)) {
                    this.setTables(connectionName, key, entry.tables, entry.idMap);
                }
            }

            console.log(`[MetadataCache] Prefetched tables/views for ${tablesByKey.size} schema(s)`);
        } catch (e) {
            console.error(`[MetadataCache] prefetchAllTablesAndViews error:`, e);
        }
    }

    private async prefetchAllColumnsForConnection(
        connectionName: string,
        runQueryFn: (query: string) => Promise<string | undefined>
    ): Promise<void> {
        try {
            // 1. Identify all tables that were just prefetched (stored in tableCache)
            const connPrefix = `${connectionName}|`;
            const allTables: { schema: string, name: string, db: string }[] = [];

            // Iterate over all cache entries for this connection
            // Note: tableCache keys are "CONN|DB.SCHEMA" or "CONN|DB.."
            for (const [key, entry] of this.tableCache) {
                if (!key.startsWith(connPrefix)) continue;

                // Parse key to get DB and Schema
                // Key format: "CONN|DBNAME.SCHEMA" or "CONN|DBNAME.."
                const parts = key.split('|');
                if (parts.length < 2) continue;

                const dbKey = parts[1]; // "DB.SCHEMA"
                const dbParts = dbKey.split('.');
                const dbName = dbParts[0];
                const schemaName = dbParts.length > 1 ? dbParts[1] : '';

                // Collect all tables from this cache entry
                for (const table of entry.data) {
                    const tableName = typeof table.label === 'string' ? table.label : table.label.label;
                    if (tableName) {
                        allTables.push({
                            schema: schemaName,
                            name: tableName,
                            db: dbName
                        });
                    }
                }
            }

            if (allTables.length === 0) {
                return;
            }

            // 2. Process in batches
            // Batch by groups of 50 tables to keep query size and result size manageable
            const BATCH_SIZE = 50;
            let fetchedCount = 0;

            for (let i = 0; i < allTables.length; i += BATCH_SIZE) {
                const batch = allTables.slice(i, i + BATCH_SIZE);

                // Group by Database to minimize context switching in queries if possible,
                // but usually we can query across schemas if fully qualified.
                // However, Netezza metadata views are often DB-specific (current DB).
                // _V_RELATION_COLUMN is system view? No, it's usually current DB context or system view.
                // Queries to system.._v_relation_column might cover all?
                // Safest is to query per DB/Schema or just rely on the fact that we can filter by DB/Schema.

                // Construct WHERE clause parts
                // (DB = 'X' AND SCHEMA = 'Y' AND OBJNAME IN (...))
                // Optimally we'd group by DB first. 
                // Let's assume most tables are in same DB for a connection context or few DBs.
                // Mixed DBs in one batch might be complex if we need to switch contexts?
                // We'll use system.._v_relation_column which should see all? 
                // Actually safer to iterate objects and build a complex OR clause or just IN clause if filtered.

                // Simplified approach: Group batch by DB first
                const batchByDb = new Map<string, typeof batch>();
                for (const item of batch) {
                    if (!batchByDb.has(item.db)) {
                        batchByDb.set(item.db, []);
                    }
                    batchByDb.get(item.db)!.push(item);
                }

                for (const [dbName, dbBatch] of batchByDb) {
                    // For this DB, build (SCHEMA, TABLE) tuples or just list of tables if schema handled
                    // To handle mixed schemas in same DB effectively with one query:
                    // WHERE (SCHEMA = 'S1' AND TABLENAME IN ('T1','T2')) OR (SCHEMA = 'S2' AND ...)
                    // This creates long query.
                    // Alternative: WHERE DBNAME = '...' AND OBJNAME IN ('T1', 'T2'...) -- collisions if same table name in diff schemas?
                    // Yes, collisions possible.
                    // So we must include schema in filter.

                    const conditions = dbBatch.map(t =>
                        `(UPPER(O.SCHEMA) = UPPER('${t.schema}') AND UPPER(O.OBJNAME) = UPPER('${t.name}'))`
                    ).join(' OR ');

                    const query = `
                        SELECT O.OBJNAME AS TABLENAME, O.SCHEMA, O.DBNAME, 
                               C.ATTNAME, C.FORMAT_TYPE, C.ATTNUM
                        FROM ${dbName}.._V_RELATION_COLUMN C
                        JOIN ${dbName}.._V_OBJECT_DATA O ON C.OBJID = O.OBJID
                        WHERE O.DBNAME = '${dbName}' 
                        AND (${conditions})
                        ORDER BY O.SCHEMA, O.OBJNAME, C.ATTNUM
                    `;

                    try {
                        const resultJson = await runQueryFn(query);
                        if (resultJson) {
                            const results = JSON.parse(resultJson);
                            const columnsByKey = new Map<string, any[]>();

                            for (const row of results) {
                                const key = `${row.DBNAME}.${row.SCHEMA || ''}.${row.TABLENAME}`;
                                if (!columnsByKey.has(key)) {
                                    columnsByKey.set(key, []);
                                }
                                columnsByKey.get(key)!.push({
                                    label: row.ATTNAME,
                                    kind: 5,
                                    detail: row.FORMAT_TYPE
                                });
                            }

                            for (const [key, columns] of columnsByKey) {
                                // Only set if not already cached (or update?)
                                if (!this.getColumns(connectionName, key)) {
                                    this.setColumns(connectionName, key, columns);
                                    fetchedCount++;
                                }
                            }
                        }
                    } catch (e) {
                        console.error(`[MetadataCache] Error fetching batch columns for DB ${dbName}:`, e);
                    }
                }

                // Yield to event loop to keep UI responsive
                await new Promise(resolve => setTimeout(resolve, 10));
            }

            console.log(`[MetadataCache] Prefetched columns for ${fetchedCount} tables/views (Batched)`);
        } catch (e) {
            console.error(`[MetadataCache] prefetchAllColumnsForConnection error:`, e);
        }
    }
}
