
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
    private dbCache: { data: any[], timestamp: number } | undefined;
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
    private allObjectsPrefetchTriggered: boolean = false; // For schema search

    constructor(private context: vscode.ExtensionContext) {
        this.loadCacheFromWorkspaceState();
    }

    private loadCacheFromWorkspaceState(): void {
        try {
            const now = Date.now();

            // Load databases cache (single entry with timestamp)
            const dbCacheEntry = this.context.workspaceState.get<{ data: any[], timestamp: number }>('sqlCompletion.dbCache');
            if (dbCacheEntry && (now - dbCacheEntry.timestamp) < this.CACHE_TTL) {
                this.dbCache = dbCacheEntry;
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
            if (typesToSave.has('db') && this.dbCache) {
                await this.context.workspaceState.update('sqlCompletion.dbCache', this.dbCache);
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

        this.dbCache = undefined;
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

    public getDatabases(): any[] | undefined {
        return this.dbCache?.data;
    }

    public setDatabases(data: any[]) {
        this.dbCache = { data, timestamp: Date.now() };
        this.scheduleSave('db');
    }

    public getSchemas(dbName: string): any[] | undefined {
        return this.schemaCache.get(dbName)?.data;
    }

    public setSchemas(dbName: string, data: any[]) {
        this.schemaCache.set(dbName, { data, timestamp: Date.now() });
        this.scheduleSave('schema');
    }

    public getTables(key: string): any[] | undefined {
        return this.tableCache.get(key)?.data;
    }

    public setTables(key: string, data: any[], idMap: Map<string, number>) {
        const now = Date.now();
        this.tableCache.set(key, { data, timestamp: now });
        this.tableIdMap.set(key, { data: idMap, timestamp: now });
        this.scheduleSave('table');
    }

    public getColumns(key: string): any[] | undefined {
        return this.columnCache.get(key)?.data;
    }

    public setColumns(key: string, data: any[]) {
        this.columnCache.set(key, { data, timestamp: Date.now() });
        this.scheduleSave('column');
    }

    public findTableId(lookupKey: string): number | undefined {
        // Search through all tableIdMap entries for the specific table
        for (const entry of this.tableIdMap.values()) {
            const found = entry.data.get(lookupKey);
            if (found !== undefined) {
                return found;
            }
        }
        return undefined;
    }

    // Search Method
    public search(term: string): { name: string, type: string, database?: string, schema?: string, parent?: string }[] {
        const results: { name: string, type: string, database?: string, schema?: string, parent?: string }[] = [];
        const lowerTerm = term.toLowerCase();

        // Search Tables (in tableCache) 
        // keys are DB.SCHEMA or DB..
        for (const [key, entry] of this.tableCache) {
            // Parse key to get context
            // Key format: "DBNAME.SCHEMA" or "DBNAME.."
            const parts = key.split('.');
            const dbName = parts[0];
            const schemaName = parts.length > 1 ? parts[1] : undefined;

            for (const item of entry.data) {
                // item is CompletionItem, so we use item.label
                // Wait, in completionProvider we stored objects that LOOK like CompletionItems or are constructed?
                // In MetadataCache we should probably store raw data to be more versatile? 
                // But for now refactoring, we stored:
                // { label, kind, detail, sortText } in serialized, and in-memory we have CompletionItems or objects.
                // The load function re-hydrates them as clean objects. 

                // Let's assume 'label' is the name.
                const name = typeof item.label === 'string' ? item.label : item.label.label;

                if (name && name.toLowerCase().includes(lowerTerm)) {
                    results.push({
                        name: name,
                        type: 'TABLE', // Or try to infer from item.detail or kind? existing code sets it as TABLE
                        database: dbName,
                        schema: schemaName || (item.detail && item.detail.includes('(') ? item.detail.match(/\((.*?)\)/)?.[1] : undefined)
                    });
                }
            }
        }

        // Search Columns (in columnCache)
        for (const [key, entry] of this.columnCache) {
            // Key: DB.SCHEMA.TABLE
            const parts = key.split('.');
            const dbName = parts[0];
            const schemaName = parts[1]; // might be empty
            const tableName = parts[2];

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

    // Background prefetch all columns for tables in a schema/database
    // runQueryFn should be a function that executes a query and returns JSON result
    public async prefetchColumnsForSchema(
        dbName: string,
        schemaName: string | undefined,
        runQueryFn: (query: string) => Promise<string | undefined>
    ): Promise<void> {
        const prefetchKey = schemaName ? `${dbName}.${schemaName}` : `${dbName}..`;

        // Check if already prefetching
        if (this.columnPrefetchInProgress.has(prefetchKey)) {
            return;
        }

        // Get tables from cache
        const tables = this.getTables(prefetchKey);
        if (!tables || tables.length === 0) {
            return;
        }

        this.columnPrefetchInProgress.add(prefetchKey);
        console.log(`[MetadataCache] Starting background column prefetch for ${prefetchKey}`);

        try {
            // Get list of tables that need columns fetched
            const tablesToFetch: string[] = [];
            for (const table of tables) {
                const tableName = typeof table.label === 'string' ? table.label : table.label?.label;
                if (!tableName) continue;

                const columnKey = `${dbName}.${schemaName || ''}.${tableName}`;
                if (!this.getColumns(columnKey)) {
                    tablesToFetch.push(tableName);
                }
            }

            if (tablesToFetch.length === 0) {
                console.log(`[MetadataCache] All columns already cached for ${prefetchKey}`);
                return;
            }

            console.log(`[MetadataCache] Fetching columns for ${tablesToFetch.length} tables in ${prefetchKey}`);

            // Fetch columns in batches to avoid overwhelming the database
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

                        // Group columns by table
                        const columnsByTable = new Map<string, any[]>();
                        for (const row of results) {
                            const tableName = row.TABLENAME;
                            if (!columnsByTable.has(tableName)) {
                                columnsByTable.set(tableName, []);
                            }
                            columnsByTable.get(tableName)!.push({
                                label: row.ATTNAME,
                                kind: 5, // vscode.CompletionItemKind.Field
                                detail: row.FORMAT_TYPE
                            });
                        }

                        // Store in cache
                        for (const [tableName, columns] of columnsByTable) {
                            const columnKey = `${dbName}.${schemaName || ''}.${tableName}`;
                            this.setColumns(columnKey, columns);
                        }
                    }
                } catch (e) {
                    console.error(`[MetadataCache] Error fetching batch columns:`, e);
                }
            }

            console.log(`[MetadataCache] Completed background column prefetch for ${prefetchKey}`);
        } finally {
            this.columnPrefetchInProgress.delete(prefetchKey);
        }
    }

    // Prefetch all objects for schema search (triggered after first search)
    public async prefetchAllObjects(
        runQueryFn: (query: string) => Promise<string | undefined>
    ): Promise<void> {
        if (this.allObjectsPrefetchTriggered) {
            return;
        }
        this.allObjectsPrefetchTriggered = true;

        console.log(`[MetadataCache] Starting background prefetch of all objects for search`);

        try {
            // Fetch all tables across all databases (from current connection context)
            const tablesQuery = `
                SELECT OBJNAME, OBJID, SCHEMA, DBNAME 
                FROM _V_OBJECT_DATA 
                WHERE OBJTYPE = 'TABLE' 
                ORDER BY DBNAME, SCHEMA, OBJNAME
            `;

            const resultJson = await runQueryFn(tablesQuery);
            if (!resultJson) return;

            const results = JSON.parse(resultJson);

            // Group tables by DB.SCHEMA
            const tablesByKey = new Map<string, { tables: any[], idMap: Map<string, number> }>();

            for (const row of results) {
                const key = row.SCHEMA ? `${row.DBNAME}.${row.SCHEMA}` : `${row.DBNAME}..`;
                if (!tablesByKey.has(key)) {
                    tablesByKey.set(key, { tables: [], idMap: new Map() });
                }
                const entry = tablesByKey.get(key)!;
                entry.tables.push({
                    label: row.OBJNAME,
                    kind: 7, // vscode.CompletionItemKind.Class
                    detail: row.SCHEMA ? 'Table' : `Table (${row.SCHEMA})`,
                    sortText: row.OBJNAME
                });

                const fullKey = row.SCHEMA
                    ? `${row.DBNAME}.${row.SCHEMA}.${row.OBJNAME}`
                    : `${row.DBNAME}..${row.OBJNAME}`;
                entry.idMap.set(fullKey, row.OBJID);
            }

            // Store in cache (only if not already cached)
            for (const [key, entry] of tablesByKey) {
                if (!this.getTables(key)) {
                    this.setTables(key, entry.tables, entry.idMap);
                }
            }

            console.log(`[MetadataCache] Prefetched tables for ${tablesByKey.size} schema(s)`);

        } catch (e) {
            console.error(`[MetadataCache] Error in prefetchAllObjects:`, e);
        }
    }

    // Check if prefetch has been triggered for schema search
    public hasAllObjectsPrefetchTriggered(): boolean {
        return this.allObjectsPrefetchTriggered;
    }
}
