/**
 * Metadata Cache - Prefetch Module
 * Background data fetching logic for eager cache population
 */

import { CacheStorage } from './cacheStorage';
import { extractLabel } from './helpers';

/**
 * Type for query execution function
 */
export type QueryRunnerFn = (query: string) => Promise<string | undefined>;

/**
 * Handles background prefetching of metadata for cache population
 */
export class CachePrefetcher {
    // Background prefetch tracking
    private columnPrefetchInProgress: Set<string> = new Set();
    private allObjectsPrefetchTriggeredSet: Set<string> = new Set();
    private connectionPrefetchTriggered: Set<string> = new Set();
    private connectionPrefetchInProgress: Set<string> = new Set();

    constructor(private storage: CacheStorage) { }

    // ========== Column Prefetch for Schema ==========

    async prefetchColumnsForSchema(
        connectionName: string,
        dbName: string,
        schemaName: string | undefined,
        runQueryFn: QueryRunnerFn
    ): Promise<void> {
        const prefetchKey = schemaName ? `${dbName}.${schemaName}` : `${dbName}..`;
        const fullPrefetchKey = `${connectionName}|${prefetchKey}`;

        if (this.columnPrefetchInProgress.has(fullPrefetchKey)) {
            return;
        }

        const tables = this.storage.getTables(connectionName, prefetchKey);
        if (!tables || tables.length === 0) {
            return;
        }

        this.columnPrefetchInProgress.add(fullPrefetchKey);

        try {
            const tablesToFetch: string[] = [];
            for (const table of tables) {
                const tableName = extractLabel(table);
                if (!tableName) continue;

                const columnKey = `${dbName}.${schemaName || ''}.${tableName}`;
                if (!this.storage.getColumns(connectionName, columnKey)) {
                    tablesToFetch.push(tableName);
                }
            }

            if (tablesToFetch.length === 0) {
                return;
            }

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
                            this.storage.setColumns(connectionName, columnKey, columns);
                        }
                    }
                } catch (e) {
                    console.error(`[CachePrefetcher] Error fetching batch columns:`, e);
                }
            }
        } finally {
            this.columnPrefetchInProgress.delete(fullPrefetchKey);
        }
    }

    // ========== All Objects Prefetch ==========

    async prefetchAllObjects(connectionName: string, runQueryFn: QueryRunnerFn): Promise<void> {
        const key = `ALL_OBJECTS|${connectionName}`;
        if (this.allObjectsPrefetchTriggeredSet.has(key)) {
            return;
        }
        this.allObjectsPrefetchTriggeredSet.add(key);

        console.log(`[CachePrefetcher] Starting background prefetch of all objects (Connection: ${connectionName})`);

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
            const tablesByKey = new Map<string, { tables: any[]; idMap: Map<string, number> }>();

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
                this.storage.setTables(connectionName, key, entry.tables, entry.idMap);
            }

            console.log(`[CachePrefetcher] Prefetched tables for ${tablesByKey.size} schema(s) on ${connectionName}`);
        } catch (e) {
            console.error(`[CachePrefetcher] Error in prefetchAllObjects:`, e);
        }
    }

    hasAllObjectsPrefetchTriggered(connectionName: string): boolean {
        return this.allObjectsPrefetchTriggeredSet.has(`ALL_OBJECTS|${connectionName}`);
    }

    // ========== Eager Connection Prefetch ==========

    hasConnectionPrefetchTriggered(connectionName: string): boolean {
        return this.connectionPrefetchTriggered.has(connectionName);
    }

    triggerConnectionPrefetch(connectionName: string, runQueryFn: QueryRunnerFn): void {
        if (
            this.connectionPrefetchTriggered.has(connectionName) ||
            this.connectionPrefetchInProgress.has(connectionName)
        ) {
            return;
        }

        this.connectionPrefetchInProgress.add(connectionName);
        console.log(`[CachePrefetcher] Starting eager prefetch for connection: ${connectionName}`);

        this.executeConnectionPrefetch(connectionName, runQueryFn)
            .catch(e => console.error(`[CachePrefetcher] Connection prefetch error:`, e))
            .finally(() => {
                this.connectionPrefetchInProgress.delete(connectionName);
                this.connectionPrefetchTriggered.add(connectionName);
                console.log(`[CachePrefetcher] Completed eager prefetch for connection: ${connectionName}`);
            });
    }

    private async executeConnectionPrefetch(connectionName: string, runQueryFn: QueryRunnerFn): Promise<void> {
        // 1. Fetch all databases
        const databases = await this.prefetchDatabases(connectionName, runQueryFn);
        if (!databases || databases.length === 0) {
            return;
        }

        // 2. For each database, fetch schemas
        for (const dbName of databases) {
            await this.prefetchSchemasForDb(connectionName, dbName, runQueryFn);
            await new Promise(resolve => setTimeout(resolve, 50));
        }

        // 3. Fetch all tables and views
        await this.prefetchAllTablesAndViews(connectionName, runQueryFn);

        // 4. Fetch columns in batches
        await this.prefetchAllColumnsForConnection(connectionName, runQueryFn);
    }

    private async prefetchDatabases(connectionName: string, runQueryFn: QueryRunnerFn): Promise<string[]> {
        if (this.storage.getDatabases(connectionName)) {
            const cached = this.storage.getDatabases(connectionName);
            return cached?.map((item: any) => extractLabel(item)).filter(Boolean) as string[] || [];
        }

        try {
            const query = 'SELECT DATABASE FROM system.._v_database ORDER BY DATABASE';
            const resultJson = await runQueryFn(query);
            if (!resultJson) return [];

            const results = JSON.parse(resultJson);
            const items = results.map((row: any) => ({
                label: row.DATABASE,
                kind: 9,
                detail: 'Database'
            }));

            this.storage.setDatabases(connectionName, items);
            return results.map((row: any) => row.DATABASE);
        } catch (e) {
            console.error('[CachePrefetcher] prefetchDatabases error:', e);
            return [];
        }
    }

    private async prefetchSchemasForDb(
        connectionName: string,
        dbName: string,
        runQueryFn: QueryRunnerFn
    ): Promise<void> {
        if (this.storage.getSchemas(connectionName, dbName)) {
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
                    kind: 19,
                    detail: `Schema in ${dbName}`,
                    insertText: row.SCHEMA,
                    sortText: row.SCHEMA,
                    filterText: row.SCHEMA
                }));

            this.storage.setSchemas(connectionName, dbName, items);
        } catch (e) {
            console.error(`[CachePrefetcher] prefetchSchemasForDb error for ${dbName}:`, e);
        }
    }

    private async prefetchAllTablesAndViews(connectionName: string, runQueryFn: QueryRunnerFn): Promise<void> {
        try {
            const query = `
                SELECT OBJNAME, OBJID, SCHEMA, DBNAME, OBJTYPE
                FROM _V_OBJECT_DATA 
                WHERE OBJTYPE IN ('TABLE', 'VIEW', 'EXTERNAL TABLE')
                ORDER BY DBNAME, SCHEMA, OBJNAME
            `;

            const resultJson = await runQueryFn(query);
            if (!resultJson) return;

            const results = JSON.parse(resultJson);
            const tablesByKey = new Map<string, { tables: any[]; idMap: Map<string, number> }>();

            for (const row of results) {
                const key = row.SCHEMA ? `${row.DBNAME}.${row.SCHEMA}` : `${row.DBNAME}..`;
                if (!tablesByKey.has(key)) {
                    tablesByKey.set(key, { tables: [], idMap: new Map() });
                }
                const entry = tablesByKey.get(key)!;

                entry.tables.push({
                    label: row.OBJNAME,
                    kind: row.OBJTYPE === 'VIEW' ? 18 : 6,
                    detail: row.SCHEMA ? row.OBJTYPE : `${row.OBJTYPE} (${row.SCHEMA})`,
                    sortText: row.OBJNAME,
                    objType: row.OBJTYPE
                });

                const fullKey = row.SCHEMA
                    ? `${row.DBNAME}.${row.SCHEMA}.${row.OBJNAME}`
                    : `${row.DBNAME}..${row.OBJNAME}`;
                entry.idMap.set(fullKey, row.OBJID);
            }

            for (const [key, entry] of tablesByKey) {
                if (!this.storage.getTables(connectionName, key)) {
                    this.storage.setTables(connectionName, key, entry.tables, entry.idMap);
                }
            }

            console.log(`[CachePrefetcher] Prefetched tables/views for ${tablesByKey.size} schema(s)`);
        } catch (e) {
            console.error(`[CachePrefetcher] prefetchAllTablesAndViews error:`, e);
        }
    }

    private async prefetchAllColumnsForConnection(
        connectionName: string,
        runQueryFn: QueryRunnerFn
    ): Promise<void> {
        try {
            const connPrefix = `${connectionName}|`;
            const allTables: { schema: string; name: string; db: string }[] = [];

            for (const [key, entry] of this.storage.tableCache) {
                if (!key.startsWith(connPrefix)) continue;

                const parts = key.split('|');
                if (parts.length < 2) continue;

                const dbKey = parts[1];
                const dbParts = dbKey.split('.');
                const dbName = dbParts[0];
                const schemaName = dbParts.length > 1 ? dbParts[1] : '';

                for (const table of entry.data) {
                    const tableName = extractLabel(table);
                    if (tableName) {
                        allTables.push({ schema: schemaName, name: tableName, db: dbName });
                    }
                }
            }

            if (allTables.length === 0) {
                return;
            }

            const BATCH_SIZE = 50;
            let fetchedCount = 0;

            for (let i = 0; i < allTables.length; i += BATCH_SIZE) {
                const batch = allTables.slice(i, i + BATCH_SIZE);

                const batchByDb = new Map<string, typeof batch>();
                for (const item of batch) {
                    if (!batchByDb.has(item.db)) {
                        batchByDb.set(item.db, []);
                    }
                    batchByDb.get(item.db)!.push(item);
                }

                for (const [dbName, dbBatch] of batchByDb) {
                    const conditions = dbBatch
                        .map(t => `(UPPER(O.SCHEMA) = UPPER('${t.schema}') AND UPPER(O.OBJNAME) = UPPER('${t.name}'))`)
                        .join(' OR ');

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
                                if (!this.storage.getColumns(connectionName, key)) {
                                    this.storage.setColumns(connectionName, key, columns);
                                    fetchedCount++;
                                }
                            }
                        }
                    } catch (e) {
                        console.error(`[CachePrefetcher] Error fetching batch columns for DB ${dbName}:`, e);
                    }
                }

                await new Promise(resolve => setTimeout(resolve, 10));
            }

            console.log(`[CachePrefetcher] Prefetched columns for ${fetchedCount} tables/views (Batched)`);
        } catch (e) {
            console.error(`[CachePrefetcher] prefetchAllColumnsForConnection error:`, e);
        }
    }
}
