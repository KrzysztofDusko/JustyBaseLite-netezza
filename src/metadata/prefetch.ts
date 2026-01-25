/**
 * Metadata Cache - Prefetch Module
 * Background data fetching logic for eager cache population
 */

import { CacheStorage } from './cacheStorage';
import { extractLabel } from './helpers';
import { TableMetadata, ColumnMetadata } from './types';
import { QueryResult } from '../types';
import { NZ_QUERIES } from './systemQueries';

/**
 * Type for query execution function (legacy - returns JSON string)
 */
export type QueryRunnerFn = (query: string) => Promise<string | undefined>;

/**
 * Type for raw query execution function (returns QueryResult directly - no JSON serialization)
 */
export type QueryRunnerRawFn = (query: string) => Promise<QueryResult | undefined>;

/**
 * Convert QueryResult (columns[] + data[][]) to array of typed objects
 * This replaces JSON.parse() and avoids double serialization/deserialization
 */
function queryResultToRows<T extends Record<string, unknown>>(result: QueryResult): T[] {
    if (!result.columns || !result.data || result.data.length === 0) {
        return [];
    }

    return result.data.map(row => {
        const obj: Record<string, unknown> = {};
        result.columns.forEach((col, index) => {
            obj[col.name] = row[index];
        });
        return obj as T;
    });
}

interface RawObjectRow {
    OBJNAME: string;
    OBJID: number;
    SCHEMA: string;
    DBNAME: string;
    OBJTYPE?: string;
    OWNER?: string;
    DESCRIPTION?: string;
    [key: string]: unknown;
}

interface RawColumnRow {
    TABLENAME: string;
    ATTNAME: string;
    FORMAT_TYPE: string;
    ATTNUM?: number;
    SCHEMA?: string;
    DBNAME?: string;
    [key: string]: unknown;
}

interface RawColumnRowWithPkFk extends RawColumnRow {
    IS_PK: number | string;
    IS_FK: number | string;
    DESCRIPTION?: string;
}

interface RawSchemaRow {
    SCHEMA: string;
    [key: string]: unknown;
}

interface RawDatabaseRow {
    DATABASE: string;
    [key: string]: unknown;
}

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
        runQueryFn: QueryRunnerRawFn
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

            // Use centralized query builder for columns with PK/FK info
            const query = NZ_QUERIES.listColumnsWithKeys(dbName, { schema: schemaName });

            try {
                const result = await runQueryFn(query);
                if (result) {
                    const results = queryResultToRows<RawColumnRowWithPkFk>(result);
                    const columnsByTable = new Map<string, ColumnMetadata[]>();
                    for (const row of results) {
                        const tableName = row.TABLENAME;
                        if (!columnsByTable.has(tableName)) {
                            columnsByTable.set(tableName, []);
                        }
                        columnsByTable.get(tableName)!.push({
                            ATTNAME: row.ATTNAME,
                            FORMAT_TYPE: row.FORMAT_TYPE,
                            label: row.ATTNAME,
                            kind: 5,
                            detail: row.FORMAT_TYPE,
                            documentation: row.DESCRIPTION || '',
                            isPk: Number(row.IS_PK) === 1,
                            isFk: Number(row.IS_FK) === 1
                        });
                    }

                    for (const [tableName, columns] of columnsByTable) {
                        const columnKey = `${dbName}.${schemaName || ''}.${tableName}`;
                        this.storage.setColumns(connectionName, columnKey, columns);
                    }
                }
            } catch (e: unknown) {
                console.error(`[CachePrefetcher] Error fetching columns:`, e);
            }
        } finally {
            this.columnPrefetchInProgress.delete(fullPrefetchKey);
        }
    }

    // ========== All Objects Prefetch ==========

    async prefetchAllObjects(connectionName: string, runQueryFn: QueryRunnerRawFn, skipIfCached = false, databases?: string[]): Promise<void> {
        const key = `ALL_OBJECTS|${connectionName}`;
        if (this.allObjectsPrefetchTriggeredSet.has(key)) {
            return;
        }
        this.allObjectsPrefetchTriggeredSet.add(key);

        console.log(`[CachePrefetcher] Starting background prefetch of all objects (Connection: ${connectionName})`);

        try {
            // Ensure we have a list of databases (required for listTablesAndViews to populate descriptions)
            let targetDatabases = databases;
            if (!targetDatabases || targetDatabases.length === 0) {
                targetDatabases = await this.prefetchDatabases(connectionName, runQueryFn);
            }

            if (!targetDatabases || targetDatabases.length === 0) {
                console.warn(`[CachePrefetcher] prefetchAllObjects aborted - no databases found for ${connectionName}`);
                return;
            }

            // Use centralized query for listing tables and views (global or per-database when provided)
            const tablesQuery = NZ_QUERIES.listTablesAndViews(targetDatabases);

            const result = await runQueryFn(tablesQuery);
            if (!result) return;

            const results = queryResultToRows<RawObjectRow>(result);
            const tablesByKey = new Map<string, { tables: TableMetadata[]; idMap: Map<string, number> }>();

            for (const row of results) {
                const key = row.SCHEMA ? `${row.DBNAME}.${row.SCHEMA}` : `${row.DBNAME}..`;
                if (!tablesByKey.has(key)) {
                    tablesByKey.set(key, { tables: [], idMap: new Map() });
                }
                const entry = tablesByKey.get(key)!;
                entry.tables.push({
                    OBJNAME: row.OBJNAME,
                    label: row.OBJNAME,
                    kind: row.OBJTYPE === 'VIEW' ? 18 : 6,
                    detail: row.SCHEMA ? (row.OBJTYPE === 'VIEW' ? 'View' : 'Table') : `${row.OBJTYPE === 'VIEW' ? 'View' : 'Table'} (${row.SCHEMA})`,
                    objType: row.OBJTYPE,
                    OBJID: row.OBJID,
                    SCHEMA: row.SCHEMA,
                    OWNER: row.OWNER,
                    DESCRIPTION: row.DESCRIPTION
                });

                const fullKey = row.SCHEMA
                    ? `${row.DBNAME}.${row.SCHEMA}.${row.OBJNAME}`
                    : `${row.DBNAME}..${row.OBJNAME}`;
                entry.idMap.set(fullKey, row.OBJID);
            }

            for (const [key, entry] of tablesByKey) {
                // Only skip if explicitly requested AND data already exists
                if (skipIfCached && this.storage.getTables(connectionName, key)) {
                    continue;
                }
                this.storage.setTables(connectionName, key, entry.tables, entry.idMap);
            }

            console.log(`[CachePrefetcher] Prefetched tables for ${tablesByKey.size} schema(s) on ${connectionName}`);
        } catch (e: unknown) {
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

    triggerConnectionPrefetch(connectionName: string, runQueryFn: QueryRunnerRawFn): void {
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

    private async executeConnectionPrefetch(connectionName: string, runQueryFn: QueryRunnerRawFn): Promise<void> {
        // 1. Fetch all databases
        const databases = await this.prefetchDatabases(connectionName, runQueryFn);
        if (!databases || databases.length === 0) {
            return;
        }

        // 2. For each database, fetch schemas (parallel)
        await Promise.all(
            databases.map(dbName => this.prefetchSchemasForDb(connectionName, dbName, runQueryFn))
        );

        // 3. Fetch all tables and views (reuse prefetchAllObjects with skipIfCached)
        await this.prefetchAllObjects(connectionName, runQueryFn, true, databases);

        // 4. Fetch columns in batches
        await this.prefetchAllColumnsForConnection(connectionName, runQueryFn);
    }

    private async prefetchDatabases(connectionName: string, runQueryFn: QueryRunnerRawFn): Promise<string[]> {
        if (this.storage.getDatabases(connectionName)) {
            const cached = this.storage.getDatabases(connectionName);
            return cached?.map((item) => extractLabel(item)).filter(Boolean) as string[] || [];
        }

        try {
            const query = NZ_QUERIES.LIST_DATABASES;
            const result = await runQueryFn(query);
            if (!result) return [];

            const results = queryResultToRows<RawDatabaseRow>(result);
            const items = results.map((row) => ({
                DATABASE: row.DATABASE,
                label: row.DATABASE,
                kind: 9,
                detail: 'Database'
            }));

            this.storage.setDatabases(connectionName, items);
            return results.map(row => row.DATABASE);
        } catch (e: unknown) {
            console.error('[CachePrefetcher] prefetchDatabases error:', e);
            return [];
        }
    }

    private async prefetchSchemasForDb(
        connectionName: string,
        dbName: string,
        runQueryFn: QueryRunnerRawFn
    ): Promise<void> {
        if (this.storage.getSchemas(connectionName, dbName)) {
            return;
        }

        try {
            const query = NZ_QUERIES.listSchemas(dbName);
            const result = await runQueryFn(query);
            if (!result) return;

            const results = queryResultToRows<RawSchemaRow>(result);
            const items = results
                .filter(row => row.SCHEMA != null && row.SCHEMA !== '')
                .map(row => ({
                    SCHEMA: row.SCHEMA,
                    label: row.SCHEMA,
                    kind: 19,
                    detail: `Schema in ${dbName}`,
                    insertText: row.SCHEMA,
                    sortText: row.SCHEMA,
                    filterText: row.SCHEMA
                }));

            this.storage.setSchemas(connectionName, dbName, items);
        } catch (e: unknown) {
            console.error(`[CachePrefetcher] prefetchSchemasForDb error for ${dbName}:`, e);
        }
    }



    private async prefetchAllColumnsForConnection(
        connectionName: string,
        runQueryFn: QueryRunnerRawFn
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

            let fetchedCount = 0;
            const prefetchStartTime = Date.now();

            const tablesByDb = new Map<string, typeof allTables>();
            for (const item of allTables) {
                if (!tablesByDb.has(item.db)) {
                    tablesByDb.set(item.db, []);
                }
                tablesByDb.get(item.db)!.push(item);
            }

            // Process all databases in parallel
            const dbPromises = Array.from(tablesByDb.entries()).map(async ([dbName, dbBatch]) => {
                // Use centralized query builder for columns with PK/FK info
                const query = NZ_QUERIES.listColumnsWithKeys(dbName);

                try {
                    const queryStartTime = Date.now();
                    const result = await runQueryFn(query);
                    const queryDuration = Date.now() - queryStartTime;

                    if (result) {
                        const parseStartTime = Date.now();
                        const results = queryResultToRows<RawColumnRowWithPkFk>(result);
                        const parseDuration = Date.now() - parseStartTime;

                        const columnsByKey = new Map<string, ColumnMetadata[]>();

                        for (const row of results) {
                            const key = `${row.DBNAME}.${row.SCHEMA || ''}.${row.TABLENAME}`;
                            if (!columnsByKey.has(key)) {
                                columnsByKey.set(key, []);
                            }
                            columnsByKey.get(key)!.push({
                                ATTNAME: row.ATTNAME,
                                FORMAT_TYPE: row.FORMAT_TYPE,
                                label: row.ATTNAME,
                                kind: 5,
                                detail: row.FORMAT_TYPE,
                                documentation: row.DESCRIPTION || '',
                                isPk: Number(row.IS_PK) === 1,
                                isFk: Number(row.IS_FK) === 1
                            });
                        }

                        for (const [key, columns] of columnsByKey) {
                            if (!this.storage.getColumns(connectionName, key)) {
                                this.storage.setColumns(connectionName, key, columns);
                                fetchedCount++;
                            }
                        }

                        console.log(`[CachePrefetcher] DB ${dbName}: ${dbBatch.length} tables (expected), ${results.length} columns (total fetched) | query=${queryDuration}ms, parse=${parseDuration}ms`);
                    }
                } catch (e: unknown) {
                    console.error(`[CachePrefetcher] Error fetching columns for DB ${dbName}:`, e);
                }
            });

            await Promise.all(dbPromises);

            const totalDuration = Date.now() - prefetchStartTime;
            console.log(`[CachePrefetcher] Prefetched columns for ${fetchedCount} tables/views in ${totalDuration}ms`);
        } catch (e: unknown) {
            console.error(`[CachePrefetcher] prefetchAllColumnsForConnection error:`, e);
        }
    }
}
