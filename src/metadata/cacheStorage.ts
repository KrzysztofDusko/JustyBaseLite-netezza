/**
 * Metadata Cache - Storage Module
 * Manages in-memory cache data structures and CRUD operations
 */

import { PerKeyEntry, CacheType, CachedObjectInfo, ObjectWithSchema } from './types';
import { extractLabel, buildIdLookupKey } from './helpers';

/**
 * Manages all in-memory cache data structures for metadata
 */
export class CacheStorage {
    // In-memory caches with per-key timestamps
    private dbCache: Map<string, { data: any[]; timestamp: number }> = new Map();
    public schemaCache: Map<string, PerKeyEntry<any[]>> = new Map();
    public tableCache: Map<string, PerKeyEntry<any[]>> = new Map(); // Key: "CONN|DB.SCHEMA" or "CONN|DB.."
    public columnCache: Map<string, PerKeyEntry<any[]>> = new Map(); // Key: "CONN|DB.SCHEMA.TABLE"
    public tableIdMap: Map<string, PerKeyEntry<Map<string, number>>> = new Map(); // Key: tableCache key -> {tableName -> OBJID}
    public typeGroupCache: Map<string, PerKeyEntry<string[]>> = new Map(); // Key: "CONN|DB" -> ['TABLE', 'VIEW', ...]

    // Callback for scheduling saves
    private onDataChange?: (cacheType: CacheType) => void;

    /**
     * Set callback to be called when data changes (for persistence)
     */
    setOnDataChange(callback: (cacheType: CacheType) => void): void {
        this.onDataChange = callback;
    }

    /**
     * Clear all caches
     */
    clearAll(): void {
        this.dbCache.clear();
        this.schemaCache.clear();
        this.tableCache.clear();
        this.columnCache.clear();
        this.tableIdMap.clear();
        this.typeGroupCache.clear();
    }

    // ========== Database Cache ==========

    getDatabases(connectionName: string): any[] | undefined {
        return this.dbCache.get(connectionName)?.data;
    }

    setDatabases(connectionName: string, data: any[]): void {
        this.dbCache.set(connectionName, { data, timestamp: Date.now() });
        this.onDataChange?.('db');
    }

    // ========== Schema Cache ==========

    getSchemas(connectionName: string, dbName: string): any[] | undefined {
        const key = `${connectionName}|${dbName}`;
        return this.schemaCache.get(key)?.data;
    }

    setSchemas(connectionName: string, dbName: string, data: any[]): void {
        const key = `${connectionName}|${dbName}`;
        this.schemaCache.set(key, { data, timestamp: Date.now() });
        this.onDataChange?.('schema');
    }

    // ========== Table Cache ==========

    getTables(connectionName: string, key: string): any[] | undefined {
        // incoming key is DB.SCHEMA or DB..
        const fullKey = `${connectionName}|${key}`;
        return this.tableCache.get(fullKey)?.data;
    }

    /**
     * Get tables from all schemas for a given database.
     * Used for double-dot pattern (DB..) where schema is not specified.
     */
    getTablesAllSchemas(connectionName: string, dbName: string): any[] | undefined {
        const prefix = `${connectionName}|${dbName}.`;
        const allTables: any[] = [];
        const seenNames = new Set<string>();

        for (const [key, entry] of this.tableCache) {
            if (key.startsWith(prefix)) {
                for (const item of entry.data) {
                    const name = extractLabel(item);
                    if (name && !seenNames.has(name.toUpperCase())) {
                        seenNames.add(name.toUpperCase());
                        allTables.push(item);
                    }
                }
            }
        }

        return allTables.length > 0 ? allTables : undefined;
    }

    setTables(connectionName: string, key: string, data: any[], idMap: Map<string, number>): void {
        const now = Date.now();
        const fullKey = `${connectionName}|${key}`;
        this.tableCache.set(fullKey, { data, timestamp: now });
        this.tableIdMap.set(fullKey, { data: idMap, timestamp: now });
        this.onDataChange?.('table');
    }

    /**
     * Get objects with schema information from all cached schemas for a database
     */
    getObjectsWithSchema(connectionName: string, dbName: string): ObjectWithSchema[] {
        const prefix = `${connectionName}|${dbName}.`;
        const results: ObjectWithSchema[] = [];
        const seenKeys = new Set<string>();

        for (const [key, entry] of this.tableCache) {
            if (key.startsWith(prefix) || key === `${connectionName}|${dbName}..`) {
                const parts = key.split('|');
                if (parts.length < 2) continue;

                const dbKey = parts[1];
                const dbParts = dbKey.split('.');
                const entrySchemaName = (dbParts.length > 1 ? dbParts[1] : '') || '';
                const idMapEntry = this.tableIdMap.get(key);

                for (const item of entry.data) {
                    const label = extractLabel(item);
                    const uniqueKey = `${entrySchemaName}.${label}`;

                    if (label && !seenKeys.has(uniqueKey)) {
                        seenKeys.add(uniqueKey);

                        let objId: number | undefined;
                        if (idMapEntry) {
                            const lookupKey = buildIdLookupKey(dbName, entrySchemaName || undefined, label);
                            objId = idMapEntry.data.get(lookupKey);
                        }

                        results.push({ item, schema: entrySchemaName, objId });
                    }
                }
            }
        }
        return results;
    }

    // ========== Column Cache ==========

    getColumns(connectionName: string, key: string): any[] | undefined {
        const fullKey = `${connectionName}|${key}`;
        return this.columnCache.get(fullKey)?.data;
    }

    setColumns(connectionName: string, key: string, data: any[]): void {
        const fullKey = `${connectionName}|${key}`;
        this.columnCache.set(fullKey, { data, timestamp: Date.now() });
        this.onDataChange?.('column');
    }

    // ========== Table ID Lookup ==========

    findTableId(connectionName: string, lookupKey: string): number | undefined {
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

    // ========== TypeGroup Cache ==========

    getTypeGroups(connectionName: string, dbName: string): string[] | undefined {
        const key = `${connectionName}|${dbName}`;
        return this.typeGroupCache.get(key)?.data;
    }

    setTypeGroups(connectionName: string, dbName: string, types: string[]): void {
        const key = `${connectionName}|${dbName}`;
        this.typeGroupCache.set(key, { data: types, timestamp: Date.now() });
        // No need to persist typeGroups - they are quick to fetch
    }

    // ========== Object Lookup with Type ==========

    /**
     * Find object in cache with type information.
     * Returns objId, objType and schema if found, undefined otherwise.
     */
    findObjectWithType(
        connectionName: string,
        dbName: string,
        schemaName: string | undefined,
        objectName: string
    ): CachedObjectInfo | undefined {
        const prefix = `${connectionName}|`;
        const upperName = objectName.toUpperCase();

        for (const [key, entry] of this.tableCache) {
            if (!key.startsWith(prefix)) continue;

            const parts = key.split('|');
            if (parts.length < 2) continue;

            const dbKey = parts[1];
            const dbParts = dbKey.split('.');
            const entryDbName = dbParts[0];
            const entrySchemaName = dbParts.length > 1 ? dbParts[1] : undefined;

            // Match database
            if (entryDbName.toUpperCase() !== dbName.toUpperCase()) continue;

            // Match schema if provided
            if (
                schemaName !== undefined &&
                schemaName !== '' &&
                entrySchemaName?.toUpperCase() !== schemaName.toUpperCase()
            )
                continue;

            // Search for object in this cache entry
            for (const item of entry.data) {
                const itemName = extractLabel(item);
                if (itemName?.toUpperCase() === upperName) {
                    const objType = item.objType || (item.kind === 18 ? 'VIEW' : 'TABLE');
                    const lookupKey = buildIdLookupKey(entryDbName, entrySchemaName, itemName);
                    const idEntry = this.tableIdMap.get(key);
                    const objId = idEntry?.data.get(lookupKey);

                    if (objId !== undefined) {
                        return { objId, objType, schema: entrySchemaName || '', name: itemName || objectName };
                    }
                }
            }
        }

        return undefined;
    }

    // ========== Schema Invalidation ==========

    /**
     * Invalidate cache for a specific schema
     */
    invalidateSchema(connectionName: string, dbName: string, schemaName?: string): void {
        const targetSuffix = schemaName ? `${dbName}.${schemaName}` : `${dbName}..`;
        const fullKey = `${connectionName}|${targetSuffix}`;

        if (this.tableCache.has(fullKey)) {
            this.tableCache.delete(fullKey);
            this.tableIdMap.delete(fullKey);
            this.onDataChange?.('table');
            console.log(`[CacheStorage] Invalidated cache for ${fullKey}`);
        }
    }

    // ========== Serialization Helpers (for persistence) ==========

    getDbCacheMap(): Map<string, { data: any[]; timestamp: number }> {
        return this.dbCache;
    }

    getSchemaCacheMap(): Map<string, PerKeyEntry<any[]>> {
        return this.schemaCache;
    }

    getTableCacheMap(): Map<string, PerKeyEntry<any[]>> {
        return this.tableCache;
    }

    getTableIdMapMap(): Map<string, PerKeyEntry<Map<string, number>>> {
        return this.tableIdMap;
    }

    // ========== Deserialization (for loading from disk) ==========

    loadDbCache(entries: Map<string, { data: any[]; timestamp: number }>): void {
        this.dbCache = entries;
    }

    loadSchemaCache(entries: Map<string, PerKeyEntry<any[]>>): void {
        this.schemaCache = entries;
    }

    loadTableCache(entries: Map<string, PerKeyEntry<any[]>>): void {
        this.tableCache = entries;
    }

    loadTableIdMap(entries: Map<string, PerKeyEntry<Map<string, number>>>): void {
        this.tableIdMap = entries;
    }
}
