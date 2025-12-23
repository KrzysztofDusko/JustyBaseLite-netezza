/**
 * Metadata Cache - Main Facade
 * Provides unified API for metadata caching with modular implementation
 * 
 * This is a facade that delegates to specialized modules:
 * - CacheStorage: CRUD operations on cache data
 * - CachePersistence: Disk I/O operations
 * - CachePrefetcher: Background data fetching
 * - searchCache: Search functionality
 */

import * as vscode from 'vscode';
import { CacheStorage } from './metadata/cacheStorage';
import { CachePersistence } from './metadata/persistence';
import { CachePrefetcher, QueryRunnerFn } from './metadata/prefetch';
import { searchCache } from './metadata/search';
import { PerKeyEntry, CacheType, SearchResult, CachedObjectInfo, ObjectWithSchema } from './metadata/types';

// Re-export types for external use
export { PerKeyEntry, CacheType } from './metadata/types';

/**
 * Main metadata cache class providing unified API
 * Facade pattern - delegates to specialized modules
 */
export class MetadataCache {
    private storage: CacheStorage;
    private persistence: CachePersistence;
    private prefetcher: CachePrefetcher;

    constructor(context: vscode.ExtensionContext) {
        this.storage = new CacheStorage();
        this.persistence = new CachePersistence(context, this.storage);
        this.prefetcher = new CachePrefetcher(this.storage);

        // Wire up save scheduling
        this.storage.setOnDataChange((cacheType: CacheType) => {
            this.persistence.scheduleSave(cacheType);
        });
    }

    // ========== Initialization ==========

    async initialize(): Promise<void> {
        return this.persistence.initialize();
    }

    // ========== Save Management ==========

    scheduleSave(cacheType: CacheType): void {
        this.persistence.scheduleSave(cacheType);
    }

    async flushSave(): Promise<void> {
        return this.persistence.flushSave();
    }

    async clearCache(): Promise<void> {
        return this.persistence.clearCache();
    }

    // ========== Database Operations ==========

    getDatabases(connectionName: string): any[] | undefined {
        return this.storage.getDatabases(connectionName);
    }

    setDatabases(connectionName: string, data: any[]): void {
        this.storage.setDatabases(connectionName, data);
    }

    // ========== Schema Operations ==========

    getSchemas(connectionName: string, dbName: string): any[] | undefined {
        return this.storage.getSchemas(connectionName, dbName);
    }

    setSchemas(connectionName: string, dbName: string, data: any[]): void {
        this.storage.setSchemas(connectionName, dbName, data);
    }

    // ========== Table Operations ==========

    getTables(connectionName: string, key: string): any[] | undefined {
        return this.storage.getTables(connectionName, key);
    }

    getTablesAllSchemas(connectionName: string, dbName: string): any[] | undefined {
        return this.storage.getTablesAllSchemas(connectionName, dbName);
    }

    setTables(connectionName: string, key: string, data: any[], idMap: Map<string, number>): void {
        this.storage.setTables(connectionName, key, data, idMap);
    }

    getObjectsWithSchema(connectionName: string, dbName: string): ObjectWithSchema[] {
        return this.storage.getObjectsWithSchema(connectionName, dbName);
    }

    // ========== Column Operations ==========

    getColumns(connectionName: string, key: string): any[] | undefined {
        return this.storage.getColumns(connectionName, key);
    }

    setColumns(connectionName: string, key: string, data: any[]): void {
        this.storage.setColumns(connectionName, key, data);
    }

    // ========== ID Lookup ==========

    findTableId(connectionName: string, lookupKey: string): number | undefined {
        return this.storage.findTableId(connectionName, lookupKey);
    }

    // ========== TypeGroup Operations ==========

    getTypeGroups(connectionName: string, dbName: string): string[] | undefined {
        return this.storage.getTypeGroups(connectionName, dbName);
    }

    setTypeGroups(connectionName: string, dbName: string, types: string[]): void {
        this.storage.setTypeGroups(connectionName, dbName, types);
    }

    // ========== Object Lookup ==========

    findObjectWithType(
        connectionName: string,
        dbName: string,
        schemaName: string | undefined,
        objectName: string
    ): CachedObjectInfo | undefined {
        return this.storage.findObjectWithType(connectionName, dbName, schemaName, objectName);
    }

    // ========== Schema Invalidation ==========

    invalidateSchema(connectionName: string, dbName: string, schemaName?: string): void {
        this.storage.invalidateSchema(connectionName, dbName, schemaName);
    }

    // ========== Search ==========

    search(term: string, connectionName?: string): SearchResult[] {
        return searchCache(this.storage, term, connectionName);
    }

    // ========== Prefetch Operations ==========

    async prefetchColumnsForSchema(
        connectionName: string,
        dbName: string,
        schemaName: string | undefined,
        runQueryFn: QueryRunnerFn
    ): Promise<void> {
        return this.prefetcher.prefetchColumnsForSchema(connectionName, dbName, schemaName, runQueryFn);
    }

    async prefetchAllObjects(connectionName: string, runQueryFn: QueryRunnerFn): Promise<void> {
        return this.prefetcher.prefetchAllObjects(connectionName, runQueryFn);
    }

    hasAllObjectsPrefetchTriggered(connectionName: string): boolean {
        return this.prefetcher.hasAllObjectsPrefetchTriggered(connectionName);
    }

    hasConnectionPrefetchTriggered(connectionName: string): boolean {
        return this.prefetcher.hasConnectionPrefetchTriggered(connectionName);
    }

    triggerConnectionPrefetch(connectionName: string, runQueryFn: QueryRunnerFn): void {
        this.prefetcher.triggerConnectionPrefetch(connectionName, runQueryFn);
    }

    // ========== Legacy Compatibility ==========
    // Expose internal caches for backward compatibility with providers

    get schemaCache(): Map<string, PerKeyEntry<any[]>> {
        return this.storage.schemaCache;
    }

    get tableCache(): Map<string, PerKeyEntry<any[]>> {
        return this.storage.tableCache;
    }

    get columnCache(): Map<string, PerKeyEntry<any[]>> {
        return this.storage.columnCache;
    }

    get tableIdMap(): Map<string, PerKeyEntry<Map<string, number>>> {
        return this.storage.tableIdMap;
    }

    get typeGroupCache(): Map<string, PerKeyEntry<string[]>> {
        return this.storage.typeGroupCache;
    }
}
