/**
 * Metadata Cache - Persistence Module
 * Handles saving and loading cache data to/from disk
 */

import * as vscode from 'vscode';
import { CacheStorage } from './cacheStorage';
import { CacheType, PerKeyEntry } from './types';
import { exportMap, exportTableIdMap } from './helpers';

/**
 * Handles persistence of cache data to disk
 */
export class CachePersistence {
    private readonly CACHE_TTL = 4 * 60 * 60 * 1000; // 4 hours in milliseconds
    private cacheFilePath: string | undefined;

    // Debounce support for saves
    private savePending: boolean = false;
    private saveTimeoutId: ReturnType<typeof setTimeout> | undefined;

    constructor(
        private context: vscode.ExtensionContext,
        private storage: CacheStorage
    ) {
        if (this.context.storageUri) {
            this.cacheFilePath = vscode.Uri.joinPath(this.context.storageUri, 'schema-cache.json').fsPath;
            this.ensureStorageDir();
        }
    }

    private async ensureStorageDir(): Promise<void> {
        if (this.context.storageUri) {
            try {
                await vscode.workspace.fs.createDirectory(this.context.storageUri);
            } catch {
                // ignore if exists
            }
        }
    }

    /**
     * Initialize cache by loading from disk and cleaning up legacy data
     */
    async initialize(): Promise<void> {
        // Cleanup legacy workspaceState to resolve "large state" warning
        try {
            const keys = [
                'sqlCompletion.dbCache',
                'sqlCompletion.schemaCache',
                'sqlCompletion.tableCache',
                'sqlCompletion.columnCache',
                'sqlCompletion.tableIdMap'
            ];
            for (const key of keys) {
                if (this.context.workspaceState.get(key) !== undefined) {
                    await this.context.workspaceState.update(key, undefined);
                }
            }
        } catch (e) {
            console.error('[CachePersistence] Error cleaning up legacy workspace state:', e);
        }

        if (!this.cacheFilePath) return;

        try {
            const fs = require('fs');
            if (fs.existsSync(this.cacheFilePath)) {
                const raw = await fs.promises.readFile(this.cacheFilePath, 'utf-8');
                const json = JSON.parse(raw);
                const now = Date.now();

                // Load dbCache
                if (json.dbCache) {
                    const dbCache = new Map<string, { data: any[]; timestamp: number }>();
                    for (const [key, entry] of Object.entries(
                        json.dbCache as Record<string, { data: any[]; timestamp: number }>
                    )) {
                        if (now - entry.timestamp < this.CACHE_TTL) {
                            dbCache.set(key, entry);
                        }
                    }
                    this.storage.loadDbCache(dbCache);
                }

                // Load schemaCache
                if (json.schemaCache) {
                    const schemaCache = new Map<string, PerKeyEntry<any[]>>();
                    for (const [key, entry] of Object.entries(
                        json.schemaCache as Record<string, { data: any[]; timestamp: number }>
                    )) {
                        if (now - entry.timestamp < this.CACHE_TTL) {
                            schemaCache.set(key, entry);
                        }
                    }
                    this.storage.loadSchemaCache(schemaCache);
                }

                // Load tableCache
                if (json.tableCache) {
                    const tableCache = new Map<string, PerKeyEntry<any[]>>();
                    for (const [key, entry] of Object.entries(
                        json.tableCache as Record<string, { data: any[]; timestamp: number }>
                    )) {
                        if (now - entry.timestamp < this.CACHE_TTL) {
                            tableCache.set(key, entry);
                        }
                    }
                    this.storage.loadTableCache(tableCache);
                }

                // Load tableIdMap
                if (json.tableIdMap) {
                    const tableIdMap = new Map<string, PerKeyEntry<Map<string, number>>>();
                    for (const [key, entry] of Object.entries(
                        json.tableIdMap as Record<string, { data: Record<string, number>; timestamp: number }>
                    )) {
                        const tableCache = this.storage.getTableCacheMap();
                        if (tableCache.has(key) || now - entry.timestamp < this.CACHE_TTL) {
                            tableIdMap.set(key, {
                                data: new Map(Object.entries(entry.data)),
                                timestamp: entry.timestamp
                            });
                        }
                    }
                    this.storage.loadTableIdMap(tableIdMap);
                }

                // columnCache is NOT loaded from disk - kept in memory only for performance
            }
        } catch (e) {
            console.error('[CachePersistence] Error loading cache from disk:', e);
        }
    }

    /**
     * Schedule a debounced save operation
     */
    scheduleSave(_cacheType: CacheType): void {
        if (!this.savePending) {
            this.savePending = true;
            this.saveTimeoutId = setTimeout(() => this.flushSave(), 2000); // 2s debounce
        }
    }

    /**
     * Immediately flush pending saves to disk
     */
    async flushSave(): Promise<void> {
        this.savePending = false;
        if (!this.cacheFilePath) return;

        try {
            const fs = require('fs');

            const data = {
                dbCache: exportMap(this.storage.getDbCacheMap()),
                schemaCache: exportMap(this.storage.getSchemaCacheMap()),
                tableCache: exportMap(this.storage.getTableCacheMap()),
                // columnCache is NOT persisted - kept in memory only for performance
                tableIdMap: exportTableIdMap(this.storage.getTableIdMapMap())
            };

            await fs.promises.writeFile(this.cacheFilePath, JSON.stringify(data), 'utf-8');
        } catch (e) {
            console.error('[CachePersistence] Error saving cache to disk:', e);
        }
    }

    /**
     * Clear all cache data from memory and disk
     */
    async clearCache(): Promise<void> {
        // Cancel pending saves
        if (this.saveTimeoutId) {
            clearTimeout(this.saveTimeoutId);
            this.saveTimeoutId = undefined;
        }
        this.savePending = false;

        this.storage.clearAll();

        if (this.cacheFilePath) {
            try {
                const fs = require('fs');
                if (fs.existsSync(this.cacheFilePath)) {
                    await fs.promises.unlink(this.cacheFilePath);
                }
            } catch (e) {
                console.error('[CachePersistence] Error clearing cache file:', e);
            }
        }
    }
}
