import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';

export interface QueryHistoryEntry {
    id: string;
    host: string;
    database: string;
    schema: string;
    query: string;
    timestamp: number;
    connectionName?: string;
    is_favorite?: boolean;
    tags?: string;
    description?: string;
}

interface StorageData {
    entries: QueryHistoryEntry[];
    version: number;
}

export class QueryHistoryManager {
    private static readonly STORAGE_KEY = 'queryHistory';
    private static readonly ACTIVE_LIMIT = 1000; // Max items in memory/active file
    private static readonly BATCH_ARCHIVE_SIZE = 100; // How many items to move to archive at once when limit reached
    private static readonly STORAGE_VERSION = 1;

    private cache: QueryHistoryEntry[] = [];
    private initialized = false;

    private historyFilePath: string | undefined;
    private archiveFilePath: string | undefined;
    private writeQueue: Promise<void> = Promise.resolve();
    private archiveQueue: Promise<void> = Promise.resolve();

    constructor(private context: vscode.ExtensionContext) {
        if (this.context.globalStorageUri) {
            const storagePath = this.context.globalStorageUri.fsPath;
            this.historyFilePath = path.join(storagePath, 'query-history.json');
            this.archiveFilePath = path.join(storagePath, 'query-history-archive.json');

            // Ensure storage directory exists
            try {
                if (!fs.existsSync(storagePath)) {
                    fs.mkdirSync(storagePath, { recursive: true });
                }
            } catch (e) {
                console.error('[QueryHistoryManager] Error creating storage directory:', e);
            }
        }
        this.initialize();
    }

    private async initialize(): Promise<void> {
        if (this.initialized) return;

        try {
            // Priority 1: Load from File
            let loadedFromFile = false;
            if (this.historyFilePath && fs.existsSync(this.historyFilePath)) {
                try {
                    const raw = await fs.promises.readFile(this.historyFilePath, 'utf-8');
                    // Check for empty or whitespace-only content
                    if (!raw || !raw.trim()) {
                        console.log('[QueryHistoryManager] File is empty, starting fresh');
                        this.cache = [];
                        loadedFromFile = true;
                    } else {
                        const stored = JSON.parse(raw);
                        if (stored && stored.entries) {
                            this.cache = stored.entries;
                            loadedFromFile = true;
                        }
                    }
                } catch (e) {
                    console.warn('[QueryHistoryManager] Corrupted history file, resetting:', e);
                    this.cache = [];
                    loadedFromFile = true;
                    try {
                        await this.saveToStorage();
                        console.log('[QueryHistoryManager] Reset corrupted file to empty state');
                    } catch (saveErr) {
                        console.error('[QueryHistoryManager] Failed to reset file:', saveErr);
                    }
                }
            }

            // Priority 2: Migration from globalState (Legacy)
            if (!loadedFromFile) {
                const stored = this.context.globalState.get<StorageData>(QueryHistoryManager.STORAGE_KEY);
                if (stored && stored.entries) {
                    this.cache = stored.entries;
                    console.log(`✅ Migrated ${this.cache.length} entries from globalState`);
                    await this.saveToStorage();
                    await this.context.globalState.update(QueryHistoryManager.STORAGE_KEY, undefined);
                }
            }

            // Check if we need to migrate excess active entries to archive (Initial Cleanup)
            if (this.cache.length > QueryHistoryManager.ACTIVE_LIMIT + QueryHistoryManager.BATCH_ARCHIVE_SIZE) {
                console.log(`[QueryHistoryManager] Cache (${this.cache.length}) exceeds active limit. Migrating to archive...`);
                await this.flushToArchive();
            }

            this.initialized = true;
        } catch (error) {
            console.error('❌ Error initializing query history:', error);
            this.cache = [];
            this.initialized = true;
        }
    }

    private async saveToStorage(): Promise<void> {
        if (!this.historyFilePath) return;

        const task = async () => {
            try {
                const data: StorageData = {
                    entries: this.cache,
                    version: QueryHistoryManager.STORAGE_VERSION
                };
                await fs.promises.writeFile(this.historyFilePath!, JSON.stringify(data), 'utf-8');
            } catch (error) {
                console.error('Error saving to storage:', error);
            }
        };

        this.writeQueue = this.writeQueue.then(task, task);
        return this.writeQueue;
    }

    private async flushToArchive(): Promise<void> {
        if (!this.archiveFilePath) return;

        const task = async () => {
            try {
                // Calculate how many to archive
                // specific logic: keep strictly ACTIVE_LIMIT, move rest
                // OR: keep ACTIVE_LIMIT, move whatever is older.

                // If we are here, we probably have too many items.
                const excessCount = this.cache.length - QueryHistoryManager.ACTIVE_LIMIT;
                if (excessCount <= 0) return;

                // Identify items to move (oldest are at the end)
                // cache is [Newest, ..., Oldest]
                // slice(ACTIVE_LIMIT) gives us the tail (Oldest items)
                const itemsToArchive = this.cache.slice(QueryHistoryManager.ACTIVE_LIMIT);
                this.cache = this.cache.slice(0, QueryHistoryManager.ACTIVE_LIMIT);

                // Persist Active immediately
                await this.saveToStorage();

                // Append to Archive File
                let existingArchive: QueryHistoryEntry[] = [];
                if (fs.existsSync(this.archiveFilePath!)) {
                    try {
                        const raw = await fs.promises.readFile(this.archiveFilePath!, 'utf-8');
                        if (raw.trim()) {
                            const data = JSON.parse(raw);
                            existingArchive = Array.isArray(data) ? data : (data.entries || []);
                        }
                    } catch (e) {
                        console.warn('Error reading archive file, starting fresh archive append:', e);
                    }
                }

                // Add new archive items to the BEGINNING of archive (if we consider archive as [NewerArchived -> OlderArchived])
                // OR just add to the list. 
                // The cache had [ActiveNew...ActiveOld]
                // itemsToArchive has [ActiveOld+1 ... ActiveOld+N] (sorted desc by time)
                // Existing Archive hopefully is [ArchivedRecent ... ArchivedAncient]
                // So we should put itemsToArchive at the START of existingArchive to maintain DESC order.
                const newArchive = [...itemsToArchive, ...existingArchive];

                // Write Archive
                await fs.promises.writeFile(this.archiveFilePath!, JSON.stringify({ entries: newArchive }), 'utf-8');
                console.log(`[QueryHistoryManager] Archived ${itemsToArchive.length} items. Active: ${this.cache.length}, Archive: ${newArchive.length}`);

            } catch (error) {
                console.error('Error flushing to archive:', error);
            }
        };

        this.archiveQueue = this.archiveQueue.then(task, task);
        return this.archiveQueue;
    }

    async addEntry(
        host: string,
        database: string,
        schema: string,
        query: string,
        connectionName?: string,
        tags?: string,
        description?: string
    ): Promise<void> {
        try {
            if (!this.initialized) {
                await this.initialize();
            }

            const id = `${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
            const timestamp = Date.now();

            const newEntry: QueryHistoryEntry = {
                id,
                host,
                database,
                schema,
                query: query.trim(),
                timestamp,
                connectionName,
                is_favorite: false,
                tags: tags || '',
                description: description || ''
            };

            // Add to beginning (newest first)
            this.cache.unshift(newEntry);

            // Check limit and flush if significantly over limit (batch effect)
            if (this.cache.length >= QueryHistoryManager.ACTIVE_LIMIT + QueryHistoryManager.BATCH_ARCHIVE_SIZE) {
                await this.flushToArchive();
            } else {
                await this.saveToStorage();
            }
        } catch (error) {
            console.error('Error adding query to history:', error);
        }
    }

    /**
     * Get active history, optionally with pagination support.
     * Note: offset/limit apply to the memory CACHE (Active) only.
     * To search deeper, use searchArchive or implementation that merges both.
     * For now, standard view just scrolls active.
     */
    async getHistory(limit?: number, offset: number = 0): Promise<QueryHistoryEntry[]> {
        if (!this.initialized) {
            await this.initialize();
        }

        if (limit !== undefined) {
            return this.cache.slice(offset, offset + limit);
        }
        return [...this.cache];
    }

    async deleteEntry(id: string): Promise<void> {
        try {
            if (!this.initialized) await this.initialize();

            const initialLen = this.cache.length;
            this.cache = this.cache.filter(entry => entry.id !== id);

            if (this.cache.length !== initialLen) {
                await this.saveToStorage();
            } else {
                // If not in cache, might be in archive? 
                // Currently DELETE only supports active items for simplicity,
                // or we need to scan archive. Implementing archive deletion is expensive (read/write all).
                // Let's defer archive deletion unless critical.
            }
        } catch (error) {
            console.error('Error deleting entry:', error);
        }
    }

    async clearHistory(): Promise<void> {
        try {
            if (!this.initialized) await this.initialize();

            // Clear Active
            this.cache = [];
            await this.saveToStorage();

            // Clear Archive
            if (this.archiveFilePath && fs.existsSync(this.archiveFilePath)) {
                await fs.promises.unlink(this.archiveFilePath);
            }

            console.log('All query history cleared');
        } catch (error) {
            console.error('Error clearing history:', error);
        }
    }

    async getStats(): Promise<{
        activeEntries: number;
        archivedEntries: number;
        totalEntries: number;
        activeFileSizeMB: number;
        archiveFileSizeMB: number;
        totalFileSizeMB: number;
    }> {
        try {
            if (!this.initialized) await this.initialize();

            const activeEntries = this.cache.length;

            // Calc active size
            const activeJson = JSON.stringify(this.cache);
            const activeSizeMB = parseFloat((activeJson.length / (1024 * 1024)).toFixed(2));

            // Calc archive stats
            let archivedEntries = 0;
            let archiveSizeMB = 0;

            if (this.archiveFilePath && fs.existsSync(this.archiveFilePath)) {
                const stats = await fs.promises.stat(this.archiveFilePath);
                archiveSizeMB = parseFloat((stats.size / (1024 * 1024)).toFixed(2));

                // We avoid reading the whole file just for count if it's huge, 
                // but for now we might have to read it once or cache the count?
                // Reading huge file to count items is slow.
                // ESTIMATION based on file size? Or read it?
                // Let's try to read it safely.
                try {
                    // Optimisation: Don't read content if file > 50MB
                    if (stats.size < 50 * 1024 * 1024) {
                        const raw = await fs.promises.readFile(this.archiveFilePath, 'utf-8');
                        const data = JSON.parse(raw);
                        if (data && data.entries) archivedEntries = data.entries.length;
                        else if (Array.isArray(data)) archivedEntries = data.length;
                    } else {
                        archivedEntries = -1; // Unknown/Many
                    }
                } catch (e) {
                    console.debug('[QueryHistoryManager] Error reading archive for stats:', e);
                }
            }

            return {
                activeEntries,
                archivedEntries: archivedEntries === -1 ? 99999 : archivedEntries,
                totalEntries: activeEntries + (archivedEntries === -1 ? 0 : archivedEntries),
                activeFileSizeMB: activeSizeMB,
                archiveFileSizeMB: archiveSizeMB,
                totalFileSizeMB: parseFloat((activeSizeMB + archiveSizeMB).toFixed(2))
            };

        } catch (error) {
            console.error('Error getting stats:', error);
            return {
                activeEntries: 0,
                archivedEntries: 0,
                totalEntries: 0,
                activeFileSizeMB: 0,
                archiveFileSizeMB: 0,
                totalFileSizeMB: 0
            };
        }
    }

    async toggleFavorite(id: string): Promise<void> {
        try {
            if (!this.initialized) await this.initialize();
            const entry = this.cache.find(e => e.id === id);
            if (entry) {
                entry.is_favorite = !entry.is_favorite;
                await this.saveToStorage();
            }
        } catch (error) {
            console.error('Error toggling favorite:', error);
        }
    }

    async updateEntry(id: string, tags?: string, description?: string): Promise<void> {
        try {
            if (!this.initialized) await this.initialize();
            const entry = this.cache.find(e => e.id === id);
            if (entry) {
                if (tags !== undefined) entry.tags = tags;
                if (description !== undefined) entry.description = description;
                await this.saveToStorage();
            }
        } catch (error) {
            console.error('Error updating entry:', error);
        }
    }

    async getFavorites(): Promise<QueryHistoryEntry[]> {
        if (!this.initialized) await this.initialize();
        return this.cache.filter(entry => entry.is_favorite);
    }

    async getByTag(tag: string): Promise<QueryHistoryEntry[]> {
        if (!this.initialized) await this.initialize();
        return this.cache.filter(entry => entry.tags?.toLowerCase().includes(tag.toLowerCase()));
    }

    async getAllTags(): Promise<string[]> {
        if (!this.initialized) await this.initialize();

        const allTags = new Set<string>();
        this.cache.forEach(entry => {
            if (entry.tags) {
                const tags = entry.tags.split(',');
                tags.forEach(tag => {
                    const cleanTag = tag.trim();
                    if (cleanTag) allTags.add(cleanTag);
                });
            }
        });

        return Array.from(allTags).sort();
    }

    async searchAll(searchTerm: string): Promise<QueryHistoryEntry[]> {
        if (!this.initialized) await this.initialize();
        const term = searchTerm.toLowerCase();

        // Search Active
        const activeMatches = this.cache.filter(
            entry =>
                entry.query.toLowerCase().includes(term) ||
                entry.host.toLowerCase().includes(term) ||
                (entry.database && entry.database.toLowerCase().includes(term)) ||
                (entry.schema && entry.schema.toLowerCase().includes(term)) ||
                entry.tags?.toLowerCase().includes(term) ||
                entry.description?.toLowerCase().includes(term)
        );

        return activeMatches;
    }

    /**
     * Dedicated method to search the archive.
     * This is an expensive operation as it reads the archive file.
     */
    async searchArchive(searchTerm: string, limit: number = 50): Promise<QueryHistoryEntry[]> {
        if (!this.archiveFilePath || !fs.existsSync(this.archiveFilePath)) return [];

        try {
            // Read file stream or full file?
            // For simple JSON without streaming parser, we have to read full file.
            // If file is huge, this crashes.
            // Ideally we should use a stream parser (like JSONStream) but we don't have it in deps.
            // We'll read standard way but with memory awareness?
            // Or maybe we just read it. If it's 50MB it fits in Node memory (limit is usually >1GB).
            // 50k entries ~ 50MB. Read File is fine.

            const raw = await fs.promises.readFile(this.archiveFilePath, 'utf-8');
            if (!raw) return [];

            const data = JSON.parse(raw);
            const entries: QueryHistoryEntry[] = Array.isArray(data) ? data : (data.entries || []);

            const term = searchTerm.toLowerCase();
            const matches = [];

            for (const entry of entries) {
                if (matches.length >= limit) break;

                if (
                    entry.query.toLowerCase().includes(term) ||
                    entry.host.toLowerCase().includes(term) ||
                    (entry.database && entry.database.toLowerCase().includes(term))
                ) {
                    matches.push(entry);
                }
            }
            return matches;
        } catch (e) {
            console.error('Error searching archive:', e);
            return [];
        }
    }

    async getFilteredHistory(
        host?: string,
        database?: string,
        schema?: string,
        limit?: number
    ): Promise<QueryHistoryEntry[]> {
        if (!this.initialized) await this.initialize();

        let filtered = this.cache.filter(entry => {
            if (host && entry.host !== host) return false;
            if (database && entry.database !== database) return false;
            if (schema && entry.schema !== schema) return false;
            return true;
        });

        if (limit) {
            filtered = filtered.slice(0, limit);
        }

        return filtered;
    }

    async getArchivedHistory(): Promise<QueryHistoryEntry[]> {
        // Return top 100 archived?
        // Not meant to be used to "Show All Archive"
        return [];
    }

    async clearArchive(): Promise<void> {
        if (this.archiveFilePath && fs.existsSync(this.archiveFilePath)) {
            await fs.promises.unlink(this.archiveFilePath);
        }
    }

    close(): void {
        console.log('Query history manager closed');
    }
}
