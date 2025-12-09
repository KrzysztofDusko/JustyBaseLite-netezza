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
    private static readonly MAX_ENTRIES = 50000;
    private static readonly CLEANUP_KEEP = 40000;
    private static readonly STORAGE_VERSION = 1;

    private cache: QueryHistoryEntry[] = [];
    private initialized = false;

    constructor(private context: vscode.ExtensionContext) {
        this.initialize();
    }

    private async initialize(): Promise<void> {
        try {
            // Try to load from VS Code storage
            const stored = this.context.globalState.get<StorageData>(QueryHistoryManager.STORAGE_KEY);

            if (stored && stored.entries) {
                this.cache = stored.entries;
                console.log(`✅ Loaded ${this.cache.length} entries from VS Code storage`);
            } else {
                // Migrate from SQLite/JSON if needed
                await this.migrateFromLegacyStorage();
            }

            this.initialized = true;
        } catch (error) {
            console.error('❌ Error initializing query history:', error);
            this.cache = [];
            this.initialized = true;
        }
    }

    private async migrateFromLegacyStorage(): Promise<void> {
        try {
            const globalStoragePath = this.context.globalStorageUri.fsPath;

            // Check for SQLite database
            const dbPath = path.join(globalStoragePath, 'query-history.db');
            if (fs.existsSync(dbPath)) {
                console.log('⚠️ SQLite database found but migration not implemented');
                console.log('💡 Consider manually exporting data before switching');
            }

            // Check for JSON files
            const jsonPath = path.join(globalStoragePath, 'query-history.json');
            if (fs.existsSync(jsonPath)) {
                const content = fs.readFileSync(jsonPath, 'utf8');
                if (content.trim()) {
                    const entries: QueryHistoryEntry[] = JSON.parse(content);
                    this.cache = entries;
                    await this.saveToStorage();
                    console.log(`✅ Migrated ${entries.length} entries from JSON`);
                }
            }

            const archivePath = path.join(globalStoragePath, 'query-history-archive.json');
            if (fs.existsSync(archivePath)) {
                const content = fs.readFileSync(archivePath, 'utf8');
                if (content.trim()) {
                    const entries: QueryHistoryEntry[] = JSON.parse(content);
                    this.cache.push(...entries);
                    await this.saveToStorage();
                    console.log(`✅ Migrated archive with ${entries.length} entries`);
                }
            }
        } catch (error) {
            console.error('Error during migration:', error);
        }
    }

    private async saveToStorage(): Promise<void> {
        try {
            const data: StorageData = {
                entries: this.cache,
                version: QueryHistoryManager.STORAGE_VERSION
            };
            await this.context.globalState.update(QueryHistoryManager.STORAGE_KEY, data);
        } catch (error) {
            console.error('Error saving to storage:', error);
        }
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

            // Cleanup if needed
            if (this.cache.length > QueryHistoryManager.MAX_ENTRIES) {
                this.cache = this.cache.slice(0, QueryHistoryManager.CLEANUP_KEEP);
                console.log(`Cleaned up old entries, keeping ${QueryHistoryManager.CLEANUP_KEEP} newest`);
            }

            await this.saveToStorage();
        } catch (error) {
            console.error('Error adding query to history:', error);
        }
    }

    async getHistory(): Promise<QueryHistoryEntry[]> {
        if (!this.initialized) {
            await this.initialize();
        }
        return [...this.cache]; // Return copy
    }

    async deleteEntry(id: string): Promise<void> {
        try {
            this.cache = this.cache.filter(entry => entry.id !== id);
            await this.saveToStorage();
        } catch (error) {
            console.error('Error deleting entry:', error);
        }
    }

    async clearHistory(): Promise<void> {
        try {
            this.cache = [];
            await this.saveToStorage();
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
            const totalEntries = this.cache.length;

            // Estimate storage size (rough calculation)
            const jsonSize = JSON.stringify(this.cache).length;
            const sizeMB = parseFloat((jsonSize / (1024 * 1024)).toFixed(2));

            return {
                activeEntries: totalEntries,
                archivedEntries: 0,
                totalEntries: totalEntries,
                activeFileSizeMB: sizeMB,
                archiveFileSizeMB: 0,
                totalFileSizeMB: sizeMB
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
        if (!this.initialized) {
            await this.initialize();
        }
        return this.cache.filter(entry => entry.is_favorite);
    }

    async getByTag(tag: string): Promise<QueryHistoryEntry[]> {
        if (!this.initialized) {
            await this.initialize();
        }
        return this.cache.filter(entry =>
            entry.tags?.toLowerCase().includes(tag.toLowerCase())
        );
    }

    async getAllTags(): Promise<string[]> {
        if (!this.initialized) {
            await this.initialize();
        }

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
        if (!this.initialized) {
            await this.initialize();
        }

        const term = searchTerm.toLowerCase();
        return this.cache.filter(entry =>
            entry.query.toLowerCase().includes(term) ||
            entry.host.toLowerCase().includes(term) ||
            entry.database.toLowerCase().includes(term) ||
            entry.schema.toLowerCase().includes(term) ||
            entry.tags?.toLowerCase().includes(term) ||
            entry.description?.toLowerCase().includes(term)
        );
    }

    async getFilteredHistory(
        host?: string,
        database?: string,
        schema?: string,
        limit?: number
    ): Promise<QueryHistoryEntry[]> {
        if (!this.initialized) {
            await this.initialize();
        }

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
        return []; // No longer applicable
    }

    async clearArchive(): Promise<void> {
        // No longer applicable
    }

    close(): void {
        // No cleanup needed with VS Code storage
        console.log('Query history manager closed');
    }
}