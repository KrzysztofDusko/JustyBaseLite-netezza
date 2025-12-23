/**
 * Metadata Cache - Types
 */

/**
 * Per-key cache entry with individual timestamps
 */
export interface PerKeyEntry<T> {
    data: T;
    timestamp: number;
}

/**
 * Cache type identifiers for selective saving
 */
export type CacheType = 'db' | 'schema' | 'table' | 'column';

/**
 * Search result item
 */
export interface SearchResult {
    name: string;
    type: string;
    database?: string;
    schema?: string;
    parent?: string; // For columns - the parent table name
}

/**
 * Object with type information from cache
 */
export interface CachedObjectInfo {
    objId: number;
    objType: string;
    schema: string;
    name: string;
}

/**
 * Object with schema information
 */
export interface ObjectWithSchema {
    item: any;
    schema: string;
    objId?: number;
}
