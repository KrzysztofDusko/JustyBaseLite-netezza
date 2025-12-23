/**
 * Metadata Cache - Search Module
 * Search functionality across cached metadata
 */

import { CacheStorage } from './cacheStorage';
import { SearchResult } from './types';
import { extractLabel, matchesConnection } from './helpers';

/**
 * Search through cached metadata for objects matching a term
 */
export function searchCache(
    storage: CacheStorage,
    term: string,
    connectionName?: string
): SearchResult[] {
    const results: SearchResult[] = [];
    const lowerTerm = term.toLowerCase();

    // Search Tables (in tableCache)
    for (const [key, entry] of storage.tableCache) {
        if (!matchesConnection(key, connectionName)) continue;

        // Key format: "CONN|DBNAME.SCHEMA" or "CONN|DBNAME.."
        const parts = key.split('|');
        if (parts.length < 2) continue;

        const dbKey = parts[1];
        const dbParts = dbKey.split('.');
        const dbName = dbParts[0];
        const schemaName = dbParts.length > 1 ? dbParts[1] : undefined;

        for (const item of entry.data) {
            const name = extractLabel(item);

            if (name && name.toLowerCase().includes(lowerTerm)) {
                // Get objType from item if available, otherwise infer from kind
                const objType = item.objType || (item.kind === 18 ? 'VIEW' : 'TABLE');
                results.push({
                    name: name,
                    type: objType,
                    database: dbName,
                    schema:
                        schemaName ||
                        (item.detail && item.detail.includes('(') ? item.detail.match(/\((.*?)\)/)?.[1] : undefined)
                });
            }
        }
    }

    // Search Columns (in columnCache)
    for (const [key, entry] of storage.columnCache) {
        if (!matchesConnection(key, connectionName)) continue;

        // Key: CONN|DB.SCHEMA.TABLE
        const parts = key.split('|');
        if (parts.length < 2) continue;

        const dbKey = parts[1];
        const dbParts = dbKey.split('.');
        const dbName = dbParts[0];
        const schemaName = dbParts[1];
        const tableName = dbParts[2];

        for (const item of entry.data) {
            const name = extractLabel(item);
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
