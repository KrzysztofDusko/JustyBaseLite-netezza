/**
 * Metadata Cache - Helper Functions
 * Extracted utility functions to reduce MetadataCache class complexity
 */

/**
 * Parse cache key to extract connection name and DB/Schema parts
 * Key formats: "CONN|DBNAME.SCHEMA" or "CONN|DBNAME.."
 */
export function parseCacheKey(key: string): {
    connectionName: string;
    dbName: string;
    schemaName: string | undefined;
} | null {
    const parts = key.split('|');
    if (parts.length < 2) return null;

    const connectionName = parts[0];
    const dbKey = parts[1];
    const dbParts = dbKey.split('.');
    const dbName = dbParts[0];
    const schemaName = dbParts.length > 1 && dbParts[1] !== '' ? dbParts[1] : undefined;

    return { connectionName, dbName, schemaName };
}

/**
 * Build a full cache key from components
 */
export function buildCacheKey(connectionName: string, dbName: string, schemaName?: string): string {
    const dbKey = schemaName ? `${dbName}.${schemaName}` : `${dbName}..`;
    return `${connectionName}|${dbKey}`;
}

/**
 * Check if a cache key belongs to a specific connection
 */
export function matchesConnection(key: string, connectionName: string | undefined): boolean {
    if (!connectionName) return true;
    return key.startsWith(`${connectionName}|`);
}

/**
 * Extract label text from cache item (handles both string and object labels)
 */
export function extractLabel(item: any): string | undefined {
    if (!item) return undefined;
    return typeof item.label === 'string' ? item.label : item.label?.label;
}

/**
 * Serialize a Map to a plain object for JSON storage
 */
export function exportMap<T>(map: Map<string, T>): Record<string, T> {
    const obj: Record<string, T> = {};
    map.forEach((v, k) => {
        obj[k] = v;
    });
    return obj;
}

/**
 * Serialize tableIdMap (Map of Maps) for JSON storage
 */
export function exportTableIdMap(
    map: Map<string, { data: Map<string, number>; timestamp: number }>
): Record<string, { data: Record<string, number>; timestamp: number }> {
    const obj: Record<string, { data: Record<string, number>; timestamp: number }> = {};
    map.forEach((entry, key) => {
        const dataObj: Record<string, number> = {};
        entry.data.forEach((v, k) => {
            dataObj[k] = v;
        });
        obj[key] = { data: dataObj, timestamp: entry.timestamp };
    });
    return obj;
}

/**
 * Infer object type from VS Code completion item kind
 */
export function inferObjectType(item: any): string {
    if (item.objType) return item.objType;
    // CompletionItemKind: 18 = Interface (used for VIEW), 6/7 = Class (used for TABLE)
    return item.kind === 18 ? 'VIEW' : 'TABLE';
}

/**
 * Build lookup key for ID map
 */
export function buildIdLookupKey(dbName: string, schemaName: string | undefined, objectName: string): string {
    return schemaName ? `${dbName}.${schemaName}.${objectName}` : `${dbName}..${objectName}`;
}
