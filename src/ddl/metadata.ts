/**
 * DDL Generator - Metadata Queries
 * Functions to fetch table/view metadata from Netezza system views
 */

import { ColumnInfo, KeyInfo } from './types';
import { executeQueryHelper } from './helpers';
import { NzConnection } from '../types';
import { NZ_QUERIES } from '../metadata/systemQueries';

/**
 * Get table column information from Netezza system views
 */
export async function getColumns(
    connection: NzConnection,
    database: string,
    schema: string,
    tableName: string
): Promise<ColumnInfo[]> {
    // Use centralized query builder for table columns
    const sql = NZ_QUERIES.getTableColumns(database, schema, tableName);

    interface ColumnRow {
        OBJID: number;
        ATTNAME: string;
        DESCRIPTION: string;
        FULL_TYPE: string;
        ATTNOTNULL: boolean | number | string;
        COLDEFAULT: string;
        ATTNUM: number;
    }
    const result = await executeQueryHelper<ColumnRow>(connection, sql);
    const columns: ColumnInfo[] = [];

    for (const row of result) {
        // Safe boolean parsing for ODBC result
        let isNotNull = false;
        const val = row.ATTNOTNULL;
        if (typeof val === 'boolean') {
            isNotNull = val;
        } else if (typeof val === 'number') {
            isNotNull = val !== 0;
        } else if (typeof val === 'string') {
            const lower = val.trim().toLowerCase();
            isNotNull = lower === 't' || lower === 'true' || lower === '1' || lower === 'yes';
        }

        columns.push({
            name: row.ATTNAME,
            description: row.DESCRIPTION || null,
            fullTypeName: row.FULL_TYPE,
            notNull: isNotNull,
            defaultValue: row.COLDEFAULT || null
        });
    }

    return columns;
}

/**
 * Get table distribution information
 */
export async function getDistributionInfo(
    connection: NzConnection,
    database: string,
    schema: string,
    tableName: string
): Promise<string[]> {
    try {
        // Use centralized query builder for distribution keys
        const sql = NZ_QUERIES.getDistributionKeys(database, schema, tableName);
        const result = await executeQueryHelper<{ ATTNAME: string }>(connection, sql);
        return result.map(row => row.ATTNAME);
    } catch {
        // Distribution info may not be available in all Netezza versions
        return [];
    }
}

/**
 * Get table organization information
 */
export async function getOrganizeInfo(
    connection: NzConnection,
    database: string,
    schema: string,
    tableName: string
): Promise<string[]> {
    try {
        // Use centralized query builder for organize columns
        const sql = NZ_QUERIES.getOrganizeColumns(database, schema, tableName);
        const result = await executeQueryHelper<{ ATTNAME: string }>(connection, sql);
        return result.map(row => row.ATTNAME);
    } catch {
        // Organization info may not be available in all Netezza versions
        return [];
    }
}

/**
 * Get table keys information (primary key, foreign key, unique)
 */
export async function getKeysInfo(
    connection: NzConnection,
    database: string,
    schema: string,
    tableName: string
): Promise<Map<string, KeyInfo>> {
    // Use centralized query builder for table keys
    const sql = NZ_QUERIES.getTableKeys(database, schema, tableName);

    const keysInfo = new Map<string, KeyInfo>();

    try {
        interface KeyRow {
            CONSTRAINTNAME: string;
            CONTYPE: string;
            ATTNAME: string;
            PKDATABASE?: string;
            PKSCHEMA?: string;
            PKRELATION?: string;
            PKATTNAME?: string;
            UPDT_TYPE?: string;
            DEL_TYPE?: string;
        }
        const result = await executeQueryHelper<KeyRow>(connection, sql);

        for (const row of result) {
            const keyName = row.CONSTRAINTNAME;

            if (!keysInfo.has(keyName)) {
                const typeCharMap: Record<string, string> = {
                    p: 'PRIMARY KEY',
                    f: 'FOREIGN KEY',
                    u: 'UNIQUE'
                };

                keysInfo.set(keyName, {
                    type: typeCharMap[row.CONTYPE] || 'UNKNOWN',
                    typeChar: row.CONTYPE,
                    columns: [],
                    pkDatabase: row.PKDATABASE || null,
                    pkSchema: row.PKSCHEMA || null,
                    pkRelation: row.PKRELATION || null,
                    pkColumns: [],
                    updateType: row.UPDT_TYPE || 'NO ACTION',
                    deleteType: row.DEL_TYPE || 'NO ACTION'
                });
            }

            const keyInfo = keysInfo.get(keyName)!;
            keyInfo.columns.push(row.ATTNAME);
            if (row.PKATTNAME) {
                keyInfo.pkColumns.push(row.PKATTNAME);
            }
        }
    } catch (e) {
        console.warn('Cannot retrieve keys info:', e);
    }

    return keysInfo;
}

/**
 * Get table comment from metadata
 */
export async function getTableComment(
    connection: NzConnection,
    database: string,
    schema: string,
    tableName: string
): Promise<string | null> {
    try {
        // Use centralized query builder for object comment
        const sql = NZ_QUERIES.getObjectComment(database, schema, tableName, 'TABLE');
        const result = await executeQueryHelper<{ DESCRIPTION: string }>(connection, sql);
        if (result.length > 0 && result[0].DESCRIPTION) {
            return result[0].DESCRIPTION;
        }
    } catch {
        // Try without OBJTYPE filter
        try {
            const sql = NZ_QUERIES.getObjectComment(database, schema, tableName);
            const result = await executeQueryHelper<{ DESCRIPTION: string }>(connection, sql);
            if (result.length > 0 && result[0].DESCRIPTION) {
                return result[0].DESCRIPTION;
            }
        } catch {
            // Silently ignore - comments are optional
        }
    }

    return null;
}

/**
 * Get table owner
 */
export async function getTableOwner(
    connection: NzConnection,
    database: string,
    schema: string,
    tableName: string
): Promise<string | null> {
    try {
        // Use centralized query builder for table owner
        const sql = NZ_QUERIES.getTableOwner(database, schema, tableName);
        const result = await executeQueryHelper<{ OWNER: string }>(connection, sql);
        if (result.length > 0 && result[0].OWNER) {
            return result[0].OWNER;
        }
    } catch {
        // Ignore errors
    }
    return null;
}
