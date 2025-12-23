/**
 * DDL Generator - Metadata Queries
 * Functions to fetch table/view metadata from Netezza system views
 */

import { ColumnInfo, KeyInfo } from './types';
import { executeQueryHelper } from './helpers';

/**
 * Get table column information from Netezza system views
 */
export async function getColumns(
    connection: any,
    database: string,
    schema: string,
    tableName: string
): Promise<ColumnInfo[]> {
    const sql = `
        SELECT 
            X.OBJID::INT AS OBJID
            , X.ATTNAME
            , X.DESCRIPTION
            , X.FORMAT_TYPE AS FULL_TYPE
            , X.ATTNOTNULL::BOOL AS ATTNOTNULL
            , X.COLDEFAULT
        FROM
            ${database.toUpperCase()}.._V_RELATION_COLUMN X
        INNER JOIN
            ${database.toUpperCase()}.._V_OBJECT_DATA D ON X.OBJID = D.OBJID
        WHERE
            X.TYPE IN ('TABLE','VIEW','EXTERNAL TABLE', 'SEQUENCE','SYSTEM VIEW','SYSTEM TABLE')
            AND X.OBJID NOT IN (4,5)
            AND D.SCHEMA = '${schema.toUpperCase()}'
            AND D.OBJNAME = '${tableName.toUpperCase()}'
        ORDER BY 
            X.OBJID, X.ATTNUM
    `;

    const result = await executeQueryHelper(connection, sql);
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
    connection: any,
    database: string,
    schema: string,
    tableName: string
): Promise<string[]> {
    try {
        const sql = `
            SELECT ATTNAME
            FROM ${database.toUpperCase()}.._V_TABLE_DIST_MAP
            WHERE SCHEMA = '${schema.toUpperCase()}'
                AND TABLENAME = '${tableName.toUpperCase()}'
            ORDER BY DISTSEQNO
        `;

        const result = await executeQueryHelper(connection, sql);
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
    connection: any,
    database: string,
    schema: string,
    tableName: string
): Promise<string[]> {
    try {
        const sql = `
            SELECT ATTNAME
            FROM ${database.toUpperCase()}.._V_TABLE_ORGANIZE_COLUMN
            WHERE SCHEMA = '${schema.toUpperCase()}'
                AND TABLENAME = '${tableName.toUpperCase()}'
            ORDER BY ORGSEQNO
        `;

        const result = await executeQueryHelper(connection, sql);
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
    connection: any,
    database: string,
    schema: string,
    tableName: string
): Promise<Map<string, KeyInfo>> {
    const sql = `
        SELECT 
            X.SCHEMA
            , X.RELATION
            , X.CONSTRAINTNAME
            , X.CONTYPE
            , X.ATTNAME
            , X.PKDATABASE
            , X.PKSCHEMA
            , X.PKRELATION
            , X.PKATTNAME
            , X.UPDT_TYPE
            , X.DEL_TYPE
        FROM 
            ${database.toUpperCase()}.._V_RELATION_KEYDATA X
        WHERE 
            X.OBJID NOT IN (4,5)
            AND X.SCHEMA = '${schema.toUpperCase()}'
            AND X.RELATION = '${tableName.toUpperCase()}'
        ORDER BY
            X.SCHEMA, X.RELATION, X.CONSEQ
    `;

    const keysInfo = new Map<string, KeyInfo>();

    try {
        const result = await executeQueryHelper(connection, sql);

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
    connection: any,
    database: string,
    schema: string,
    tableName: string
): Promise<string | null> {
    try {
        const sql = `
            SELECT DESCRIPTION
            FROM ${database.toUpperCase()}.._V_OBJECT_DATA
            WHERE SCHEMA = '${schema.toUpperCase()}'
                AND OBJNAME = '${tableName.toUpperCase()}'
                AND OBJTYPE = 'TABLE'
        `;

        const result = await executeQueryHelper(connection, sql);
        if (result.length > 0 && result[0].DESCRIPTION) {
            return result[0].DESCRIPTION;
        }
    } catch {
        // Try without OBJTYPE filter
        try {
            const sql = `
                SELECT DESCRIPTION
                FROM ${database.toUpperCase()}.._V_OBJECT_DATA
                WHERE SCHEMA = '${schema.toUpperCase()}'
                    AND OBJNAME = '${tableName.toUpperCase()}'
            `;

            const result = await executeQueryHelper(connection, sql);
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
    connection: any,
    database: string,
    schema: string,
    tableName: string
): Promise<string | null> {
    try {
        const sql = `
            SELECT OWNER
            FROM ${database.toUpperCase()}.._V_TABLE
            WHERE SCHEMA = '${schema.toUpperCase()}'
                AND TABLENAME = '${tableName.toUpperCase()}'
        `;

        const result = await executeQueryHelper(connection, sql);
        if (result.length > 0 && result[0].OWNER) {
            return result[0].OWNER;
        }
    } catch {
        // Ignore errors
    }
    return null;
}
