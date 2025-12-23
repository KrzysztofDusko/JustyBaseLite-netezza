/**
 * Table Metadata Provider
 * Centralized module for fetching table and column metadata from Netezza system views.
 * Replaces duplicated SQL queries across editDataProvider, schemaProvider, etc.
 */

/**
 * Column metadata structure
 */
export interface ColumnMetadata {
    attname: string;
    formatType: string;
    isNotNull: boolean;
    colDefault: string | null;
    description: string;
    isPk: boolean;
    isFk: boolean;
}

/**
 * Complete table metadata structure
 */
export interface TableMetadata {
    tableComment: string | null;
    columns: ColumnMetadata[];
}

/**
 * Raw column data as returned from SQL query (uppercase keys)
 */
export interface RawColumnRow {
    ATTNAME: string;
    FORMAT_TYPE: string;
    IS_NOT_NULL: number | string | boolean;
    COLDEFAULT: string | null;
    DESCRIPTION: string;
    IS_PK: number | string;
    IS_FK: number | string;
}

/**
 * Build SQL query to fetch table comment/description
 */
export function buildTableCommentQuery(database: string, schema: string, tableName: string): string {
    return `SELECT description FROM ${database}.._v_object_data WHERE objtype='TABLE' AND objname='${tableName}' AND schema='${schema}'`;
}

/**
 * Build SQL query to fetch column metadata with PK/FK indicators
 *
 * This is the canonical query that should be used everywhere when fetching
 * full column metadata including primary/foreign key status.
 */
export function buildColumnMetadataQuery(database: string, schema: string, tableName: string): string {
    // Note: Returns standardized 1/0 integers to avoid JS string-boolean confusion
    return `
        SELECT 
            X.ATTNAME
            , X.FORMAT_TYPE
            , CASE WHEN X.ATTNOTNULL THEN 1 ELSE 0 END AS IS_NOT_NULL
            , X.COLDEFAULT
            , COALESCE(X.DESCRIPTION, '') AS DESCRIPTION
            , MAX(CASE WHEN K.CONTYPE = 'p' THEN 1 ELSE 0 END) AS IS_PK
            , MAX(CASE WHEN K.CONTYPE = 'f' THEN 1 ELSE 0 END) AS IS_FK
        FROM
            ${database}.._V_RELATION_COLUMN X
        INNER JOIN
            ${database}.._V_OBJECT_DATA O ON X.OBJID = O.OBJID
        LEFT JOIN
            ${database}.._V_RELATION_KEYDATA K 
            ON UPPER(K.RELATION) = UPPER(O.OBJNAME) 
            AND UPPER(K.SCHEMA) = UPPER(O.SCHEMA)
            AND UPPER(K.ATTNAME) = UPPER(X.ATTNAME)
            AND K.CONTYPE IN ('p', 'f')
        WHERE
            UPPER(O.OBJNAME) = UPPER('${tableName}')
            AND UPPER(O.DBNAME) = UPPER('${database}')
            AND UPPER(O.SCHEMA) = UPPER('${schema}')
        GROUP BY 
            X.ATTNAME, X.FORMAT_TYPE, X.ATTNOTNULL, X.COLDEFAULT, X.DESCRIPTION, X.ATTNUM
        ORDER BY 
            X.ATTNUM
    `;
}

/**
 * Convert raw SQL row to normalized ColumnMetadata
 */
export function parseColumnRow(row: RawColumnRow): ColumnMetadata {
    // Handle various boolean representations from ODBC driver
    let isNotNull = false;
    const notNullVal = row.IS_NOT_NULL;
    if (typeof notNullVal === 'boolean') {
        isNotNull = notNullVal;
    } else if (typeof notNullVal === 'number') {
        isNotNull = notNullVal === 1;
    } else if (typeof notNullVal === 'string') {
        isNotNull = notNullVal === '1' || notNullVal.toLowerCase() === 'true' || notNullVal.toLowerCase() === 't';
    }

    return {
        attname: row.ATTNAME,
        formatType: row.FORMAT_TYPE,
        isNotNull,
        colDefault: row.COLDEFAULT || null,
        description: row.DESCRIPTION || '',
        isPk: Number(row.IS_PK) === 1,
        isFk: Number(row.IS_FK) === 1
    };
}

/**
 * Parse table comment from query result
 */
export function parseTableComment(resultJson: string | undefined): string | null {
    if (!resultJson) return null;
    try {
        const rows = JSON.parse(resultJson);
        if (rows.length > 0 && rows[0].DESCRIPTION) {
            return rows[0].DESCRIPTION;
        }
    } catch {
        // Ignore parse errors
    }
    return null;
}

/**
 * Parse column metadata from query result
 */
export function parseColumnMetadata(resultJson: string | undefined): ColumnMetadata[] {
    if (!resultJson) return [];
    try {
        const rows: RawColumnRow[] = JSON.parse(resultJson);
        return rows.map(parseColumnRow);
    } catch (e) {
        console.error('[TableMetadataProvider] Error parsing column metadata:', e);
        return [];
    }
}

/**
 * Fetch complete table metadata (comment + columns with PK/FK info)
 *
 * @param runQueryFn - Query execution function that returns JSON string
 * @param database - Database name
 * @param schema - Schema name
 * @param tableName - Table name
 * @returns TableMetadata object with normalized data
 */
export async function getTableMetadata(
    runQueryFn: (query: string) => Promise<string | undefined>,
    database: string,
    schema: string,
    tableName: string
): Promise<TableMetadata> {
    const commentQuery = buildTableCommentQuery(database, schema, tableName);
    const columnQuery = buildColumnMetadataQuery(database, schema, tableName);

    const [commentResult, columnResult] = await Promise.all([runQueryFn(commentQuery), runQueryFn(columnQuery)]);

    return {
        tableComment: parseTableComment(commentResult),
        columns: parseColumnMetadata(columnResult)
    };
}

/**
 * Convert ColumnMetadata to the format expected by webview (uppercase keys for compatibility)
 */
export function toWebviewFormat(columns: ColumnMetadata[]): RawColumnRow[] {
    return columns.map(col => ({
        ATTNAME: col.attname,
        FORMAT_TYPE: col.formatType,
        IS_NOT_NULL: col.isNotNull ? 1 : 0,
        COLDEFAULT: col.colDefault,
        DESCRIPTION: col.description,
        IS_PK: col.isPk ? 1 : 0,
        IS_FK: col.isFk ? 1 : 0
    }));
}
