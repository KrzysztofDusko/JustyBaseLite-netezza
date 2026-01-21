/**
 * Netezza System Views and Queries
 * 
 * This module centralizes all references to Netezza system catalog views.
 * Use these constants and query builders throughout the codebase to ensure
 * consistency and make maintenance/updates easier.
 * 
 * System views documentation:
 * - Views prefixed with _V_ are virtual system views
 * - Most views exist per-database (accessed as DATABASE.._V_VIEWNAME)
 * - Some global views are in SYSTEM database (SYSTEM.._V_DATABASE)
 * 
 * ============================================================================
 * CRITICAL NOTES ABOUT NETEZZA SYSTEM VIEW LIMITATIONS:
 * ============================================================================
 * 
 * 1. _V_OBJECT_DATA - DESCRIPTION column limitation:
 *    -------------------------------------------------
 *    When querying DATABASE.._V_OBJECT_DATA:
 *    - It returns objects from ALL databases (not just DATABASE)
 *    - BUT: The DESCRIPTION column is ONLY populated for objects belonging to DATABASE!
 *    - Objects from other databases will have NULL/empty DESCRIPTION values
 *    
 *    Solution: Always use WHERE DBNAME = 'DATABASE' filter when you need descriptions.
 * 
 * 2. _V_VIEW - DEFINITION column limitation:
 *    ----------------------------------------
 *    The DEFINITION column (view SQL source code) is ONLY accessible when:
 *    - The connection is established TO THE SAME DATABASE where the view exists!
 *    - Using DATABASE.._V_VIEW is NOT enough - you must BE CONNECTED to DATABASE
 *    
 *    Example: If connected to SYSTEM database and query MYDB.._V_VIEW,
 *    the DEFINITION column will be NULL/empty even though the view exists.
 *    
 *    Solution: To get view definitions, ensure the connection's current database
 *    matches the database containing the view.
 * 
 * NOTE: _V_PROCEDURE does NOT have this limitation - PROCEDURESOURCE is accessible
 * cross-database without needing to connect to the specific database.
 * 
 * ============================================================================
 */

// =============================================================================
// SYSTEM VIEW NAMES
// =============================================================================

/**
 * Netezza system view names (without database prefix)
 */
export const NZ_SYSTEM_VIEWS = {
    // Object/table related
    OBJECT_DATA: '_V_OBJECT_DATA',           // All objects (tables, views, etc.) with metadata
    TABLE: '_V_TABLE',                        // Tables and views basic info
    VIEW: '_V_VIEW',                          // View definitions
    PROCEDURE: '_V_PROCEDURE',                // Stored procedures
    SYNONYM: '_V_SYNONYM',                    // Synonyms
    
    // Column/structure related
    RELATION_COLUMN: '_V_RELATION_COLUMN',    // Column definitions for tables/views
    RELATION_KEYDATA: '_V_RELATION_KEYDATA',  // Primary/Foreign/Unique key definitions
    TABLE_DIST_MAP: '_V_TABLE_DIST_MAP',      // Distribution key information
    TABLE_ORGANIZE_COLUMN: '_V_TABLE_ORGANIZE_COLUMN', // Clustering/organize columns
    
    // External tables
    EXTERNAL: '_V_EXTERNAL',                  // External table definitions
    EXTOBJECT: '_V_EXTOBJECT',                // External object metadata (data source paths)
    
    // Database/schema
    DATABASE: '_V_DATABASE',                  // All databases (in SYSTEM database)
    SCHEMA: '_V_SCHEMA',                      // Schemas within a database
} as const;

/**
 * Common column names in system views
 */
export const NZ_SYSTEM_COLUMNS = {
    // Object identification
    OBJID: 'OBJID',
    OBJNAME: 'OBJNAME',
    OBJTYPE: 'OBJTYPE',
    DBNAME: 'DBNAME',
    DATABASE: 'DATABASE',
    SCHEMA: 'SCHEMA',
    OWNER: 'OWNER',
    DESCRIPTION: 'DESCRIPTION',
    
    // Column metadata
    ATTNAME: 'ATTNAME',
    ATTNUM: 'ATTNUM',
    FORMAT_TYPE: 'FORMAT_TYPE',
    ATTNOTNULL: 'ATTNOTNULL',
    COLDEFAULT: 'COLDEFAULT',
    
    // Key/constraint related
    CONSTRAINTNAME: 'CONSTRAINTNAME',
    CONTYPE: 'CONTYPE',           // 'p' = primary, 'f' = foreign, 'u' = unique
    RELATION: 'RELATION',
    PKDATABASE: 'PKDATABASE',
    PKSCHEMA: 'PKSCHEMA',
    PKRELATION: 'PKRELATION',
    PKATTNAME: 'PKATTNAME',
    UPDT_TYPE: 'UPDT_TYPE',
    DEL_TYPE: 'DEL_TYPE',
    CONSEQ: 'CONSEQ',
    
    // Distribution
    DISTSEQNO: 'DISTSEQNO',
    DISTATTNUM: 'DISTATTNUM',
    
    // Organization
    ORGSEQNO: 'ORGSEQNO',
    
    // View/procedure specific
    DEFINITION: 'DEFINITION',
    PROCEDURESIGNATURE: 'PROCEDURESIGNATURE',
    PROCEDURESOURCE: 'PROCEDURESOURCE',
    
    // Table names (in _V_TABLE)
    TABLENAME: 'TABLENAME',
    VIEWNAME: 'VIEWNAME',
    RELKIND: 'RELKIND',           // 'r' = table, 'v' = view
    
    // External table specific
    EXTOBJNAME: 'EXTOBJNAME',
    DATAOBJECT: 'DATAOBJECT',
} as const;

/**
 * Object types used in OBJTYPE column
 */
export const NZ_OBJECT_TYPES = {
    TABLE: 'TABLE',
    VIEW: 'VIEW',
    MATERIALIZED_VIEW: 'MATERIALIZED VIEW',
    EXTERNAL_TABLE: 'EXTERNAL TABLE',
    PROCEDURE: 'PROCEDURE',
    SEQUENCE: 'SEQUENCE',
    SYSTEM_VIEW: 'SYSTEM VIEW',
    SYSTEM_TABLE: 'SYSTEM TABLE',
    SYNONYM: 'SYNONYM',
} as const;

/**
 * Constraint types in _V_RELATION_KEYDATA
 */
export const NZ_CONSTRAINT_TYPES = {
    PRIMARY_KEY: 'p',
    FOREIGN_KEY: 'f',
    UNIQUE: 'u',
} as const;

// =============================================================================
// QUERY BUILDERS
// =============================================================================

/**
 * Build fully qualified system view name: DATABASE.._V_VIEWNAME
 * @param database Database name
 * @param viewName System view name from NZ_SYSTEM_VIEWS
 */
export function qualifySystemView(database: string, viewName: string): string {
    return `${database.toUpperCase()}..${viewName}`;
}

/**
 * Build a reference to SYSTEM database view (for cross-database queries)
 */
export function systemDatabaseView(viewName: string): string {
    return `SYSTEM..${viewName.toLowerCase()}`;
}

// =============================================================================
// COMMON QUERY TEMPLATES
// =============================================================================

/**
 * Query templates for common operations.
 * Use these with string interpolation for database/schema/table names.
 */
export const NZ_QUERIES = {
    /**
     * Get all databases
     * Returns: DATABASE column
     */
    LIST_DATABASES: `
        SELECT DATABASE 
        FROM ${systemDatabaseView(NZ_SYSTEM_VIEWS.DATABASE)} 
        ORDER BY DATABASE
    `.trim(),

    /**
     * Get schemas in a database
     * @param database - Database name
     */
    listSchemas: (database: string): string => `
        SELECT SCHEMA 
        FROM ${qualifySystemView(database, NZ_SYSTEM_VIEWS.SCHEMA)} 
        ORDER BY SCHEMA
    `.trim(),

    /**
     * Get all tables and views with metadata from a database
     * Returns: OBJNAME, OBJID, SCHEMA, DBNAME, OBJTYPE, OWNER, DESCRIPTION
     * 
     * IMPORTANT: When database is specified, adds DBNAME filter to ensure proper DESCRIPTION values.
     * When database is NOT specified (global query), DESCRIPTION will be empty for most objects!
     * 
     * @param database - Database name (optional, if not provided uses global view - descriptions will be empty!)
     */
    listTablesAndViews: (database?: string): string => {
        const objTypes = `'${NZ_OBJECT_TYPES.TABLE}', '${NZ_OBJECT_TYPES.VIEW}', '${NZ_OBJECT_TYPES.EXTERNAL_TABLE}'`;
        
        if (database) {
            // Query specific database WITH DBNAME filter to get proper DESCRIPTION values
            return `
                SELECT OBJNAME, OBJID, SCHEMA, DBNAME, OBJTYPE, OWNER, COALESCE(DESCRIPTION, '') AS DESCRIPTION
                FROM ${qualifySystemView(database, NZ_SYSTEM_VIEWS.OBJECT_DATA)}
                WHERE DBNAME = '${database.toUpperCase()}'
                AND OBJTYPE IN (${objTypes})
                ORDER BY SCHEMA, OBJNAME
            `.trim();
        }
        // Global query (searches all databases) - WARNING: DESCRIPTION will be empty!
        // This is a Netezza limitation: _V_OBJECT_DATA returns objects from all DBs
        // but DESCRIPTION is only populated for objects in the queried database
        return `
            SELECT OBJNAME, OBJID, SCHEMA, DBNAME, OBJTYPE, OWNER, '' AS DESCRIPTION
            FROM ${NZ_SYSTEM_VIEWS.OBJECT_DATA}
            WHERE OBJTYPE IN (${objTypes})
            ORDER BY DBNAME, SCHEMA, OBJNAME
        `.trim();
    },

    /**
     * Get column metadata for tables in a database with optional PK/FK info
     * Returns: TABLENAME, SCHEMA, ATTNAME, FORMAT_TYPE, ATTNUM, DESCRIPTION, IS_PK, IS_FK
     * 
     * Note: Uses DBNAME filter to ensure we only get objects from the specified database.
     * Column DESCRIPTION comes from _V_RELATION_COLUMN which doesn't have the cross-DB issue.
     * 
     * @param database - Database name
     * @param options - Optional filters: schema, tableName
     */
    listColumnsWithKeys: (database: string, options?: { schema?: string; tableName?: string; objTypes?: string[] }): string => {
        const db = database.toUpperCase();
        const objTypes = options?.objTypes || [NZ_OBJECT_TYPES.TABLE, NZ_OBJECT_TYPES.VIEW, NZ_OBJECT_TYPES.EXTERNAL_TABLE];
        const objTypesStr = objTypes.map(t => `'${t}'`).join(', ');
        
        // Always filter by DBNAME to ensure we get proper data from this database only
        let whereClause = `O.DBNAME = '${db}' AND O.OBJTYPE IN (${objTypesStr})`;
        if (options?.schema) {
            whereClause += ` AND UPPER(O.SCHEMA) = UPPER('${options.schema}')`;
        }
        if (options?.tableName) {
            whereClause += ` AND UPPER(O.OBJNAME) = UPPER('${options.tableName}')`;
        }

        return `
            SELECT 
                O.OBJNAME AS TABLENAME,
                O.SCHEMA,
                O.DBNAME,
                C.ATTNAME,
                C.FORMAT_TYPE,
                C.ATTNUM,
                COALESCE(C.DESCRIPTION, '') AS DESCRIPTION,
                MAX(CASE WHEN K.CONTYPE = '${NZ_CONSTRAINT_TYPES.PRIMARY_KEY}' THEN 1 ELSE 0 END) AS IS_PK,
                MAX(CASE WHEN K.CONTYPE = '${NZ_CONSTRAINT_TYPES.FOREIGN_KEY}' THEN 1 ELSE 0 END) AS IS_FK
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.RELATION_COLUMN)} C
            JOIN ${qualifySystemView(db, NZ_SYSTEM_VIEWS.OBJECT_DATA)} O ON C.OBJID = O.OBJID
            LEFT JOIN ${qualifySystemView(db, NZ_SYSTEM_VIEWS.RELATION_KEYDATA)} K 
                ON UPPER(K.RELATION) = UPPER(O.OBJNAME) 
                AND UPPER(K.SCHEMA) = UPPER(O.SCHEMA)
                AND UPPER(K.ATTNAME) = UPPER(C.ATTNAME)
                AND K.CONTYPE IN ('${NZ_CONSTRAINT_TYPES.PRIMARY_KEY}', '${NZ_CONSTRAINT_TYPES.FOREIGN_KEY}')
            WHERE ${whereClause}
            GROUP BY O.OBJNAME, O.SCHEMA, O.DBNAME, C.ATTNAME, C.FORMAT_TYPE, C.ATTNUM, C.DESCRIPTION
            ORDER BY O.SCHEMA, O.OBJNAME, C.ATTNUM
        `.trim();
    },

    /**
     * Get basic column information for a specific table
     * Returns: OBJID, ATTNAME, DESCRIPTION, FULL_TYPE, ATTNOTNULL, COLDEFAULT
     * @param database - Database name
     * @param schema - Schema name
     * @param tableName - Table name
     */
    getTableColumns: (database: string, schema: string, tableName: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT 
                X.OBJID::INT AS OBJID,
                X.ATTNAME,
                X.DESCRIPTION,
                X.FORMAT_TYPE AS FULL_TYPE,
                X.ATTNOTNULL::BOOL AS ATTNOTNULL,
                X.COLDEFAULT
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.RELATION_COLUMN)} X
            INNER JOIN ${qualifySystemView(db, NZ_SYSTEM_VIEWS.OBJECT_DATA)} D ON X.OBJID = D.OBJID
            WHERE X.TYPE IN ('${NZ_OBJECT_TYPES.TABLE}','${NZ_OBJECT_TYPES.VIEW}','${NZ_OBJECT_TYPES.EXTERNAL_TABLE}','${NZ_OBJECT_TYPES.SEQUENCE}','${NZ_OBJECT_TYPES.SYSTEM_VIEW}','${NZ_OBJECT_TYPES.SYSTEM_TABLE}')
                AND X.OBJID NOT IN (4,5)
                AND D.SCHEMA = '${schema.toUpperCase()}'
                AND D.OBJNAME = '${tableName.toUpperCase()}'
            ORDER BY X.OBJID, X.ATTNUM
        `.trim();
    },

    /**
     * Get distribution key columns for a table
     * Returns: ATTNAME
     * @param database - Database name
     * @param schema - Schema name
     * @param tableName - Table name
     */
    getDistributionKeys: (database: string, schema: string, tableName: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT ATTNAME
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.TABLE_DIST_MAP)}
            WHERE SCHEMA = '${schema.toUpperCase()}'
                AND TABLENAME = '${tableName.toUpperCase()}'
            ORDER BY DISTSEQNO
        `.trim();
    },

    /**
     * Get organization/clustering columns for a table
     * Returns: ATTNAME
     * @param database - Database name
     * @param schema - Schema name
     * @param tableName - Table name
     */
    getOrganizeColumns: (database: string, schema: string, tableName: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT ATTNAME
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.TABLE_ORGANIZE_COLUMN)}
            WHERE SCHEMA = '${schema.toUpperCase()}'
                AND TABLENAME = '${tableName.toUpperCase()}'
            ORDER BY ORGSEQNO
        `.trim();
    },

    /**
     * Get key constraints (PK, FK, UNIQUE) for a table
     * Returns: CONSTRAINTNAME, CONTYPE, ATTNAME, PK* columns for FK references
     * @param database - Database name
     * @param schema - Schema name
     * @param tableName - Table name
     */
    getTableKeys: (database: string, schema: string, tableName: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT 
                X.SCHEMA,
                X.RELATION,
                X.CONSTRAINTNAME,
                X.CONTYPE,
                X.ATTNAME,
                X.PKDATABASE,
                X.PKSCHEMA,
                X.PKRELATION,
                X.PKATTNAME,
                X.UPDT_TYPE,
                X.DEL_TYPE
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.RELATION_KEYDATA)} X
            WHERE X.OBJID NOT IN (4,5)
                AND X.SCHEMA = '${schema.toUpperCase()}'
                AND X.RELATION = '${tableName.toUpperCase()}'
            ORDER BY X.SCHEMA, X.RELATION, X.CONSEQ
        `.trim();
    },

    /**
     * Get foreign key relationships for a schema (for ERD diagrams)
     * Returns: FK constraint details with source and target table/column info
     * @param database - Database name
     * @param schema - Schema name
     */
    getForeignKeyRelationships: (database: string, schema: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT 
                X.SCHEMA,
                X.RELATION AS FROM_TABLE,
                X.CONSTRAINTNAME,
                X.ATTNAME AS FROM_COLUMN,
                X.PKDATABASE,
                X.PKSCHEMA,
                X.PKRELATION AS TO_TABLE,
                X.PKATTNAME AS TO_COLUMN,
                X.UPDT_TYPE,
                X.DEL_TYPE,
                X.CONSEQ
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.RELATION_KEYDATA)} X
            WHERE X.CONTYPE = '${NZ_CONSTRAINT_TYPES.FOREIGN_KEY}'
                AND X.SCHEMA = '${schema.toUpperCase()}'
            ORDER BY X.CONSTRAINTNAME, X.CONSEQ
        `.trim();
    },

    /**
     * Get table/object comment (DESCRIPTION)
     * 
     * IMPORTANT: Uses DBNAME filter to ensure proper DESCRIPTION value.
     * Without DBNAME filter, DESCRIPTION would be empty for objects from other databases.
     * 
     * @param database - Database name
     * @param schema - Schema name
     * @param objectName - Object name
     * @param objectType - Optional object type filter
     */
    getObjectComment: (database: string, schema: string, objectName: string, objectType?: string): string => {
        const db = database.toUpperCase();
        const typeFilter = objectType ? ` AND OBJTYPE = '${objectType}'` : '';
        return `
            SELECT DESCRIPTION
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.OBJECT_DATA)}
            WHERE DBNAME = '${db}'
                AND SCHEMA = '${schema.toUpperCase()}'
                AND OBJNAME = '${objectName.toUpperCase()}'${typeFilter}
        `.trim();
    },

    /**
     * Get table owner
     * Returns: OWNER
     * @param database - Database name
     * @param schema - Schema name
     * @param tableName - Table name
     */
    getTableOwner: (database: string, schema: string, tableName: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT OWNER
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.TABLE)}
            WHERE SCHEMA = '${schema.toUpperCase()}'
                AND TABLENAME = '${tableName.toUpperCase()}'
        `.trim();
    },

    /**
     * Get view definition
     * Returns: SCHEMA, VIEWNAME, DEFINITION, OWNER
     * 
     * ⚠️ CRITICAL: The DEFINITION column will ONLY contain the view's SQL source
     * if the DATABASE connection is established to the SAME database where the view exists!
     * Simply using DATABASE.._V_VIEW is NOT sufficient - you must be CONNECTED to DATABASE.
     * 
     * If connected to a different database, DEFINITION will be NULL/empty.
     * 
     * @param database - Database name (connection must be to this database for DEFINITION to work)
     * @param viewName - View name
     * @param schema - Optional schema name
     */
    getViewDefinition: (database: string, viewName: string, schema?: string): string => {
        const db = database.toUpperCase();
        let whereClause = `UPPER(VIEWNAME) = '${viewName.toUpperCase()}'`;
        if (schema) {
            whereClause += ` AND UPPER(SCHEMA) = '${schema.toUpperCase()}'`;
        }
        return `
            SELECT SCHEMA, VIEWNAME, DEFINITION, OWNER
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.VIEW)}
            WHERE ${whereClause}
        `.trim();
    },

    /**
     * Get procedure definition
     * Returns: SCHEMA, PROCEDURE, PROCEDURESIGNATURE, PROCEDURESOURCE, RETURNS, OWNER
     * 
     * NOTE: Unlike _V_VIEW.DEFINITION, the PROCEDURESOURCE column is accessible
     * cross-database - no need to connect to the specific database.
     * 
     * @param database - Database name
     * @param procedureName - Procedure name
     * @param schema - Optional schema name
     */
    getProcedureDefinition: (database: string, procedureName: string, schema?: string): string => {
        const db = database.toUpperCase();
        let whereClause = `UPPER(PROCEDURE) = '${procedureName.toUpperCase()}'`;
        if (schema) {
            whereClause += ` AND UPPER(SCHEMA) = '${schema.toUpperCase()}'`;
        }
        return `
            SELECT SCHEMA, PROCEDURE, PROCEDURESIGNATURE, PROCEDURESOURCE, 
                   RESULT AS RETURNS, OWNER
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.PROCEDURE)}
            WHERE ${whereClause}
        `.trim();
    },

    /**
     * List procedures in a database
     * Returns: SCHEMA, PROCEDURE, PROCEDURESIGNATURE, RESULT, OWNER
     * @param database - Optional database name (if not provided, searches all)
     * @param schema - Optional schema filter
     */
    listProcedures: (database?: string, schema?: string): string => {
        if (database) {
            let whereClause = `DATABASE = '${database.toUpperCase()}'`;
            if (schema) {
                whereClause += ` AND SCHEMA = '${schema.toUpperCase()}'`;
            }
            return `
                SELECT SCHEMA, PROCEDURE, PROCEDURESIGNATURE, RESULT AS RETURNS, OWNER, DATABASE
                FROM ${qualifySystemView(database, NZ_SYSTEM_VIEWS.PROCEDURE)}
                WHERE ${whereClause}
                ORDER BY SCHEMA, PROCEDURE
            `.trim();
        }
        // Search all databases - not directly supported, caller should iterate
        return '';
    },

    /**
     * List views in a database
     * Returns: SCHEMA, VIEWNAME, OWNER
     * @param database - Optional database name
     * @param schema - Optional schema filter
     */
    listViews: (database?: string, schema?: string): string => {
        if (database) {
            let whereClause = '1=1';
            if (schema) {
                whereClause = `SCHEMA = '${schema.toUpperCase()}'`;
            }
            return `
                SELECT SCHEMA, VIEWNAME, OWNER, '${database}' AS DATABASE
                FROM ${qualifySystemView(database, NZ_SYSTEM_VIEWS.VIEW)}
                WHERE ${whereClause}
                ORDER BY SCHEMA, VIEWNAME
            `.trim();
        }
        return '';
    },

    /**
     * Get external table metadata
     * Returns: External table details with data object info
     * @param database - Database name
     * @param schema - Optional schema filter
     */
    getExternalTables: (database: string, schema?: string): string => {
        const db = database.toUpperCase();
        let whereClause = '1=1';
        if (schema) {
            whereClause = `E1.SCHEMA = '${schema.toUpperCase()}'`;
        }
        return `
            SELECT 
                E1.TABLENAME,
                E1.SCHEMA,
                E1.OWNER,
                E1.DATABASE,
                E2.EXTOBJNAME AS DATAOBJECT
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.EXTERNAL)} E1
            LEFT JOIN ${qualifySystemView(db, NZ_SYSTEM_VIEWS.EXTOBJECT)} E2 
                ON E1.DATABASE = E2.DATABASE 
                AND E1.SCHEMA = E2.SCHEMA 
                AND E1.TABLENAME = E2.TABLENAME
            WHERE ${whereClause}
            ORDER BY E1.SCHEMA, E1.TABLENAME
        `.trim();
    },

    /**
     * Find schema for a table in a database
     * Returns: SCHEMA
     * 
     * Uses DBNAME filter to search only in the specified database.
     * 
     * @param database - Database name
     * @param tableName - Table name
     */
    findTableSchema: (database: string, tableName: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT SCHEMA
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.OBJECT_DATA)}
            WHERE DBNAME = '${db}'
                AND UPPER(OBJNAME) = '${tableName.toUpperCase()}'
                AND OBJTYPE IN ('${NZ_OBJECT_TYPES.TABLE}', '${NZ_OBJECT_TYPES.VIEW}', '${NZ_OBJECT_TYPES.EXTERNAL_TABLE}')
            LIMIT 1
        `.trim();
    },

    /**
     * Search for tables/views by name pattern
     * @param database - Database name (optional, if not provided uses global view)
     * @param pattern - Search pattern (use % for wildcards)
     */
    searchTables: (pattern: string, database?: string): string => {
        const objTypes = `'${NZ_OBJECT_TYPES.TABLE}', '${NZ_OBJECT_TYPES.VIEW}', '${NZ_OBJECT_TYPES.MATERIALIZED_VIEW}', '${NZ_OBJECT_TYPES.EXTERNAL_TABLE}'`;
        
        if (database) {
            return `
                SELECT '${database}' AS DATABASE, SCHEMA, TABLENAME, 
                    CASE RELKIND WHEN 'r' THEN '${NZ_OBJECT_TYPES.TABLE}' WHEN 'v' THEN '${NZ_OBJECT_TYPES.VIEW}' ELSE RELKIND END AS TYPE
                FROM ${qualifySystemView(database, NZ_SYSTEM_VIEWS.TABLE)}
                WHERE UPPER(TABLENAME) LIKE '${pattern.toUpperCase()}'
                ORDER BY SCHEMA, TABLENAME
                LIMIT 100
            `.trim();
        }
        // Global search
        return `
            SELECT DBNAME AS DATABASE, SCHEMA, OBJNAME AS TABLENAME, 
                CASE OBJTYPE WHEN '${NZ_OBJECT_TYPES.TABLE}' THEN '${NZ_OBJECT_TYPES.TABLE}' 
                     WHEN '${NZ_OBJECT_TYPES.VIEW}' THEN '${NZ_OBJECT_TYPES.VIEW}' 
                     WHEN '${NZ_OBJECT_TYPES.MATERIALIZED_VIEW}' THEN 'MVIEW' 
                     WHEN '${NZ_OBJECT_TYPES.EXTERNAL_TABLE}' THEN 'EXTERNAL' 
                     ELSE OBJTYPE END AS TYPE
            FROM ${NZ_SYSTEM_VIEWS.OBJECT_DATA}
            WHERE UPPER(OBJNAME) LIKE '${pattern.toUpperCase()}'
                AND OBJTYPE IN (${objTypes})
            ORDER BY DBNAME, SCHEMA, OBJNAME
            LIMIT 100
        `.trim();
    },

    /**
     * Search for columns by name pattern
     * @param database - Database name
     * @param pattern - Search pattern (use % for wildcards)
     */
    searchColumns: (database: string, pattern: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT '${database}' AS DATABASE, t.SCHEMA, t.TABLENAME, c.ATTNAME AS COLUMN_NAME, c.FORMAT_TYPE AS DATA_TYPE
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.TABLE)} t
            JOIN ${qualifySystemView(db, NZ_SYSTEM_VIEWS.RELATION_COLUMN)} c ON t.OBJID = c.OBJID
            WHERE UPPER(c.ATTNAME) LIKE '${pattern.toUpperCase()}'
            ORDER BY t.SCHEMA, t.TABLENAME, c.ATTNAME
            LIMIT 100
        `.trim();
    },

    /**
     * Get table stats info (for distribution/owner)
     * @param database - Database name
     * @param schema - Schema name
     * @param tableName - Table name
     */
    getTableStats: (database: string, schema: string, tableName: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT 
                d.ATTNAME AS DIST_KEY,
                t.OWNER
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.TABLE)} t
            LEFT JOIN ${qualifySystemView(db, NZ_SYSTEM_VIEWS.TABLE_DIST_MAP)} d ON t.OBJID = d.OBJID
            WHERE t.SCHEMA = '${schema.toUpperCase()}' AND t.TABLENAME = '${tableName.toUpperCase()}'
        `.trim();
    },

    /**
     * Find views that depend on an object by searching in their DEFINITION
     * 
     * ⚠️ WARNING: The DEFINITION column is only populated when connected to the database
     * containing the views. If connected to a different database, this query will find nothing
     * because DEFINITION will be NULL/empty. Ensure the connection is to the correct database.
     * 
     * @param database - Database name (connection must be to this database)
     * @param objectName - Object name to search for in view definitions
     */
    findDependentViews: (database: string, objectName: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT v.SCHEMA, v.VIEWNAME, v.OWNER
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.VIEW)} v
            WHERE UPPER(v.DEFINITION) LIKE '%${objectName.toUpperCase()}%'
                AND v.VIEWNAME != '${objectName.toUpperCase()}'
            ORDER BY v.SCHEMA, v.VIEWNAME
            LIMIT 50
        `.trim();
    },

    /**
     * Find procedures that reference an object by searching in their PROCEDURESOURCE
     * 
     * NOTE: Unlike _V_VIEW.DEFINITION, PROCEDURESOURCE is accessible cross-database.
     * 
     * @param database - Database name
     * @param objectName - Object name to search for in procedure source
     */
    findDependentProcedures: (database: string, objectName: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT SCHEMA, PROCEDURE AS PROC_NAME, OWNER
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.PROCEDURE)}
            WHERE UPPER(PROCEDURESOURCE) LIKE '%${objectName.toUpperCase()}%'
            ORDER BY SCHEMA, PROCEDURE
            LIMIT 25
        `.trim();
    },

    /**
     * Get all objects of a type from a database (for DDL batch export)
     * @param database - Database name
     * @param objType - Object type (TABLE, VIEW, etc.)
     * @param schema - Optional schema filter
     */
    listObjectsOfType: (database: string, objType: string, schema?: string): string => {
        const db = database.toUpperCase();
        
        // Special handling for procedures
        if (objType === NZ_OBJECT_TYPES.PROCEDURE) {
            let query = `SELECT PROCEDURESIGNATURE AS OBJNAME, SCHEMA FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.PROCEDURE)} WHERE DATABASE = '${db}'`;
            if (schema) {
                query += ` AND SCHEMA = '${schema.toUpperCase()}'`;
            }
            return query + ` ORDER BY SCHEMA, PROCEDURESIGNATURE`;
        }
        
        // All other object types
        let query = `SELECT OBJNAME, SCHEMA FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.OBJECT_DATA)} WHERE DBNAME = '${db}' AND OBJTYPE = '${objType}'`;
        if (schema) {
            query += ` AND SCHEMA = '${schema.toUpperCase()}'`;
        }
        return query + ` ORDER BY SCHEMA, OBJNAME`;
    },

    /**
     * Get distinct object types in a database
     * @param database - Database name
     */
    getObjectTypes: (database: string): string => {
        const db = database.toUpperCase();
        return `
            SELECT DISTINCT OBJTYPE 
            FROM ${qualifySystemView(db, NZ_SYSTEM_VIEWS.OBJECT_DATA)} 
            WHERE DBNAME = '${db}' 
            ORDER BY OBJTYPE
        `.trim();
    },
} as const;
