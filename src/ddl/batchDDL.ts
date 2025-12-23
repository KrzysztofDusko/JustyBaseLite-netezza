/**
 * DDL Generator - Batch DDL Generation
 * Bulk fetches metadata and generates DDL for multiple objects
 */

import { ColumnInfo, KeyInfo, BatchDDLOptions, BatchDDLResult } from './types';
import { executeQueryHelper, parseConnectionString } from './helpers';
import { buildTableDDLFromCache } from './tableDDL';
import { generateViewDDL } from './viewDDL';
import { generateProcedureDDL } from './procedureDDL';
import { generateExternalTableDDL } from './externalTableDDL';
import { generateSynonymDDL } from './synonymDDL';

/**
 * Generate DDL for multiple objects in a database
 * OPTIMIZED: Uses bulk queries to fetch all metadata at once instead of per-object queries
 */
export async function generateBatchDDL(options: BatchDDLOptions): Promise<BatchDDLResult> {
    let connection: any = null;
    const errors: string[] = [];
    const ddlParts: string[] = [];
    let objectCount = 0;
    let skipped = 0;

    // Supported object types for DDL generation
    const supportedTypes = ['TABLE', 'VIEW', 'PROCEDURE', 'EXTERNAL TABLE', 'SYNONYM'];

    try {
        const config = parseConnectionString(options.connectionString);
        if (!config.port) config.port = 5480;

        const NzConnection = require('../../driver/dist/NzConnection');
        connection = new NzConnection(config);
        await connection.connect();

        const database = options.database.toUpperCase();
        const schemaFilter = options.schema ? options.schema.toUpperCase() : null;

        // Determine which object types to process
        let typesToProcess = options.objectTypes
            ? options.objectTypes.map(t => t.toUpperCase()).filter(t => supportedTypes.includes(t))
            : supportedTypes;

        if (typesToProcess.length === 0) {
            typesToProcess = supportedTypes;
        }

        // Add header
        ddlParts.push(`-- ============================================`);
        ddlParts.push(`-- Batch DDL Export`);
        ddlParts.push(`-- Database: ${database}`);
        if (schemaFilter) {
            ddlParts.push(`-- Schema: ${schemaFilter}`);
        }
        ddlParts.push(`-- Object Types: ${typesToProcess.join(', ')}`);
        ddlParts.push(`-- Generated: ${new Date().toISOString()}`);
        ddlParts.push(`-- ============================================`);
        ddlParts.push('');

        // =====================================================
        // BULK FETCH: Fetch all metadata in a few large queries
        // =====================================================

        // Bulk data maps
        const allColumns = new Map<string, ColumnInfo[]>(); // key: "SCHEMA.OBJNAME"
        const allDistribution = new Map<string, string[]>();
        const allOrganize = new Map<string, string[]>();
        const allKeys = new Map<string, Map<string, KeyInfo>>();
        const allComments = new Map<string, string>();

        const processTables = typesToProcess.includes('TABLE');
        const processViews = typesToProcess.includes('VIEW');
        const processExternalTables = typesToProcess.includes('EXTERNAL TABLE');

        // Bulk fetch columns for all tables/views
        if (processTables || processViews || processExternalTables) {
            const schemaClause = schemaFilter ? `AND D.SCHEMA = '${schemaFilter}'` : '';
            const columnsQuery = `
                SELECT 
                    D.SCHEMA,
                    D.OBJNAME,
                    D.OBJTYPE,
                    X.ATTNAME,
                    X.DESCRIPTION,
                    X.FORMAT_TYPE AS FULL_TYPE,
                    X.ATTNOTNULL::BOOL AS ATTNOTNULL,
                    X.COLDEFAULT
                FROM ${database}.._V_RELATION_COLUMN X
                INNER JOIN ${database}.._V_OBJECT_DATA D ON X.OBJID = D.OBJID
                WHERE D.DBNAME = '${database}'
                    AND D.OBJTYPE IN ('TABLE', 'VIEW', 'EXTERNAL TABLE')
                    ${schemaClause}
                ORDER BY D.SCHEMA, D.OBJNAME, X.ATTNUM
            `;
            try {
                const colResults = await executeQueryHelper(connection, columnsQuery);
                for (const row of colResults) {
                    const key = `${row.SCHEMA}.${row.OBJNAME}`;
                    if (!allColumns.has(key)) {
                        allColumns.set(key, []);
                    }
                    let isNotNull = false;
                    const val = row.ATTNOTNULL;
                    if (typeof val === 'boolean') isNotNull = val;
                    else if (typeof val === 'number') isNotNull = val !== 0;
                    else if (typeof val === 'string') {
                        const lower = val.trim().toLowerCase();
                        isNotNull = lower === 't' || lower === 'true' || lower === '1';
                    }
                    allColumns.get(key)!.push({
                        name: row.ATTNAME,
                        description: row.DESCRIPTION || null,
                        fullTypeName: row.FULL_TYPE,
                        notNull: isNotNull,
                        defaultValue: row.COLDEFAULT || null
                    });
                }
            } catch (e: any) {
                errors.push(`Error bulk fetching columns: ${e.message}`);
            }
        }

        // Bulk fetch distribution info for tables
        if (processTables) {
            const schemaClause = schemaFilter ? `AND SCHEMA = '${schemaFilter}'` : '';
            const distQuery = `
                SELECT SCHEMA, TABLENAME, ATTNAME
                FROM ${database}.._V_TABLE_DIST_MAP
                WHERE 1=1 ${schemaClause}
                ORDER BY SCHEMA, TABLENAME, DISTSEQNO
            `;
            try {
                const distResults = await executeQueryHelper(connection, distQuery);
                for (const row of distResults) {
                    const key = `${row.SCHEMA}.${row.TABLENAME}`;
                    if (!allDistribution.has(key)) {
                        allDistribution.set(key, []);
                    }
                    allDistribution.get(key)!.push(row.ATTNAME);
                }
            } catch {
                // May not be available in all versions
            }
        }

        // Bulk fetch organize info for tables
        if (processTables) {
            const schemaClause = schemaFilter ? `AND SCHEMA = '${schemaFilter}'` : '';
            const orgQuery = `
                SELECT SCHEMA, TABLENAME, ATTNAME
                FROM ${database}.._V_TABLE_ORGANIZE_COLUMN
                WHERE 1=1 ${schemaClause}
                ORDER BY SCHEMA, TABLENAME, ORGSEQNO
            `;
            try {
                const orgResults = await executeQueryHelper(connection, orgQuery);
                for (const row of orgResults) {
                    const key = `${row.SCHEMA}.${row.TABLENAME}`;
                    if (!allOrganize.has(key)) {
                        allOrganize.set(key, []);
                    }
                    allOrganize.get(key)!.push(row.ATTNAME);
                }
            } catch {
                // May not be available in all versions
            }
        }

        // Bulk fetch keys info for tables
        if (processTables) {
            const schemaClause = schemaFilter ? `AND X.SCHEMA = '${schemaFilter}'` : '';
            const keysQuery = `
                SELECT 
                    X.SCHEMA, X.RELATION, X.CONSTRAINTNAME, X.CONTYPE,
                    X.ATTNAME, X.PKDATABASE, X.PKSCHEMA, X.PKRELATION, X.PKATTNAME,
                    X.UPDT_TYPE, X.DEL_TYPE
                FROM ${database}.._V_RELATION_KEYDATA X
                WHERE X.OBJID NOT IN (4,5) ${schemaClause}
                ORDER BY X.SCHEMA, X.RELATION, X.CONSTRAINTNAME, X.CONSEQ
            `;
            try {
                const keysResults = await executeQueryHelper(connection, keysQuery);
                for (const row of keysResults) {
                    const tableKey = `${row.SCHEMA}.${row.RELATION}`;
                    if (!allKeys.has(tableKey)) {
                        allKeys.set(tableKey, new Map<string, KeyInfo>());
                    }
                    const tableKeys = allKeys.get(tableKey)!;
                    const keyName = row.CONSTRAINTNAME;
                    if (!tableKeys.has(keyName)) {
                        const typeCharMap: Record<string, string> = {
                            p: 'PRIMARY KEY',
                            f: 'FOREIGN KEY',
                            u: 'UNIQUE'
                        };
                        tableKeys.set(keyName, {
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
                    const keyInfo = tableKeys.get(keyName)!;
                    keyInfo.columns.push(row.ATTNAME);
                    if (row.PKATTNAME) keyInfo.pkColumns.push(row.PKATTNAME);
                }
            } catch {
                // Keys may not exist
            }
        }

        // Bulk fetch table comments
        if (processTables) {
            const schemaClause = schemaFilter ? `AND SCHEMA = '${schemaFilter}'` : '';
            const commentQuery = `
                SELECT SCHEMA, OBJNAME, DESCRIPTION
                FROM ${database}.._V_OBJECT_DATA
                WHERE DBNAME = '${database}' AND OBJTYPE = 'TABLE' AND DESCRIPTION IS NOT NULL ${schemaClause}
            `;
            try {
                const commentResults = await executeQueryHelper(connection, commentQuery);
                for (const row of commentResults) {
                    if (row.DESCRIPTION) {
                        allComments.set(`${row.SCHEMA}.${row.OBJNAME}`, row.DESCRIPTION);
                    }
                }
            } catch {
                // Comments optional
            }
        }

        // =====================================================
        // GENERATE DDL using pre-fetched data
        // =====================================================

        // Process each object type
        for (const objType of typesToProcess) {
            let objects: { name: string; schema: string }[] = [];

            try {
                if (objType === 'PROCEDURE') {
                    let query = `SELECT PROCEDURESIGNATURE AS OBJNAME, SCHEMA FROM ${database}.._V_PROCEDURE WHERE DATABASE = '${database}'`;
                    if (schemaFilter) query += ` AND SCHEMA = '${schemaFilter}'`;
                    query += ` ORDER BY SCHEMA, PROCEDURESIGNATURE`;
                    const result = await executeQueryHelper(connection, query);
                    objects = result.map(r => ({ name: r.OBJNAME, schema: r.SCHEMA }));
                } else {
                    let query = `SELECT OBJNAME, SCHEMA FROM ${database}.._V_OBJECT_DATA WHERE DBNAME = '${database}' AND OBJTYPE = '${objType}'`;
                    if (schemaFilter) query += ` AND SCHEMA = '${schemaFilter}'`;
                    query += ` ORDER BY SCHEMA, OBJNAME`;
                    const result = await executeQueryHelper(connection, query);
                    objects = result.map(r => ({ name: r.OBJNAME, schema: r.SCHEMA }));
                }
            } catch (e: any) {
                errors.push(`Error querying ${objType}s: ${e.message}`);
                continue;
            }

            if (objects.length === 0) continue;

            ddlParts.push(`-- ----------------------------------------`);
            ddlParts.push(`-- ${objType}S (${objects.length})`);
            ddlParts.push(`-- ----------------------------------------`);
            ddlParts.push('');

            for (const obj of objects) {
                try {
                    let ddlCode: string;
                    const key = `${obj.schema}.${obj.name}`;

                    switch (objType) {
                        case 'TABLE':
                            ddlCode = buildTableDDLFromCache(
                                database,
                                obj.schema,
                                obj.name,
                                allColumns.get(key) || [],
                                allDistribution.get(key) || [],
                                allOrganize.get(key) || [],
                                allKeys.get(key) || new Map(),
                                allComments.get(key) || null
                            );
                            break;
                        case 'VIEW':
                            // Views still need individual query for DEFINITION
                            ddlCode = await generateViewDDL(connection, database, obj.schema, obj.name);
                            break;
                        case 'PROCEDURE':
                            // Procedures still need individual query for source
                            ddlCode = await generateProcedureDDL(connection, database, obj.schema, obj.name);
                            break;
                        case 'EXTERNAL TABLE':
                            // External tables still need individual query for USING clause
                            ddlCode = await generateExternalTableDDL(connection, database, obj.schema, obj.name);
                            break;
                        case 'SYNONYM':
                            ddlCode = await generateSynonymDDL(connection, database, obj.schema, obj.name);
                            break;
                        default:
                            skipped++;
                            continue;
                    }

                    ddlParts.push(`-- ${objType}: ${database}.${obj.schema}.${obj.name}`);
                    ddlParts.push(ddlCode);
                    ddlParts.push('');
                    objectCount++;
                } catch (e: any) {
                    errors.push(
                        `Error generating DDL for ${objType} ${database}.${obj.schema}.${obj.name}: ${e.message}`
                    );
                    skipped++;
                }
            }
        }

        // Add footer
        ddlParts.push(`-- ============================================`);
        ddlParts.push(`-- End of Batch DDL Export`);
        ddlParts.push(`-- Total objects: ${objectCount}`);
        if (skipped > 0) ddlParts.push(`-- Skipped: ${skipped}`);
        if (errors.length > 0) ddlParts.push(`-- Errors: ${errors.length}`);
        ddlParts.push(`-- ============================================`);

        return {
            success: true,
            ddlCode: ddlParts.join('\n'),
            objectCount,
            errors,
            skipped
        };
    } catch (e: any) {
        return {
            success: false,
            objectCount: 0,
            errors: [`Batch DDL generation error: ${e.message || e}`],
            skipped: 0
        };
    } finally {
        if (connection) {
            try {
                await connection.close();
            } catch {
                // Ignore connection close errors during cleanup
            }
        }
    }
}
