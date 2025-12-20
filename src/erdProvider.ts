/**
 * ERD (Entity Relationship Diagram) Provider
 * Retrieves foreign key relationships and builds graph data for visualization
 */

import * as vscode from 'vscode';
import { runQuery } from './queryRunner';
import { ConnectionManager } from './connectionManager';

/**
 * Represents a table node in the ERD
 */
export interface TableNode {
    database: string;
    schema: string;
    tableName: string;
    fullName: string;
    columns: ColumnInfo[];
    primaryKeyColumns: string[];
}

/**
 * Column information for a table
 */
export interface ColumnInfo {
    name: string;
    dataType: string;
    isPrimaryKey: boolean;
    isForeignKey: boolean;
}

/**
 * Represents a relationship (foreign key) edge in the ERD
 */
export interface RelationshipEdge {
    constraintName: string;
    fromTable: string;      // schema.table
    toTable: string;        // schema.table (referenced table)
    fromColumns: string[];
    toColumns: string[];
    onDelete: string;
    onUpdate: string;
}

/**
 * Complete ERD data structure
 */
export interface ERDData {
    database: string;
    schema: string;
    tables: TableNode[];
    relationships: RelationshipEdge[];
}

/**
 * Get all foreign key relationships for a schema
 */
export async function getForeignKeysForSchema(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionName: string,
    database: string,
    schema: string
): Promise<RelationshipEdge[]> {
    const sql = `
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
        FROM 
            ${database.toUpperCase()}.._V_RELATION_KEYDATA X
        WHERE 
            X.CONTYPE = 'f'
            AND X.SCHEMA = '${schema.toUpperCase()}'
        ORDER BY
            X.CONSTRAINTNAME, X.CONSEQ
    `;

    const relationships = new Map<string, RelationshipEdge>();

    try {
        const resultJson = await runQuery(context, sql, true, connectionName, connectionManager);

        if (!resultJson) {
            return [];
        }

        const rows = JSON.parse(resultJson);

        for (const row of rows) {
            const constraintName = row.CONSTRAINTNAME;
            const fromTable = `${row.SCHEMA}.${row.FROM_TABLE}`;
            const toTable = `${row.PKSCHEMA}.${row.TO_TABLE}`;

            if (!relationships.has(constraintName)) {
                relationships.set(constraintName, {
                    constraintName,
                    fromTable,
                    toTable,
                    fromColumns: [],
                    toColumns: [],
                    onDelete: row.DEL_TYPE || 'NO ACTION',
                    onUpdate: row.UPDT_TYPE || 'NO ACTION'
                });
            }

            const rel = relationships.get(constraintName)!;
            rel.fromColumns.push(row.FROM_COLUMN);
            rel.toColumns.push(row.TO_COLUMN);
        }
    } catch (e) {
        console.warn('Cannot retrieve FK relationships:', e);
    }

    return Array.from(relationships.values());
}

/**
 * Get tables involved in relationships (have FK or are referenced by FK)
 */
export async function getTablesInSchema(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionName: string,
    database: string,
    schema: string
): Promise<TableNode[]> {
    // Get all tables in schema
    const tablesSql = `
        SELECT 
            T.TABLENAME,
            T.OWNER
        FROM 
            ${database.toUpperCase()}.._V_TABLE T
        WHERE 
            T.SCHEMA = '${schema.toUpperCase()}'
        ORDER BY T.TABLENAME
    `;

    // Get columns for all tables
    const columnsSql = `
        SELECT 
            A.NAME AS TABLENAME,
            A.ATTNAME,
            A.FORMAT_TYPE
        FROM 
            ${database.toUpperCase()}.._V_RELATION_COLUMN A
        WHERE 
            A.SCHEMA = '${schema.toUpperCase()}'
            AND A.TYPE = 'TABLE'
        ORDER BY A.NAME, A.ATTNUM
    `;

    // Get primary keys
    const pkSql = `
        SELECT 
            X.RELATION,
            X.ATTNAME
        FROM 
            ${database.toUpperCase()}.._V_RELATION_KEYDATA X
        WHERE 
            X.CONTYPE = 'p'
            AND X.SCHEMA = '${schema.toUpperCase()}'
        ORDER BY X.RELATION, X.CONSEQ
    `;

    const tables = new Map<string, TableNode>();

    try {
        // Get tables
        const tablesJson = await runQuery(context, tablesSql, true, connectionName, connectionManager);
        if (tablesJson) {
            const tablesRows = JSON.parse(tablesJson);
            for (const row of tablesRows) {
                const tableName = row.TABLENAME;
                tables.set(tableName, {
                    database,
                    schema,
                    tableName,
                    fullName: `${schema}.${tableName}`,
                    columns: [],
                    primaryKeyColumns: []
                });
            }
        }

        // Get columns
        const columnsJson = await runQuery(context, columnsSql, true, connectionName, connectionManager);
        if (columnsJson) {
            const columnsRows = JSON.parse(columnsJson);
            for (const row of columnsRows) {
                const table = tables.get(row.TABLENAME);
                if (table) {
                    table.columns.push({
                        name: row.ATTNAME,
                        dataType: row.FORMAT_TYPE,
                        isPrimaryKey: false,
                        isForeignKey: false
                    });
                }
            }
        }

        // Get primary keys
        const pkJson = await runQuery(context, pkSql, true, connectionName, connectionManager);
        if (pkJson) {
            const pkRows = JSON.parse(pkJson);
            for (const row of pkRows) {
                const table = tables.get(row.RELATION);
                if (table) {
                    table.primaryKeyColumns.push(row.ATTNAME);
                    const col = table.columns.find(c => c.name === row.ATTNAME);
                    if (col) {
                        col.isPrimaryKey = true;
                    }
                }
            }
        }
    } catch (e) {
        console.warn('Cannot retrieve table information:', e);
    }

    return Array.from(tables.values());
}

/**
 * Build complete ERD data for a schema
 */
export async function buildERDData(
    context: vscode.ExtensionContext,
    connectionManager: ConnectionManager,
    connectionName: string,
    database: string,
    schema: string
): Promise<ERDData> {
    // Get all tables and relationships
    const [tables, relationships] = await Promise.all([
        getTablesInSchema(context, connectionManager, connectionName, database, schema),
        getForeignKeysForSchema(context, connectionManager, connectionName, database, schema)
    ]);

    // Mark FK columns
    for (const rel of relationships) {
        const fromTableName = rel.fromTable.split('.')[1];
        const table = tables.find(t => t.tableName === fromTableName);
        if (table) {
            for (const colName of rel.fromColumns) {
                const col = table.columns.find(c => c.name === colName);
                if (col) {
                    col.isForeignKey = true;
                }
            }
        }
    }

    // Filter to only tables that participate in relationships
    const tablesInRelationships = new Set<string>();
    for (const rel of relationships) {
        tablesInRelationships.add(rel.fromTable.split('.')[1]);
        tablesInRelationships.add(rel.toTable.split('.')[1]);
    }

    const filteredTables = tables.filter(t =>
        tablesInRelationships.has(t.tableName) || tables.length <= 20
    );

    return {
        database,
        schema,
        tables: filteredTables,
        relationships
    };
}
