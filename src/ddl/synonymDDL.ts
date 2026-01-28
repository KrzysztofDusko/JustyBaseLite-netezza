/**
 * DDL Generator - Synonym DDL Generation
 */

import { executeQueryHelper, quoteNameIfNeeded } from './helpers';
import { NzConnection } from '../types';

/**
 * Build synonym DDL from metadata
 */
export function buildSynonymDDLFromCache(
    database: string,
    synonymName: string,
    refObjName: string,
    owner: string,
    schema: string,
    description: string | null
): string {
    const cleanDatabase = quoteNameIfNeeded(database);
    const ownerSchema = quoteNameIfNeeded(owner || schema);
    const cleanSynonymName = quoteNameIfNeeded(synonymName);

    const ddlLines: string[] = [];
    ddlLines.push(`CREATE SYNONYM ${cleanDatabase}.${ownerSchema}.${cleanSynonymName} FOR ${refObjName};`);

    if (description) {
        const cleanComment = description.replace(/'/g, "''");
        ddlLines.push(`COMMENT ON SYNONYM ${cleanSynonymName} IS '${cleanComment}';`);
    }

    return ddlLines.join('\n');
}

/**
 * Generate DDL code for creating a synonym in Netezza
 */
export async function generateSynonymDDL(
    connection: NzConnection,
    database: string,
    schema: string,
    synonymName: string
): Promise<string> {
    const sql = `
        SELECT 
            SCHEMA,
            OWNER,
            SYNONYM_NAME,
            REFOBJNAME,
            DESCRIPTION
        FROM ${database.toUpperCase()}.._V_SYNONYM
        WHERE DATABASE = '${database.toUpperCase()}'
            AND SCHEMA = '${schema.toUpperCase()}'
            AND SYNONYM_NAME = '${synonymName.toUpperCase()}'
    `;

    interface SynonymRow {
        SCHEMA: string;
        OWNER: string;
        SYNONYM_NAME: string;
        REFOBJNAME: string;
        DESCRIPTION: string;
    }
    const result = await executeQueryHelper<SynonymRow>(connection, sql);
    const rows = result;

    if (rows.length === 0) {
        throw new Error(`Synonym ${database}.${schema}.${synonymName} not found`);
    }

    const row = rows[0];
    return buildSynonymDDLFromCache(
        database,
        synonymName,
        row.REFOBJNAME,
        row.OWNER,
        schema,
        row.DESCRIPTION
    );
}
