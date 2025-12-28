/**
 * DDL Generator - Synonym DDL Generation
 */

import { executeQueryHelper, quoteNameIfNeeded } from './helpers';
import { NzConnection } from '../types';

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
    const cleanDatabase = quoteNameIfNeeded(database);
    const ownerSchema = quoteNameIfNeeded(row.OWNER || schema);
    const cleanSynonymName = quoteNameIfNeeded(synonymName);
    const refObjName = row.REFOBJNAME;

    const ddlLines: string[] = [];
    ddlLines.push(`CREATE SYNONYM ${cleanDatabase}.${ownerSchema}.${cleanSynonymName} FOR ${refObjName};`);

    if (row.DESCRIPTION) {
        const cleanComment = row.DESCRIPTION.replace(/'/g, "''");
        ddlLines.push(`COMMENT ON SYNONYM ${cleanSynonymName} IS '${cleanComment}';`);
    }

    return ddlLines.join('\n');
}
