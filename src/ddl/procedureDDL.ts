/**
 * DDL Generator - Procedure DDL Generation
 */

import { ProcedureInfo } from './types';
import { executeQueryHelper, quoteNameIfNeeded, fixProcReturnType } from './helpers';

/**
 * Generate DDL code for creating a procedure in Netezza
 */
export async function generateProcedureDDL(
    connection: any,
    database: string,
    schema: string,
    procName: string
): Promise<string> {
    const sql = `
        SELECT 
            SCHEMA,
            PROCEDURESOURCE,
            OBJID::INT,
            RETURNS,
            EXECUTEDASOWNER,
            DESCRIPTION,
            PROCEDURESIGNATURE,
            PROCEDURE,
            ARGUMENTS,
            NULL AS LANGUAGE
        FROM ${database.toUpperCase()}.._V_PROCEDURE
        WHERE DATABASE = '${database.toUpperCase()}'
            AND SCHEMA = '${schema.toUpperCase()}'
            AND PROCEDURESIGNATURE = '${procName.toUpperCase()}'
        ORDER BY 1, 2, 3
    `;

    const result = await executeQueryHelper(connection, sql);
    const rows = result;

    if (rows.length === 0) {
        throw new Error(`Procedure ${database}.${schema}.${procName} not found`);
    }

    const row = rows[0];
    const procInfo: ProcedureInfo = {
        schema: row.SCHEMA,
        procedureSource: row.PROCEDURESOURCE,
        objId: row.OBJID,
        returns: fixProcReturnType(row.RETURNS),
        executeAsOwner: Boolean(row.EXECUTEDASOWNER),
        description: row.DESCRIPTION || null,
        procedureSignature: row.PROCEDURESIGNATURE,
        procedureName: row.PROCEDURE,
        arguments: row.ARGUMENTS || null
    };

    const cleanDatabase = quoteNameIfNeeded(database);
    const cleanSchema = quoteNameIfNeeded(schema);
    const cleanProcName = quoteNameIfNeeded(procInfo.procedureName);

    const ddlLines: string[] = [];
    let procHeader = `CREATE OR REPLACE PROCEDURE ${cleanDatabase}.${cleanSchema}.${cleanProcName}`;

    // Add arguments
    if (procInfo.arguments) {
        const args = procInfo.arguments.trim();
        // Check if parens already present
        if (args.startsWith('(') && args.endsWith(')')) {
            procHeader += args;
        } else {
            procHeader += `(${args})`;
        }
    } else {
        procHeader += '()';
    }

    ddlLines.push(procHeader);
    ddlLines.push(`RETURNS ${procInfo.returns}`);

    if (procInfo.executeAsOwner) {
        ddlLines.push('EXECUTE AS OWNER');
    } else {
        ddlLines.push('EXECUTE AS CALLER');
    }

    ddlLines.push('LANGUAGE NZPLSQL AS');
    ddlLines.push('BEGIN_PROC');
    ddlLines.push(procInfo.procedureSource);
    ddlLines.push('END_PROC;');

    if (procInfo.description) {
        const cleanComment = procInfo.description.replace(/'/g, "''");
        ddlLines.push(`COMMENT ON PROCEDURE ${cleanProcName} IS '${cleanComment}';`);
    }

    return ddlLines.join('\n');
}
