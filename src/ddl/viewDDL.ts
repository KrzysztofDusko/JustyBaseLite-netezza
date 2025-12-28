/**
 * DDL Generator - View DDL Generation
 */

import { executeQueryHelper, quoteNameIfNeeded } from './helpers';
import { NzConnection } from '../types';

/**
 * Generate DDL code for creating a view in Netezza
 */
export async function generateViewDDL(
    connection: NzConnection,
    database: string,
    schema: string,
    viewName: string
): Promise<string> {
    const sql = `
        SELECT 
            SCHEMA,
            VIEWNAME,
            DEFINITION,
            OBJID::INT
        FROM ${database.toUpperCase()}.._V_VIEW
        WHERE DATABASE = '${database.toUpperCase()}'
            AND SCHEMA = '${schema.toUpperCase()}'
            AND VIEWNAME = '${viewName.toUpperCase()}'
    `;

    interface ViewRow {
        SCHEMA: string;
        VIEWNAME: string;
        DEFINITION: string;
        OBJID: number;
    }
    const result = await executeQueryHelper<ViewRow>(connection, sql);
    const rows = result;

    if (rows.length === 0) {
        throw new Error(`View ${database}.${schema}.${viewName} not found`);
    }

    const row = rows[0];
    const cleanDatabase = quoteNameIfNeeded(database);
    const cleanSchema = quoteNameIfNeeded(schema);
    const cleanViewName = quoteNameIfNeeded(viewName);

    const ddlLines: string[] = [];
    ddlLines.push(`CREATE OR REPLACE VIEW ${cleanDatabase}.${cleanSchema}.${cleanViewName} AS`);
    ddlLines.push(row.DEFINITION || '');

    return ddlLines.join('\n');
}
