/**
 * Netezza DDL Generator
 * Generates DDL code for creating tables in Netezza based on existing table definitions.
 * TypeScript port of ddl_generator.py
 */

// import * as odbc from 'odbc'; // Removed odbc dependency

interface ColumnInfo {
    name: string;
    description: string | null;
    fullTypeName: string;
    notNull: boolean;
    defaultValue: string | null;
}

interface KeyInfo {
    type: string;
    typeChar: string;
    columns: string[];
    pkDatabase: string | null;
    pkSchema: string | null;
    pkRelation: string | null;
    pkColumns: string[];
    updateType: string;
    deleteType: string;
}

export interface DDLResult {
    success: boolean;
    ddlCode?: string;
    objectInfo?: {
        database: string;
        schema: string;
        objectName: string;
        objectType: string;
    };
    error?: string;
    note?: string;
}

/**
 * Execute query and return array of objects (shim for odbc.query)
 */
async function executeQueryHelper(connection: any, sql: string): Promise<any[]> {
    const cmd = connection.createCommand(sql);
    const reader = await cmd.executeReader();
    const results: any[] = [];

    // Read all rows
    while (await reader.read()) {
        const row: any = {};
        for (let i = 0; i < reader.fieldCount; i++) {
            row[reader.getName(i)] = reader.getValue(i);
        }
        results.push(row);
    }
    return results;
}

/**
 * Quote identifier name if needed (contains special characters or is mixed case)
 */
export function quoteNameIfNeeded(name: string): string {
    if (!name) {
        return name;
    }

    // Check if name contains only uppercase letters, digits, and underscores
    // and starts with a letter or underscore
    const isSimpleIdentifier = /^[A-Z_][A-Z0-9_]*$/i.test(name) && name === name.toUpperCase();

    if (isSimpleIdentifier) {
        return name;
    }

    // Quote name and double internal quotes
    return `"${name.replace(/"/g, '""')}"`;
}

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
                    'p': 'PRIMARY KEY',
                    'f': 'FOREIGN KEY',
                    'u': 'UNIQUE'
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

/**
 * Generate complete DDL code for creating a table in Netezza
 */
async function generateTableDDL(
    connection: any,
    database: string,
    schema: string,
    tableName: string
): Promise<string> {
    // Get table data
    const columns = await getColumns(connection, database, schema, tableName);
    if (columns.length === 0) {
        throw new Error(`Table ${database}.${schema}.${tableName} not found or has no columns`);
    }

    const distributionColumns = await getDistributionInfo(connection, database, schema, tableName);
    const organizeColumns = await getOrganizeInfo(connection, database, schema, tableName);
    const keysInfo = await getKeysInfo(connection, database, schema, tableName);
    const tableComment = await getTableComment(connection, database, schema, tableName);

    // Prepare clean names
    const cleanDatabase = quoteNameIfNeeded(database);
    const cleanSchema = quoteNameIfNeeded(schema);
    const cleanTableName = quoteNameIfNeeded(tableName);

    // Start building DDL
    const ddlLines: string[] = [];
    ddlLines.push(`CREATE TABLE ${cleanDatabase}.${cleanSchema}.${cleanTableName}`);
    ddlLines.push('(');

    // Add columns
    const columnDefinitions: string[] = [];
    for (const column of columns) {
        const cleanColumnName = quoteNameIfNeeded(column.name);
        let columnDef = `    ${cleanColumnName} ${column.fullTypeName}`;

        if (column.notNull) {
            columnDef += ' NOT NULL';
        }

        if (column.defaultValue !== null) {
            columnDef += ` DEFAULT ${column.defaultValue}`;
        }

        columnDefinitions.push(columnDef);
    }

    ddlLines.push(columnDefinitions.join(',\n'));

    // Add distribution info
    if (distributionColumns.length > 0) {
        const cleanDistColumns = distributionColumns.map(col => quoteNameIfNeeded(col));
        ddlLines.push(`)\nDISTRIBUTE ON (${cleanDistColumns.join(', ')})`);
    } else {
        ddlLines.push(')\nDISTRIBUTE ON RANDOM');
    }

    // Add organization info
    if (organizeColumns.length > 0) {
        const cleanOrgColumns = organizeColumns.map(col => quoteNameIfNeeded(col));
        ddlLines.push(`ORGANIZE ON (${cleanOrgColumns.join(', ')})`);
    }

    ddlLines.push(';');
    ddlLines.push('');

    // Add keys
    for (const [keyName, keyInfo] of keysInfo) {
        const cleanKeyName = quoteNameIfNeeded(keyName);
        const cleanColumns = keyInfo.columns.map(col => quoteNameIfNeeded(col));

        if (keyInfo.typeChar === 'f') {  // Foreign Key
            const cleanPkColumns = keyInfo.pkColumns.filter(col => col).map(col => quoteNameIfNeeded(col));
            if (cleanPkColumns.length > 0) {
                ddlLines.push(
                    `ALTER TABLE ${cleanDatabase}.${cleanSchema}.${cleanTableName} ` +
                    `ADD CONSTRAINT ${cleanKeyName} ${keyInfo.type} ` +
                    `(${cleanColumns.join(', ')}) ` +
                    `REFERENCES ${keyInfo.pkDatabase}.${keyInfo.pkSchema}.${keyInfo.pkRelation} ` +
                    `(${cleanPkColumns.join(', ')}) ` +
                    `ON DELETE ${keyInfo.deleteType} ON UPDATE ${keyInfo.updateType};`
                );
            }
        } else if (keyInfo.typeChar === 'p' || keyInfo.typeChar === 'u') {  // Primary Key or Unique
            ddlLines.push(
                `ALTER TABLE ${cleanDatabase}.${cleanSchema}.${cleanTableName} ` +
                `ADD CONSTRAINT ${cleanKeyName} ${keyInfo.type} ` +
                `(${cleanColumns.join(', ')});`
            );
        }
    }

    // Add table comment
    if (tableComment) {
        const cleanComment = tableComment.replace(/'/g, "''");
        ddlLines.push('');
        ddlLines.push(`COMMENT ON TABLE ${cleanDatabase}.${cleanSchema}.${cleanTableName} IS '${cleanComment}';`);
    }

    // Add column comments
    for (const column of columns) {
        if (column.description) {
            const cleanColumnName = quoteNameIfNeeded(column.name);
            const cleanDesc = column.description.replace(/'/g, "''");
            ddlLines.push(
                `COMMENT ON COLUMN ${cleanDatabase}.${cleanSchema}.${cleanTableName}.${cleanColumnName} ` +
                `IS '${cleanDesc}';`
            );
        }
    }

    return ddlLines.join('\n');
}

/**
 * Generate DDL code for creating a view in Netezza
 */
async function generateViewDDL(
    connection: any,
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

    const result = await executeQueryHelper(connection, sql);
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

interface ProcedureInfo {
    schema: string;
    procedureSource: string;
    objId: number;
    returns: string;
    executeAsOwner: boolean;
    description: string | null;
    procedureSignature: string;
    procedureName: string;
    arguments: string | null;
}

/**
 * Fix Netezza procedure return type syntax for ANY length types
 */
function fixProcReturnType(procReturns: string): string {
    if (!procReturns) return procReturns;

    const upper = procReturns.trim().toUpperCase();
    if (upper === "CHARACTER VARYING") {
        return "CHARACTER VARYING(ANY)";
    }
    else if (upper === "NATIONAL CHARACTER VARYING") {
        return "NATIONAL CHARACTER VARYING(ANY)";
    }
    else if (upper === "NATIONAL CHARACTER") {
        return "NATIONAL CHARACTER(ANY)";
    }
    else if (upper === "CHARACTER") {
        return "CHARACTER(ANY)";
    }
    return procReturns;
}

/**
 * Generate DDL code for creating a procedure in Netezza
 */
async function generateProcedureDDL(
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
        // Check if parens already present (unlikely for ARGUMENTS column but safe to check)
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

interface ExternalTableInfo {
    schema: string;
    tableName: string;
    dataObject: string | null;
    delimiter: string | null;
    encoding: string | null;
    timeStyle: string | null;
    remoteSource: string | null;
    skipRows: number | null;
    maxErrors: number | null;
    escapeChar: string | null;
    logDir: string | null;
    decimalDelim: string | null;
    quotedValue: string | null;
    nullValue: string | null;
    crInString: boolean | null;
    truncString: boolean | null;
    ctrlChars: boolean | null;
    ignoreZero: boolean | null;
    timeExtraZeros: boolean | null;
    y2Base: number | null;
    fillRecord: boolean | null;
    compress: string | null;
    includeHeader: boolean | null;
    lfInString: boolean | null;
    dateStyle: string | null;
    dateDelim: string | null;
    timeDelim: string | null;
    boolStyle: string | null;
    format: string | null;
    socketBufSize: number | null;
    recordDelim: string | null;
    maxRows: number | null;
    requireQuotes: boolean | null;
    recordLength: string | null;
    dateTimeDelim: string | null;
    rejectFile: string | null;
}

/**
 * Generate DDL code for creating an external table in Netezza
 */
async function generateExternalTableDDL(
    connection: any,
    database: string,
    schema: string,
    tableName: string
): Promise<string> {
    // Get external table properties
    const sql = `
        SELECT 
            E1.SCHEMA,
            E1.TABLENAME,
            E2.EXTOBJNAME,
            E2.OBJID::INT,
            E1.DELIM,
            E1.ENCODING,
            E1.TIMESTYLE,
            E1.REMOTESOURCE,
            E1.SKIPROWS,
            E1.MAXERRORS,
            E1.ESCAPE,
            E1.LOGDIR,
            E1.DECIMALDELIM,
            E1.QUOTEDVALUE,
            E1.NULLVALUE,
            E1.CRINSTRING,
            E1.TRUNCSTRING,
            E1.CTRLCHARS,
            E1.IGNOREZERO,
            E1.TIMEEXTRAZEROS,
            E1.Y2BASE,
            E1.FILLRECORD,
            E1.COMPRESS,
            E1.INCLUDEHEADER,
            E1.LFINSTRING,
            E1.DATESTYLE,
            E1.DATEDELIM,
            E1.TIMEDELIM,
            E1.BOOLSTYLE,
            E1.FORMAT,
            E1.SOCKETBUFSIZE,
            E1.RECORDDELIM,
            E1.MAXROWS,
            E1.REQUIREQUOTES,
            E1.RECORDLENGTH,
            E1.DATETIMEDELIM,
            E1.REJECTFILE
        FROM 
            ${database.toUpperCase()}.._V_EXTERNAL E1
            JOIN ${database.toUpperCase()}.._V_EXTOBJECT E2 ON E1.DATABASE = E2.DATABASE
                AND E1.SCHEMA = E2.SCHEMA
                AND E1.TABLENAME = E2.TABLENAME
        WHERE 
            E1.DATABASE = '${database.toUpperCase()}'
            AND E1.SCHEMA = '${schema.toUpperCase()}'
            AND E1.TABLENAME = '${tableName.toUpperCase()}'
    `;

    const result = await executeQueryHelper(connection, sql);
    const rows = result;

    if (rows.length === 0) {
        throw new Error(`External table ${database}.${schema}.${tableName} not found`);
    }

    const row = rows[0];
    const extInfo: ExternalTableInfo = {
        schema: row.SCHEMA,
        tableName: row.TABLENAME,
        dataObject: row.EXTOBJNAME || null,
        delimiter: row.DELIM || null,
        encoding: row.ENCODING || null,
        timeStyle: row.TIMESTYLE || null,
        remoteSource: row.REMOTESOURCE || null,
        skipRows: row.SKIPROWS || null,
        maxErrors: row.MAXERRORS || null,
        escapeChar: row.ESCAPE || null,
        logDir: row.LOGDIR || null,
        decimalDelim: row.DECIMALDELIM || null,
        quotedValue: row.QUOTEDVALUE || null,
        nullValue: row.NULLVALUE || null,
        crInString: row.CRINSTRING ?? null,
        truncString: row.TRUNCSTRING ?? null,
        ctrlChars: row.CTRLCHARS ?? null,
        ignoreZero: row.IGNOREZERO ?? null,
        timeExtraZeros: row.TIMEEXTRAZEROS ?? null,
        y2Base: row.Y2BASE || null,
        fillRecord: row.FILLRECORD ?? null,
        compress: row.COMPRESS || null,
        includeHeader: row.INCLUDEHEADER ?? null,
        lfInString: row.LFINSTRING ?? null,
        dateStyle: row.DATESTYLE || null,
        dateDelim: row.DATEDELIM || null,
        timeDelim: row.TIMEDELIM || null,
        boolStyle: row.BOOLSTYLE || null,
        format: row.FORMAT || null,
        socketBufSize: row.SOCKETBUFSIZE || null,
        recordDelim: row.RECORDDELIM ? String(row.RECORDDELIM).replace(/\r/g, '\\r').replace(/\n/g, '\\n') : null,
        maxRows: row.MAXROWS || null,
        requireQuotes: row.REQUIREQUOTES ?? null,
        recordLength: row.RECORDLENGTH || null,
        dateTimeDelim: row.DATETIMEDELIM || null,
        rejectFile: row.REJECTFILE || null
    };

    // Get columns
    const columns = await getColumns(connection, database, schema, tableName);

    const cleanDatabase = quoteNameIfNeeded(database);
    const cleanSchema = quoteNameIfNeeded(schema);
    const cleanTableName = quoteNameIfNeeded(tableName);

    const ddlLines: string[] = [];
    ddlLines.push(`CREATE EXTERNAL TABLE ${cleanDatabase}.${cleanSchema}.${cleanTableName}`);
    ddlLines.push('(');

    // Add columns
    const columnDefs = columns.map(col => {
        let def = `    ${quoteNameIfNeeded(col.name)} ${col.fullTypeName}`;
        if (col.notNull) {
            def += ' NOT NULL';
        }
        return def;
    });
    ddlLines.push(columnDefs.join(',\n'));
    ddlLines.push(')');

    ddlLines.push('USING');
    ddlLines.push('(');

    // Add external table options
    if (extInfo.dataObject !== null) {
        ddlLines.push(`    DATAOBJECT('${extInfo.dataObject}')`);
    }
    if (extInfo.delimiter !== null) {
        ddlLines.push(`    DELIMITER '${extInfo.delimiter}'`);
    }
    if (extInfo.encoding !== null) {
        ddlLines.push(`    ENCODING '${extInfo.encoding}'`);
    }
    if (extInfo.timeStyle !== null) {
        ddlLines.push(`    TIMESTYLE '${extInfo.timeStyle}'`);
    }
    if (extInfo.remoteSource !== null) {
        ddlLines.push(`    REMOTESOURCE '${extInfo.remoteSource}'`);
    }
    if (extInfo.maxErrors !== null) {
        ddlLines.push(`    MAXERRORS ${extInfo.maxErrors}`);
    }
    if (extInfo.escapeChar !== null) {
        ddlLines.push(`    ESCAPECHAR '${extInfo.escapeChar}'`);
    }
    if (extInfo.decimalDelim !== null) {
        ddlLines.push(`    DECIMALDELIM '${extInfo.decimalDelim}'`);
    }
    if (extInfo.logDir !== null) {
        ddlLines.push(`    LOGDIR '${extInfo.logDir}'`);
    }
    if (extInfo.quotedValue !== null) {
        ddlLines.push(`    QUOTEDVALUE '${extInfo.quotedValue}'`);
    }
    if (extInfo.nullValue !== null) {
        ddlLines.push(`    NULLVALUE '${extInfo.nullValue}'`);
    }
    if (extInfo.crInString !== null) {
        ddlLines.push(`    CRINSTRING ${extInfo.crInString}`);
    }
    if (extInfo.truncString !== null) {
        ddlLines.push(`    TRUNCSTRING ${extInfo.truncString}`);
    }
    if (extInfo.ctrlChars !== null) {
        ddlLines.push(`    CTRLCHARS ${extInfo.ctrlChars}`);
    }
    if (extInfo.ignoreZero !== null) {
        ddlLines.push(`    IGNOREZERO ${extInfo.ignoreZero}`);
    }
    if (extInfo.timeExtraZeros !== null) {
        ddlLines.push(`    TIMEEXTRAZEROS ${extInfo.timeExtraZeros}`);
    }
    if (extInfo.y2Base !== null) {
        ddlLines.push(`    Y2BASE ${extInfo.y2Base}`);
    }
    if (extInfo.fillRecord !== null) {
        ddlLines.push(`    FILLRECORD ${extInfo.fillRecord}`);
    }
    if (extInfo.compress !== null) {
        ddlLines.push(`    COMPRESS ${extInfo.compress}`);
    }
    if (extInfo.includeHeader !== null) {
        ddlLines.push(`    INCLUDEHEADER ${extInfo.includeHeader}`);
    }
    if (extInfo.lfInString !== null) {
        ddlLines.push(`    LFINSTRING ${extInfo.lfInString}`);
    }
    if (extInfo.dateStyle !== null) {
        ddlLines.push(`    DATESTYLE '${extInfo.dateStyle}'`);
    }
    if (extInfo.dateDelim !== null) {
        ddlLines.push(`    DATEDELIM '${extInfo.dateDelim}'`);
    }
    if (extInfo.timeDelim !== null) {
        ddlLines.push(`    TIMEDELIM '${extInfo.timeDelim}'`);
    }
    if (extInfo.boolStyle !== null) {
        ddlLines.push(`    BOOLSTYLE '${extInfo.boolStyle}'`);
    }
    if (extInfo.format !== null) {
        ddlLines.push(`    FORMAT '${extInfo.format}'`);
    }
    if (extInfo.socketBufSize !== null) {
        ddlLines.push(`    SOCKETBUFSIZE ${extInfo.socketBufSize}`);
    }
    if (extInfo.recordDelim !== null) {
        ddlLines.push(`    RECORDDELIM '${extInfo.recordDelim}'`);
    }
    if (extInfo.maxRows !== null) {
        ddlLines.push(`    MAXROWS ${extInfo.maxRows}`);
    }
    if (extInfo.requireQuotes !== null) {
        ddlLines.push(`    REQUIREQUOTES ${extInfo.requireQuotes}`);
    }
    if (extInfo.recordLength !== null) {
        ddlLines.push(`    RECORDLENGTH ${extInfo.recordLength}`);
    }
    if (extInfo.dateTimeDelim !== null) {
        ddlLines.push(`    DATETIMEDELIM '${extInfo.dateTimeDelim}'`);
    }
    if (extInfo.rejectFile !== null) {
        ddlLines.push(`    REJECTFILE '${extInfo.rejectFile}'`);
    }

    ddlLines.push(');');

    return ddlLines.join('\n');
}

/**
 * Generate DDL code for creating a synonym in Netezza
 */
async function generateSynonymDDL(
    connection: any,
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

    const result = await executeQueryHelper(connection, sql);
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

function parseConnectionString(connStr: string): any {
    const parts = connStr.split(';');
    const config: any = {};
    for (const part of parts) {
        const idx = part.indexOf('=');
        if (idx > 0) {
            const key = part.substring(0, idx).trim().toUpperCase();
            const value = part.substring(idx + 1).trim();
            if (key === 'SERVER') config.host = value;
            else if (key === 'PORT') config.port = parseInt(value);
            else if (key === 'DATABASE') config.database = value;
            else if (key === 'UID') config.user = value;
            else if (key === 'PWD') config.password = value;
        }
    }
    return config;
}

/**
 * Generate DDL code for a database object
 */
export async function generateDDL(
    connectionString: string,
    database: string,
    schema: string,
    objectName: string,
    objectType: string
): Promise<DDLResult> {
    let connection: any = null;

    try {
        const config = parseConnectionString(connectionString);
        if (!config.port) config.port = 5480;

        const NzConnection = require('../driver/dist/NzConnection');
        connection = new NzConnection(config);
        await connection.connect();

        const upperType = objectType.toUpperCase();

        if (upperType === 'TABLE') {
            const ddlCode = await generateTableDDL(connection, database, schema, objectName);
            return {
                success: true,
                ddlCode,
                objectInfo: { database, schema, objectName, objectType }
            };
        } else if (upperType === 'VIEW') {
            const ddlCode = await generateViewDDL(connection, database, schema, objectName);
            return {
                success: true,
                ddlCode,
                objectInfo: { database, schema, objectName, objectType }
            };
        } else if (upperType === 'PROCEDURE') {
            const ddlCode = await generateProcedureDDL(connection, database, schema, objectName);
            return {
                success: true,
                ddlCode,
                objectInfo: { database, schema, objectName, objectType }
            };
        } else if (upperType === 'EXTERNAL TABLE') {
            const ddlCode = await generateExternalTableDDL(connection, database, schema, objectName);
            return {
                success: true,
                ddlCode,
                objectInfo: { database, schema, objectName, objectType }
            };
        } else if (upperType === 'SYNONYM') {
            const ddlCode = await generateSynonymDDL(connection, database, schema, objectName);
            return {
                success: true,
                ddlCode,
                objectInfo: { database, schema, objectName, objectType }
            };
        } else {
            // For other object types, return placeholder (can be extended later)
            const ddlCode = `-- DDL generation for ${objectType} not yet implemented
-- Object: ${database}.${schema}.${objectName}
-- Type: ${objectType}
--
-- This feature can be extended to support:
-- - FUNCTION: Query _V_FUNCTION system table
-- - AGGREGATE: Query _V_AGGREGATE system table
`;
            return {
                success: true,
                ddlCode,
                objectInfo: { database, schema, objectName, objectType },
                note: `${objectType} DDL generation not yet implemented`
            };
        }

    } catch (e: any) {
        return {
            success: false,
            error: `DDL generation error: ${e.message || e}`
        };
    } finally {
        if (connection) {
            try {
                await connection.close();
            } catch {
                // Ignore close errors
            }
        }
    }
}

/**
 * Options for batch DDL generation
 */
export interface BatchDDLOptions {
    connectionString: string;
    database: string;
    schema?: string;           // Optional: filter by specific schema
    objectTypes?: string[];    // Optional: filter by object types, e.g. ['TABLE', 'VIEW']
}

/**
 * Result of batch DDL generation
 */
export interface BatchDDLResult {
    success: boolean;
    ddlCode?: string;
    objectCount: number;
    errors: string[];
    skipped: number;
}

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

        const NzConnection = require('../driver/dist/NzConnection');
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
            } catch (e: any) {
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
            } catch (e: any) {
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
                            'p': 'PRIMARY KEY', 'f': 'FOREIGN KEY', 'u': 'UNIQUE'
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
            } catch (e: any) {
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
            } catch (e: any) {
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
                                database, obj.schema, obj.name,
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
                    errors.push(`Error generating DDL for ${objType} ${database}.${obj.schema}.${obj.name}: ${e.message}`);
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
            try { await connection.close(); } catch { }
        }
    }
}

/**
 * Build table DDL from pre-fetched cache data (no DB queries)
 */
function buildTableDDLFromCache(
    database: string,
    schema: string,
    tableName: string,
    columns: ColumnInfo[],
    distributionColumns: string[],
    organizeColumns: string[],
    keysInfo: Map<string, KeyInfo>,
    tableComment: string | null
): string {
    if (columns.length === 0) {
        return `-- Table ${database}.${schema}.${tableName} has no columns or was not found`;
    }

    const cleanDatabase = quoteNameIfNeeded(database);
    const cleanSchema = quoteNameIfNeeded(schema);
    const cleanTableName = quoteNameIfNeeded(tableName);

    const ddlLines: string[] = [];
    ddlLines.push(`CREATE TABLE ${cleanDatabase}.${cleanSchema}.${cleanTableName}`);
    ddlLines.push('(');

    // Columns
    const columnDefs: string[] = [];
    for (const column of columns) {
        const cleanColumnName = quoteNameIfNeeded(column.name);
        let colDef = `    ${cleanColumnName} ${column.fullTypeName}`;
        if (column.notNull) colDef += ' NOT NULL';
        if (column.defaultValue !== null) colDef += ` DEFAULT ${column.defaultValue}`;
        columnDefs.push(colDef);
    }
    ddlLines.push(columnDefs.join(',\n'));

    // Distribution
    if (distributionColumns.length > 0) {
        const cleanDistCols = distributionColumns.map(c => quoteNameIfNeeded(c));
        ddlLines.push(`)\nDISTRIBUTE ON (${cleanDistCols.join(', ')})`);
    } else {
        ddlLines.push(')\nDISTRIBUTE ON RANDOM');
    }

    // Organize
    if (organizeColumns.length > 0) {
        const cleanOrgCols = organizeColumns.map(c => quoteNameIfNeeded(c));
        ddlLines.push(`ORGANIZE ON (${cleanOrgCols.join(', ')})`);
    }

    ddlLines.push(';');
    ddlLines.push('');

    // Keys
    for (const [keyName, keyInfo] of keysInfo) {
        const cleanKeyName = quoteNameIfNeeded(keyName);
        const cleanColumns = keyInfo.columns.map(c => quoteNameIfNeeded(c));

        if (keyInfo.typeChar === 'f') {
            const cleanPkCols = keyInfo.pkColumns.filter(c => c).map(c => quoteNameIfNeeded(c));
            if (cleanPkCols.length > 0) {
                ddlLines.push(
                    `ALTER TABLE ${cleanDatabase}.${cleanSchema}.${cleanTableName} ` +
                    `ADD CONSTRAINT ${cleanKeyName} ${keyInfo.type} ` +
                    `(${cleanColumns.join(', ')}) ` +
                    `REFERENCES ${keyInfo.pkDatabase}.${keyInfo.pkSchema}.${keyInfo.pkRelation} ` +
                    `(${cleanPkCols.join(', ')}) ` +
                    `ON DELETE ${keyInfo.deleteType} ON UPDATE ${keyInfo.updateType};`
                );
            }
        } else if (keyInfo.typeChar === 'p' || keyInfo.typeChar === 'u') {
            ddlLines.push(
                `ALTER TABLE ${cleanDatabase}.${cleanSchema}.${cleanTableName} ` +
                `ADD CONSTRAINT ${cleanKeyName} ${keyInfo.type} ` +
                `(${cleanColumns.join(', ')});`
            );
        }
    }

    // Table comment
    if (tableComment) {
        const cleanComment = tableComment.replace(/'/g, "''");
        ddlLines.push('');
        ddlLines.push(`COMMENT ON TABLE ${cleanDatabase}.${cleanSchema}.${cleanTableName} IS '${cleanComment}';`);
    }

    // Column comments
    for (const column of columns) {
        if (column.description) {
            const cleanColumnName = quoteNameIfNeeded(column.name);
            const cleanDesc = column.description.replace(/'/g, "''");
            ddlLines.push(
                `COMMENT ON COLUMN ${cleanDatabase}.${cleanSchema}.${cleanTableName}.${cleanColumnName} IS '${cleanDesc}';`
            );
        }
    }

    return ddlLines.join('\n');
}

