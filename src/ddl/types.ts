/**
 * DDL Generator - Type Definitions
 */

/**
 * Column information for DDL generation
 */
export interface ColumnInfo {
    name: string;
    description: string | null;
    fullTypeName: string;
    notNull: boolean;
    defaultValue: string | null;
}

/**
 * Key/constraint information
 */
export interface KeyInfo {
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

/**
 * Result of DDL generation
 */
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
 * Procedure information for DDL generation
 */
export interface ProcedureInfo {
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
 * External table information for DDL generation
 */
export interface ExternalTableInfo {
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
 * Options for batch DDL generation
 */
export interface BatchDDLOptions {
    connectionString: string;
    database: string;
    schema?: string;
    objectTypes?: string[];
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
