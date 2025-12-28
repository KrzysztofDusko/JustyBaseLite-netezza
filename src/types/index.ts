
export interface QueryResult {
    columns: ColumnDefinition[];
    data: unknown[][];
    rowsAffected?: number;
    message?: string;
    limitReached?: boolean;
    sql?: string;
    // UI specific fields
    isLog?: boolean;
    executionTimestamp?: number;
    name?: string;
}

export type ResultSet = QueryResult;

export interface ColumnDefinition {
    name: string;
    type?: string;
}

export interface ConnectionDetails {
    name?: string;
    host: string;
    port?: number;
    database: string;
    user: string;
    password?: string;
    dbType?: string;
}

// Placeholder for the Netezza driver types if we can't import them
export interface NzConnection {
    connect(): Promise<void>;
    close(): Promise<void>;
    createCommand(sql: string): NzCommand;
    on(event: string, listener: (arg: unknown) => void): void;
    removeListener(event: string, listener: (arg: unknown) => void): void;
    _connected?: boolean;
}

export interface NzCommand {
    commandTimeout: number;
    executeReader(): Promise<NzDataReader>;
    cancel(): Promise<void>;
    execute(): Promise<void>;
}

export interface NzDataReader {
    read(): Promise<boolean>;
    nextResult(): Promise<boolean>;
    close(): Promise<void>;
    fieldCount: number;
    getName(i: number): string;
    getTypeName(i: number): string;
    getValue(i: number): unknown;
}
