/**
 * DDL Generator - Helper Functions
 */

/**
 * Execute query and return array of objects (shim for NzConnection)
 */
export async function executeQueryHelper(connection: any, sql: string): Promise<any[]> {
    const cmd = connection.createCommand(sql);
    const reader = await cmd.executeReader();
    const results: any[] = [];

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
 * Parse ODBC-style connection string into config object
 */
export function parseConnectionString(connStr: string): {
    host?: string;
    port?: number;
    database?: string;
    user?: string;
    password?: string;
} {
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
 * Fix Netezza procedure return type syntax for ANY length types
 */
export function fixProcReturnType(procReturns: string): string {
    if (!procReturns) return procReturns;

    const upper = procReturns.trim().toUpperCase();
    if (upper === 'CHARACTER VARYING') {
        return 'CHARACTER VARYING(ANY)';
    } else if (upper === 'NATIONAL CHARACTER VARYING') {
        return 'NATIONAL CHARACTER VARYING(ANY)';
    } else if (upper === 'NATIONAL CHARACTER') {
        return 'NATIONAL CHARACTER(ANY)';
    } else if (upper === 'CHARACTER') {
        return 'CHARACTER(ANY)';
    }
    return procReturns;
}

/**
 * Create NzConnection from connection string
 */
export async function createConnection(connectionString: string): Promise<any> {
    const config = parseConnectionString(connectionString);
    if (!config.port) config.port = 5480;

    const NzConnection = require('../../driver/dist/NzConnection');
    const connection = new NzConnection(config);
    await connection.connect();
    return connection;
}
