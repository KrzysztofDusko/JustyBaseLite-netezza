import * as vscode from 'vscode';
import * as fs from 'fs';

// import * as odbc from 'odbc'; // Removed odbc dependency

import { NzConnection, ConnectionDetails } from '../types';

// ConnectionDetails used directly - no parseConnectionString needed

export async function exportToCsv(
    _context: vscode.ExtensionContext,
    connectionDetails: ConnectionDetails,
    query: string,
    filePath: string,
    progress?: vscode.Progress<{ message?: string; increment?: number }>,
    timeout?: number
): Promise<void> {
    let connection: NzConnection | null = null;

    try {
        if (progress) {
            progress.report({ message: 'Connecting to database...' });
        }

        const config = {
            host: connectionDetails.host,
            port: connectionDetails.port || 5480,
            database: connectionDetails.database,
            user: connectionDetails.user,
            password: connectionDetails.password
        };

        const NzConnection = require('../../libs/driver/src/NzConnection');
        connection = new NzConnection(config) as NzConnection;
        await connection.connect();

        if (progress) {
            progress.report({ message: 'Executing query...' });
        }

        // executeReader returns a reader that allows streaming rows
        const cmd = connection!.createCommand(query);
        if (timeout) {
            cmd.commandTimeout = timeout;
        }
        const reader = await cmd.executeReader();

        if (progress) {
            progress.report({ message: 'Writing to CSV...' });
        }

        // Use larger buffer for better write performance
        const writeStream = fs.createWriteStream(filePath, {
            encoding: 'utf8',
            highWaterMark: 64 * 1024 // 64KB buffer
        });

        // Get headers
        const headers: string[] = [];
        for (let i = 0; i < reader.fieldCount; i++) {
            headers.push(reader.getName(i));
        }

        // Write headers
        if (headers.length > 0) {
            writeStream.write(headers.map(escapeCsvField).join(',') + '\n');
        }

        // Stream rows
        let totalRows = 0;
        let rowBuffer: string[] = []; // Buffer multiple rows before writing
        const BUFFER_SIZE = 500; // Increased buffer size

        while (await reader.read()) {
            totalRows++;

            // Build row string
            const rowValues: string[] = [];
            for (let i = 0; i < reader.fieldCount; i++) {
                rowValues.push(escapeCsvField(reader.getValue(i)));
            }

            rowBuffer.push(rowValues.join(','));

            // Write buffer when it reaches BUFFER_SIZE
            if (rowBuffer.length >= BUFFER_SIZE) {
                const canWrite = writeStream.write(rowBuffer.join('\n') + '\n');
                rowBuffer = []; // Clear buffer

                // Handle backpressure
                if (!canWrite) {
                    await new Promise<void>(resolve => writeStream.once('drain', resolve));
                }

                if (progress && totalRows % 1000 === 0) {
                    progress.report({ message: `Processed ${totalRows} rows...` });
                }
            }
        }

        // Write remaining buffered rows
        if (rowBuffer.length > 0) {
            writeStream.write(rowBuffer.join('\n') + '\n');
        }

        writeStream.end();

        await new Promise<void>((resolve, reject) => {
            writeStream.on('finish', resolve);
            writeStream.on('error', reject);
        });

        if (progress) {
            progress.report({ message: `Completed: ${totalRows} rows exported` });
        }
    } finally {
        if (connection) {
            try {
                await connection.close();
            } catch (e: unknown) {
                console.error('Error closing connection:', e);
            }
        }
    }
}

function escapeCsvField(field: unknown): string {
    if (field === null || field === undefined) {
        return '';
    }

    let stringValue = '';
    if (typeof field === 'bigint') {
        if (field >= Number.MIN_SAFE_INTEGER && field <= Number.MAX_SAFE_INTEGER) {
            stringValue = Number(field).toString();
        } else {
            stringValue = field.toString();
        }
    } else if (field instanceof Date) {
        // Format date as ISO string
        stringValue = field.toISOString();
    } else if (typeof field === 'object' && Buffer.isBuffer(field)) {
        // Handle binary data as hex string
        stringValue = field.toString('hex');
    } else if (typeof field === 'object') {
        stringValue = JSON.stringify(field);
    } else {
        stringValue = String(field);
    }

    // Escape quotes
    if (
        stringValue.includes('"') ||
        stringValue.includes(',') ||
        stringValue.includes('\n') ||
        stringValue.includes('\r')
    ) {
        return `"${stringValue.replace(/"/g, '""')}"`;
    }

    return stringValue;
}
