import * as vscode from 'vscode';
import * as fs from 'fs';

let odbc: any;
try {
    odbc = require('odbc');
} catch (err) {
    console.error('odbc package not installed. Run: npm install odbc');
}

export async function exportToCsv(
    context: vscode.ExtensionContext,
    connectionString: string,
    query: string,
    filePath: string,
    progress?: vscode.Progress<{ message?: string; increment?: number }>
): Promise<void> {
    if (!odbc) {
        throw new Error('odbc package not installed. Please run: npm install odbc');
    }

    const connection = await odbc.connect(connectionString);

    try {
        if (progress) {
            progress.report({ message: 'Executing query...' });
        }

        // Use cursor-based query to stream results row-by-row instead of loading all into memory
        // fetchSize determines how many rows are fetched per batch from the database
        const cursor = await connection.query(query, { cursor: true, fetchSize: 1000 });

        if (progress) {
            progress.report({ message: 'Writing to CSV...' });
        }

        // Use larger buffer for better write performance
        const writeStream = fs.createWriteStream(filePath, {
            encoding: 'utf8',
            highWaterMark: 64 * 1024 // 64KB buffer
        });

        // Get headers from cursor columns
        let headers: string[] = [];
        if (cursor.columns) {
            headers = cursor.columns.map((col: any) => col.name);
        }

        // Write headers
        if (headers.length > 0) {
            writeStream.write(headers.map(escapeCsvField).join(',') + '\n');
        }

        // Stream rows from cursor with buffering for better performance
        let totalRows = 0;
        let batch: any[] = [];
        let rowBuffer: string[] = []; // Buffer multiple rows before writing
        const BUFFER_SIZE = 100; // Write every 100 rows

        // Fetch rows in batches using cursor
        do {
            batch = await cursor.fetch();

            for (const row of batch) {
                totalRows++;

                // Build row string
                let rowValues: string[];
                if (headers.length > 0) {
                    rowValues = headers.map(header => escapeCsvField(row[header]));
                } else {
                    rowValues = Object.values(row).map(val => escapeCsvField(val));
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
                }
            }

            if (progress && batch.length > 0) {
                progress.report({ message: `Processed ${totalRows} rows...` });
            }

        } while (batch.length > 0 && !cursor.noData);

        // Write remaining buffered rows
        if (rowBuffer.length > 0) {
            writeStream.write(rowBuffer.join('\n') + '\n');
        }

        // Close cursor
        await cursor.close();

        writeStream.end();

        await new Promise<void>((resolve, reject) => {
            writeStream.on('finish', resolve);
            writeStream.on('error', reject);
        });

        if (progress) {
            progress.report({ message: `Completed: ${totalRows} rows exported` });
        }

    } finally {
        try {
            await connection.close();
        } catch (e) {
            console.error('Error closing connection:', e);
        }
    }
}

function escapeCsvField(field: any): string {
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
        // Format date as ISO string or similar, matching Python's default behavior if possible
        // Python's default str(date) is usually YYYY-MM-DD HH:MM:SS or similar. 
        // ISO is safe.
        stringValue = field.toISOString();
    } else if (field instanceof Buffer) {
        // Handle binary data as hex string
        stringValue = field.toString('hex');
    } else if (typeof field === 'object') {
        stringValue = JSON.stringify(field);
    } else {
        stringValue = String(field);
    }

    // Escape quotes
    if (stringValue.includes('"') || stringValue.includes(',') || stringValue.includes('\n') || stringValue.includes('\r')) {
        return `"${stringValue.replace(/"/g, '""')}"`;
    }

    return stringValue;
}
