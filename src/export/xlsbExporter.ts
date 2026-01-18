import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as vscode from 'vscode';
import { spawn } from 'child_process';
const XlsbWriter = require('../../libs/ExcelHelpersTs/XlsbWriter').default as new (filePath: string) => {
    addSheet(sheetName: string, hidden?: boolean): void;
    writeSheet(rows: unknown[][], headers: string[] | null, doAutofilter?: boolean): void;
    // Streaming API methods
    startSheet(sheetName: string, columnCount: number, headers?: string[], options?: { hidden?: boolean; doAutofilter?: boolean }): void;
    writeRow(row: unknown[]): void;
    endSheet(): void;
    finalize(): Promise<void>;
};
import { NzConnection, ConnectionDetails } from '../types';


/**
 * Convert a value to number if it's a numeric string (for proper Excel formatting).
 * This handles numeric/decimal types that may be returned as strings for precision preservation.
 * @param val - Value to potentially convert
 * @returns The value as number if it's a numeric string, otherwise the original value
 */
function convertToNumberIfNumericString(val: unknown): unknown {
    if (typeof val === 'string' && val.length > 0) {
        // PRECISION SAFETY: Don't convert very long strings to numbers
        if (val.length > 15) {
            return val;
        }

        // LEADING ZERO SAFETY: Don't convert strings with leading zeros (e.g. "0123")
        // unless it is exactly "0" or a decimal starting with "0." (e.g. "0.123")
        // Regex checks for: Optional sign, then "0", then at least one more digit.
        if (/^-?0\d+/.test(val)) {
            return val;
        }

        // Check if it's a numeric string (including negatives and decimals)
        // Match: optional minus, digits, optional decimal part
        if (/^-?\d+(\.\d+)?$/.test(val)) {
            const num = parseFloat(val);
            // Only convert if it's a finite number
            if (Number.isFinite(num)) {
                return num;
            }
        }
    }
    return val;
}

/**
 * Convert all numeric strings in a row to numbers for proper Excel export
 * @param row - Array of values
 * @returns New array with numeric strings converted to numbers
 */
function convertRowNumericStrings(row: unknown[]): unknown[] {
    return row.map(convertToNumberIfNumericString);
}


/**
 * Progress callback function type
 */
export type ProgressCallback = (message: string) => void;

/**
 * Export result interface
 */
export interface ExportResult {
    success: boolean;
    message: string;
    details?: {
        rows_exported: number;
        columns: number;
        file_size_mb: number;
        file_path: string;
        clipboard_success?: boolean;
    };
}

// ConnectionDetails used directly - no parseConnectionString needed

/**
 * Export SQL query results to XLSB file
 * @param connectionDetails Database connection details
 * @param query SQL query to execute
 * @param outputPath Path where to save the XLSB file
 * @param copyToClipboard If true, also copy file to clipboard (Windows only)
 * @param progressCallback Optional callback for progress updates
 * @param timeout Optional query timeout in seconds
 * @param cancellationToken Optional cancellation token to abort export
 * @returns Export result with success status and details
 */
export async function exportQueryToXlsb(
    connectionDetails: ConnectionDetails,
    query: string,
    outputPath: string,
    copyToClipboard: boolean = false,
    progressCallback?: ProgressCallback,
    timeout?: number,
    cancellationToken?: vscode.CancellationToken
): Promise<ExportResult> {
    let connection: NzConnection | null = null;

    try {
        // Check cancellation before starting
        if (cancellationToken?.isCancellationRequested) {
            throw new Error('Export cancelled by user');
        }

        // Connect to database
        if (progressCallback) {
            progressCallback('Connecting to database...');
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

        // Use XlsbWriter
        const writer = new XlsbWriter(outputPath);

        // Split queries
        const { SqlParser } = await import('../sql/sqlParser');
        const queries = SqlParser.splitStatements(query);

        let totalRows = 0;
        let totalCols = 0;
        let wasCancelled = false;

        for (let qIndex = 0; qIndex < queries.length; qIndex++) {
            const currentQuery = queries[qIndex];
            if (!currentQuery.trim()) continue;

            const sheetName = queries.length > 1 ? `Result ${qIndex + 1}` : 'Query Results';

            if (progressCallback) {
                progressCallback(`Executing query ${qIndex + 1}/${queries.length}...`);
            }

            try {
                const cmd = connection!.createCommand(currentQuery);
                if (timeout) {
                    cmd.commandTimeout = timeout;
                }

                // Set up cancellation listener to cancel the SQL command when user clicks Cancel
                let cancelListener: vscode.Disposable | undefined;
                if (cancellationToken) {
                    cancelListener = cancellationToken.onCancellationRequested(async () => {
                        wasCancelled = true;
                        if (progressCallback) {
                            progressCallback('Cancelling query...');
                        }
                        try {
                            await cmd.cancel();
                        } catch (cancelErr) {
                            console.error('Error cancelling command:', cancelErr);
                        }
                    });
                }

                try {
                    const reader = await cmd.executeReader();

                // Prepare headers for XLSB
                const headers: string[] = [];
                const colIsNumeric: boolean[] = [];

                // Numeric types whitelist
                const numericTypes = new Set(['INT8', 'INT2', 'INT4', 'NUMERIC', 'FLOAT4', 'FLOAT8']);

                for (let i = 0; i < reader.fieldCount; i++) {
                    const colName = reader.getName(i);
                    headers.push(colName);

                    // Determine if column is numeric based on database type
                    try {
                        const typeName = reader.getTypeName(i);
                        colIsNumeric.push(numericTypes.has(typeName));
                    } catch {
                        // Fallback to safe default (treat as string) if type unknown
                        colIsNumeric.push(false);
                    }
                }

                const columnCount = headers.length;
                totalCols = Math.max(totalCols, columnCount);

                // Use streaming API - start sheet with headers
                writer.startSheet(sheetName, columnCount, headers, { doAutofilter: true });

                let rowCount = 0;

                try {
                    // Stream rows directly from database reader to Excel writer
                    while (await reader.read()) {
                        // Check for cancellation - wasCancelled is set by the listener
                        if (wasCancelled || cancellationToken?.isCancellationRequested) {
                            wasCancelled = true;
                            if (progressCallback) {
                                progressCallback(`Export cancelled - finalizing ${rowCount.toLocaleString()} rows...`);
                            }
                            break; // Exit loop but continue to finalize
                        }

                        const row: unknown[] = [];
                        for (let i = 0; i < reader.fieldCount; i++) {
                            let val = reader.getValue(i);

                            // Apply conversion ONLY if column is strictly numeric in database
                            if (colIsNumeric[i]) {
                                val = convertToNumberIfNumericString(val);
                            }

                            row.push(val);
                        }
                        // Write row immediately
                        writer.writeRow(row);
                        rowCount++;

                        // Every 500 rows: yield to event loop to allow cancellation callback to execute
                        // This is critical because the loop is so tight that callbacks never get a chance
                        if (rowCount % 500 === 0) {
                            await new Promise(resolve => setImmediate(resolve));
                            
                            // Check again after yielding
                            if (wasCancelled) {
                                if (progressCallback) {
                                    progressCallback(`Export cancelled - finalizing ${rowCount.toLocaleString()} rows...`);
                                }
                                break;
                            }
                        }

                        // Progress update every 10000 rows
                        if (rowCount % 10000 === 0 && progressCallback) {
                            progressCallback(`Streaming ${rowCount.toLocaleString()} rows to "${sheetName}"...`);
                        }
                    }
                } catch (readErr: unknown) {
                    // Handle read errors (including cancellation) but still finalize the sheet with partial data
                    const readErrMsg = readErr instanceof Error ? readErr.message : String(readErr);
                    // Don't log cancellation as error
                    if (!wasCancelled && progressCallback) {
                        progressCallback(`Read error after ${rowCount} rows: ${readErrMsg}`);
                    }
                }

                // Finalize the sheet (always, even if cancelled)
                writer.endSheet();

                totalRows += rowCount;

                if (progressCallback) {
                    const status = wasCancelled ? '(cancelled)' : '';
                    progressCallback(`Written ${rowCount.toLocaleString()} rows to sheet "${sheetName}" ${status}`);
                }

                // If cancelled, stop processing further queries
                if (wasCancelled) {
                    break;
                }
                } finally {
                    // Clean up cancellation listener
                    if (cancelListener) {
                        cancelListener.dispose();
                    }
                }
            } catch (err: unknown) {
                // If one query fails, create an error sheet
                const errorMsg = err instanceof Error ? err.message : String(err);
                writer.startSheet(`Error ${qIndex + 1}`, 1, ['Error'], { doAutofilter: false });
                writer.writeRow([`Error executing query: ${errorMsg}`]);
                writer.endSheet();
            }
        }

        // Final sheet: SQL Code (using streaming API)
        const sqlLines = query.split('\n');
        writer.startSheet('SQL Code', 1, undefined, { doAutofilter: false });
        writer.writeRow(['SQL Query:']);
        for (const line of sqlLines) {
            writer.writeRow([line]);
        }
        writer.endSheet();

        if (progressCallback) {
            progressCallback('Finalizing file...');
        }
        await writer.finalize();

        // Check file size
        const stats = fs.statSync(outputPath);
        const fileSizeMb = stats.size / (1024 * 1024);

        if (progressCallback) {
            if (wasCancelled) {
                progressCallback(`XLSB file created with partial data (export was cancelled)`);
            } else {
                progressCallback(`XLSB file created successfully`);
            }
            progressCallback(`  - Total Rows: ${totalRows.toLocaleString()}`);
            progressCallback(`  - File size: ${fileSizeMb.toFixed(1)} MB`);
            progressCallback(`  - Location: ${outputPath}`);
        }

        const exportResult: ExportResult = {
            success: true,
            message: wasCancelled 
                ? `Export cancelled - saved ${totalRows} rows to ${outputPath}`
                : `Successfully exported ${totalRows} rows to ${outputPath}`,
            details: {
                rows_exported: totalRows,
                columns: totalCols,
                file_size_mb: parseFloat(fileSizeMb.toFixed(1)),
                file_path: outputPath
            }
        };

        // Copy to clipboard if requested
        if (copyToClipboard) {
            if (progressCallback) {
                progressCallback('Copying to clipboard...');
            }
            const clipboardSuccess = await copyFileToClipboard(outputPath);
            if (exportResult.details) {
                exportResult.details.clipboard_success = clipboardSuccess;
            }
        }

        return exportResult;
    } catch (error: unknown) {
        const errorMsg = error instanceof Error ? error.message : String(error);
        return {
            success: false,
            message: `Export error: ${errorMsg}`
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
 * Export CSV content to XLSB file
 * @param csvContent CSV content as string
 * @param outputPath Path where to save the XLSB file
 * @param copyToClipboard If true, also copy file to clipboard (Windows only)
 * @param metadata Optional metadata (source info, etc.)
 * @param progressCallback Optional callback for progress updates
 * @returns Export result with success status and details
 */
export interface CsvExportItem {
    csv: string;
    sql?: string; // Made optional as not all items might have SQL
    name: string;
}

/**
 * Structured export item with column types (used by Grid export)
 */
export interface StructuredExportItem {
    columns: { name: string; type?: string }[];
    rows: unknown[][];
    sql?: string;
    name: string;
}

/** Numeric types whitelist for type-aware conversion */
const numericTypes = new Set(['INT8', 'INT2', 'INT4', 'NUMERIC', 'FLOAT4', 'FLOAT8', 'INTEGER', 'BIGINT', 'SMALLINT', 'DECIMAL', 'REAL', 'DOUBLE PRECISION']);

/** Check if a value should be converted to number based on column type */
function shouldConvertToNumber(type?: string): boolean {
    if (!type) return false;
    return numericTypes.has(type.toUpperCase());
}

export async function exportCsvToXlsb(
    csvContent: string | CsvExportItem[],
    outputPath: string,
    copyToClipboard: boolean = false,
    metadata: { source: string; sql?: string } = { source: 'Unknown' },
    progressCallback?: ProgressCallback
): Promise<ExportResult> {
    try {
        if (progressCallback) {
            progressCallback('Initializing XLSB writer...');
        }

        const writer = new XlsbWriter(outputPath);

        let totalRows = 0;
        let totalColumns = 0;
        const sqlItems: { name: string; sql: string }[] = [];

        // Helper to process a single CSV string using streaming API
        const processCsv = (csv: string, sheetName: string) => {
            const lines = csv.split(/\r?\n/); // Handle both \n and \r\n

            // Simple regex parser for CSV lines
            const parseCsvLine = (line: string): string[] => {
                const result = [];
                let start = 0;
                let inQuotes = false;
                for (let i = 0; i < line.length; i++) {
                    if (line[i] === '"') {
                        inQuotes = !inQuotes;
                    } else if (line[i] === ',' && !inQuotes) {
                        let field = line.substring(start, i);
                        // Unescape quotes
                        if (field.startsWith('"') && field.endsWith('"')) {
                            field = field.substring(1, field.length - 1).replace(/""/g, '"');
                        }
                        result.push(field);
                        start = i + 1;
                    }
                }
                // Last field
                let field = line.substring(start);
                if (field.startsWith('"') && field.endsWith('"')) {
                    field = field.substring(1, field.length - 1).replace(/""/g, '"');
                }
                result.push(field);
                return result;
            };

            // Find first non-empty line for headers
            let headerIndex = 0;
            while (headerIndex < lines.length && !lines[headerIndex].trim()) {
                headerIndex++;
            }

            if (headerIndex >= lines.length) return;

            const headers = parseCsvLine(lines[headerIndex]);
            if (headers.length === 0) return;

            totalColumns = Math.max(totalColumns, headers.length);

            // Start streaming sheet with headers
            writer.startSheet(sheetName, headers.length, headers, { doAutofilter: true });

            let currentRowCount = 0;

            // Stream data rows
            for (let i = headerIndex + 1; i < lines.length; i++) {
                const line = lines[i];
                if (!line.trim()) continue;

                const fields = parseCsvLine(line);
                // Convert numeric strings to numbers and write immediately
                writer.writeRow(convertRowNumericStrings(fields));
                currentRowCount++;
            }

            writer.endSheet();
            totalRows += currentRowCount;
        };

        if (Array.isArray(csvContent)) {
            // Multiple results
            if (progressCallback) {
                progressCallback(`Processing ${csvContent.length} result sets...`);
            }

            csvContent.forEach((item, index) => {
                const sheetName = item.name || `Result ${index + 1}`;
                if (progressCallback) {
                    progressCallback(`Processing sheet "${sheetName}"...`);
                }
                processCsv(item.csv, sheetName);
                if (item.sql) {
                    sqlItems.push({ name: sheetName, sql: item.sql });
                }
            });
        } else {
            // Single result (legacy)
            if (progressCallback) {
                progressCallback('Reading CSV content...');
            }
            processCsv(csvContent, 'Query Results');
            if (metadata.sql) {
                sqlItems.push({ name: 'Query Results', sql: metadata.sql });
            } else if (metadata.source) {
                // If no SQL, but a source, add a source sheet
                writer.startSheet('CSV Source', 1, undefined, { doAutofilter: false });
                writer.writeRow(['CSV Source:']);
                writer.writeRow([metadata.source]);
                writer.endSheet();
            }
        }

        // Add SQL Code sheet if we have any SQL (using streaming API)
        if (sqlItems.length > 0) {
            writer.startSheet('SQL Code', 1, undefined, { doAutofilter: false });

            sqlItems.forEach(item => {
                writer.writeRow([`--- SQL for ${item.name} ---`]);
                // Split multiline SQL
                item.sql.split('\n').forEach(line => {
                    writer.writeRow([line]);
                });
                writer.writeRow(['']); // Spacer
            });

            writer.endSheet();
        }

        if (progressCallback) {
            progressCallback('Finalizing XLSB file...');
        }
        await writer.finalize();

        // Check file size
        const stats = fs.statSync(outputPath);
        const fileSizeMb = stats.size / (1024 * 1024);

        if (progressCallback) {
            progressCallback(`XLSB file created successfully`);
            progressCallback(`  - Rows: ${totalRows.toLocaleString()}`);
            progressCallback(`  - File size: ${fileSizeMb.toFixed(1)} MB`);
        }

        const exportResult: ExportResult = {
            success: true,
            message: `Successfully exported ${totalRows} rows to ${outputPath}`,
            details: {
                rows_exported: totalRows,
                columns: totalColumns,
                file_size_mb: parseFloat(fileSizeMb.toFixed(1)),
                file_path: outputPath
            }
        };

        // Copy to clipboard logic if needed
        if (copyToClipboard) {
            if (progressCallback) {
                progressCallback('Copying file to clipboard...');
            }
            const clipboardSuccess = await copyFileToClipboard(outputPath);
            if (exportResult.details) {
                exportResult.details.clipboard_success = clipboardSuccess;
            }
        }

        return exportResult;
    } catch (err: unknown) {
        const errorMsg = err instanceof Error ? err.message : String(err);
        return {
            success: false,
            message: `Export failed: ${errorMsg}`
        };
    }
}

/**
 * Export structured data (with column types) to XLSB file.
 * This is used by Grid exports where we have type metadata from the database.
 * @param items Structured export items with columns, rows, and type metadata
 * @param outputPath Path where to save the XLSB file
 * @param copyToClipboard If true, also copy file to clipboard (Windows only)
 * @param progressCallback Optional callback for progress updates
 * @returns Export result with success status and details
 */
export async function exportStructuredToXlsb(
    items: StructuredExportItem[],
    outputPath: string,
    copyToClipboard: boolean = false,
    progressCallback?: ProgressCallback
): Promise<ExportResult> {
    try {
        if (progressCallback) {
            progressCallback('Initializing XLSB writer...');
        }

        const writer = new XlsbWriter(outputPath);
        let totalRows = 0;
        let totalColumns = 0;
        const sqlItems: { name: string; sql: string }[] = [];

        for (const item of items) {
            const sheetName = item.name || 'Sheet';
            if (progressCallback) {
                progressCallback(`Processing sheet "${sheetName}"...`);
            }

            const headers = item.columns.map(c => c.name);
            const colIsNumeric = item.columns.map(c => shouldConvertToNumber(c.type));

            totalColumns = Math.max(totalColumns, headers.length);

            // Start sheet with headers
            writer.startSheet(sheetName, headers.length, headers, { doAutofilter: true });

            // Write rows with type-aware conversion
            for (const row of item.rows) {
                const processedRow = row.map((val, i) => {
                    if (colIsNumeric[i]) {
                        // Column is numeric type - apply conversion
                        return convertToNumberIfNumericString(val);
                    }
                    // Column is text type - keep as is
                    return val;
                });
                writer.writeRow(processedRow);
                totalRows++;
            }

            writer.endSheet();

            if (item.sql) {
                sqlItems.push({ name: sheetName, sql: item.sql });
            }
        }

        // Add SQL Code sheet if we have any SQL
        if (sqlItems.length > 0) {
            writer.startSheet('SQL Code', 1, undefined, { doAutofilter: false });
            sqlItems.forEach(item => {
                writer.writeRow([`--- SQL for ${item.name} ---`]);
                item.sql.split('\n').forEach(line => {
                    writer.writeRow([line]);
                });
                writer.writeRow(['']); // Spacer
            });
            writer.endSheet();
        }

        if (progressCallback) {
            progressCallback('Finalizing XLSB file...');
        }
        await writer.finalize();

        const stats = fs.statSync(outputPath);
        const fileSizeMb = stats.size / (1024 * 1024);

        if (progressCallback) {
            progressCallback(`XLSB file created successfully`);
            progressCallback(`  - Rows: ${totalRows.toLocaleString()}`);
            progressCallback(`  - File size: ${fileSizeMb.toFixed(1)} MB`);
        }

        const exportResult: ExportResult = {
            success: true,
            message: `Successfully exported ${totalRows} rows to ${outputPath}`,
            details: {
                rows_exported: totalRows,
                columns: totalColumns,
                file_size_mb: parseFloat(fileSizeMb.toFixed(1)),
                file_path: outputPath
            }
        };

        if (copyToClipboard) {
            if (progressCallback) {
                progressCallback('Copying file to clipboard...');
            }
            const clipboardSuccess = await copyFileToClipboard(outputPath);
            if (exportResult.details) {
                exportResult.details.clipboard_success = clipboardSuccess;
            }
        }

        return exportResult;
    } catch (err: unknown) {
        const errorMsg = err instanceof Error ? err.message : String(err);
        return {
            success: false,
            message: `Export failed: ${errorMsg}`
        };
    }
}

/**
 * Copy file to Windows clipboard using PowerShell
 * This allows pasting the file in Windows Explorer or other applications
 * @param filePath Absolute path to the file to copy
 * @returns True if successful, false otherwise
 */
export async function copyFileToClipboard(filePath: string): Promise<boolean> {
    // Only works on Windows
    if (os.platform() !== 'win32') {
        console.error('Clipboard file copy is only supported on Windows');
        return false;
    }

    return new Promise<boolean>(resolve => {
        try {
            const normalizedPath = path.normalize(path.resolve(filePath));

            // Use PowerShell to copy file to clipboard
            // The Set-Clipboard -Path command copies the file object, not the content
            const powershellCommand = `Set-Clipboard -Path "${normalizedPath.replace(/"/g, '`"')}"`;

            const ps = spawn('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', powershellCommand]);

            let errorOutput = '';

            ps.stderr.on('data', (data: Buffer) => {
                errorOutput += data.toString();
            });

            ps.on('close', (code: number) => {
                if (code !== 0) {
                    console.error(`PowerShell clipboard copy failed: ${errorOutput}`);
                    resolve(false);
                } else {
                    console.log(`File copied to clipboard: ${normalizedPath}`);
                    resolve(true);
                }
            });

            ps.on('error', (err: Error) => {
                console.error(`Error spawning PowerShell: ${err.message}`);
                resolve(false);
            });
        } catch (error: unknown) {
            const errorMsg = error instanceof Error ? error.message : String(error);
            console.error(`Error copying file to clipboard: ${errorMsg}`);
            resolve(false);
        }
    });
}

/**
 * Generate temporary file path for XLSB file
 * @returns Temporary file path
 */
export function getTempFilePath(): string {
    const tempDir = os.tmpdir();
    const timestamp = Date.now();
    // Using .xlsb extension for correct binary format
    const tempFilename = `netezza_export_${timestamp}.xlsb`;
    return path.join(tempDir, tempFilename);
}
