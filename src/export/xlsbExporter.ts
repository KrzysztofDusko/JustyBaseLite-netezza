import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
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
 * @returns Export result with success status and details
 */
export async function exportQueryToXlsb(
    connectionDetails: ConnectionDetails,
    query: string,
    outputPath: string,
    copyToClipboard: boolean = false,
    progressCallback?: ProgressCallback
): Promise<ExportResult> {
    let connection: NzConnection | null = null;

    try {
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

        for (let qIndex = 0; qIndex < queries.length; qIndex++) {
            const currentQuery = queries[qIndex];
            if (!currentQuery.trim()) continue;

            const sheetName = queries.length > 1 ? `Result ${qIndex + 1}` : 'Query Results';

            if (progressCallback) {
                progressCallback(`Executing query ${qIndex + 1}/${queries.length}...`);
            }

            try {
                const cmd = connection!.createCommand(currentQuery);
                const reader = await cmd.executeReader();

                // Prepare headers for XLSB
                const headers: string[] = [];
                for (let i = 0; i < reader.fieldCount; i++) {
                    headers.push(reader.getName(i));
                }

                const columnCount = headers.length;
                totalCols = Math.max(totalCols, columnCount);

                // Use streaming API - start sheet with headers
                writer.startSheet(sheetName, columnCount, headers, { doAutofilter: true });

                let rowCount = 0;

                // Stream rows directly from database reader to Excel writer
                while (await reader.read()) {
                    const row: unknown[] = [];
                    for (let i = 0; i < reader.fieldCount; i++) {
                        row.push(reader.getValue(i));
                    }
                    // Convert numeric strings to numbers and write immediately
                    writer.writeRow(convertRowNumericStrings(row));
                    rowCount++;

                    // Progress update every 10000 rows
                    if (rowCount % 10000 === 0 && progressCallback) {
                        progressCallback(`Streaming ${rowCount.toLocaleString()} rows to "${sheetName}"...`);
                    }
                }

                // Finalize the sheet
                writer.endSheet();

                totalRows += rowCount;

                if (progressCallback) {
                    progressCallback(`Written ${rowCount.toLocaleString()} rows to sheet "${sheetName}"`);
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
            progressCallback(`XLSB file created successfully`);
            progressCallback(`  - Total Rows: ${totalRows.toLocaleString()}`);
            progressCallback(`  - File size: ${fileSizeMb.toFixed(1)} MB`);
            progressCallback(`  - Location: ${outputPath}`);
        }

        const exportResult: ExportResult = {
            success: true,
            message: `Successfully exported ${totalRows} rows to ${outputPath}`,
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
