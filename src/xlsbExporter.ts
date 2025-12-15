import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn } from 'child_process';
// @ts-ignore
import XlsbWriter = require('../ExcelHelpers/XlsbWriter');

// const odbc = require('odbc'); // Removed odbc dependency

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
 * Export SQL query results to XLSB file
 * @param connectionString Database connection string
 * @param query SQL query to execute
 * @param outputPath Path where to save the XLSB file
 * @param copyToClipboard If true, also copy file to clipboard (Windows only)
 * @param progressCallback Optional callback for progress updates
 * @returns Export result with success status and details
 */
export async function exportQueryToXlsb(
    connectionString: string,
    query: string,
    outputPath: string,
    copyToClipboard: boolean = false,
    progressCallback?: ProgressCallback
): Promise<ExportResult> {
    let connection: any = null;

    try {
        // Connect to database
        if (progressCallback) {
            progressCallback('Connecting to database...');
        }

        const config = parseConnectionString(connectionString);
        if (!config.port) config.port = 5480;

        const NzConnection = require('../driver/src/NzConnection');
        connection = new NzConnection(config);
        await connection.connect();

        // Use XlsbWriter
        const writer = new XlsbWriter(outputPath);

        // Split queries
        const { SqlParser } = await import('./sqlParser');
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
                const cmd = connection.createCommand(currentQuery);
                const reader = await cmd.executeReader();

                // Prepare data for XLSB
                const headers: string[] = [];
                for (let i = 0; i < reader.fieldCount; i++) {
                    headers.push(reader.getName(i));
                }

                const columnCount = headers.length;
                totalCols = Math.max(totalCols, columnCount);

                const rows: any[][] = [];
                let rowCount = 0;

                while (await reader.read()) {
                    const row: any[] = [];
                    for (let i = 0; i < reader.fieldCount; i++) {
                        row.push(reader.getValue(i));
                    }
                    rows.push(row);
                    rowCount++;
                }

                totalRows += rowCount;

                if (progressCallback) {
                    progressCallback(`Writing ${rowCount.toLocaleString()} rows to sheet "${sheetName}"`);
                }

                // Add Sheet
                writer.addSheet(sheetName);
                writer.writeSheet(rows, headers);

            } catch (err: any) {
                // If one query fails, maybe we log it to a sheet or just continue? 
                // Let's create an error sheet
                writer.addSheet(`Error ${qIndex + 1}`);
                writer.writeSheet([[`Error executing query: ${err.message}`]], ['Error'], false);
            }
        }

        // Final sheet: SQL Code
        writer.addSheet('SQL Code');
        const sqlRows = [['SQL Query:'], ...query.split('\n').map(line => [line])];
        writer.writeSheet(sqlRows, null, false); // No header, no autofilter for SQL

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

    } catch (error: any) {
        return {
            success: false,
            message: `Export error: ${error.message || error}`
        };
    } finally {
        if (connection) {
            try {
                await connection.close();
            } catch (e) {
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
interface CsvExportItem {
    csv: string;
    sql?: string; // Made optional as not all items might have SQL
    name: string;
}

export async function exportCsvToXlsb(
    csvContent: string | CsvExportItem[],
    outputPath: string,
    copyToClipboard: boolean = false,
    metadata: { source: string, sql?: string } = { source: 'Unknown' },
    progressCallback?: ProgressCallback
): Promise<ExportResult> {
    try {
        if (progressCallback) {
            progressCallback('Initializing XLSB writer...');
        }

        const writer = new XlsbWriter(outputPath);

        let totalRows = 0;
        let totalColumns = 0;
        let sqlItems: { name: string, sql: string }[] = [];

        // Helper to process a single CSV string
        const processCsv = (csv: string, sheetName: string) => {
            const rows: any[][] = [];
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

            let headers: string[] = [];
            let currentRowCount = 0;

            lines.forEach((line, index) => {
                if (!line.trim()) return;

                const fields = parseCsvLine(line);
                if (index === 0) {
                    headers = fields;
                } else {
                    rows.push(fields);
                    currentRowCount++;
                }
            });

            if (headers.length > 0) {
                totalColumns = Math.max(totalColumns, headers.length);
                writer.addSheet(sheetName);
                writer.writeSheet(rows, headers);
                totalRows += currentRowCount; // Accumulate total rows
            }
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
                writer.addSheet('CSV Source');
                const sourceRows = [['CSV Source:'], [metadata.source]];
                writer.writeSheet(sourceRows, null, false);
            }
        }

        // Add SQL Code sheet if we have any SQL
        if (sqlItems.length > 0) {
            writer.addSheet('SQL Code');
            const sqlRows: string[][] = [];

            sqlItems.forEach(item => {
                sqlRows.push([`--- SQL for ${item.name} ---`]);
                // Split multiline SQL
                item.sql.split('\n').forEach(line => {
                    sqlRows.push([line]);
                });
                sqlRows.push(['']); // Spacer
            });

            writer.writeSheet(sqlRows, null, false);
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

    } catch (err: any) {
        return {
            success: false,
            message: `Export failed: ${err.message}`
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

    return new Promise<boolean>((resolve) => {
        try {
            const normalizedPath = path.normalize(path.resolve(filePath));

            // Use PowerShell to copy file to clipboard
            // The Set-Clipboard -Path command copies the file object, not the content
            const powershellCommand = `Set-Clipboard -Path "${normalizedPath.replace(/"/g, '`"')}"`;

            const ps = spawn('powershell.exe', [
                '-NoProfile',
                '-NonInteractive',
                '-Command',
                powershellCommand
            ]);

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

        } catch (error: any) {
            console.error(`Error copying file to clipboard: ${error.message}`);
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
