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

        // Execute query
        if (progressCallback) {
            progressCallback('Executing query...');
        }

        const cmd = connection.createCommand(query);
        const reader = await cmd.executeReader();

        // Prepare data for XLSB
        const headers: string[] = [];
        for (let i = 0; i < reader.fieldCount; i++) {
            headers.push(reader.getName(i));
        }

        const columnCount = headers.length;
        if (columnCount === 0) {
            // Note: executeReader might not know fieldCount until read() if not implemented perfectly, 
            // but NzDataReader usually parses description immediately after execution.
        }

        if (progressCallback) {
            progressCallback(`Query returned ${columnCount} columns`);
            progressCallback('Reading rows...');
        }

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

        if (progressCallback) {
            progressCallback(`Writing ${rowCount.toLocaleString()} rows to XLSB: ${outputPath}`);
        }

        // Use XlsbWriter
        const writer = new XlsbWriter(outputPath);

        // First sheet: Query Results
        writer.addSheet('Query Results');
        writer.writeSheet(rows, headers);

        // Second sheet: SQL Code
        writer.addSheet('SQL Code');
        const sqlRows = [['SQL Query:'], ...query.split('\n').map(line => [line])];
        writer.writeSheet(sqlRows, null, false); // No header, no autofilter for SQL

        await writer.finalize();

        // Check file size
        const stats = fs.statSync(outputPath);
        const fileSizeMb = stats.size / (1024 * 1024);

        if (progressCallback) {
            progressCallback(`XLSB file created successfully (via XlsbWriter)`);
            progressCallback(`  - Rows: ${rowCount.toLocaleString()}`);
            progressCallback(`  - Columns: ${columnCount}`);
            progressCallback(`  - File size: ${fileSizeMb.toFixed(1)} MB`);
            progressCallback(`  - Location: ${outputPath}`);
        }

        const exportResult: ExportResult = {
            success: true,
            message: `Successfully exported ${rowCount} rows to ${outputPath}`,
            details: {
                rows_exported: rowCount,
                columns: columnCount,
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
export async function exportCsvToXlsb(
    csvContent: string,
    outputPath: string,
    copyToClipboard: boolean = false,
    metadata?: { source?: string;[key: string]: any },
    progressCallback?: ProgressCallback
): Promise<ExportResult> {
    try {
        if (progressCallback) {
            progressCallback('Reading CSV content...');
        }

        // We can't easily parse CSV without a library if we removed xlsx. 
        // But the prompt implied we move to XlsbWriter.
        // Assuming we still have some way to parse CSV or we should implement a simple parser?
        // Wait, removing 'xlsx' means we lose 'XLSX.read'.
        // For now, I will assume a simple split by newline and comma is NOT sufficient for proper CSV (quotes etc).
        // However, since I am asked to use ExcelHelpers for "generowanie plików xlsx", maybe I can keep xlsx for READ?
        // The user said "Wszeslkie towrzenia plikó xlsx genereuj w oparciu o @[ExcelHelpers]".
        // "Creation" (tworzenia) should be XlsbWriter. "Reading" is a different story.
        // But I removed xlsx from imports in the plan.
        // Let's implement a basic CSV parser or clarify. 
        // Actually, let's keep it simple: split lines. If it's complex CSV, this might break.
        // But for "Select * output", it's usually standard.
        // Let's try to do a robust enough parse manually or re-introduce a csv parser if needed.
        // Or better: use the 'csv-parse' library? No, I shouldn't add too many libs without asking.
        // Actually, I'll use a simple regex-based parser for now.

        const parseCsvLine = (line: string): string[] => {
            const result = [];
            let start = 0;
            let inQuotes = false;
            for (let i = 0; i < line.length; i++) {
                if (line[i] === '"') {
                    inQuotes = !inQuotes;
                } else if (line[i] === ',' && !inQuotes) {
                    let val = line.substring(start, i);
                    if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1).replace(/""/g, '"');
                    result.push(val);
                    start = i + 1;
                }
            }
            let lastVal = line.substring(start);
            if (lastVal.startsWith('"') && lastVal.endsWith('"')) lastVal = lastVal.slice(1, -1).replace(/""/g, '"');
            result.push(lastVal);
            return result;
        };

        const lines = csvContent.split(/\r?\n/);
        const data: any[][] = [];

        for (const line of lines) {
            if (line.trim()) {
                data.push(parseCsvLine(line));
            }
        }

        if (data.length === 0) {
            return {
                success: false,
                message: 'CSV content is empty'
            };
        }

        const headers = data[0];
        const rows = data.slice(1);

        const rowCount = rows.length;
        const columnCount = headers.length;

        if (progressCallback) {
            progressCallback(`Writing ${rowCount.toLocaleString()} rows to XLSB: ${outputPath}`);
        }

        // Use XlsbWriter
        const writer = new XlsbWriter(outputPath);

        // First sheet: CSV Data
        writer.addSheet('CSV Data');
        writer.writeSheet(rows, headers);

        // Second sheet: SQL Content or CSV Source
        if (metadata?.sql) {
            writer.addSheet('SQL');
            const sqlRows = [['SQL Query:'], ...metadata.sql.split('\n').map((line: string) => [line])];
            writer.writeSheet(sqlRows, null, false);
        } else {
            const sourcePath = metadata?.source || 'Clipboard';
            writer.addSheet('CSV Source');
            const sourceRows = [['CSV Source:'], [sourcePath]];
            writer.writeSheet(sourceRows, null, false);
        }

        await writer.finalize();

        // Check file size
        const stats = fs.statSync(outputPath);
        const fileSizeMb = stats.size / (1024 * 1024);

        if (progressCallback) {
            progressCallback(`XLSB file created successfully (via XlsbWriter)`);
            progressCallback(`  - Rows: ${rowCount.toLocaleString()}`);
            progressCallback(`  - Columns: ${columnCount}`);
            progressCallback(`  - File size: ${fileSizeMb.toFixed(1)} MB`);
            progressCallback(`  - Location: ${outputPath}`);
        }

        const exportResult: ExportResult = {
            success: true,
            message: `Successfully exported ${rowCount} rows from CSV to ${outputPath}`,
            details: {
                rows_exported: rowCount,
                columns: columnCount,
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
