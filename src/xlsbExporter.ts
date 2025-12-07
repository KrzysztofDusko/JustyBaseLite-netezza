import * as vscode from 'vscode';
import * as XLSX from 'xlsx';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn } from 'child_process';

const odbc = require('odbc');

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

/**
 * Export SQL query results to XLSX file
 * @param connectionString Database connection string
 * @param query SQL query to execute
 * @param outputPath Path where to save the XLSX file
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

        connection = await odbc.connect(connectionString);

        // Execute query
        if (progressCallback) {
            progressCallback('Executing query...');
        }

        const result = await connection.query(query);

        if (!result || !result.columns || result.columns.length === 0) {
            return {
                success: false,
                message: 'Query did not return any results (no columns)'
            };
        }

        const columnCount = result.columns.length;
        if (progressCallback) {
            progressCallback(`Query returned ${columnCount} columns`);
        }

        // Prepare data for XLSX
        const headers = result.columns.map((col: any) => col.name);
        const rows = result.map((row: any) => headers.map((header: string) => row[header]));

        const rowCount = rows.length;

        if (progressCallback) {
            progressCallback(`Writing ${rowCount.toLocaleString()} rows to XLSX: ${outputPath}`);
        }

        // Create workbook
        const wb = XLSX.utils.book_new();

        // First sheet: Query Results
        const wsData = [headers, ...rows];
        const ws = XLSX.utils.aoa_to_sheet(wsData);
        XLSX.utils.book_append_sheet(wb, ws, 'Query Results');

        // Second sheet: SQL Code
        const sqlLines = [['SQL Query:'], ...query.split('\n').map(line => [line])];
        const wsSql = XLSX.utils.aoa_to_sheet(sqlLines);
        XLSX.utils.book_append_sheet(wb, wsSql, 'SQL Code');

        // Write to file with XLSX format
        XLSX.writeFile(wb, outputPath, { bookType: 'xlsx', compression: true });

        // Check file size
        const stats = fs.statSync(outputPath);
        const fileSizeMb = stats.size / (1024 * 1024);

        if (progressCallback) {
            progressCallback(`XLSX file created successfully`);
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
 * Export CSV content to XLSX file
 * @param csvContent CSV content as string
 * @param outputPath Path where to save the XLSX file
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

        // Parse CSV content
        const wb = XLSX.read(csvContent, { type: 'string', raw: true });

        if (!wb.SheetNames || wb.SheetNames.length === 0) {
            return {
                success: false,
                message: 'CSV content is empty or invalid'
            };
        }

        // Get the first sheet and convert to array
        const ws = wb.Sheets[wb.SheetNames[0]];
        const data: any[][] = XLSX.utils.sheet_to_json(ws, { header: 1 });

        if (data.length === 0) {
            return {
                success: false,
                message: 'CSV file is empty or contains no headers'
            };
        }

        const rowCount = data.length - 1; // Exclude header
        const columnCount = data[0] ? data[0].length : 0;

        if (progressCallback) {
            progressCallback(`Writing ${rowCount.toLocaleString()} rows to XLSX: ${outputPath}`);
        }

        // Create new workbook
        const newWb = XLSX.utils.book_new();

        // First sheet: CSV Data
        const dataWs = XLSX.utils.aoa_to_sheet(data);
        XLSX.utils.book_append_sheet(newWb, dataWs, 'CSV Data');

        // Second sheet: CSV Source
        const sourcePath = metadata?.source || 'Clipboard';
        const sourceLines = [['CSV Source:'], [sourcePath]];
        const sourceWs = XLSX.utils.aoa_to_sheet(sourceLines);
        XLSX.utils.book_append_sheet(newWb, sourceWs, 'CSV Source');

        // Write to file with XLSX format
        XLSX.writeFile(newWb, outputPath, { bookType: 'xlsx', compression: true });

        // Check file size
        const stats = fs.statSync(outputPath);
        const fileSizeMb = stats.size / (1024 * 1024);

        if (progressCallback) {
            progressCallback(`XLSX file created successfully`);
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
 * Generate temporary file path for XLSX file
 * @returns Temporary file path
 */
export function getTempFilePath(): string {
    const tempDir = os.tmpdir();
    const timestamp = Date.now();
    const tempFilename = `netezza_export_${timestamp}.xlsx`;
    return path.join(tempDir, tempFilename);
}
