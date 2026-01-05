import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn } from 'child_process';
const XlsxWriter = require('../../libs/ExcelHelpersTs/XlsxWriter').default as new (filePath: string) => {
    addSheet(sheetName: string, hidden?: boolean): void;
    writeSheet(rows: unknown[][], headers: string[] | null, doAutofilter?: boolean): void;
    finalize(): Promise<void>;
};

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
 * CSV export item interface
 */
export interface CsvExportItem {
    csv: string;
    sql?: string;
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

/**
 * Convert a value to number if it's a numeric string (for proper Excel formatting).
 */
function convertToNumberIfNumericString(val: unknown): unknown {
    if (typeof val === 'string' && val.length > 0) {
        // PRECISION SAFETY: Don't convert very long strings to numbers as JS/Excel
        // will lose precision (IEEE 754 double precision only has ~15-17 significant digits).
        // e.g. "11111111111111111111" would become 1.1111111111111112e+19
        if (val.length > 15) {
            return val;
        }

        // LEADING ZERO SAFETY: Don't convert strings with leading zeros (e.g. "0123")
        // unless it is exactly "0" or a decimal starting with "0." (e.g. "0.123")
        if (/^-?0\d+/.test(val)) {
            return val;
        }

        if (/^-?\d+(\.\d+)?$/.test(val)) {
            const num = parseFloat(val);
            if (Number.isFinite(num)) {
                return num;
            }
        }
    }
    return val;
}

/**
 * Convert all numeric strings in a row to numbers for proper Excel export
 */
function convertRowNumericStrings(row: unknown[]): unknown[] {
    return row.map(convertToNumberIfNumericString);
}

/**
 * Export CSV content to XLSX file
 * @param csvContent CSV content as string or array of CsvExportItems
 * @param outputPath Path where to save the XLSX file
 * @param copyToClipboard If true, also copy file to clipboard (Windows only)
 * @param metadata Optional metadata (source info, etc.)
 * @param progressCallback Optional callback for progress updates
 * @returns Export result with success status and details
 */
export async function exportCsvToXlsx(
    csvContent: string | CsvExportItem[],
    outputPath: string,
    copyToClipboard: boolean = false,
    metadata: { source: string; sql?: string } = { source: 'Unknown' },
    progressCallback?: ProgressCallback
): Promise<ExportResult> {
    try {
        if (progressCallback) {
            progressCallback('Initializing XLSX writer...');
        }

        const writer = new XlsxWriter(outputPath);

        let totalRows = 0;
        let totalColumns = 0;
        const sqlItems: { name: string; sql: string }[] = [];

        // Helper to process a single CSV string
        const processCsv = (csv: string, sheetName: string) => {
            const lines = csv.split(/\r?\n/);

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
                        if (field.startsWith('"') && field.endsWith('"')) {
                            field = field.substring(1, field.length - 1).replace(/""/g, '"');
                        }
                        result.push(field);
                        start = i + 1;
                    }
                }
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

            // Build rows array
            const rows: unknown[][] = [];
            for (let i = headerIndex + 1; i < lines.length; i++) {
                const line = lines[i];
                if (!line.trim()) continue;

                const fields = parseCsvLine(line);
                rows.push(convertRowNumericStrings(fields));
            }

            // Add sheet and write data
            writer.addSheet(sheetName);
            writer.writeSheet(rows, headers, true);

            totalRows += rows.length;
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
            }
        }

        // Add SQL Code sheet if we have any SQL
        if (sqlItems.length > 0) {
            writer.addSheet('SQL Code');
            const sqlRows: unknown[][] = [];
            sqlItems.forEach(item => {
                sqlRows.push([`--- SQL for ${item.name} ---`]);
                item.sql.split('\n').forEach(line => {
                    sqlRows.push([line]);
                });
                sqlRows.push(['']);
            });
            writer.writeSheet(sqlRows, null, false);
        }

        if (progressCallback) {
            progressCallback('Finalizing XLSX file...');
        }
        await writer.finalize();

        // Check file size
        const stats = fs.statSync(outputPath);
        const fileSizeMb = stats.size / (1024 * 1024);

        if (progressCallback) {
            progressCallback(`XLSX file created successfully`);
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
 * Export structured data (with column types) to XLSX file.
 * This is used by Grid exports where we have type metadata from the database.
 * @param items Structured export items with columns, rows, and type metadata
 * @param outputPath Path where to save the XLSX file
 * @param copyToClipboard If true, also copy file to clipboard (Windows only)
 * @param progressCallback Optional callback for progress updates
 * @returns Export result with success status and details
 */
export async function exportStructuredToXlsx(
    items: StructuredExportItem[],
    outputPath: string,
    copyToClipboard: boolean = false,
    progressCallback?: ProgressCallback
): Promise<ExportResult> {
    try {
        if (progressCallback) {
            progressCallback('Initializing XLSX writer...');
        }

        const writer = new XlsxWriter(outputPath);
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

            // Build rows with type-aware conversion
            const rows: unknown[][] = item.rows.map(row =>
                row.map((val, i) => {
                    if (colIsNumeric[i]) {
                        return convertToNumberIfNumericString(val);
                    }
                    return val;
                })
            );

            // Add sheet and write data
            writer.addSheet(sheetName);
            writer.writeSheet(rows, headers, true);

            totalRows += rows.length;

            if (item.sql) {
                sqlItems.push({ name: sheetName, sql: item.sql });
            }
        }

        // Add SQL Code sheet if we have any SQL
        if (sqlItems.length > 0) {
            writer.addSheet('SQL Code');
            const sqlRows: unknown[][] = [];
            sqlItems.forEach(item => {
                sqlRows.push([`--- SQL for ${item.name} ---`]);
                item.sql.split('\n').forEach(line => {
                    sqlRows.push([line]);
                });
                sqlRows.push(['']);
            });
            writer.writeSheet(sqlRows, null, false);
        }

        if (progressCallback) {
            progressCallback('Finalizing XLSX file...');
        }
        await writer.finalize();

        const stats = fs.statSync(outputPath);
        const fileSizeMb = stats.size / (1024 * 1024);

        if (progressCallback) {
            progressCallback(`XLSX file created successfully`);
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
 */
export async function copyFileToClipboard(filePath: string): Promise<boolean> {
    if (os.platform() !== 'win32') {
        console.error('Clipboard file copy is only supported on Windows');
        return false;
    }

    return new Promise<boolean>(resolve => {
        try {
            const normalizedPath = path.normalize(path.resolve(filePath));
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
 * Generate temporary file path for XLSX file
 */
export function getTempFilePath(): string {
    const tempDir = os.tmpdir();
    const timestamp = Date.now();
    const tempFilename = `netezza_export_${timestamp}.xlsx`;
    return path.join(tempDir, tempFilename);
}
