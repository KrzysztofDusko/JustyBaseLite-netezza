/**
 * Clipboard Data Importer for Netezza
 * Handles importing data from clipboard in text and XML Spreadsheet formats
 * Ported from Python clipboard_importer.py
 */

import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';
import { ColumnTypeChooser, NetezzaDataType, ProgressCallback, ImportResult } from './dataImporter';

// ODBC import
// let odbc: any;
// try {
//     odbc = require('odbc');
// } catch (e) {
//     console.error('ODBC module not available');
// }

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
 * Clipboard data processor - handles text and XML Spreadsheet formats
 */
export class ClipboardDataProcessor {
    private processedData: string[][] = [];

    /**
     * Process XML Spreadsheet format from Excel
     * Based on C# sequential XML reading approach
     */
    processXmlSpreadsheet(xmlData: string, progressCallback?: ProgressCallback): string[][] {
        progressCallback?.('Processing XML Spreadsheet data...');

        // Simple XML parsing without external dependencies
        const rows: string[][] = [];
        let expandedColumnCount = 0;
        let currentRow: string[] = [];
        let rowNum = 0;

        // Extract ExpandedColumnCount
        const colMatch = xmlData.match(/ExpandedColumnCount="(\d+)"/);
        if (colMatch) {
            expandedColumnCount = parseInt(colMatch[1]);
            progressCallback?.(`Table has ${expandedColumnCount} columns`);
        }

        // Extract ExpandedRowCount
        const rowMatch = xmlData.match(/ExpandedRowCount="(\d+)"/);
        if (rowMatch) {
            progressCallback?.(`Table has ${rowMatch[1]} rows`);
        }

        // Parse rows - look for <Row> elements
        const rowRegex = /<Row[^>]*>([\s\S]*?)<\/Row>/gi;
        let rowMatchResult: RegExpExecArray | null;

        while ((rowMatchResult = rowRegex.exec(xmlData)) !== null) {
            const rowContent = rowMatchResult[1];
            currentRow = new Array(expandedColumnCount).fill('');

            // Parse cells - look for <Cell> and <Data> elements
            const cellRegex = /<Cell(?:\s+[^>]*ss:Index="(\d+)")?[^>]*>[\s\S]*?<Data[^>]*>([^<]*)<\/Data>[\s\S]*?<\/Cell>|<Cell(?:\s+[^>]*ss:Index="(\d+)")?[^>]*\/>/gi;
            let cellMatch: RegExpExecArray | null;
            let colNum = 0;

            // Reset for simpler parsing
            const simpleDataRegex = /<Cell(?:[^>]*ss:Index="(\d+)")?[^>]*>(?:[\s\S]*?<Data[^>]*(?:\s+ss:Type="([^"]*)")?[^>]*>([^<]*)<\/Data>)?[\s\S]*?<\/Cell>/gi;

            while ((cellMatch = simpleDataRegex.exec(rowContent)) !== null) {
                // Check for ss:Index attribute (sparse data handling)
                if (cellMatch[1]) {
                    colNum = parseInt(cellMatch[1]) - 1; // XML indexes from 1
                }

                const dataType = cellMatch[2] || '';
                let value = cellMatch[3] || '';

                // Handle Boolean type conversion
                if (dataType === 'Boolean') {
                    value = value === '0' ? 'False' : 'True';
                }

                if (colNum < expandedColumnCount) {
                    currentRow[colNum] = value;
                }
                colNum++;
            }

            // Only add row if it has some content
            if (currentRow.some(cell => cell.trim())) {
                rows.push([...currentRow]);
            }

            rowNum++;
            if (rowNum % 10000 === 0) {
                progressCallback?.(`Analyzed ${rowNum.toLocaleString()} rows...`);
            }
        }

        progressCallback?.(`XML processing complete: ${rows.length} rows, ${expandedColumnCount} columns`);
        this.processedData = rows;
        return rows;
    }

    /**
     * Process plain text clipboard data with auto-delimiter detection
     */
    processTextData(textData: string, progressCallback?: ProgressCallback): string[][] {
        progressCallback?.('Processing text data...');

        if (!textData.trim()) {
            return [];
        }

        let lines = textData.split('\n');
        // Remove empty lines at the end
        while (lines.length && !lines[lines.length - 1].trim()) {
            lines.pop();
        }

        if (!lines.length) {
            return [];
        }

        // Auto-detect delimiter by analyzing the first few lines
        const delimiters = ['\t', ',', ';', '|'];
        const delimiterScores: { [key: string]: [number, number] } = {};

        for (const delimiter of delimiters) {
            const scores: number[] = [];
            for (const line of lines.slice(0, Math.min(5, lines.length))) {
                if (line.trim()) {
                    const parts = line.split(delimiter);
                    scores.push(parts.length);
                }
            }

            if (scores.length) {
                const avgCols = scores.reduce((a, b) => a + b, 0) / scores.length;
                const variance = scores.reduce((sum, s) => sum + Math.pow(s - avgCols, 2), 0) / scores.length;
                delimiterScores[delimiter] = [avgCols, -variance];
            }
        }

        // Choose the best delimiter
        let bestDelimiter = '\t';
        if (Object.keys(delimiterScores).length) {
            bestDelimiter = Object.keys(delimiterScores).reduce((best, d) => {
                const [avgA, varA] = delimiterScores[best] || [0, 0];
                const [avgB, varB] = delimiterScores[d];
                // Higher column count and lower variance is better
                return avgB > avgA || (avgB === avgA && varB > varA) ? d : best;
            }, '\t');
        }

        progressCallback?.(`Auto-detected delimiter: '${bestDelimiter === '\t' ? '\\t' : bestDelimiter}'`);

        // Split all lines using the detected delimiter
        const rowsData: string[][] = [];
        let maxCols = 0;

        for (const line of lines) {
            if (line.trim()) {
                const row = line.split(bestDelimiter).map(cell => cell.trim());
                rowsData.push(row);
                maxCols = Math.max(maxCols, row.length);
            }
        }

        // Normalize all rows to have the same number of columns
        for (const row of rowsData) {
            while (row.length < maxCols) {
                row.push('');
            }
        }

        progressCallback?.(`Text processing complete: ${rowsData.length} rows, ${maxCols} columns`);
        this.processedData = rowsData;
        return rowsData;
    }

    /**
     * Get clipboard text content using VS Code API
     */
    async getClipboardText(): Promise<string> {
        return await vscode.env.clipboard.readText();
    }

    /**
     * Process clipboard data - detects format and processes accordingly
     */
    async processClipboardData(formatPreference?: string | null, progressCallback?: ProgressCallback): Promise<[string[][], string]> {
        progressCallback?.('Getting clipboard data...');

        const rawData = await this.getClipboardText();

        if (!rawData) {
            throw new Error('No data found in clipboard');
        }

        progressCallback?.(`Data size: ${rawData.length} characters`);

        // Detect format
        let detectedFormat = 'TEXT';

        if (formatPreference === 'XML Spreadsheet' ||
            (!formatPreference && rawData.includes('<Workbook') && rawData.includes('<Worksheet'))) {
            detectedFormat = 'XML Spreadsheet';
        } else if (formatPreference === 'TEXT') {
            detectedFormat = 'TEXT';
        }

        progressCallback?.(`Detected format: ${detectedFormat}`);

        // Process based on format
        let processedData: string[][];
        if (detectedFormat === 'XML Spreadsheet') {
            processedData = this.processXmlSpreadsheet(rawData, progressCallback);
        } else {
            processedData = this.processTextData(rawData, progressCallback);
        }

        progressCallback?.(`Processed ${processedData.length} rows`);
        if (processedData.length) {
            progressCallback?.(`Columns per row: ${processedData[0].length}`);
        }

        return [processedData, detectedFormat];
    }
}

/**
 * Clean column name for SQL compatibility
 */
function cleanColumnName(colName: string): string {
    let cleanName = String(colName).trim();
    cleanName = cleanName.replace(/[^0-9a-zA-Z]+/g, '_').toUpperCase();
    if (!cleanName || /^\d/.test(cleanName)) {
        cleanName = 'COL_' + cleanName;
    }
    return cleanName;
}

/**
 * Escape special characters for Netezza import
 */
function escapeValue(val: string, escapechar: string, valuesToEscape: string[]): string {
    let result = String(val).trim();
    for (const char of valuesToEscape) {
        result = result.split(char).join(`${escapechar}${char}`);
    }
    return result;
}

/**
 * Format value according to column type
 */
function formatValue(val: string, colIndex: number, dataTypes: ColumnTypeChooser[], escapechar: string, valuesToEscape: string[]): string {
    let result = escapeValue(val, escapechar, valuesToEscape);
    if (colIndex < dataTypes.length && dataTypes[colIndex].currentType.dbType === 'DATETIME') {
        result = result.replace('T', ' ');
    }
    return result;
}

/**
 * Import clipboard data to Netezza table
 */
export async function importClipboardDataToNetezza(
    targetTable: string,
    connectionString: string,
    formatPreference?: string | null,
    options?: any,
    progressCallback?: ProgressCallback
): Promise<ImportResult> {
    const startTime = Date.now();
    let tempFilePath: string | null = null;
    let connection: any = null;

    try {
        // Validate parameters
        if (!targetTable) {
            return {
                success: false,
                message: 'Target table name is required'
            };
        }

        if (!connectionString) {
            return {
                success: false,
                message: 'Connection string is required'
            };
        }

        progressCallback?.('Starting clipboard import process...');
        progressCallback?.(`  Target table: ${targetTable}`);
        progressCallback?.(`  Format preference: ${formatPreference || 'auto-detect'}`);

        // Process clipboard data
        const processor = new ClipboardDataProcessor();
        const [clipboardData, detectedFormat] = await processor.processClipboardData(formatPreference, progressCallback);

        if (!clipboardData || !clipboardData.length) {
            return {
                success: false,
                message: 'No data found in clipboard'
            };
        }

        if (clipboardData.length < 2) {
            return {
                success: false,
                message: 'Clipboard data must contain at least headers and one data row'
            };
        }

        progressCallback?.(`  Detected format: ${detectedFormat}`);
        progressCallback?.(`  Rows: ${clipboardData.length}`);
        progressCallback?.(`  Columns: ${clipboardData[0].length}`);

        // Extract headers and data
        const sqlHeaders = clipboardData[0].map(col => cleanColumnName(col));
        const dataRows = clipboardData.slice(1);

        // Detect decimal delimiter
        progressCallback?.('Detecting decimal separator...');
        let decimalDelimiter = '.';

        // Check first 100 rows for numeric patterns
        let dotCount = 0;
        let commaCount = 0;
        const checkRows = dataRows.slice(0, 100);

        for (const row of checkRows) {
            for (const cell of row) {
                if (!cell || !cell.trim()) continue;
                const val = cell.trim();
                // Check for patterns like 12.34 vs 12,34
                // We want to avoid date confusion if possible, but simplest is checking for n,n vs n.n
                if (/^\d+\.\d+$/.test(val)) dotCount++;
                if (/^\d+,\d+$/.test(val)) commaCount++;
            }
        }

        if (commaCount > dotCount && commaCount > 0) {
            decimalDelimiter = ',';
        }
        progressCallback?.(`Detected decimal separator: '${decimalDelimiter}'`);

        // Analyze data types
        progressCallback?.('Analyzing clipboard data types...');
        const dataTypes: ColumnTypeChooser[] = sqlHeaders.map(() => new ColumnTypeChooser(decimalDelimiter));

        for (let i = 0; i < dataRows.length; i++) {
            const row = dataRows[i];
            for (let j = 0; j < row.length; j++) {
                if (j < dataTypes.length && row[j] && row[j].trim()) {
                    dataTypes[j].refreshCurrentType(row[j].trim());
                }
            }

            if ((i + 1) % 1000 === 0) {
                progressCallback?.(`Analyzed ${(i + 1).toLocaleString()} rows...`);
            }
        }

        progressCallback?.(`Analysis complete: ${dataRows.length.toLocaleString()} data rows`);

        // Create temp directory and data file
        const tempDir = path.join(require('os').tmpdir(), 'netezza_clipboard_logs');
        if (!fs.existsSync(tempDir)) {
            fs.mkdirSync(tempDir, { recursive: true });
        }

        const delimiter = '\t';
        const delimiterPlain = '\\t';
        const recordDelim = '\n';
        const recordDelimPlain = '\\n';
        const escapechar = '\\';
        const valuesToEscape = [escapechar, recordDelim, '\r', delimiter];

        // Create temp file with formatted data
        tempFilePath = path.join(tempDir, `netezza_clipboard_import_${Math.floor(Math.random() * 1000)}.txt`);
        progressCallback?.(`Creating temporary data file: ${tempFilePath}`);

        const outputLines: string[] = [];
        for (let i = 0; i < dataRows.length; i++) {
            const row = dataRows[i];
            const formattedRow = row.map((value, j) => {
                let val = formatValue(value, j, dataTypes, escapechar, valuesToEscape);

                // If it is a NUMERIC type and we are using comma as delimiter, replace it with dot for DB consistency
                if (dataTypes[j].currentType.dbType === 'NUMERIC' && decimalDelimiter === ',') {
                    val = val.replace(',', '.');
                }

                // If DATETIME, check if we need to reformat dd.mm.yyyy
                if (dataTypes[j].currentType.dbType === 'DATETIME') {
                    const dateTimeMatch = val.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?$/);
                    if (dateTimeMatch) {
                        const day = dateTimeMatch[1];
                        const month = dateTimeMatch[2];
                        const year = dateTimeMatch[3];
                        const hour = dateTimeMatch[4] || '00';
                        const min = dateTimeMatch[5] || '00';
                        const sec = dateTimeMatch[6] || '00';
                        val = `${year}-${month}-${day} ${hour}:${min}:${sec}`;
                    }
                }

                return val;
            });
            outputLines.push(formattedRow.join(delimiter));

            if ((i + 1) % 1000 === 0) {
                progressCallback?.(`Processed ${(i + 1).toLocaleString()} rows...`);
            }
        }

        fs.writeFileSync(tempFilePath, outputLines.join(recordDelim), 'utf-8');
        const pipeName = tempFilePath.replace(/\\/g, '/');
        progressCallback?.(`Data file created: ${pipeName}`);

        // Generate CREATE TABLE SQL
        const columns: string[] = [];
        for (let i = 0; i < sqlHeaders.length; i++) {
            columns.push(`        ${sqlHeaders[i]} ${dataTypes[i].currentType.toString()}`);
        }

        const logDirUnix = tempDir.replace(/\\/g, '/');

        const createSql = `CREATE TABLE ${targetTable} AS 
(
    SELECT * FROM EXTERNAL '${pipeName}'
    (
${columns.join(',\n')}
    )
    USING
    (
        REMOTESOURCE 'jdbc'
        DELIMITER '${delimiterPlain}'
        RecordDelim '${recordDelimPlain}'
        ESCAPECHAR '${escapechar}'
        NULLVALUE ''
        ENCODING 'utf-8'
        TIMESTYLE '24HOUR'
        SKIPROWS 0
        MAXERRORS 10
        LOGDIR '${logDirUnix}'
    )
) DISTRIBUTE ON RANDOM;`;

        progressCallback?.('Generated SQL:');
        progressCallback?.(createSql);

        // Execute import
        progressCallback?.('Connecting to Netezza...');

        const config = parseConnectionString(connectionString);
        if (!config.port) config.port = 5480;

        const NzConnection = require('../driver/src/NzConnection');
        connection = new NzConnection(config);
        await connection.connect();

        try {
            progressCallback?.('Executing CREATE TABLE with EXTERNAL clipboard data...');
            // NzConnection should handle the external table protocol automatically
            const cmd = connection.createCommand(createSql);
            await cmd.execute();

            progressCallback?.('Clipboard import completed successfully');
        } finally {
            await connection.close();
        }

        const processingTime = (Date.now() - startTime) / 1000;

        return {
            success: true,
            message: 'Clipboard import completed successfully',
            details: {
                targetTable: targetTable,
                format: detectedFormat,
                rowsProcessed: dataRows.length,
                rowsInserted: dataRows.length,
                processingTime: `${processingTime.toFixed(1)}s`,
                columns: sqlHeaders.length,
                detectedDelimiter: delimiter
            }
        };

    } catch (e: any) {
        const processingTime = (Date.now() - startTime) / 1000;
        return {
            success: false,
            message: `Clipboard import failed: ${e.message}`,
            details: {
                processingTime: `${processingTime.toFixed(1)}s`
            }
        };
    } finally {
        if (connection && connection._connected) {
            try { await connection.close(); } catch { }
        }

        // Clean up temp file
        if (tempFilePath && fs.existsSync(tempFilePath)) {
            try {
                fs.unlinkSync(tempFilePath);
                progressCallback?.('Temporary clipboard data file cleaned up');
            } catch (e: any) {
                progressCallback?.(`Warning: Could not clean up temp file: ${e.message}`);
            }
        }
    }
}
