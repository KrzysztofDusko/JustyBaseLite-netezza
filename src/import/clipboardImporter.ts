/**
 * Clipboard Data Importer for Netezza
 * Handles importing data from clipboard in text and XML Spreadsheet formats
 * Ported from Python clipboard_importer.py
 */

import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';
import { Readable } from 'stream';
import { ColumnTypeChooser, ProgressCallback, ImportResult } from './dataImporter';
import { NzConnection, ConnectionDetails } from '../types';

// Helper to unblock event loop
const delay = () => new Promise(resolve => setTimeout(resolve, 0));


// ConnectionDetails is imported from '../types' - no need for parseConnectionString

/**
 * Clipboard data processor - handles text and XML Spreadsheet formats
 */
export class ClipboardDataProcessor {

    /**
     * Process XML Spreadsheet format from Excel
     * Based on C# sequential XML reading approach
     */
    async processXmlSpreadsheet(xmlData: string, progressCallback?: ProgressCallback): Promise<string[][]> {
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
            let cellMatch: RegExpExecArray | null;
            let colNum = 0;

            // Reset for simpler parsing
            const simpleDataRegex =
                /<Cell(?:[^>]*ss:Index="(\d+)")?[^>]*>(?:[\s\S]*?<Data[^>]*(?:\s+ss:Type="([^"]*)")?[^>]*>([^<]*)<\/Data>)?[\s\S]*?<\/Cell>/gi;

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
                progressCallback?.(`Analyzed ${rowNum.toLocaleString()} rows...`, undefined, false);
                // Unblock UI
                await delay();
            }
        }

        progressCallback?.(`XML processing complete: ${rows.length} rows, ${expandedColumnCount} columns`);
        return rows;
    }

    /**
     * Process plain text clipboard data with auto-delimiter detection
     */
    async processTextData(textData: string, progressCallback?: ProgressCallback): Promise<string[][]> {
        progressCallback?.('Processing text data...');

        if (!textData.trim()) {
            return [];
        }

        const lines = textData.split('\n');
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
    async processClipboardData(
        formatPreference?: string | null,
        progressCallback?: ProgressCallback
    ): Promise<[string[][], string]> {
        progressCallback?.('Getting clipboard data...');

        const rawData = await this.getClipboardText();

        if (!rawData) {
            throw new Error('No data found in clipboard');
        }

        progressCallback?.(`Data size: ${rawData.length} characters`);

        // Detect format
        let detectedFormat = 'TEXT';

        if (
            formatPreference === 'XML Spreadsheet' ||
            (!formatPreference && rawData.includes('<Workbook') && rawData.includes('<Worksheet'))
        ) {
            detectedFormat = 'XML Spreadsheet';
        } else if (formatPreference === 'TEXT') {
            detectedFormat = 'TEXT';
        }

        progressCallback?.(`Detected format: ${detectedFormat}`);

        // Process based on format
        let processedData: string[][];
        if (detectedFormat === 'XML Spreadsheet') {
            processedData = await this.processXmlSpreadsheet(rawData, progressCallback);
        } else {
            // Text processing is fast enough usually, but strictly we could add delay there too 
            // if we implement async logic in processTextData.
            // For now leaving as is unless user reports lockup on text paste.
            processedData = await this.processTextData(rawData, progressCallback);
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
    if (!cleanName || /^\d/.test(cleanName) || cleanName.startsWith('_')) {
        cleanName = 'COL' + (cleanName.startsWith('_') ? '' : '_') + cleanName;
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
function formatValue(
    val: string,
    colIndex: number,
    dataTypes: ColumnTypeChooser[],
    escapechar: string,
    valuesToEscape: string[]
): string {
    let result = escapeValue(val, escapechar, valuesToEscape);
    if (colIndex < dataTypes.length && dataTypes[colIndex].currentType.dbType === 'DATETIME') {
        result = result.replace('T', ' ');
    }
    return result;
}

/**
 * Clipboard Data Generator Stream
 * Generates formatted rows on demand for Netezza import
 */
class ClipboardDataStream extends Readable {
    private rows: string[][];
    private currentIndex: number = 0;
    private dataTypes: ColumnTypeChooser[];
    private delimiter: string;
    private recordDelim: string;
    private escapechar: string;
    private valuesToEscape: string[];
    private decimalDelimiter: string;
    public byteLength: number = 0;
    private progressCallback?: ProgressCallback;
    private lastReportedIndex: number = -1;
    private lastReportTime: number = 0;
    private lastReportedMessage: string = '';

    constructor(
        rows: string[][],
        dataTypes: ColumnTypeChooser[],
        delimiter: string,
        recordDelim: string,
        escapechar: string,
        valuesToEscape: string[],
        decimalDelimiter: string,
        progressCallback?: ProgressCallback
    ) {
        super();
        this.rows = rows;
        this.dataTypes = dataTypes;
        this.delimiter = delimiter;
        this.recordDelim = recordDelim;
        this.escapechar = escapechar;
        this.valuesToEscape = valuesToEscape;
        this.decimalDelimiter = decimalDelimiter;
        this.progressCallback = progressCallback;
        this.currentIndex = 0;
        this.lastReportedIndex = -1;
        this.lastReportTime = 0;
        this.lastReportedMessage = '';

        // Estimate byte size roughly
        try {
            this.byteLength = rows.reduce((acc, row) => {
                const rowLen = row.reduce((rAcc, val) => rAcc + (val ? val.length : 0), 0);
                return acc + rowLen + row.length; // + delimiters
            }, 0);
        } catch {
            this.byteLength = 1024 * 1024;
        }
    }

    _read(_size: number): void {
        try {
            let more = true;
            while (more && this.currentIndex < this.rows.length) {
                const row = this.rows[this.currentIndex];
                const formattedRow = row.map((value, j) => {
                    let val = formatValue(value, j, this.dataTypes, this.escapechar, this.valuesToEscape);

                    // If it is a NUMERIC type and we are using comma as delimiter, replace it with dot for DB consistency
                    if (this.dataTypes[j].currentType.dbType === 'NUMERIC' && this.decimalDelimiter === ',') {
                        val = val.replace(',', '.');
                    }

                    // If DATETIME, check if we need to reformat dd.mm.yyyy
                    if (this.dataTypes[j].currentType.dbType === 'DATETIME') {
                        const dateTimeMatch = val.match(
                            /^(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?$/
                        );
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

                const line = formattedRow.join(this.delimiter) + this.recordDelim;
                more = this.push(Buffer.from(line, 'utf8'));
                this.currentIndex++;
            }

            // Report progress logic
            const total = this.rows.length;


            if (this.currentIndex > this.lastReportedIndex) {
                const now = Date.now();
                const isComplete = this.currentIndex >= total;
                // Report if complete OR at least 1 second passed since last report
                const shouldReport = isComplete || (now - this.lastReportTime >= 1000);

                if (shouldReport) {
                    const percent = Math.floor((this.currentIndex / total) * 100);
                    const message = `Streaming data: ${percent}% (${this.currentIndex.toLocaleString()}/${total.toLocaleString()})`;

                    const processedDelta = this.currentIndex - (this.lastReportedIndex < 0 ? 0 : this.lastReportedIndex);
                    const increment = (processedDelta / total) * 100;

                    if (message !== this.lastReportedMessage) {
                        this.progressCallback?.(message, increment, false);
                        this.lastReportedIndex = this.currentIndex;
                        this.lastReportTime = now;
                        this.lastReportedMessage = message;
                    }
                }
            }

            if (this.currentIndex >= this.rows.length) {
                this.push(null);
            }
        } catch (e) {
            this.emit('error', e);
        }
    }
}

/**
 * Import clipboard data to Netezza table
 */
export async function importClipboardDataToNetezza(
    targetTable: string,
    connectionDetails: ConnectionDetails,
    formatPreference?: string | null,
    _options?: unknown,
    progressCallback?: ProgressCallback
): Promise<ImportResult> {
    const startTime = Date.now();
    let virtualFileName: string | null = null;
    let connection: NzConnection | null = null;

    try {
        // Validate parameters
        if (!targetTable) {
            return {
                success: false,
                message: 'Target table name is required'
            };
        }

        if (!connectionDetails || !connectionDetails.host) {
            return {
                success: false,
                message: 'Connection details are required'
            };
        }

        progressCallback?.('Starting clipboard import process...');
        progressCallback?.(`  Target table: ${targetTable}`);
        progressCallback?.(`  Format preference: ${formatPreference || 'auto-detect'}`);

        // Process clipboard data
        const processor = new ClipboardDataProcessor();
        const [clipboardData, detectedFormat] = await processor.processClipboardData(
            formatPreference,
            progressCallback
        );

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
                progressCallback?.(`Analyzed ${(i + 1).toLocaleString()} rows...`, undefined, false);
                await delay();
            }
        }

        progressCallback?.(`Analysis complete: ${dataRows.length.toLocaleString()} data rows`);

        // Create temp directory and data file
        // Create temp directory (still useful for logs if needed, but not for data)
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

        // Create data stream (in-memory)
        progressCallback?.('Preparing in-memory data stream...');

        const dataStream = new ClipboardDataStream(
            dataRows,
            dataTypes,
            delimiter,
            recordDelim,
            escapechar,
            valuesToEscape,
            decimalDelimiter,
            progressCallback
        );

        virtualFileName = `virtual_clipboard_import_${Date.now()}_${Math.floor(Math.random() * 1000)}.txt`;
        progressCallback?.(`Registered virtual stream: ${virtualFileName}`);

        // Register stream with driver static registry
        const NzConnectionClass = require('../../libs/driver/src/NzConnection');
        if (NzConnectionClass && NzConnectionClass.registerImportStream) {
            NzConnectionClass.registerImportStream(virtualFileName, dataStream);
        } else {
            progressCallback?.('Warning: NzConnection driver does not support stream registry. Import might fail.');
        }

        // Generate CREATE TABLE SQL
        const columns: string[] = [];
        for (let i = 0; i < sqlHeaders.length; i++) {
            columns.push(`        ${sqlHeaders[i]} ${dataTypes[i].currentType.toString()}`);
        }

        const logDirUnix = tempDir.replace(/\\/g, '/');

        const createSql = `CREATE TABLE ${targetTable} AS 
(
    SELECT * FROM EXTERNAL '${virtualFileName}'
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

        const config = {
            host: connectionDetails.host,
            port: connectionDetails.port || 5480,
            database: connectionDetails.database,
            user: connectionDetails.user,
            password: connectionDetails.password
        };

        const NzDriver = require('../../libs/driver/src/NzConnection');
        connection = new NzDriver(config) as NzConnection;
        await connection.connect();

        try {
            progressCallback?.('Executing CREATE TABLE with EXTERNAL clipboard data...');
            // NzConnection should handle the external table protocol automatically
            const cmd = connection!.createCommand(createSql);

            // Set 60-minute timeout for large clipboard imports
            cmd.commandTimeout = 3600;

            await cmd.execute();

            progressCallback?.('Clipboard import completed successfully');
        } finally {
            await connection!.close();
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
    } catch (e: unknown) {
        const processingTime = (Date.now() - startTime) / 1000;
        const errorMsg = e instanceof Error ? e.message : String(e);
        return {
            success: false,
            message: `Clipboard import failed: ${errorMsg}`,
            details: {
                processingTime: `${processingTime.toFixed(1)}s`
            }
        };
    } finally {
        if (connection && connection._connected) {
            try {
                await connection.close();
            } catch {
                // Ignore connection close errors during cleanup
            }
        }

        // Clean up registry
        if (virtualFileName) {
            const NzConnectionClass = require('../../libs/driver/src/NzConnection');
            if (NzConnectionClass && NzConnectionClass.unregisterImportStream) {
                NzConnectionClass.unregisterImportStream(virtualFileName);
            }
        }
    }
}
