/**
 * Data Importer for Netezza
 * Handles importing data from various file formats to Netezza tables
 * Ported from Python data_importer.py
 */

import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';

// XLSX import for Excel file support
let XLSX: any;
try {
    XLSX = require('xlsx');
} catch (e) {
    console.error('XLSX module not available');
}

// ODBC import
let odbc: any;
try {
    odbc = require('odbc');
} catch (e) {
    console.error('ODBC module not available');
}

/**
 * Netezza data type representation
 */
export class NetezzaDataType {
    constructor(
        public dbType: string,
        public precision?: number,
        public scale?: number,
        public length?: number
    ) { }

    toString(): string {
        if (['BIGINT', 'DATE', 'DATETIME'].includes(this.dbType)) {
            return this.dbType;
        }
        if (this.dbType === 'NUMERIC') {
            return `${this.dbType}(${this.precision},${this.scale})`;
        }
        if (this.dbType === 'NVARCHAR') {
            return `${this.dbType}(${this.length})`;
        }
        return `TODO !!! ${this.dbType}`;
    }
}

/**
 * Column type chooser - determines the best data type for a column
 */
export class ColumnTypeChooser {
    currentType: NetezzaDataType;
    private decimalDelimInCsv: string = '.';
    private firstTime: boolean = true;

    constructor() {
        this.currentType = new NetezzaDataType('BIGINT');
    }

    private getType(strVal: string): NetezzaDataType {
        const currentDbType = this.currentType.dbType;
        const strLen = strVal.length;

        // BIGINT check
        if (currentDbType === 'BIGINT' && /^\d+$/.test(strVal) && strLen < 15) {
            this.firstTime = false;
            return new NetezzaDataType('BIGINT');
        }

        // NUMERIC check
        const decimalCnt = (strVal.match(new RegExp(`\\${this.decimalDelimInCsv}`, 'g')) || []).length;
        if (['BIGINT', 'NUMERIC'].includes(currentDbType) && decimalCnt <= 1) {
            const strValClean = strVal.replace(this.decimalDelimInCsv, '');
            if (/^\d+$/.test(strValClean) && strLen < 15 && (!strValClean.startsWith('0') || decimalCnt > 0)) {
                this.firstTime = false;
                return new NetezzaDataType('NUMERIC', 16, 6);
            }
        }

        // DATE check (2024-06-07, 2024-6-7)
        if ((currentDbType === 'DATE' || this.firstTime) && (strVal.match(/-/g) || []).length === 2 && strLen >= 8 && strLen <= 10) {
            const parts = strVal.split('-');
            if (parts.length === 3 && parts.every(part => /^\d+$/.test(part))) {
                try {
                    const date = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
                    if (!isNaN(date.getTime())) {
                        this.firstTime = false;
                        return new NetezzaDataType('DATE');
                    }
                } catch {
                    // Invalid date, continue
                }
            }
        }

        // DATETIME check
        if ((currentDbType === 'DATETIME' || this.firstTime) && (strVal.match(/-/g) || []).length === 2 && strLen >= 12 && strLen <= 20) {
            const result = strVal.match(/^(\d{4})-(\d{1,2})-(\d{1,2})[\s|T](\d{2}):(\d{2})(:?(\d{2}))?$/);
            if (result) {
                try {
                    const sec = result[7] ? parseInt(result[7]) : 0;
                    const date = new Date(
                        parseInt(result[1]),
                        parseInt(result[2]) - 1,
                        parseInt(result[3]),
                        parseInt(result[4]),
                        parseInt(result[5]),
                        sec
                    );
                    if (!isNaN(date.getTime())) {
                        this.firstTime = false;
                        return new NetezzaDataType('DATETIME');
                    }
                } catch {
                    // Invalid datetime, continue
                }
            }
        }

        // Default to NVARCHAR
        let tmpLen = Math.max(strLen + 5, 20);
        if (this.currentType.length !== undefined && tmpLen < this.currentType.length) {
            tmpLen = this.currentType.length;
        }

        this.firstTime = false;
        return new NetezzaDataType('NVARCHAR', undefined, undefined, tmpLen);
    }

    refreshCurrentType(strVal: string): NetezzaDataType {
        this.currentType = this.getType(strVal);
        return this.currentType;
    }
}

/**
 * Import options
 */
export interface ImportOptions {
    delimiter?: string;
    encoding?: string;
    skipRows?: number;
    maxErrors?: number;
}

/**
 * Import result
 */
export interface ImportResult {
    success: boolean;
    message: string;
    details?: {
        sourceFile?: string;
        targetTable?: string;
        fileSize?: number;
        format?: string;
        rowsProcessed?: number;
        rowsInserted?: number;
        processingTime?: string;
        columns?: number;
        detectedDelimiter?: string;
    };
}

/**
 * Progress callback function type
 */
export type ProgressCallback = (message: string) => void;

/**
 * Netezza Data Importer class
 */
export class NetezzaImporter {
    private filePath: string;
    private targetTable: string;
    private connectionString: string;
    private logDir: string;

    // Pipe settings
    private pipeName: string;
    private delimiter: string = '\t';
    private delimiterPlain: string = '\\t';
    private recordDelim: string = '\n';
    private recordDelimPlain: string = '\\n';
    private escapechar: string = '\\';

    // CSV settings
    private csvDelimiter: string = ',';

    // Excel data storage (when reading xlsx/xlsb files)
    private excelData: string[][] = [];
    private isExcelFile: boolean = false;

    // Data analysis
    private sqlHeaders: string[] = [];
    private dataTypes: ColumnTypeChooser[] = [];
    private rowsCount: number = 0;
    private valuesToEscape: string[] = [];

    constructor(filePath: string, targetTable: string, connectionString: string, logDir?: string) {
        this.filePath = filePath;
        this.targetTable = targetTable;
        this.connectionString = connectionString;
        this.logDir = logDir || path.join(path.dirname(filePath), 'netezza_logs');

        // Check if this is an Excel file
        const fileExt = path.extname(filePath).toLowerCase();
        this.isExcelFile = ['.xlsx', '.xlsb'].includes(fileExt);

        // Pipe settings
        const pipeNum = Math.floor(Math.random() * 1000);
        this.pipeName = `\\\\.\\pipe\\NETEZZA_IMPORT_${pipeNum}`;
        this.valuesToEscape = [this.escapechar, this.recordDelim, '\r', this.delimiter];

        // Ensure log directory exists
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
    }

    /**
     * Auto-detect CSV delimiter
     */
    private detectCsvDelimiter(): void {
        const content = fs.readFileSync(this.filePath, 'utf-8');
        let firstLine = content.split('\n')[0] || '';

        // Handle UTF-8 BOM
        if (firstLine.startsWith('\ufeff')) {
            firstLine = firstLine.slice(1);
        }

        // Count delimiters and choose the most frequent
        const delimiters = [';', '\t', '|', ','];
        const counts: { [key: string]: number } = {};

        for (const delim of delimiters) {
            counts[delim] = (firstLine.match(new RegExp(delim === '|' ? '\\|' : delim, 'g')) || []).length;
        }

        const maxCount = Math.max(...Object.values(counts));
        if (maxCount > 0) {
            this.csvDelimiter = Object.keys(counts).find(k => counts[k] === maxCount) || ',';
        }
    }

    /**
     * Clean column name for SQL compatibility
     */
    private cleanColumnName(colName: string): string {
        let cleanName = String(colName).trim();
        cleanName = cleanName.replace(/[^0-9a-zA-Z]+/g, '_').toUpperCase();
        if (!cleanName || /^\d/.test(cleanName)) {
            cleanName = 'COL_' + cleanName;
        }
        return cleanName;
    }

    /**
     * Parse a CSV line handling quoted fields
     */
    private parseCsvLine(line: string): string[] {
        const result: string[] = [];
        let current = '';
        let inQuotes = false;

        for (let i = 0; i < line.length; i++) {
            const char = line[i];

            if (char === '"') {
                if (inQuotes && line[i + 1] === '"') {
                    current += '"';
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (char === this.csvDelimiter && !inQuotes) {
                result.push(current);
                current = '';
            } else {
                current += char;
            }
        }
        result.push(current);

        return result;
    }

    /**
     * Read Excel file (xlsx/xlsb) and convert to 2D array
     */
    private readExcelFile(progressCallback?: ProgressCallback): string[][] {
        if (!XLSX) {
            throw new Error('XLSX module not available');
        }

        progressCallback?.('Reading Excel file...');
        const workbook = XLSX.readFile(this.filePath, { type: 'file' });

        // Get the first sheet
        const sheetName = workbook.SheetNames[0];
        if (!sheetName) {
            throw new Error('Excel file has no sheets');
        }

        progressCallback?.(`Processing sheet: ${sheetName}`);
        const sheet = workbook.Sheets[sheetName];

        // Convert sheet to 2D array (with header row)
        const data: string[][] = XLSX.utils.sheet_to_json(sheet, {
            header: 1,
            raw: false,
            defval: ''
        }) as string[][];

        // Convert all values to strings
        const stringData: string[][] = data.map(row =>
            row.map(cell => cell !== null && cell !== undefined ? String(cell) : '')
        );

        progressCallback?.(`Excel file loaded: ${stringData.length} rows, ${stringData[0]?.length || 0} columns`);
        return stringData;
    }

    /**
     * Analyze file to determine column types (supports CSV, TXT, XLSX, XLSB)
     */
    async analyzeDataTypes(progressCallback?: ProgressCallback): Promise<ColumnTypeChooser[]> {
        progressCallback?.('Analyzing data types...');

        let rows: string[][];

        if (this.isExcelFile) {
            // Read Excel file
            this.excelData = this.readExcelFile(progressCallback);
            rows = this.excelData;
        } else {
            // Read CSV/TXT file
            this.detectCsvDelimiter();

            let content = fs.readFileSync(this.filePath, 'utf-8');

            // Handle UTF-8 BOM
            if (content.startsWith('\ufeff')) {
                content = content.slice(1);
            }

            const lines = content.split(/\r?\n/);
            rows = [];

            for (const line of lines) {
                if (line.trim()) {
                    rows.push(this.parseCsvLine(line));
                }
            }
        }

        if (!rows || rows.length === 0) {
            throw new Error('No data found in file');
        }

        const dataTypes: ColumnTypeChooser[] = [];

        // Process headers (first row)
        this.sqlHeaders = rows[0].map(col => this.cleanColumnName(col || 'COLUMN'));
        for (let j = 0; j < rows[0].length; j++) {
            dataTypes.push(new ColumnTypeChooser());
        }

        // Process data rows
        for (let i = 1; i < rows.length; i++) {
            const row = rows[i];
            for (let j = 0; j < row.length; j++) {
                if (j < dataTypes.length && row[j] && row[j].trim()) {
                    dataTypes[j].refreshCurrentType(row[j].trim());
                }
            }

            if (i % 10000 === 0) {
                progressCallback?.(`Analyzed ${i.toLocaleString()} rows...`);
            }
        }

        this.rowsCount = rows.length - 1; // Exclude header
        progressCallback?.(`Analysis complete: ${this.rowsCount.toLocaleString()} rows`);

        this.dataTypes = dataTypes;
        return dataTypes;
    }

    /**
     * Escape special characters for Netezza import
     */
    private escapeValue(val: string): string {
        let result = String(val).trim();
        for (const char of this.valuesToEscape) {
            result = result.split(char).join(`${this.escapechar}${char}`);
        }
        return result;
    }

    /**
     * Format value according to column type
     */
    private formatValue(val: string, colIndex: number): string {
        let result = this.escapeValue(val);
        if (colIndex < this.dataTypes.length && this.dataTypes[colIndex].currentType.dbType === 'DATETIME') {
            result = result.replace('T', ' ');
        }
        return result;
    }

    /**
     * Generate CREATE TABLE SQL with detected column types
     */
    generateCreateTableSql(): string {
        const columns: string[] = [];
        for (let i = 0; i < this.sqlHeaders.length; i++) {
            const header = this.sqlHeaders[i];
            const typeChooser = this.dataTypes[i];
            columns.push(`        ${header} ${typeChooser.currentType.toString()}`);
        }

        const logDirUnix = this.logDir.replace(/\\/g, '/');

        return `CREATE TABLE ${this.targetTable} AS 
(
    SELECT * FROM EXTERNAL '${this.pipeName}'
    (
${columns.join(',\n')}
    )
    USING
    (
        REMOTESOURCE 'odbc'
        DELIMITER '${this.delimiterPlain}'
        RecordDelim '${this.recordDelimPlain}'
        ESCAPECHAR '${this.escapechar}'
        NULLVALUE ''
        ENCODING 'utf-8'
        TIMESTYLE '24HOUR'
        SKIPROWS 0
        MAXERRORS 10
        LOGDIR '${logDirUnix}'
    )
) DISTRIBUTE ON RANDOM;`;
    }

    /**
     * Create temporary data file for import (file-based approach, supports CSV/TXT and Excel)
     */
    async createDataFile(progressCallback?: ProgressCallback): Promise<string> {
        const tempFile = path.join(this.logDir, `netezza_import_data_${Math.floor(Math.random() * 1000)}.txt`);
        progressCallback?.(`Creating temporary data file: ${tempFile}`);

        try {
            let rows: string[][];

            if (this.isExcelFile) {
                // Use cached Excel data (headers excluded)
                if (!this.excelData || this.excelData.length === 0) {
                    throw new Error('Excel data not loaded. Call analyzeDataTypes first.');
                }
                rows = this.excelData.slice(1); // Skip header row
            } else {
                // Read CSV/TXT file
                let content = fs.readFileSync(this.filePath, 'utf-8');

                // Handle UTF-8 BOM
                if (content.startsWith('\ufeff')) {
                    content = content.slice(1);
                }

                const lines = content.split(/\r?\n/);
                rows = [];
                let skipFirst = true;

                for (const line of lines) {
                    if (!line.trim()) continue;

                    if (skipFirst) {
                        // Skip header
                        skipFirst = false;
                        continue;
                    }

                    rows.push(this.parseCsvLine(line));
                }
            }

            // Format and write data
            const outputLines: string[] = [];
            for (let i = 0; i < rows.length; i++) {
                const row = rows[i];
                const formattedRow = row.map((value, j) => this.formatValue(value || '', j));
                outputLines.push(formattedRow.join(this.delimiter));

                if ((i + 1) % 10000 === 0) {
                    progressCallback?.(`Processed ${(i + 1).toLocaleString()} rows...`);
                }
            }

            fs.writeFileSync(tempFile, outputLines.join(this.recordDelim), 'utf-8');

            // Update pipe name to point to the file
            this.pipeName = tempFile.replace(/\\/g, '/');
            progressCallback?.(`Data file created: ${this.pipeName}`);

            return tempFile;
        } catch (e: any) {
            throw new Error(`Error creating data file: ${e.message}`);
        }
    }

    /**
     * Get rows count
     */
    getRowsCount(): number {
        return this.rowsCount;
    }

    /**
     * Get SQL headers
     */
    getSqlHeaders(): string[] {
        return this.sqlHeaders;
    }

    /**
     * Get CSV delimiter
     */
    getCsvDelimiter(): string {
        return this.csvDelimiter;
    }
}

/**
 * Import data from a file to Netezza table
 */
export async function importDataToNetezza(
    filePath: string,
    targetTable: string,
    connectionString: string,
    options?: ImportOptions,
    progressCallback?: ProgressCallback
): Promise<ImportResult> {
    const startTime = Date.now();

    try {
        // Validate parameters
        if (!filePath || !fs.existsSync(filePath)) {
            return {
                success: false,
                message: `Source file not found: ${filePath}`
            };
        }

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

        // Get file info
        const fileStats = fs.statSync(filePath);
        const fileSize = fileStats.size;
        const fileExt = path.extname(filePath).toLowerCase();

        // Check supported formats
        const supportedFormats = ['.csv', '.txt', '.xlsx', '.xlsb'];
        if (!supportedFormats.includes(fileExt)) {
            return {
                success: false,
                message: `Unsupported file format: ${fileExt}. Supported: ${supportedFormats.join(', ')}`
            };
        }

        // Check if XLSX module is available for Excel files
        const excelFormats = ['.xlsx', '.xlsb'];
        if (excelFormats.includes(fileExt) && !XLSX) {
            return {
                success: false,
                message: 'XLSX module not available. Please run: npm install xlsx'
            };
        }

        progressCallback?.('Starting import process...');
        progressCallback?.(`  Source file: ${filePath}`);
        progressCallback?.(`  Target table: ${targetTable}`);
        progressCallback?.(`  File size: ${fileSize.toLocaleString()} bytes`);
        progressCallback?.(`  File format: ${fileExt}`);

        // Create importer instance
        const importer = new NetezzaImporter(filePath, targetTable, connectionString);

        // Analyze data types
        await importer.analyzeDataTypes(progressCallback);

        // Create data file (file-based approach for Node.js)
        progressCallback?.('Using file-based import...');
        const tempFile = await importer.createDataFile(progressCallback);

        // Generate SQL
        const createSql = importer.generateCreateTableSql();
        progressCallback?.('Generated SQL:');
        progressCallback?.(createSql);

        // Execute import
        progressCallback?.('Connecting to Netezza...');

        if (!odbc) {
            throw new Error('ODBC module not available');
        }

        const connection = await odbc.connect(connectionString);

        try {
            progressCallback?.('Executing CREATE TABLE with EXTERNAL data...');
            await connection.query(createSql);
            progressCallback?.('Import completed successfully');
        } finally {
            await connection.close();

            // Clean up temp file
            try {
                if (fs.existsSync(tempFile)) {
                    fs.unlinkSync(tempFile);
                    progressCallback?.('Temporary data file cleaned up');
                }
            } catch (e: any) {
                progressCallback?.(`Warning: Could not clean up temp file: ${e.message}`);
            }
        }

        const processingTime = (Date.now() - startTime) / 1000;

        return {
            success: true,
            message: 'Import completed successfully',
            details: {
                sourceFile: filePath,
                targetTable: targetTable,
                fileSize: fileSize,
                format: fileExt,
                rowsProcessed: importer.getRowsCount(),
                rowsInserted: importer.getRowsCount(),
                processingTime: `${processingTime.toFixed(1)}s`,
                
                columns: importer.getSqlHeaders().length,
                detectedDelimiter: importer.getCsvDelimiter()
            }
        };

    } catch (e: any) {
        const processingTime = (Date.now() - startTime) / 1000;
        return {
            success: false,
            message: `Import failed: ${e.message}`,
            details: {
                processingTime: `${processingTime.toFixed(1)}s`
            }
        };
    }
}
