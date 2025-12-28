/**
 * Data Importer for Netezza
 * Handles importing data from various file formats to Netezza tables
 * Ported from Python data_importer.py
 */

import * as fs from 'fs';
import * as path from 'path';
import { Readable } from 'stream';
import { ConnectionDetails, NzConnection } from '../types';

// Helper to unblock event loop
const delay = () => new Promise(resolve => setTimeout(resolve, 0));

// XLSX import for Excel file support
// Custom Excel Reader from ExcelHelpersTs
interface IExcelReader {
    open(path: string): Promise<void>;
    read(): Promise<boolean>;
    close(): Promise<void>;
    _currentRow: unknown[];
}

interface IReaderFactory {
    create(path: string): IExcelReader;
}

let ReaderFactory: IReaderFactory | undefined;
try {
    const readerModule = require('../../libs/ExcelHelpersTs/ReaderFactory');
    // Handle both ES module default export and named export
    ReaderFactory = (readerModule.default || readerModule.ReaderFactory || readerModule) as IReaderFactory;
} catch (e: unknown) {
    console.error('libs/ExcelHelpersTs/ReaderFactory module not available', e);
}

// ConnectionDetails is imported from '../types' - no need for parseConnectionString

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

    constructor(decimalDelimiter: string = '.') {
        this.currentType = new NetezzaDataType('BIGINT');
        this.decimalDelimInCsv = decimalDelimiter;
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
        // Escape delimiter for regex if it's a special char
        const delim = this.decimalDelimInCsv === '.' ? '\\.' : this.decimalDelimInCsv;
        const decimalCnt = (strVal.match(new RegExp(`${delim}`, 'g')) || []).length;

        if (['BIGINT', 'NUMERIC'].includes(currentDbType) && decimalCnt <= 1) {
            const strValClean = strVal.replace(this.decimalDelimInCsv, '');
            // Also handle if some people use thousand separators?
            // For now, let's assume raw clipboard data usually just has the decimal sep if it's "12,5"
            // If we want to be robust against "1.000,00", that's more complex.
            // The request specifically mentions "12,5 ; 37,5", suggesting simple locale decimal.

            if (
                /^\d+$/.test(strValClean) &&
                strLen < 15 &&
                (!strValClean.startsWith('0') || decimalCnt > 0 || strValClean === '0')
            ) {
                this.firstTime = false;
                return new NetezzaDataType('NUMERIC', 16, 6);
            }
        }

        // DATE check (2024-06-07, 2024-6-7)
        if (
            (currentDbType === 'DATE' || this.firstTime) &&
            (strVal.match(/-/g) || []).length === 2 &&
            strLen >= 8 &&
            strLen <= 10
        ) {
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
        if (
            (currentDbType === 'DATETIME' || this.firstTime) &&
            (strVal.match(/-/g) || []).length === 2 &&
            strLen >= 12 &&
            strLen <= 20
        ) {
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

        // DATETIME check (dd.mm.yyyy HH:mm)
        // Also accepts dd.mm.yyyy without time, but that might be DATE?
        // User requested: "dd.mm.yyyy hh24:mi jako datetime"
        if ((currentDbType === 'DATETIME' || this.firstTime) && (strVal.match(/\./g) || []).length >= 2) {
            const result = strVal.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?$/);
            if (result) {
                try {
                    const day = parseInt(result[1]);
                    const month = parseInt(result[2]) - 1;
                    const year = parseInt(result[3]);
                    const hour = result[4] ? parseInt(result[4]) : 0;
                    const min = result[5] ? parseInt(result[5]) : 0;
                    const sec = result[6] ? parseInt(result[6]) : 0;

                    // Validate day/month/year ranges roughly
                    if (month >= 0 && month <= 11 && day >= 1 && day <= 31) {
                        const date = new Date(year, month, day, hour, min, sec);
                        if (
                            !isNaN(date.getTime()) &&
                            date.getFullYear() === year &&
                            date.getMonth() === month &&
                            date.getDate() === day
                        ) {
                            this.firstTime = false;
                            return new NetezzaDataType('DATETIME');
                        }
                    }
                } catch {
                    // Invalid datetime
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
export type ProgressCallback = (message: string, increment?: number, logToOutput?: boolean) => void;


/**
 * Data Generator Stream
 * Generates formatted rows on demand for Netezza import
 */
class DataGeneratorStream extends Readable {
    private rows: string[][];
    private currentIndex: number = 0;
    private importer: NetezzaImporter;
    public byteLength: number = 0; // Estimation
    private progressCallback?: ProgressCallback;
    private lastReportedIndex: number = -1;
    private lastReportTime: number = 0;
    private lastReportedMessage: string = '';

    constructor(importer: NetezzaImporter, rows: string[][], progressCallback?: ProgressCallback) {
        super();
        this.importer = importer;
        this.rows = rows;
        this.progressCallback = progressCallback;
        this.currentIndex = 0;
        this.lastReportedIndex = -1;
        this.lastReportTime = 0;
        this.lastReportedMessage = '';

        // Estimate byte size roughly (can be expensive to calculate exactly)
        // We'll just sum raw lengths + delimiters as a rough guess for progress bar
        try {
            this.byteLength = rows.reduce((acc, row) => {
                const rowLen = row.reduce((rAcc, val) => rAcc + (val ? val.length : 0), 0);
                return acc + rowLen + row.length; // + delimiters
            }, 0);
        } catch {
            this.byteLength = 1024 * 1024; // Default 1MB if calculation fails
        }
    }

    _read(_size: number): void {
        try {
            // Push rows until we hit highWaterMark or end
            let more = true;


            while (more && this.currentIndex < this.rows.length) {
                const row = this.rows[this.currentIndex];
                const formattedRow = row.map((value, j) => this.importer.formatValue(value || '', j));
                const line = formattedRow.join(this.importer.getDelimiter()) + this.importer.getRecordDelim();

                more = this.push(Buffer.from(line, 'utf8'));
                this.currentIndex++;
            }

            // Report progress logic
            const total = this.rows.length;

            // Should report if:
            // 1. We are at the end
            // 2. OR we have advanced significantly since last report AND enough percentage has passed
            // 3. AND we haven't reported this exact index before (suppress duplicates)

            if (this.currentIndex > this.lastReportedIndex) {
                const now = Date.now();
                const isComplete = this.currentIndex >= total;
                // Report if complete OR at least 1 second passed since last report
                const shouldReport = isComplete || (now - this.lastReportTime >= 1000);

                if (shouldReport) {
                    const percent = Math.floor((this.currentIndex / total) * 100);
                    const message = `Importing: ${percent}% complete (${this.currentIndex.toLocaleString()} / ${total.toLocaleString()} rows)`;

                    // Determine increment based on % change since last report? 
                    // Or just use 0 if we assume previous increments were handled?
                    // Better to calculate strict increment:
                    const processedDelta = this.currentIndex - (this.lastReportedIndex < 0 ? 0 : this.lastReportedIndex);
                    const increment = (processedDelta / total) * 100;

                    if (message !== this.lastReportedMessage) {
                        this.progressCallback?.(message, increment, false); // Don't log to Output
                        this.lastReportedIndex = this.currentIndex;
                        this.lastReportTime = now;
                        this.lastReportedMessage = message;
                    }
                }
            }

            if (this.currentIndex >= this.rows.length) {
                this.push(null); // End of stream
            }
        } catch (e) {
            this.emit('error', e);
        }
    }
}


/**
 * Netezza Data Importer class
 */
export class NetezzaImporter {
    private filePath: string;
    private targetTable: string;
    private logDir: string;

    // Pipe settings
    private virtualFileName: string;
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

    constructor(filePath: string, targetTable: string, logDir?: string) {
        this.filePath = filePath;
        this.targetTable = targetTable;
        this.logDir = logDir || path.join(path.dirname(filePath), 'netezza_logs');

        // Check if this is an Excel file
        const fileExt = path.extname(filePath).toLowerCase();
        this.isExcelFile = ['.xlsx', '.xlsb'].includes(fileExt);

        // Pipe settings
        // Use a generic virtual filename that the modified driver will recognize
        this.virtualFileName = `virtual_import_${Date.now()}_${Math.floor(Math.random() * 1000)}.txt`;
        this.valuesToEscape = [this.escapechar, this.recordDelim, '\r', this.delimiter];

        // Log dir is still useful for log files from Netezza if any (though mapped through stream now?)
        // Actually, Netezza logs come back as data streams too in the new driver version
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
    }

    getDelimiter(): string { return this.delimiter; }
    getRecordDelim(): string { return this.recordDelim; }


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
        if (!cleanName || /^\d/.test(cleanName) || cleanName.startsWith('_')) {
            cleanName = 'COL' + (cleanName.startsWith('_') ? '' : '_') + cleanName;
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
    /**
     * Read Excel file (xlsx/xlsb) and convert to 2D array
     */
    private async readExcelFile(progressCallback?: ProgressCallback): Promise<string[][]> {
        if (!ReaderFactory) {
            throw new Error('ReaderFactory module not available');
        }

        progressCallback?.('Reading Excel file...');

        const reader = ReaderFactory.create(this.filePath);
        await reader.open(this.filePath);

        const rows: string[][] = [];

        // Helper to format values
        const valToString = (val: unknown): string => {
            if (val === null || val === undefined) return '';
            if (val instanceof Date) {
                // Format Date to YYYY-MM-DD HH:mm:ss
                // We use local parts to avoid timezone shifting if the reader produced local time
                const pad = (n: number) => (n < 10 ? '0' + n : n);
                return `${val.getFullYear()}-${pad(val.getMonth() + 1)}-${pad(val.getDate())} ${pad(val.getHours())}:${pad(val.getMinutes())}:${pad(val.getSeconds())}`;
            }
            return String(val);
        };

        let rowCount = 0;
        while (await reader.read()) {
            const row: string[] = [];
            // Assuming fieldCount gives the max column index found so far.
            // But for the current row, we might need to rely on internal array or just loop safe amount?
            // XlsxReader and XlsbReader update _currentRow.
            // We can access it directly if we cast reader to any, or use fieldCount.
            // However, fieldCount is "max field count seen so far".
            // A safer way is to use the length of the internal row array if accessible, or just getValue loop.
            // XlsxReader internal: this._currentRow

            // We'll iterate up to fieldCount? No, fieldCount might grow.
            // _currentRow.length is reliable if we trust the reader implementation.
            const currentRow = reader._currentRow;
            if (currentRow && Array.isArray(currentRow)) {
                for (let i = 0; i < currentRow.length; i++) {
                    row.push(valToString(currentRow[i]));
                }
            }

            rows.push(row);
            rowCount++;

            if (rowCount % 10000 === 0) {
                progressCallback?.(`Processed ${rowCount.toLocaleString()} rows...`, undefined, false);
                // Unblock UI
                await delay();
            }
        }

        // Reader cleanup (close zip if needed)
        // XlsxReader has close()?
        if (typeof reader.close === 'function') {
            await reader.close();
        }

        progressCallback?.(`Excel file loaded: ${rows.length} rows`);
        return rows;
    }

    /**
     * Analyze file to determine column types (supports CSV, TXT, XLSX, XLSB)
     */
    async analyzeDataTypes(progressCallback?: ProgressCallback): Promise<ColumnTypeChooser[]> {
        progressCallback?.('Analyzing data types...');

        let rows: string[][];

        if (this.isExcelFile) {
            // Read Excel file
            this.excelData = await this.readExcelFile(progressCallback);
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
                progressCallback?.(`Analyzed ${i.toLocaleString()} rows...`, undefined, false);
                // Unblock UI
                await delay();
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
    formatValue(val: string, colIndex: number): string {
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
    SELECT * FROM EXTERNAL '${this.virtualFileName}'
    (
${columns.join(',\n')}
    )
    USING
    (
        REMOTESOURCE 'jdbc'
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
     * Create data stream (in-memory) from file content (CSV/Excel)
     */
    async createDataStream(progressCallback?: ProgressCallback): Promise<Readable> {
        progressCallback?.('Preparing data stream...');

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

            this.rowsCount = rows.length; // Ensure count is accurate

            return new DataGeneratorStream(this, rows, progressCallback);
        } catch (e: unknown) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            progressCallback?.(`Error preparing stream: ${errorMsg}`);
            throw e;
        }
    }

    // Public getter for pipeName to register it
    getVirtualFileName(): string {
        return this.virtualFileName;
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
    connectionDetails: ConnectionDetails,
    progressCallback?: ProgressCallback
): Promise<ImportResult> {
    const startTime = Date.now();
    let connection: NzConnection | null = null;

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

        if (!connectionDetails || !connectionDetails.host) {
            return {
                success: false,
                message: 'Connection details are required'
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

        progressCallback?.('Starting import process...');
        progressCallback?.(`  Source file: ${filePath}`);
        progressCallback?.(`  Target table: ${targetTable}`);
        progressCallback?.(`  File size: ${fileSize.toLocaleString()} bytes`);
        progressCallback?.(`  File format: ${fileExt}`);

        // Create importer instance (logDir defaults to netezza_logs alongside source file)
        const importer = new NetezzaImporter(filePath, targetTable);

        // Analyze data types
        await importer.analyzeDataTypes(progressCallback);

        // Create data stream (in-memory)
        progressCallback?.('Preparing in-memory data stream...');
        const dataStream = await importer.createDataStream(progressCallback);
        const virtualFileName = importer.getVirtualFileName();
        progressCallback?.(`Registered virtual stream: ${virtualFileName}`);

        // Register stream with driver static registry
        // We need to access the class statically, so we require it
        const NzConnectionClass = require('../../libs/driver/src/NzConnection');
        if (NzConnectionClass && NzConnectionClass.registerImportStream) {
            NzConnectionClass.registerImportStream(virtualFileName, dataStream);
        } else {
            progressCallback?.('Warning: NzConnection driver does not support stream registry. Import might fail.');
        }

        // Generate SQL
        const createSql = importer.generateCreateTableSql();
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
        await connection!.connect();

        try {
            progressCallback?.('Executing CREATE TABLE with EXTERNAL data...');
            // Create command for the CREATE TABLE AS SELECT ... FROM EXTERNAL
            // NzConnection should handle the external table protocol automatically
            const cmd = connection!.createCommand(createSql);

            // Set 60-minute timeout for large file imports
            cmd.commandTimeout = 3600;

            // Listen for import progress events
            const totalRows = importer.getRowsCount();
            connection!.on('importProgress', (progressData: unknown) => {
                const progress = progressData as { bytesSent: number, totalSize: number, percentComplete: number };
                const estimatedRows = totalRows > 0
                    ? Math.round((progress.percentComplete / 100) * totalRows)
                    : 0;
                progressCallback?.(`Importing: ${progress.percentComplete}% complete (${estimatedRows.toLocaleString()} / ${totalRows.toLocaleString()} rows)`);
            });

            await cmd.execute();

            progressCallback?.('Import completed successfully');
        } finally {
            await connection.close();

            // Clean up registry
            if (NzConnectionClass && NzConnectionClass.unregisterImportStream) {
                NzConnectionClass.unregisterImportStream(virtualFileName);
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
    } catch (e: unknown) {
        const processingTime = (Date.now() - startTime) / 1000;
        const errorMsg = e instanceof Error ? e.message : String(e);
        return {
            success: false,
            message: `Import failed: ${errorMsg}`,
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
    }
}
