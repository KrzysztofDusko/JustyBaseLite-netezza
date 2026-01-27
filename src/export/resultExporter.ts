import * as fs from 'fs';
import { ResultSet } from '../types';

export interface ExportOptions {
    format: 'csv' | 'json' | 'xml' | 'sql' | 'markdown';
    rowIndices?: number[];
    columnIds?: string[]; // IDs of visible columns
}

export async function exportResultSetToFile(
    resultSet: ResultSet,
    filePath: string,
    options: ExportOptions
): Promise<void> {
    const { format, rowIndices, columnIds } = options;
    const writeStream = fs.createWriteStream(filePath, { encoding: 'utf8' });

    try {
        const rowsToExport = rowIndices
            ? rowIndices.map(idx => resultSet.data[idx]).filter(row => row !== undefined)
            : resultSet.data;

        // Determine which columns to export
        const columns = resultSet.columns;
        const visibleColumnIndices = columnIds
            ? columnIds.map(id => parseInt(id)).filter(idx => !isNaN(idx) && idx >= 0 && idx < columns.length)
            : columns.map((_, i) => i);

        const exportedColumns = visibleColumnIndices.map(idx => columns[idx]);

        switch (format) {
            case 'csv':
                await streamCsv(writeStream, exportedColumns, rowsToExport, visibleColumnIndices);
                break;
            case 'json':
                await streamJson(writeStream, exportedColumns, rowsToExport, visibleColumnIndices);
                break;
            case 'xml':
                await streamXml(writeStream, exportedColumns, rowsToExport, visibleColumnIndices);
                break;
            case 'sql':
                await streamSql(writeStream, exportedColumns, rowsToExport, visibleColumnIndices);
                break;
            case 'markdown':
                await streamMarkdown(writeStream, exportedColumns, rowsToExport, visibleColumnIndices);
                break;
        }
    } finally {
        writeStream.end();
        await new Promise<void>((resolve, reject) => {
            writeStream.on('finish', resolve);
            writeStream.on('error', reject);
        });
    }
}

async function streamCsv(
    stream: fs.WriteStream,
    columns: { name: string }[],
    rows: unknown[][],
    columnIndices: number[]
) {
    // Header
    stream.write(columns.map(c => escapeCsv(c.name)).join(',') + '\n');

    for (const row of rows) {
        const line = columnIndices.map(idx => escapeCsv(row[idx])).join(',') + '\n';
        if (!stream.write(line)) {
            await new Promise(resolve => stream.once('drain', resolve));
        }
    }
}

async function streamJson(
    stream: fs.WriteStream,
    columns: { name: string }[],
    rows: unknown[][],
    columnIndices: number[]
) {
    stream.write('[\n');
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const obj: Record<string, unknown> = {};
        columnIndices.forEach((colIdx, j) => {
            obj[columns[j].name] = row[colIdx];
        });

        const line = '  ' + JSON.stringify(obj, bigIntReplacer) + (i < rows.length - 1 ? ',' : '') + '\n';
        if (!stream.write(line)) {
            await new Promise(resolve => stream.once('drain', resolve));
        }
    }
    stream.write(']');
}

async function streamXml(
    stream: fs.WriteStream,
    columns: { name: string }[],
    rows: unknown[][],
    columnIndices: number[]
) {
    stream.write('<?xml version="1.0" encoding="UTF-8"?>\n<results>\n');
    for (const row of rows) {
        stream.write('  <row>\n');
        columnIndices.forEach((colIdx, j) => {
            const val = row[colIdx];
            const tagName = columns[j].name.replace(/[^a-zA-Z0-9_-]/g, '_');
            const content = escapeXml(val);
            stream.write(`    <${tagName}>${content}</${tagName}>\n`);
        });
        stream.write('  </row>\n');

        // Yield if needed? For 50k rows this is fine but let's be safe
        await new Promise(resolve => setImmediate(resolve));
    }
    stream.write('</results>');
}

async function streamSql(
    stream: fs.WriteStream,
    columns: { name: string; type?: string }[],
    rows: unknown[][],
    columnIndices: number[]
) {
    const tableName = 'EXPORT_TABLE';
    const colNames = columns.map(c => c.name.replace(/[^a-zA-Z0-9_]/g, '') || 'COL').join(', ');

    for (const row of rows) {
        const values = columnIndices.map((colIdx, j) => {
            const val = row[colIdx];
            const type = columns[j].type;
            return formatSqlValue(val, type);
        });

        const line = `INSERT INTO ${tableName} (${colNames}) VALUES (${values.join(', ')});\n`;
        if (!stream.write(line)) {
            await new Promise(resolve => stream.once('drain', resolve));
        }
    }
}

async function streamMarkdown(
    stream: fs.WriteStream,
    columns: { name: string }[],
    rows: unknown[][],
    columnIndices: number[]
) {
    // Header
    stream.write('| ' + columns.map(c => c.name.replace(/\|/g, '\\|')).join(' | ') + ' |\n');
    // Separator
    stream.write('| ' + columns.map(() => '---').join(' | ') + ' |\n');

    for (const row of rows) {
        const rowData = columnIndices.map(colIdx => {
            const val = row[colIdx];
            if (val === null || val === undefined) return '';
            return String(val).replace(/\|/g, '\\|').replace(/\r?\n/g, ' ');
        });
        const line = '| ' + rowData.join(' | ') + ' |\n';
        if (!stream.write(line)) {
            await new Promise(resolve => stream.once('drain', resolve));
        }
    }
}

function escapeCsv(val: unknown): string {
    if (val === null || val === undefined) return '';
    const str = String(val);
    if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
        return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
}

function escapeXml(val: unknown): string {
    if (val === null || val === undefined) return '';
    return String(val)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;');
}

function formatSqlValue(val: unknown, type?: string): string {
    if (val === null || val === undefined) return 'NULL';

    // Check type if possible
    const upperType = type?.toUpperCase();
    if (upperType === 'BOOLEAN') return val ? 'TRUE' : 'FALSE';
    if (['INTEGER', 'BIGINT', 'SMALLINT', 'DECIMAL', 'NUMERIC', 'REAL', 'DOUBLE PRECISION', 'FLOAT4', 'FLOAT8', 'INT2', 'INT4', 'INT8'].includes(upperType || '')) {
        return String(val);
    }

    if (typeof val === 'number') return String(val);
    if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';

    return `'${String(val).replace(/'/g, "''")}'`;
}

const bigIntReplacer = (_key: string, value: unknown) => {
    if (typeof value === 'bigint') {
        if (value >= Number.MIN_SAFE_INTEGER && value <= Number.MAX_SAFE_INTEGER) {
            return Number(value);
        }
        return value.toString();
    }
    return value;
};
