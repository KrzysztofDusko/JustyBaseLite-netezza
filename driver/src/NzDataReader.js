const { NzType } = require('./protocol/constants');

/**
 * Data reader for query results
 * Port of C# NzDataReader.cs
 */
class NzDataReader {


    // Changing strategy: Modify NzConnection.js to peek and pass nextItem.
    // Here I just accept it.
    constructor(command, generator, columns, releaseCallback, initialNextItem) {
        this.command = command;
        this.generator = generator;
        this.columnDescriptions = columns || [];
        this.releaseCallback = releaseCallback;

        this.currentRow = null;
        this.closed = false;

        // Internal state
        this._initNameIndex();
        this._pendingColumns = null;
        this._isFinished = false;

        this._nextItem = initialNextItem;
        this._hasRows = !!(this._nextItem && this._nextItem.type === 'DataRow');
    }

    get hasRows() {
        return this._hasRows;
    }

    _initNameIndex() {
        this._nameIndex = {};
        if (this.columnDescriptions) {
            for (let i = 0; i < this.columnDescriptions.length; i++) {
                this._nameIndex[this.columnDescriptions[i].name.toLowerCase()] = i;
            }
        }
    }

    // Auto-release connection when reader is fully consumed
    _markFinished() {
        this._isFinished = true;
        this._nextItem = null;
        this._hasRows = false;
        // Auto-release connection so another command can execute
        if (this.releaseCallback) {
            this.releaseCallback();
            this.releaseCallback = null;
        }
    }

    async nextResult() {
        if (this._pendingColumns) {
            this.columnDescriptions = this._pendingColumns;
            this._pendingColumns = null;
            this._initNameIndex();
            this.currentRow = null;

            // Loop past any RowDescriptionStandard to find actual DataRow or end marker
            while (true) {
                const nextRes = await this.generator.next();
                if (nextRes.done) {
                    this._nextItem = null;
                    this._hasRows = false;
                    break;
                }
                const val = nextRes.value;
                if (val.type === 'RowDescriptionStandard') {
                    // Skip binary format descriptor, continue looping
                    continue;
                } else if (val.type === 'DataRow') {
                    this._nextItem = val;
                    this._hasRows = true;
                    break;
                } else if (val.type === 'CommandComplete') {
                    // No rows in this result set
                    this._nextItem = val;
                    this._hasRows = false;
                    break;
                } else {
                    // Other types (ReadyForQuery, etc)
                    this._nextItem = val;
                    this._hasRows = false;
                    break;
                }
            }

            return true;
        }

        if (this.closed || this._isFinished) return false;

        // Consume generator until next RowDescription or End
        while (true) {
            // We might have _nextItem consumed? No, nextResult implies we are done with current set.
            // But if we didn't read all rows?
            // "nextResult" should skip remaining rows of current set?
            // Yes.

            // If we have _nextItem and it is DataRow, we discard it and continue.
            // Loop:

            let res;
            if (this._nextItem) {
                res = { value: this._nextItem, done: false };
                this._nextItem = null;
            } else {
                res = await this.generator.next();
            }

            if (res.done) {
                this._markFinished();
                return false;
            }
            const val = res.value;

            // Handle text format RowDescription - has full column metadata
            if (val.type === 'RowDescription') {
                this.columnDescriptions = val.columns;
                this._initNameIndex();
                this.currentRow = null;
                // Continue looping to skip past RowDescriptionStandard and find actual DataRow
                continue;
            }

            if (val.type === 'RowDescriptionStandard') {
                // For RowDescriptionStandard, get columns from command's PreparedStatement if we don't have them
                const ps = this.command?._preparedStatement;
                if (this.columnDescriptions.length === 0 && ps && ps.description) {
                    this.columnDescriptions = ps.description;
                    this._initNameIndex();
                }
                this.currentRow = null;
                // Continue looping to find actual DataRow or CommandComplete
                continue;
            }

            // We've reached actual data or end-of-result marker
            if (val.type === 'DataRow') {
                // Found rows for this result set
                this._nextItem = val;
                this._hasRows = true;
                return true;
            }

            if (val.type === 'CommandComplete') {
                // End of this result set, no rows or all rows consumed
                // Keep looping to find next result set
                continue;
            }

            if (val.type === 'ErrorResponse') {
                throw new Error(val.message || 'Unknown Netezza Error');
            }

            if (val.type === 'ReadyForQuery') {
                this._markFinished();
                return false;
            }
        }
    }

    /**
     * Get schema description of columns
     * @returns {Array<Object>}
     */
    getSchemaTable() {
        if (!this.columnDescriptions) return [];

        const table = [];
        for (let i = 0; i < this.columnDescriptions.length; i++) {
            const col = this.columnDescriptions[i];
            const row = {
                ColumnName: col.name,
                ColumnOrdinal: i + 1, // 1-based index
                ColumnSize: -1,
                NumericPrecision: 0,
                NumericScale: 0,
                DataType: String, // Default
                ProviderType: col.typeOid,
                AllowDBNull: true,
                IsReadOnly: true,
                IsLong: false
            };

            const mod = col.typeMod;
            const oid = col.typeOid;

            // Postgres/Netezza OIDs
            const Oid = {
                Bool: 16,
                Bytea: 17,
                Char: 18,
                Name: 19,
                Int8: 20,
                Int2: 21,
                Int4: 23,
                Text: 25,
                ShowDate: 2530,
                BpChar: 1042,
                VarChar: 1043,
                Date: 1082,
                Time: 1083,
                Timestamp: 1114,
                TimestampTz: 1184,
                Interval: 1186,
                TimeTz: 1266,
                Numeric: 1700
            };

            const TYPE_MOD_OFFSET = 16;

            // Type Mapping Logic
            switch (oid) {
                case Oid.Bool:
                    row.DataType = Boolean;
                    row.ColumnSize = 1;
                    break;
                case Oid.Int2:
                    row.DataType = Number;
                    row.ColumnSize = 2;
                    break;
                case Oid.Int4:
                    row.DataType = Number;
                    row.ColumnSize = 4;
                    break;
                case Oid.Int8:
                    row.DataType = Number;
                    row.ColumnSize = 8;
                    break;
                case 700: // Float4
                case 701: // Float8
                    row.DataType = Number;
                    row.ColumnSize = oid === 701 ? 8 : 4;
                    row.NumericPrecision = oid === 701 ? 53 : 24;
                    break;
                case Oid.Numeric:
                    row.DataType = Number;
                    if (mod > TYPE_MOD_OFFSET) {
                        const p = (mod - TYPE_MOD_OFFSET) >> 16;
                        const s = (mod - TYPE_MOD_OFFSET) & 0xFFFF;
                        row.NumericPrecision = p;
                        row.NumericScale = s;
                        row.ColumnSize = Math.floor(p / 2) + 1;
                        if (row.ColumnSize < col.typeLen && col.typeLen > 0) row.ColumnSize = col.typeLen;
                    } else {
                        row.ColumnSize = col.typeLen;
                    }
                    break;
                case Oid.Date:
                case Oid.Timestamp:
                case Oid.TimestampTz:
                case Oid.Time:
                case Oid.TimeTz:
                    row.DataType = Date;
                    if (oid === Oid.Time || oid === Oid.TimeTz) {
                        row.DataType = Object;
                    }
                    row.ColumnSize = col.typeLen;
                    break;
                case Oid.Char:
                case Oid.BpChar:
                case Oid.VarChar:
                case Oid.Text:
                case Oid.Name:
                case Oid.ShowDate:
                    row.DataType = String;
                    if (mod > TYPE_MOD_OFFSET) {
                        row.ColumnSize = mod - TYPE_MOD_OFFSET;
                    } else {
                        row.ColumnSize = -1;
                        if (col.typeLen > 0) row.ColumnSize = col.typeLen;
                    }
                    if (row.ColumnSize > 8000) row.IsLong = true;
                    break;
                default:
                    row.DataType = String;
                    row.ColumnSize = col.typeLen;
                    break;
            }
            table.push(row);
        }
        return { Rows: table, Columns: { Count: table.length } };
    }

    /**
     * Get data type name of column
     * @param {number} i - column index
     * @returns {string}
     */
    getTypeName(i) {
        if (i < 0 || i >= this.columnDescriptions.length) {
            throw new Error(`Column ordinal ${i} is out of range`);
        }
        const col = this.columnDescriptions[i];

        // Use OID to determine type name
        // Helper mapping based on common OIDs
        const oid = col.typeOid;
        const Oid = {
            Bool: 16,
            Bytea: 17,
            Char: 18,
            Name: 19,
            Int8: 20,
            Int2: 21,
            Int4: 23,
            Text: 25,
            ShowDate: 2530,
            BpChar: 1042,
            VarChar: 1043,
            Date: 1082,
            Time: 1083,
            Timestamp: 1114,
            TimestampTz: 1184,
            Interval: 1186,
            TimeTz: 1266,
            Numeric: 1700,
            Float4: 700,
            Float8: 701
        };

        switch (oid) {
            case Oid.Bool: return 'BOOL';
            case Oid.Bytea: return 'BYTEA';
            case Oid.Char: return 'CHAR';
            case Oid.Name: return 'NAME';
            case Oid.Int8: return 'INT8';
            case Oid.Int2: return 'INT2';
            case Oid.Int4: return 'INT4';
            case Oid.Text: return 'TEXT';
            case Oid.BpChar: return 'CHAR'; // Netezza often treats bpchar as CHAR
            case Oid.VarChar: return 'VARCHAR';
            case Oid.Date: return 'DATE';
            case Oid.Time: return 'TIME';
            case Oid.Timestamp: return 'TIMESTAMP';
            case Oid.TimestampTz: return 'TIMESTAMPTZ';
            case Oid.Interval: return 'INTERVAL';
            case Oid.TimeTz: return 'TIMETZ';
            case Oid.Numeric: return 'NUMERIC';
            case Oid.Float4: return 'FLOAT4';
            case Oid.Float8: return 'FLOAT8';
            case 2530: return 'DATE'; // ShowDate
            case 15: return 'CHAR'; // NzChar in binary?
            case 16: return 'BOOL';
            default: return `UNKNOWN(${oid})`;
        }
    }

    /**
     * Advance to next row
     * @returns {Promise<boolean>}
     */
    async read() {
        if (this.closed || this._isFinished) return false;
        if (this._pendingColumns) return false;

        let res;
        if (this._nextItem) {
            res = { value: this._nextItem, done: false };
            this._nextItem = null;
        } else {
            res = await this.generator.next();
        }

        if (res.done) {
            this._markFinished();
            this.currentRow = null;
            return false;
        }

        const val = res.value;
        if (val.type === 'DataRow') {
            this.currentRow = val.row;
            return true;
        }

        if (val.type === 'RowDescription') {
            this._pendingColumns = val.columns;
            this.currentRow = null;
            return false;
        }

        if (val.type === 'RowDescriptionStandard') {
            // Get columns from command's PreparedStatement, or keep existing
            const ps = this.command?._preparedStatement;
            this._pendingColumns = (ps && ps.description) ? ps.description : this.columnDescriptions;
            this.currentRow = null;
            return false;
        }

        if (val.type === 'CommandComplete') {
            // Loop to skip or just return false?
            // Usually CommandComplete means end of rows.
            this.currentRow = null;
            return this.read(); // recurse to find next item (e.g. ReadyForQuery or next Loop)
        }

        if (val.type === 'ErrorResponse') {
            throw new Error(val.message || 'Unknown Netezza Error');
        }

        if (val.type === 'ReadyForQuery') {
            this._markFinished();
            this.currentRow = null;
            return false;
        }

        return this.read();
    }

    /**
     * Get value at column index
     * @param {number} i - column index
     * @returns {any}
     */
    getValue(i) {
        this._validateOrdinal(i);
        return this.currentRow[i];
    }

    /**
     * Get value by column name
     * @param {string} name - column name
     * @returns {any}
     */
    getValueByName(name) {
        const i = this.getOrdinal(name);
        return this.getValue(i);
    }

    /**
     * Get column name
     * @param {number} i - column index
     * @returns {string}
     */
    getName(i) {
        // this._validateOrdinal(i); // getName doesn't need current row necessarily, just metadata
        if (i < 0 || i >= this.columnDescriptions.length) {
            throw new Error(`Column ordinal ${i} is out of range`);
        }
        return this.columnDescriptions[i].name;
    }

    /**
     * Get column index by name
     * @param {string} name - column name
     * @returns {number}
     */
    getOrdinal(name) {
        const idx = this._nameIndex[name.toLowerCase()];
        if (idx === undefined) {
            throw new Error(`Column '${name}' not found`);
        }
        return idx;
    }

    get fieldCount() {
        return this.columnDescriptions?.length || 0;
    }

    get FieldCount() {
        return this.fieldCount;
    }

    isDBNull(i) {
        this._validateOrdinal(i);
        return this.currentRow[i] === null;
    }

    getBoolean(i) {
        const val = this.getValue(i);
        if (val === null) return false;
        if (typeof val === 'boolean') return val;
        if (typeof val === 'string') return val.toLowerCase() === 't' || val === '1' || val.toLowerCase() === 'true';
        return Boolean(val);
    }

    getByte(i) {
        const val = this.getValue(i);
        return val === null ? 0 : Number(val) & 0xFF;
    }

    getInt16(i) {
        const val = this.getValue(i);
        return val === null ? 0 : Number(val);
    }

    getInt32(i) {
        const val = this.getValue(i);
        return val === null ? 0 : Number(val);
    }

    getInt64(i) {
        const val = this.getValue(i);
        if (val === null) return 0;
        if (typeof val === 'bigint') return val;
        return Number(val);
    }

    getFloat(i) {
        const val = this.getValue(i);
        return val === null ? 0.0 : Number(val);
    }

    getDouble(i) {
        const val = this.getValue(i);
        return val === null ? 0.0 : Number(val);
    }

    getDecimal(i) {
        const val = this.getValue(i);
        if (val === null) return 0;
        if (typeof val === 'string') return val;
        return Number(val);
    }

    getString(i) {
        const val = this.getValue(i);
        if (val === null) return null;
        if (typeof val === 'string') return val;
        if (val.toString) return val.toString();
        return String(val);
    }

    getDateTime(i) {
        const val = this.getValue(i);
        if (val === null) return null;
        if (val instanceof Date) return val;
        return new Date(val);
    }

    getTimeSpan(i) {
        const val = this.getValue(i);
        if (val === null) return null;
        if (typeof val === 'object' && 'hours' in val) return val;
        if (typeof val === 'string') {
            const parts = val.split(':');
            if (parts.length >= 2) {
                const secParts = (parts[2] || '0').split('.');
                return {
                    hours: parseInt(parts[0], 10),
                    minutes: parseInt(parts[1], 10),
                    seconds: parseInt(secParts[0], 10),
                    microseconds: secParts.length > 1 ? parseInt(secParts[1].padEnd(6, '0'), 10) : 0,
                    toString() {
                        return val;
                    }
                };
            }
        }
        return val;
    }

    getRowObject() {
        if (!this.currentRow) return null;
        const obj = {};
        for (let i = 0; i < this.columnDescriptions.length; i++) {
            obj[this.columnDescriptions[i].name] = this.currentRow[i];
        }
        return obj;
    }

    getValues() {
        if (!this.currentRow) return [];
        return [...this.currentRow];
    }

    async close() {
        if (!this.closed) {
            this.closed = true;
            if (!this._isFinished && this.generator) {
                try {
                    for await (const val of this.generator) {
                        if (val.type === 'ReadyForQuery') break;
                    }
                } catch (e) {
                    // ignore
                }
            }
            if (this.releaseCallback) {
                this.releaseCallback();
                this.releaseCallback = null;
            }
        }
    }

    get isClosed() {
        return this.closed;
    }



    _validateOrdinal(i) {
        if (!this.currentRow) {
            throw new Error('No current row. Did you call read()?');
        }
        if (i < 0 || i >= this.columnDescriptions.length) {
            throw new Error(`Column ordinal ${i} is out of range`);
        }
    }

    // Async iterator
    async *[Symbol.asyncIterator]() {
        while (await this.read()) {
            yield this.getRowObject();
        }
    }
}

module.exports = NzDataReader;
