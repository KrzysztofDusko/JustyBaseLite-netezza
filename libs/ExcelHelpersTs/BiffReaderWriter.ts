export class BiffReaderWriter {
    private buffer: Buffer;
    private pos: number;
    private length: number;

    _isSheet: boolean = false;
    _workbookId: number = 0;
    _recId: string | null = null;
    _workbookName: string | null = null;

    private _inCellXf: boolean = false;
    private _inCellStyleXf: boolean = false;
    private _inNumberFormat: boolean = false;
    private _parentCellStyleXf: number = 0;
    private _numberFormatIndex: number = 0;
    private _format: number = 0;
    private _formatString: string | null = null;

    _sharedStringValue: string | null = null;
    _sharedStringUniqueCount: number = 0;

    _cellType: number = 0;
    _intValue: number = 0;
    _doubleVal: number = 0.0;
    _boolValue: boolean = false;
    _stringValue: string | null = null;
    _columnNum: number = -1;
    _xfIndex: number = 0;
    _readCell: boolean = false;
    _rowIndex: number = -1;

    // Start of the current record data (excluding ID and Length)
    private _recordStart: number = 0;

    _xfIndexToNumFmtId: number[] = [];
    _customNumFmts: Set<number> = new Set();

    constructor(buffer: Buffer) {
        if (!Buffer.isBuffer(buffer)) {
            throw new Error("BiffReaderWriter expects a Buffer");
        }
        this.buffer = buffer;
        this.pos = 0;
        this.length = buffer.length;
    }

    private _tryReadVariableValue(): number | null {
        if (this.pos >= this.length) return null;

        const b1 = this.buffer[this.pos++];
        let value = (b1 & 0x7F) >>> 0;

        if ((b1 & 0x80) === 0) return value;

        if (this.pos >= this.length) return null;
        const b2 = this.buffer[this.pos++];
        value = (((b2 & 0x7F) << 7) | value) >>> 0;

        if ((b2 & 0x80) === 0) return value;

        if (this.pos >= this.length) return null;
        const b3 = this.buffer[this.pos++];
        value = (((b3 & 0x7F) << 14) | value) >>> 0;

        if ((b3 & 0x80) === 0) return value;

        if (this.pos >= this.length) return null;
        const b4 = this.buffer[this.pos++];
        value = (((b4 & 0x7F) << 21) | value) >>> 0;

        return value;
    }

    private _getDWord(offset: number): number {
        return this.buffer.readUInt32LE(this._recordStart + offset);
    }

    private _getInt32(offset: number): number {
        return this.buffer.readInt32LE(this._recordStart + offset);
    }

    private _getWord(offset: number): number {
        return this.buffer.readUInt16LE(this._recordStart + offset);
    }

    private _getByte(offset: number): number {
        return this.buffer[this._recordStart + offset];
    }

    private _getDouble(offset: number): number {
        return this.buffer.readDoubleLE(this._recordStart + offset);
    }

    private _getString(offset: number, length: number): string {
        const start = this._recordStart + offset;
        const end = start + (length * 2);
        if (end > this.buffer.length) return "";
        return this.buffer.toString('utf16le', start, end);
    }

    private _getNullableString(offsetRef: { val: number }): string | null {
        const length = this._getDWord(offsetRef.val);
        offsetRef.val += 4;

        if (length === 0xFFFFFFFF) return null;

        const str = this._getString(offsetRef.val, length);
        offsetRef.val += (length * 2);
        return str;
    }

    readWorkbook(): boolean {
        const recordId = this._tryReadVariableValue();
        const recordLength = this._tryReadVariableValue();

        if (recordId === null || recordLength === null) return false;

        const startPos = this.pos;
        if (startPos + recordLength > this.length) return false;

        this._recordStart = startPos;
        this.pos += recordLength;

        this._isSheet = false;

        if (recordId === 0x9C) {
            this._workbookId = this._getDWord(4);

            const offsetRef = { val: 8 };
            this._recId = this._getNullableString(offsetRef);

            const nameLength = this._getDWord(offsetRef.val);
            this._workbookName = this._getString(offsetRef.val + 4, nameLength);
            this._isSheet = true;
        }

        return true;
    }

    readSharedStrings(): boolean {
        const recordId = this._tryReadVariableValue();
        const recordLength = this._tryReadVariableValue();

        if (recordId === null || recordLength === null) return false;

        const startPos = this.pos;
        if (startPos + recordLength > this.length) return false;

        this._recordStart = startPos;
        this.pos += recordLength;

        this._sharedStringValue = null;

        if (recordId === 0x13) {
            const length = this._getDWord(1);
            this._sharedStringValue = this._getString(5, length);
        }
        else if (recordId === 159) {
            this._sharedStringUniqueCount = this._getDWord(4);
        }

        return true;
    }

    readStyles(): boolean {
        const recordId = this._tryReadVariableValue();
        const recordLength = this._tryReadVariableValue();

        if (recordId === null || recordLength === null) return false;

        const startPos = this.pos;
        if (startPos + recordLength > this.length) return false;

        this._recordStart = startPos;
        this.pos += recordLength;

        const _cellXfStart = 0x269;
        const _cellXfEnd = 0x26a;
        const _cellStyleXfStart = 0x272;
        const _cellStyleXfEnd = 0x273;
        const _numberFormatStart = 0x267;
        const _numberFormatEnd = 0x268;
        const _xf = 0x2f;
        const _numberFormat = 0x2c;

        switch (recordId) {
            case _cellXfStart:
                this._inCellXf = true;
                break;
            case _cellXfEnd:
                this._inCellXf = false;
                break;
            case _cellStyleXfStart:
                this._inCellStyleXf = true;
                break;
            case _cellStyleXfEnd:
                this._inCellStyleXf = false;
                break;
            case _numberFormatStart:
                this._inNumberFormat = true;
                break;
            case _numberFormatEnd:
                this._inNumberFormat = false;
                break;

            case _xf:
                if (this._inCellXf) {
                    this._parentCellStyleXf = this._getWord(0);
                    this._numberFormatIndex = this._getWord(2);
                    this._xfIndexToNumFmtId.push(this._numberFormatIndex);
                }
                break;

            case _numberFormat:
                if (this._inNumberFormat) {
                    this._format = this._getWord(0);
                    const length = this._getDWord(2);
                    this._formatString = this._getString(6, length);

                    const code = this._formatString.toLowerCase();
                    if (code.includes('yy') || code.includes('mm') || code.includes('dd') || code.includes('h:mm')) {
                        this._customNumFmts.add(this._format);
                    }
                }
                break;
        }

        return true;
    }

    private _getRkNumber(offset: number): number {
        const flags = this._getByte(offset);
        let result = 0;

        if ((flags & 0x02) !== 0) {
            const intVal = this._getInt32(offset);
            result = intVal >> 2;
        } else {
            const raw = this._getInt32(offset);
            const highBits = (raw & 0xFFFFFFFC);

            // Need to construct a double from high bits.
            // Since we can't easily write to a reused buffer without risk,
            // we'll just alloc 8 bytes here. It's rare for RK to be double.
            // Alternatively, we could reuse a small buffer if we cared enough.
            const dBuf = Buffer.allocUnsafe(8);
            dBuf.writeInt32LE(0, 0);
            dBuf.writeInt32LE(highBits, 4);

            result = dBuf.readDoubleLE(0);
        }

        if ((flags & 0x01) !== 0) {
            result /= 100;
        }

        return result;
    }

    readWorksheet(): boolean {
        const recordId = this._tryReadVariableValue();
        const recordLength = this._tryReadVariableValue();

        if (recordId === null || recordLength === null) return false;

        const startPos = this.pos;
        if (startPos + recordLength > this.length) return false;

        this._recordStart = startPos;
        this.pos += recordLength;

        this._readCell = false;
        this._columnNum = -1;

        const _row = 0x00;
        const _blank = 0x01;
        const _number = 0x02;
        const _boolError = 0x03;
        const _bool = 0x04;
        const _float = 0x05;
        const _string = 0x06;
        const _sharedString = 0x07;
        const _formulaString = 0x08;
        const _formulaNumber = 0x09;
        const _formulaBool = 0x0a;
        const _formulaError = 0x0b;

        switch (recordId) {
            case _row:
                this._rowIndex = this._getInt32(0);
                break;

            case _blank:
            case _boolError:
            case _formulaError:
                this._readCell = true;
                this._cellType = 0;
                break;

            case _number:
                this._doubleVal = this._getRkNumber(8);
                this._readCell = true;
                this._cellType = 3;
                break;

            case _bool:
            case _formulaBool:
                this._boolValue = (this._getByte(8) === 1);
                this._readCell = true;
                this._cellType = 4;
                break;

            case _formulaNumber:
            case _float:
                this._doubleVal = this._getDouble(8);
                this._readCell = true;
                this._cellType = 3;
                break;

            case _string:
            case _formulaString:
                {
                    const length = this._getDWord(8);
                    this._stringValue = this._getString(12, length);
                    this._readCell = true;
                    this._cellType = 5;
                    break;
                }

            case _sharedString:
                this._intValue = this._getDWord(8);
                this._readCell = true;
                this._cellType = 2;
                break;
        }

        if (this._readCell) {
            this._columnNum = this._getDWord(0);
            this._xfIndex = this._getDWord(4) & 0xffffff;
        }

        return true;
    }
}

export default BiffReaderWriter;
