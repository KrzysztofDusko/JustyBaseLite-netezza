const { TextDecoder } = require('util');

class BiffReaderWriter {
    constructor(buffer) {
        if (!Buffer.isBuffer(buffer)) {
            throw new Error("BiffReaderWriter expects a Buffer");
        }
        this.buffer = buffer;
        this.pos = 0;
        this.length = buffer.length;

        // Internal State
        this._isSheet = false;
        this._workbookId = 0;
        this._recId = null;
        this._workbookName = null;

        // Styles state
        this._inCellXf = false;
        this._inCellStyleXf = false;
        this._inNumberFormat = false;
        this._parentCellStyleXf = 0;
        this._numberFormatIndex = 0;
        this._format = 0;
        this._formatString = null;

        // Shared Strings State
        this._sharedStringValue = null;
        this._sharedStringUniqueCount = 0;

        // Worksheet State
        this._cellType = 0; // 0=Null, 1=Int, etc (Enum)
        this._intValue = 0;
        this._doubleVal = 0.0;
        this._boolValue = false;
        this._stringValue = null;
        this._columnNum = -1;
        this._xfIndex = 0;
        this._readCell = false;
        this._rowIndex = -1;

        // Internal reusable buffer for record reading
        this._tempBuf = Buffer.alloc(128);

        // Arrays to store style mapping information
        this._xfIndexToNumFmtId = []; // Index = xfId, Value = numFmtId
        this._customNumFmts = new Set(); // Set of numFmtIds that are custom dates
    }

    _tryReadVariableValue() {
        if (this.pos >= this.length) return null;

        let b1 = this.buffer[this.pos++];
        let value = (b1 & 0x7F) >>> 0;

        if ((b1 & 0x80) === 0) return value;

        if (this.pos >= this.length) return null;
        let b2 = this.buffer[this.pos++];
        value = (((b2 & 0x7F) << 7) | value) >>> 0;

        if ((b2 & 0x80) === 0) return value;

        if (this.pos >= this.length) return null;
        let b3 = this.buffer[this.pos++];
        value = (((b3 & 0x7F) << 14) | value) >>> 0;

        if ((b3 & 0x80) === 0) return value;

        if (this.pos >= this.length) return null;
        let b4 = this.buffer[this.pos++];
        value = (((b4 & 0x7F) << 21) | value) >>> 0;

        return value;
    }

    _getDWord(buf, offset) {
        return buf.readUInt32LE(offset);
    }

    _getInt32(buf, offset) {
        return buf.readInt32LE(offset);
    }

    _getWord(buf, offset) {
        return buf.readUInt16LE(offset);
    }

    _getString(buf, offset, length) {
        // UTF-16LE string
        // length is number of CHARACTERS (2 bytes each)
        const start = offset;
        const end = offset + (length * 2);
        if (end > buf.length) return ""; // Safety
        return buf.toString('utf16le', start, end);
    }

    _getNullableString(buf, offsetRef) {
        // offsetRef is an object { val: number } to simulate ref
        const length = this._getDWord(buf, offsetRef.val);
        offsetRef.val += 4;

        if (length === 0xFFFFFFFF) return null; // uint.MaxValue

        const str = this._getString(buf, offsetRef.val, length);
        offsetRef.val += (length * 2);
        return str;
    }

    // --- Core Logic ---

    readWorkbook() {
        const recordId = this._tryReadVariableValue();
        const recordLength = this._tryReadVariableValue();

        if (recordId === null || recordLength === null) return false;

        // Move pos to read record data (mimic Stream.Read)
        const startPos = this.pos;
        if (startPos + recordLength > this.length) return false;

        // Use a slice of the main buffer for processing the record
        // This avoids copying
        const recBuf = this.buffer.subarray(startPos, startPos + recordLength);
        this.pos += recordLength;

        this._isSheet = false;

        if (recordId === 0x9C) { // _sheet
            this._workbookId = this._getDWord(recBuf, 4);

            let offsetRef = { val: 8 };
            this._recId = this._getNullableString(recBuf, offsetRef);

            // Re-read name length from updated offset logic
            // In C#: GetDWord(buffer, offset);
            // My _getNullableString updates offsetRef.val to point after string

            const nameLength = this._getDWord(recBuf, offsetRef.val);
            this._workbookName = this._getString(recBuf, offsetRef.val + 4, nameLength);
            this._isSheet = true;
        }

        return true;
    }

    readSharedStrings() {
        const recordId = this._tryReadVariableValue();
        const recordLength = this._tryReadVariableValue();

        if (recordId === null || recordLength === null) return false;

        const startPos = this.pos;
        if (startPos + recordLength > this.length) return false;

        // Avoid slicing if not needed for simple checks, but here we need content
        const recBuf = this.buffer.subarray(startPos, startPos + recordLength);
        this.pos += recordLength;

        this._sharedStringValue = null;

        if (recordId === 0x13) { // _stringItem
            // standard string Item
            const length = this._getDWord(recBuf, 1);
            this._sharedStringValue = this._getString(recBuf, 5, length);
        }
        else if (recordId === 159) { // _sharedStringStart
            this._sharedStringUniqueCount = this._getDWord(recBuf, 4);
        }

        return true;
    }

    readStyles() {
        const recordId = this._tryReadVariableValue();
        const recordLength = this._tryReadVariableValue();

        if (recordId === null || recordLength === null) return false;

        const startPos = this.pos;
        if (startPos + recordLength > this.length) return false;
        const recBuf = this.buffer.subarray(startPos, startPos + recordLength);
        this.pos += recordLength;

        // defined consts from C#
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
                    this._parentCellStyleXf = this._getWord(recBuf, 0);
                    this._numberFormatIndex = this._getWord(recBuf, 2);
                    // Add to mapping list
                    this._xfIndexToNumFmtId.push(this._numberFormatIndex);
                }
                break;

            case _numberFormat:
                if (this._inNumberFormat) {
                    this._format = this._getWord(recBuf, 0);
                    const length = this._getDWord(recBuf, 2);
                    this._formatString = this._getString(recBuf, 6, length);

                    // Track custom date formats
                    const code = this._formatString.toLowerCase();
                    if (code.includes('yy') || code.includes('mm') || code.includes('dd') || code.includes('h:mm')) {
                        this._customNumFmts.add(this._format);
                    }
                }
                break;
        }

        return true;
    }

    _getRkNumber(buf, offset) {
        const flags = buf[offset];
        let result = 0;

        if ((flags & 0x02) !== 0) {
            // Int32 >> 2
            const intVal = buf.readInt32LE(offset);
            result = intVal >> 2;
        } else {
            // Hi 30 bits of 64-bit float
            // We need to read UInt32, mask lower 2 bits (bitwise AND -4 which is ...111100)
            // Then shift left 32 to become high bits of double

            // In JS, bitwise ops are 32-bit. We can't shift left 32 directly into a number easily for Double reconstruction without DataView or BigInt interaction, 
            // OR we can just write to a temp buffer and read as double.

            const raw = buf.readInt32LE(offset);
            const highBits = (raw & 0xFFFFFFFC);

            // Create a temp 8-byte buffer
            const dBuf = Buffer.alloc(8);
            dBuf.writeInt32LE(0, 0); // Low bits 0
            dBuf.writeInt32LE(highBits, 4); // High bits

            result = dBuf.readDoubleLE(0);
        }

        if ((flags & 0x01) !== 0) {
            result /= 100;
        }

        return result;
    }

    readWorksheet() {
        const recordId = this._tryReadVariableValue();
        const recordLength = this._tryReadVariableValue();

        if (recordId === null || recordLength === null) return false;

        const startPos = this.pos;
        if (startPos + recordLength > this.length) return false;
        const recBuf = this.buffer.subarray(startPos, startPos + recordLength);
        this.pos += recordLength;

        this._readCell = false;
        this._columnNum = -1;

        // Constants
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
                this._rowIndex = this._getInt32(recBuf, 0);
                break;

            case _blank:
            case _boolError:
            case _formulaError:
                this._readCell = true;
                this._cellType = 0; // null
                break;

            case _number:
                this._doubleVal = this._getRkNumber(recBuf, 8);
                this._readCell = true;
                this._cellType = 3; // double
                break;

            case _bool:
            case _formulaBool:
                this._boolValue = (recBuf[8] === 1);
                this._readCell = true;
                this._cellType = 4; // bool
                break;

            case _formulaNumber:
            case _float:
                this._doubleVal = recBuf.readDoubleLE(8);
                this._readCell = true;
                this._cellType = 3; // double
                break;

            case _string:
            case _formulaString:
                {
                    const length = this._getDWord(recBuf, 8);
                    this._stringValue = this._getString(recBuf, 12, length);
                    this._readCell = true;
                    this._cellType = 5; // string
                    break;
                }

            case _sharedString:
                this._intValue = this._getDWord(recBuf, 8);
                this._readCell = true;
                this._cellType = 2; // sharedString
                break;
        }

        if (this._readCell) {
            this._columnNum = this._getDWord(recBuf, 0);
            this._xfIndex = this._getDWord(recBuf, 4) & 0xffffff;
        }

        return true;
    }
}

module.exports = BiffReaderWriter;
