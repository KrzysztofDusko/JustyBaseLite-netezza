const AdmZip = require('adm-zip');
const ExcelReaderAbstract = require('./ExcelReaderAbstract');
const BiffReaderWriter = require('./BiffReaderWriter');

class XlsbReader extends ExcelReaderAbstract {
    constructor() {
        super();
        this.zip = null;
        this.sharedStrings = []; // array of strings
        this.styles = []; // map of numFmts
        this.sheetNames = [];
        this.sheets = []; // { name, id, rId, path }

        // State
        this._currentSheetIndex = -1;
        this._reader = null; // BiffReaderWriter instance
        this._currentRow = [];
        this._pendingRowIndex = -1;
        this._eof = false;
        // Logic: we read ahead to find the next Row record.
    }

    open(path, readSharedStrings = true) {
        this.zip = new AdmZip(path);

        // 1. Read Workbook (xl/workbook.bin)
        const wbEntry = this.zip.getEntry('xl/workbook.bin');
        if (wbEntry) {
            const buf = wbEntry.getData();
            const reader = new BiffReaderWriter(buf);
            while (reader.readWorkbook()) {
                if (reader._isSheet) {
                    // reader._recId is the rId. 
                    const name = reader._workbookName;
                    const rId = reader._recId;
                    this.sheetNames.push(name);
                    this.sheets.push({ name, rId, path: null });
                }
            }
        }

        // 2. Resolve Relationships (xl/_rels/workbook.bin.rels) to find paths
        const relsEntry = this.zip.getEntry('xl/_rels/workbook.bin.rels');
        if (relsEntry) {
            const xml = relsEntry.getData().toString('utf8');
            const relRegex = /<Relationship[^>]*Id="([^"]*)"[^>]*Target="([^"]*)"/g;
            let match;
            let rIdToTarget = {};
            while ((match = relRegex.exec(xml)) !== null) {
                rIdToTarget[match[1]] = match[2];
            }

            for (let sheet of this.sheets) {
                let target = rIdToTarget[sheet.rId];
                if (target) {
                    if (target.startsWith('/')) target = target.substring(1);
                    if (!target.startsWith('xl/')) target = 'xl/' + target;
                    sheet.path = target;
                }
            }
        }

        // 3. Shared Strings
        if (readSharedStrings) {
            // Find shared strings path
            let ssPath = 'xl/sharedStrings.bin';
            if (this.zip.getEntry(ssPath)) {
                this._readSharedStrings(ssPath);
            }
        }

        // 4. Styles
        let stylesPath = 'xl/styles.bin';
        if (this.zip.getEntry(stylesPath)) {
            this._readStyles(stylesPath);
        }

        this.resultsCount = this.sheets.length;
        this._currentSheetIndex = -1;
    }

    _readSharedStrings(path) {
        const entry = this.zip.getEntry(path);
        if (!entry) return;
        const reader = new BiffReaderWriter(entry.getData());

        // Loop
        while (reader.readSharedStrings()) {
            if (reader._sharedStringValue !== null) {
                this.sharedStrings.push(reader._sharedStringValue);
            }
        }
    }

    _readStyles(path) {
        const entry = this.zip.getEntry(path);
        if (!entry) return;

        // Use a temporary BiffReader just to populate style maps
        // We need to keep this reader state or extract the maps?
        // Actually, BiffReaderWriter state is transient per buffer read. 
        // We should extract the maps from it after reading.

        const reader = new BiffReaderWriter(entry.getData());
        while (reader.readStyles()) {
            // just consume
        }

        // Extract maps
        this.xfIdToNumFmtId = reader._xfIndexToNumFmtId;
        this.customDateFormats = reader._customNumFmts;
    }

    getSheetNames() {
        return this.sheetNames;
    }

    read() {
        // Init sheet
        if (this._currentSheetIndex === -1) {
            this._currentSheetIndex = 0;
            if (!this._initSheet(0)) return false;
        }

        if (this._eof) return false;

        // "Read" logic:
        // We are currently positioned at the start of a Row (or just initialized).
        // _pendingRowIndex holds the index of the row we correspond to.

        // Reset current row
        this._currentRow = [];

        // Loop until next Row record or End
        while (true) {
            const hasRecord = this._reader.readWorksheet();
            if (!hasRecord) {
                this._eof = true;
                return true; // Return the last gathered row data
            }

            // Check record types via reader state
            if (this._reader._rowIndex !== -1 && this._reader._rowIndex !== this._pendingRowIndex) {
                // We found a NEW row index.
                const oldRowIndex = this._pendingRowIndex;
                this._pendingRowIndex = this._reader._rowIndex;
                return true;
            }

            if (this._reader._readCell) {
                const col = this._reader._columnNum;
                let val = null;
                switch (this._reader._cellType) {
                    case 2: // SharedString
                        val = this.sharedStrings[this._reader._intValue];
                        break;
                    case 3: // Double
                        val = this._reader._doubleVal;

                        // Date check
                        let xfIndex = this._reader._xfIndex;
                        let numFmtId = 0;
                        if (this.xfIdToNumFmtId && xfIndex < this.xfIdToNumFmtId.length) {
                            numFmtId = this.xfIdToNumFmtId[xfIndex];
                        }

                        const isDate = (numFmtId >= 14 && numFmtId <= 22) ||
                            (numFmtId >= 45 && numFmtId <= 47) ||
                            (this.customDateFormats && this.customDateFormats.has(numFmtId));

                        if (isDate) {
                            try {
                                val = this.getDateTimeFromOaDate(val);
                            } catch (e) { }
                        }
                        break;
                    case 4: // Bool
                        val = this._reader._boolValue;
                        break;
                    case 5: // String
                        val = this._reader._stringValue;
                        break;
                    default:
                        val = null;
                }

                this._currentRow[col] = val;
                if (col >= this.fieldCount) this.fieldCount = col + 1;
            }
        }
    }

    _initSheet(index) {
        if (index >= this.sheets.length) return false;
        const sheet = this.sheets[index];
        const entry = this.zip.getEntry(sheet.path);
        if (!entry) return false;

        this._reader = new BiffReaderWriter(entry.getData());
        this._eof = false;
        this._pendingRowIndex = -1;

        // Scan to first Row
        while (this._reader.readWorksheet()) {
            if (this._reader._rowIndex !== -1) {
                this._pendingRowIndex = this._reader._rowIndex;
                return true;
            }
        }

        // No rows found
        this._eof = true;
        return false;
    }

    getValue(i) {
        if (i < 0 || i >= this._currentRow.length) return null;
        return this._currentRow[i];
    }
}

module.exports = XlsbReader;
