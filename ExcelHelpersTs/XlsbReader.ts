import AdmZip from 'adm-zip';
import { ExcelReaderAbstract } from './ExcelReaderAbstract';
import { BiffReaderWriter } from './BiffReaderWriter';

interface SheetInfo {
    name: string;
    rId: string;
    path: string | null;
}

export class XlsbReader extends ExcelReaderAbstract {
    private zip: AdmZip | null = null;
    private sharedStrings: string[] = [];
    private styles: any[] = [];
    private sheetNames: string[] = [];
    private sheets: SheetInfo[] = [];

    private _currentSheetIndex: number = -1;
    private _reader: BiffReaderWriter | null = null;
    private _currentRow: any[] = [];
    private _pendingRowIndex: number = -1;
    private _eof: boolean = false;

    xfIdToNumFmtId: number[] = [];
    customDateFormats: Set<number> | null = null;

    constructor() {
        super();
    }

    async open(path: string, readSharedStrings: boolean = true): Promise<void> {
        this.zip = new AdmZip(path);

        const wbEntry = this.zip.getEntry('xl/workbook.bin');
        if (wbEntry) {
            const buf = wbEntry.getData();
            const reader = new BiffReaderWriter(buf);
            while (reader.readWorkbook()) {
                if (reader._isSheet) {
                    const name = reader._workbookName!;
                    const rId = reader._recId!;
                    this.sheetNames.push(name);
                    this.sheets.push({ name, rId, path: null });
                }
            }
        }

        const relsEntry = this.zip.getEntry('xl/_rels/workbook.bin.rels');
        if (relsEntry) {
            const xml = relsEntry.getData().toString('utf8');
            const relRegex = /<Relationship[^>]*Id="([^"]*)"[^>]*Target="([^"]*)"/g;
            let match;
            const rIdToTarget: Record<string, string> = {};
            while ((match = relRegex.exec(xml)) !== null) {
                rIdToTarget[match[1]] = match[2];
            }

            for (const sheet of this.sheets) {
                let target = rIdToTarget[sheet.rId];
                if (target) {
                    if (target.startsWith('/')) target = target.substring(1);
                    if (!target.startsWith('xl/')) target = 'xl/' + target;
                    sheet.path = target;
                }
            }
        }

        if (readSharedStrings) {
            const ssPath = 'xl/sharedStrings.bin';
            if (this.zip.getEntry(ssPath)) {
                this._readSharedStrings(ssPath);
            }
        }

        const stylesPath = 'xl/styles.bin';
        if (this.zip.getEntry(stylesPath)) {
            this._readStyles(stylesPath);
        }

        this.resultsCount = this.sheets.length;
        this._currentSheetIndex = -1;
    }

    private _readSharedStrings(path: string): void {
        const entry = this.zip!.getEntry(path);
        if (!entry) return;
        const reader = new BiffReaderWriter(entry.getData());

        while (reader.readSharedStrings()) {
            if (reader._sharedStringValue !== null) {
                this.sharedStrings.push(reader._sharedStringValue);
            }
        }
    }

    private _readStyles(path: string): void {
        const entry = this.zip!.getEntry(path);
        if (!entry) return;

        const reader = new BiffReaderWriter(entry.getData());
        while (reader.readStyles()) {
            // just consume
        }

        this.xfIdToNumFmtId = reader._xfIndexToNumFmtId;
        this.customDateFormats = reader._customNumFmts;
    }

    getSheetNames(): string[] {
        return this.sheetNames;
    }

    read(): boolean {
        if (this._currentSheetIndex === -1) {
            this._currentSheetIndex = 0;
            if (!this._initSheet(0)) return false;
        }

        if (this._eof) return false;

        this._currentRow = [];

        while (true) {
            const hasRecord = this._reader!.readWorksheet();
            if (!hasRecord) {
                this._eof = true;
                return true;
            }

            if (this._reader!._rowIndex !== -1 && this._reader!._rowIndex !== this._pendingRowIndex) {
                this._pendingRowIndex = this._reader!._rowIndex;
                return true;
            }

            if (this._reader!._readCell) {
                const col = this._reader!._columnNum;
                let val: any = null;
                switch (this._reader!._cellType) {
                    case 2:
                        val = this.sharedStrings[this._reader!._intValue];
                        break;
                    case 3: {
                        val = this._reader!._doubleVal;

                        const xfIndex = this._reader!._xfIndex;
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
                            } catch {
                                // ignore date conversion errors
                            }
                        }
                        break;
                    }
                    case 4:
                        val = this._reader!._boolValue;
                        break;
                    case 5:
                        val = this._reader!._stringValue;
                        break;
                    default:
                        val = null;
                }

                this._currentRow[col] = val;
                if (col >= this.fieldCount) this.fieldCount = col + 1;
            }
        }
    }

    private _initSheet(index: number): boolean {
        if (index >= this.sheets.length) return false;
        const sheet = this.sheets[index];
        const entry = this.zip!.getEntry(sheet.path!);
        if (!entry) return false;

        this._reader = new BiffReaderWriter(entry.getData());
        this._eof = false;
        this._pendingRowIndex = -1;

        while (this._reader.readWorksheet()) {
            if (this._reader._rowIndex !== -1) {
                this._pendingRowIndex = this._reader._rowIndex;
                return true;
            }
        }

        this._eof = true;
        return false;
    }

    getValue(i: number): any {
        if (i < 0 || i >= this._currentRow.length) return null;
        return this._currentRow[i];
    }
}

export default XlsbReader;
