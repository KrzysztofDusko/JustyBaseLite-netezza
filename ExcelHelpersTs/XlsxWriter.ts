import * as fs from 'fs';
import archiver from 'archiver';
import { Readable } from 'stream';
import { BigBuffer } from './BigBuffer';

const COLUMN_LETTERS = (() => {
    const letters: string[] = [];
    for (let i = 65; i < 91; i++) {
        letters.push(String.fromCharCode(i));
    }
    const temp: string[] = [];
    for (const p of letters) {
        for (let i = 65; i < 91; i++) {
            temp.push(p + String.fromCharCode(i));
        }
    }
    letters.push(...temp);
    return letters;
})();

const INVALID_SHEET_NAME_CHARS = /[\\/*?[\]:]/g;

interface SheetInfo {
    name: string;
    pathInArchive: string;
    hidden: boolean;
    nameInArchive: string;
    sheetId: number;
    rId: string;
    filterHeaderRange: string | null;
}

export class XlsxWriter {
    private filePath: string;
    private output: fs.WriteStream;
    private archive: archiver.Archiver;

    private sheetCount: number = 0;
    private sheetList: SheetInfo[] = [];
    private sstArray: string[] = [];
    private sstMap: Map<string, number> = new Map();
    private sstCntAll: number = 0;
    private colWidths: number[] = [];
    private _autofilterIsOn: boolean = false;

    private _oaEpoch: number;

    constructor(filePath: string) {
        this.filePath = filePath;
        this.output = fs.createWriteStream(filePath);
        this.archive = archiver('zip');

        this.archive.pipe(this.output);

        this._oaEpoch = Date.UTC(1899, 11, 30);
    }

    private _getColumnLetter(colIndex: number): string {
        return colIndex < COLUMN_LETTERS.length ? COLUMN_LETTERS[colIndex] : 'A';
    }

    private _sanitizeSheetName(name: string): string {
        if (!name || typeof name !== 'string') {
            return `Sheet${this.sheetCount + 1}`;
        }

        let sanitized = name.replace(INVALID_SHEET_NAME_CHARS, '_');

        if (sanitized.length > 31) {
            sanitized = sanitized.substring(0, 31);
        }

        if (sanitized.trim().length === 0) {
            sanitized = `Sheet${this.sheetCount + 1}`;
        }

        return sanitized;
    }

    private _escapeSheetNameForXml(name: string): string {
        return name
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&apos;');
    }

    private _formatSheetNameForFormula(name: string): string {
        if (/[\s\-+=()!@#$%^&]/.test(name) || /^[0-9]/.test(name)) {
            const escaped = name.replace(/'/g, "''");
            return `'${escaped}'`;
        }
        return name;
    }

    addSheet(sheetName: string, hidden: boolean = false): void {
        const sanitizedName = this._sanitizeSheetName(sheetName);

        this.sheetCount++;
        const rId = `rId${this.sheetCount}`;
        const sheetFileName = `sheet${this.sheetCount}.xml`;
        this.sheetList.push({
            name: sanitizedName,
            pathInArchive: `xl/worksheets/${sheetFileName}`,
            hidden: hidden,
            nameInArchive: sheetFileName,
            sheetId: this.sheetCount,
            rId: rId,
            filterHeaderRange: null
        });
    }

    private _needsEscape(str: string): boolean {
        for (let i = 0; i < str.length; i++) {
            const c = str.charCodeAt(i);
            if (c === 38 || c === 60 || c === 62 || c === 34 || c === 39 ||
                (c >= 0 && c <= 8) || c === 11 || c === 12 || (c >= 14 && c <= 31)) {
                return true;
            }
        }
        return false;
    }

    private _escape(str: string): string {
        if (typeof str !== 'string') return str;
        if (!this._needsEscape(str)) return str;

        return str.replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&apos;')
            // eslint-disable-next-line no-control-regex
            .replace(/[\x00-\x08\x0b\x0c\x0e-\x1f]/g, '');
    }

    writeSheet(rows: any[][], headers: string[] | null = null, doAutofilter: boolean = true): void {
        const bigBuf = new BigBuffer();

        let columnCount = 0;
        if (rows.length > 0) {
            columnCount = rows[0].length;
        } else if (headers) {
            columnCount = headers.length;
        }

        this.colWidths = new Array(columnCount).fill(-1.0);

        const colLetters = new Array(columnCount);
        for (let i = 0; i < columnCount; i++) {
            colLetters[i] = this._getColumnLetter(i);
        }

        if (headers) {
            for (let i = 0; i < columnCount; i++) {
                const len = headers[i] ? headers[i].length : 0;
                let width = 1.25 * len + 2;
                if (width > 80) width = 80;
                if (this.colWidths[i] < width) this.colWidths[i] = width;
            }
        }

        for (let r = 0; r < Math.min(rows.length, 100); r++) {
            const row = rows[r];
            for (let c = 0; c < row.length; c++) {
                const val = row[c];
                if (val === null || val === undefined) continue;
                const len = val instanceof Date ? 10 : val.toString().length;
                let width = 1.25 * len + 2;
                if (width > 80) width = 80;
                if (this.colWidths[c] < width) this.colWidths[c] = width;
            }
        }

        bigBuf.writeString('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>');
        bigBuf.writeString('<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">');

        const totalRows = rows.length + (headers ? 1 : 0);
        if (totalRows > 0 && columnCount > 0) {
            bigBuf.writeString(`<dimension ref="A1:${colLetters[columnCount - 1]}${totalRows}"/>`);
        } else {
            bigBuf.writeString('<dimension ref="A1"/>');
        }

        const isFirstSheet = this.sheetCount === 1;
        if (doAutofilter && headers) {
            if (isFirstSheet) {
                bigBuf.writeString('<sheetViews><sheetView tabSelected="1" workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen" /><selection pane="bottomLeft" /></sheetView></sheetViews><sheetFormatPr defaultRowHeight="15"/>');
            } else {
                bigBuf.writeString('<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen" /><selection pane="bottomLeft" /></sheetView></sheetViews><sheetFormatPr defaultRowHeight="15"/>');
            }
        } else {
            if (isFirstSheet) {
                bigBuf.writeString('<sheetViews><sheetView tabSelected="1" workbookViewId="0"/></sheetViews><sheetFormatPr defaultRowHeight="15"/>');
            } else {
                bigBuf.writeString('<sheetViews><sheetView workbookViewId="0"/></sheetViews><sheetFormatPr defaultRowHeight="15"/>');
            }
        }

        bigBuf.writeString('<cols>');
        for (let i = 0; i < columnCount; i++) {
            const width = this.colWidths[i] > 0 ? this.colWidths[i] : 10;
            bigBuf.writeString(`<col min="${i + 1}" max="${i + 1}" width="${width}" bestFit="1" customWidth="1" />`);
        }
        bigBuf.writeString('</cols><sheetData>');

        let rowNum = 0;

        if (headers) {
            rowNum++;
            bigBuf.writeString(`<row r="${rowNum}">`);
            for (let c = 0; c < headers.length; c++) {
                this._writeStringCell(bigBuf, headers[c], colLetters[c], rowNum);
            }
            bigBuf.writeString('</row>');
        }

        for (let r = 0; r < rows.length; r++) {
            rowNum++;
            bigBuf.writeString(`<row r="${rowNum}">`);
            const row = rows[r];
            for (let c = 0; c < row.length; c++) {
                const val = row[c];
                if (val === null || val === undefined) continue;

                const colRef = colLetters[c];
                if (typeof val === 'number') {
                    if (Number.isFinite(val)) {
                        bigBuf.writeString(`<c r="${colRef}${rowNum}"><v>${val}</v></c>`);
                    } else {
                        this._writeStringCell(bigBuf, val.toString(), colRef, rowNum);
                    }
                } else if (typeof val === 'bigint') {
                    this._writeStringCell(bigBuf, val.toString(), colRef, rowNum);
                } else if (typeof val === 'boolean') {
                    bigBuf.writeString(`<c r="${colRef}${rowNum}" t="b"><v>${val ? 1 : 0}</v></c>`);
                } else if (val instanceof Date) {
                    const oaDate = this._toOADate(val);
                    if (Number.isFinite(oaDate)) {
                        bigBuf.writeString(`<c r="${colRef}${rowNum}" s="1"><v>${oaDate}</v></c>`);
                    } else {
                        this._writeStringCell(bigBuf, val.toString(), colRef, rowNum);
                    }
                } else {
                    this._writeStringCell(bigBuf, val.toString(), colRef, rowNum);
                }
            }
            bigBuf.writeString('</row>');
        }

        bigBuf.writeString('</sheetData>');

        if (doAutofilter && headers && columnCount > 0) {
            this._autofilterIsOn = true;
            const filterRef = `A1:${colLetters[columnCount - 1]}${totalRows}`;
            bigBuf.writeString(`<autoFilter ref="${filterRef}"/>`);

            const sheet = this.sheetList[this.sheetCount - 1];
            const formulaSheetName = this._formatSheetNameForFormula(sheet.name);
            sheet.filterHeaderRange = `${formulaSheetName}!$A$1:$${colLetters[columnCount - 1]}$${totalRows}`;
        }

        bigBuf.writeString('</worksheet>');

        this.archive.append(Readable.from(bigBuf.getChunks()), {
            name: this.sheetList[this.sheetCount - 1].pathInArchive
        });
    }

    private _writeStringCell(bigBuf: BigBuffer, val: string, colRef: string, rowNum: number): void {
        let index = this.sstMap.get(val);
        if (index === undefined) {
            index = this.sstArray.length;
            this.sstArray.push(val);
            this.sstMap.set(val, index);
        }
        this.sstCntAll++;
        bigBuf.writeString(`<c r="${colRef}${rowNum}" t="s"><v>${index}</v></c>`);
    }

    private _toOADate(date: Date): number {
        const timezoneOffset = date.getTimezoneOffset() * 60000;
        return (date.getTime() - timezoneOffset - this._oaEpoch) / 86400000;
    }

    finalize(): Promise<void> {
        return new Promise((resolve, reject) => {
            try {
                this._writeSharedStrings();
                this._writeStyles();
                this._writeWorkbook();
                this._writeContentTypes();
                this._writeRels();

                this.output.on('close', () => {
                    resolve();
                });

                this.archive.on('error', (err) => {
                    reject(err);
                });

                this.archive.finalize();
            } catch (err) {
                reject(err);
            }
        });
    }

    private _writeSharedStrings(): void {
        const bigBuf = new BigBuffer();

        bigBuf.writeString('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>');
        bigBuf.writeString(`<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="${this.sstCntAll}" uniqueCount="${this.sstArray.length}">`);

        for (let i = 0; i < this.sstArray.length; i++) {
            const txt = this.sstArray[i];
            const cleanTxt = this._escape(txt);
            if (cleanTxt.length > 0 && (cleanTxt[0] === ' ' || cleanTxt[cleanTxt.length - 1] === ' ' || /[\t\n\r]/.test(cleanTxt))) {
                bigBuf.writeString(`<si><t xml:space="preserve">${cleanTxt}</t></si>`);
            } else {
                bigBuf.writeString(`<si><t>${cleanTxt}</t></si>`);
            }
        }

        bigBuf.writeString('</sst>');

        this.archive.append(Readable.from(bigBuf.getChunks()), { name: 'xl/sharedStrings.xml' });
    }

    private _writeStyles(): void {
        const styles = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<numFmts count="1">
    <numFmt numFmtId="164" formatCode="yyyy\\-mm\\-dd\\ hh:mm:ss"/>
</numFmts>
<fonts count="1">
<font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/><scheme val="minor"/></font>
</fonts>
<fills count="2">
<fill><patternFill patternType="none"/></fill>
<fill><patternFill patternType="gray125"/></fill>
</fills>
<borders count="1">
<border><left/><right/><top/><bottom/><diagonal/></border>
</borders>
<cellStyleXfs count="1">
<xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
</cellStyleXfs>
<cellXfs count="3">
<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
<xf numFmtId="14" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>
<xf numFmtId="164" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>
</cellXfs>
<cellStyles count="1">
<cellStyle name="Normal" xfId="0" builtinId="0"/>
</cellStyles>
<dxfs count="0"/>
<tableStyles count="0" defaultTableStyle="TableStyleMedium2" defaultPivotStyle="PivotStyleLight16"/>
</styleSheet>`;
        this.archive.append(styles, { name: 'xl/styles.xml' });
    }

    private _writeWorkbook(): void {
        let xml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets>`;

        for (const sheet of this.sheetList) {
            const state = sheet.hidden ? ' state="hidden"' : '';
            const escapedName = this._escapeSheetNameForXml(sheet.name);
            xml += `<sheet name="${escapedName}" sheetId="${sheet.sheetId}"${state} r:id="${sheet.rId}"/>`;
        }

        xml += `</sheets>`;

        if (this._autofilterIsOn) {
            xml += '<definedNames>';
            for (const sheet of this.sheetList) {
                if (sheet.filterHeaderRange) {
                    const localSheetId = sheet.sheetId - 1;
                    const escapedRange = this._escape(sheet.filterHeaderRange);
                    xml += `<definedName name="_xlnm._FilterDatabase" localSheetId="${localSheetId}" hidden="1">${escapedRange}</definedName>`;
                }
            }
            xml += '</definedNames>';
        }

        xml += `</workbook>`;
        this.archive.append(xml, { name: 'xl/workbook.xml' });
    }

    private _writeContentTypes(): void {
        let xml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>`;

        for (const sheet of this.sheetList) {
            xml += `<Override PartName="/${sheet.pathInArchive}" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`;
        }

        xml += `</Types>`;
        this.archive.append(xml, { name: '[Content_Types].xml' });
    }

    private _writeRels(): void {
        const globalRels = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>`;
        this.archive.append(globalRels, { name: '_rels/.rels' });

        let wbRels = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`;

        for (const sheet of this.sheetList) {
            wbRels += `<Relationship Id="${sheet.rId}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/${sheet.nameInArchive}"/>`;
        }

        let nextId = this.sheetList.length + 1;
        wbRels += `<Relationship Id="rId${nextId++}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>`;
        wbRels += `<Relationship Id="rId${nextId++}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>`;

        wbRels += `</Relationships>`;
        this.archive.append(wbRels, { name: 'xl/_rels/workbook.xml.rels' });
    }
}

export default XlsxWriter;
