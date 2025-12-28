import yauzl from 'yauzl';
import { ExcelReaderAbstract } from './ExcelReaderAbstract';

interface SheetInfo {
    name: string;
    sheetId: string;
    rId: string;
    path: string;
}

export class XlsxReader extends ExcelReaderAbstract {
    private zipfile: yauzl.ZipFile | null = null;
    private sharedStrings: string[] = [];
    private styles: any[] = [];
    private sheetNames: string[] = [];
    private sheets: SheetInfo[] = [];
    private entries: Map<string, yauzl.Entry> = new Map();

    private _currentSheetIndex: number = -1;
    private _sheetXml: string | null = null;
    private _xmlPos: number = 0;

    private _currentRow: any[] = [];

    xfIdToNumFmtId: number[] = [];
    customDateFormats: Set<number> = new Set();

    constructor() {
        super();
    }

    async open(path: string, readSharedStrings: boolean = true): Promise<void> {
        return new Promise((resolve, reject) => {
            yauzl.open(path, { lazyEntries: true, autoClose: false }, async (err, zipfile) => {
                if (err) return reject(err);
                this.zipfile = zipfile;

                this.entries = new Map();

                zipfile.on('entry', (entry: yauzl.Entry) => {
                    this.entries.set(entry.fileName, entry);
                    zipfile.readEntry();
                });

                zipfile.on('end', async () => {
                    try {
                        const wbRelsContent = await this._readZipEntryContent('xl/_rels/workbook.xml.rels');
                        const rIdToTarget: Record<string, string> = {};
                        if (wbRelsContent) {
                            const relRegex = /<Relationship[^>]*Id="([^"]*)"[^>]*Target="([^"]*)"/g;
                            let match;
                            while ((match = relRegex.exec(wbRelsContent)) !== null) {
                                rIdToTarget[match[1]] = match[2];
                            }
                        }

                        const wbContent = await this._readZipEntryContent('xl/workbook.xml');
                        if (wbContent) {
                            const sheetRegex = /<sheet[^>]*name="([^"]*)"[^>]*sheetId="([^"]*)"[^>]*r:id="([^"]*)"/g;
                            let match;
                            while ((match = sheetRegex.exec(wbContent)) !== null) {
                                const name = this._unescapeXml(match[1]);
                                const sheetId = match[2];
                                const rId = match[3];

                                const target = rIdToTarget[rId];
                                let fullPath = target;
                                if (!fullPath.startsWith('xl/')) {
                                    fullPath = 'xl/' + fullPath;
                                }

                                this.sheetNames.push(name);
                                this.sheets.push({ name, sheetId, rId, path: fullPath });
                            }
                        }

                        if (readSharedStrings) {
                            const ssContent = await this._readZipEntryContent('xl/sharedStrings.xml');
                            if (ssContent) {
                                this._parseSharedStrings(ssContent);
                            }
                        }

                        const stylesContent = await this._readZipEntryContent('xl/styles.xml');
                        if (stylesContent) {
                            this._parseStyles(stylesContent);
                        }

                        this.resultsCount = this.sheets.length;
                        this._currentSheetIndex = -1;
                        resolve();
                    } catch (e) {
                        reject(e);
                    }
                });

                zipfile.readEntry();
            });
        });
    }

    async close(): Promise<void> {
        if (this.zipfile) {
            this.zipfile.close();
        }
    }

    private async _readZipEntryContent(path: string): Promise<string | null> {
        const entry = this.entries.get(path);
        if (!entry) return null;

        return new Promise((resolve, reject) => {
            this.zipfile!.openReadStream(entry, (err, readStream) => {
                if (err) return reject(err);

                const chunks: Buffer[] = [];
                readStream!.on('data', (chunk: Buffer) => chunks.push(chunk));
                readStream!.on('end', () => {
                    resolve(Buffer.concat(chunks).toString('utf8'));
                });
                readStream!.on('error', reject);
            });
        });
    }

    private _unescapeXml(str: string): string {
        if (!str) return "";
        if (str.indexOf('&') === -1) return str;
        return str.replace(/&amp;/g, '&')
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&quot;/g, '"')
            .replace(/&apos;/g, "'");
    }

    private _parseSharedStrings(xml: string): void {
        let pos = 0;
        while (true) {
            const siStart = xml.indexOf('<si>', pos);
            if (siStart === -1) break;

            const siEnd = xml.indexOf('</si>', siStart);
            if (siEnd === -1) break;

            const content = xml.substring(siStart, siEnd);
            let val = "";
            let tPos = 0;
            while (true) {
                const tStart = content.indexOf('<t', tPos);
                if (tStart === -1) break;
                const tagEnd = content.indexOf('>', tStart);
                const tEnd = content.indexOf('</t>', tagEnd);
                if (tEnd === -1) break;

                val += content.substring(tagEnd + 1, tEnd);
                tPos = tEnd + 4;
            }

            this.sharedStrings.push(this._unescapeXml(val));
            pos = siEnd + 5;
        }
    }

    private _parseStyles(xml: string): void {
        const numFmtRegex = /<numFmt\s+[^>]*numFmtId="(\d+)"[^>]*formatCode="([^"]*)"/g;
        let match;
        this.customDateFormats = new Set();

        while ((match = numFmtRegex.exec(xml)) !== null) {
            const id = parseInt(match[1]);
            const code = match[2];
            if (code.toLowerCase().includes('yy') || code.toLowerCase().includes('mm') || code.toLowerCase().includes('dd') || code.toLowerCase().includes('h:mm')) {
                this.customDateFormats.add(id);
            }
        }

        this.xfIdToNumFmtId = [];
        const cellXfsStart = xml.indexOf('<cellXfs');
        if (cellXfsStart !== -1) {
            const cellXfsEnd = xml.indexOf('</cellXfs>', cellXfsStart);
            if (cellXfsEnd !== -1) {
                const cellXfsContent = xml.substring(cellXfsStart, cellXfsEnd);
                const xfRegex = /<xf\s+[^>]*numFmtId="(\d+)"/g;
                while ((match = xfRegex.exec(cellXfsContent)) !== null) {
                    const numFmtId = parseInt(match[1]);
                    this.xfIdToNumFmtId.push(numFmtId);
                }
            }
        }
    }

    getSheetNames(): string[] {
        return this.sheetNames;
    }

    async read(): Promise<boolean> {
        if (this._currentSheetIndex === -1) {
            this._currentSheetIndex = 0;
            await this._initSheet(this._currentSheetIndex);
        }

        if (this._readNextRow()) {
            return true;
        } else {
            return false;
        }
    }

    private async _initSheet(index: number): Promise<void> {
        if (index >= this.sheets.length) return;
        const sheet = this.sheets[index];
        this.actualSheetName = sheet.name;

        this._sheetXml = await this._readZipEntryContent(sheet.path);

        if (!this._sheetXml) {
            this._sheetXml = "";
        }

        this._xmlPos = 0;
        const sd = this._sheetXml.indexOf('<sheetData>');
        if (sd !== -1) this._xmlPos = sd + 11;
    }

    private _readNextRow(): boolean {
        if (!this._sheetXml) return false;

        const rowStart = this._sheetXml.indexOf('<row', this._xmlPos);
        if (rowStart === -1) return false;

        const rowEnd = this._sheetXml.indexOf('</row>', rowStart);
        if (rowEnd === -1) return false;

        this._xmlPos = rowEnd + 6;
        this._parseRowByIndex(rowStart, rowEnd);
        return true;
    }

    private _parseRowByIndex(rowStart: number, rowEnd: number): void {
        this._currentRow = [];
        const xml = this._sheetXml!;

        let pos = xml.indexOf('>', rowStart) + 1;

        while (pos < rowEnd) {
            const cStart = xml.indexOf('<c', pos);
            if (cStart === -1 || cStart >= rowEnd) break;

            const nextTagClose = xml.indexOf('>', cStart);

            let colIndex = -1;
            let type = 'n';
            let styleIndex = 0;

            let currentIdx = cStart + 2;
            while (currentIdx < nextTagClose) {
                while (xml.charCodeAt(currentIdx) <= 32 && currentIdx < nextTagClose) currentIdx++;
                if (currentIdx >= nextTagClose) break;

                const keyStart = currentIdx;
                while (xml.charCodeAt(currentIdx) !== 61 && currentIdx < nextTagClose) currentIdx++;
                const key = xml.substring(keyStart, currentIdx);

                currentIdx++;

                if (xml.charCodeAt(currentIdx) === 34) {
                    currentIdx++;
                    const valStart = currentIdx;
                    while (xml.charCodeAt(currentIdx) !== 34 && currentIdx < nextTagClose) currentIdx++;
                    const val = xml.substring(valStart, currentIdx);
                    currentIdx++;

                    if (key === 'r') {
                        let letterLen = 0;
                        while (letterLen < val.length && val.charCodeAt(letterLen) >= 65) letterLen++;
                        colIndex = this._columnLetterToIndex(val.substring(0, letterLen));
                    } else if (key === 't') {
                        type = val;
                    } else if (key === 's') {
                        styleIndex = parseInt(val, 10);
                    }
                }
            }

            let val: string | null = null;
            if (xml.charCodeAt(nextTagClose - 1) === 47) {
                pos = nextTagClose + 1;
            } else {
                const cEnd = xml.indexOf('</c>', nextTagClose);
                const cellContentEnd = (cEnd !== -1 && cEnd < rowEnd) ? cEnd : rowEnd;

                const vStart = xml.indexOf('<v>', nextTagClose);
                if (vStart !== -1 && vStart < cellContentEnd) {
                    const vInnerStart = vStart + 3;
                    const vEnd = xml.indexOf('</v>', vInnerStart);
                    if (vEnd !== -1 && vEnd < cellContentEnd) {
                        val = xml.substring(vInnerStart, vEnd);
                    }
                } else {
                    if (type === 'inlineStr') {
                        const tStart = xml.indexOf('<t', nextTagClose);
                        if (tStart !== -1 && tStart < cellContentEnd) {
                            const tContentStart = xml.indexOf('>', tStart) + 1;
                            const tEnd = xml.indexOf('</t>', tContentStart);
                            if (tEnd !== -1 && tEnd < cellContentEnd) {
                                val = xml.substring(tContentStart, tEnd);
                            }
                        }
                    }
                }
                pos = cellContentEnd + 4;
            }

            let finalVal: any = null;
            if (val !== null) {
                if (type === 's') {
                    const idx = parseInt(val, 10);
                    finalVal = this.sharedStrings[idx];
                } else if (type === 'b') {
                    finalVal = (val === '1' || val === 'true');
                } else if (type === 'inlineStr') {
                    finalVal = this._unescapeXml(val);
                } else if (type === 'n' || type === '') {
                    finalVal = parseFloat(val);

                    if (styleIndex > 0 && this.xfIdToNumFmtId) {
                        let numFmtId = 0;
                        if (styleIndex < this.xfIdToNumFmtId.length) {
                            numFmtId = this.xfIdToNumFmtId[styleIndex];
                        }

                        const isDate = (numFmtId >= 14 && numFmtId <= 22) ||
                            (numFmtId >= 45 && numFmtId <= 47) ||
                            (this.customDateFormats && this.customDateFormats.has(numFmtId));

                        if (isDate) {
                            try {
                                finalVal = this.getDateTimeFromOaDate(finalVal);
                            } catch {
                                // ignore date conversion errors
                            }
                        }
                    }
                }
            }

            if (colIndex !== -1) {
                this._currentRow[colIndex] = finalVal;
                if (colIndex >= this.fieldCount) this.fieldCount = colIndex + 1;
            }
        }
    }

    private _columnLetterToIndex(letter: string): number {
        let column = 0;
        const length = letter.length;
        for (let i = 0; i < length; i++) {
            column += (letter.charCodeAt(i) - 64) * Math.pow(26, length - i - 1);
        }
        return column - 1;
    }

    getValue(i: number): any {
        if (i < 0 || i >= this._currentRow.length) return null;
        return this._currentRow[i];
    }
}

export default XlsxReader;
