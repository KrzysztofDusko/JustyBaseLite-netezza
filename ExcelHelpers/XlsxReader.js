const yauzl = require('yauzl');
const ExcelReaderAbstract = require('./ExcelReaderAbstract');

class XlsxReader extends ExcelReaderAbstract {
    constructor() {
        super();
        this.zipfile = null;
        this.sharedStrings = [];
        this.styles = [];
        this.sheetNames = [];
        this.sheets = []; // { name, id, rId, path }

        // State for iteration
        this._currentSheetIndex = -1;
        this._sheetXml = null;
        this._xmlPos = 0;

        // Current Row State
        this._currentRow = [];
        this.fieldCount = 0;
    }

    async open(path, readSharedStrings = true) {
        return new Promise((resolve, reject) => {
            yauzl.open(path, { lazyEntries: true, autoClose: false }, async (err, zipfile) => {
                if (err) return reject(err);
                this.zipfile = zipfile;

                // We need to read all entries first to map paths
                this.entries = new Map(); // path -> entry

                zipfile.on('entry', (entry) => {
                    this.entries.set(entry.fileName, entry);
                    zipfile.readEntry();
                });

                zipfile.on('end', async () => {
                    try {
                        // 1. Read Workbook Relationships
                        const wbRelsContent = await this._readZipEntryContent('xl/_rels/workbook.xml.rels');
                        let rIdToTarget = {};
                        if (wbRelsContent) {
                            // Simple regex parse for Relationships
                            const relRegex = /<Relationship[^>]*Id="([^"]*)"[^>]*Target="([^"]*)"/g;
                            let match;
                            while ((match = relRegex.exec(wbRelsContent)) !== null) {
                                rIdToTarget[match[1]] = match[2];
                            }
                            // Fix targets
                            for (let k in rIdToTarget) {
                                let t = rIdToTarget[k];
                                if (t.startsWith('/')) t = t.substring(1);
                                if (!t.startsWith('xl/') && !t.startsWith('worksheets/') && !t.startsWith('theme/') && !t.startsWith('styles') && !t.startsWith('sharedStrings')) {
                                    // maybe adjust relative path logic here if needed
                                }
                            }
                        }

                        // 2. Read Workbook to get sheets
                        const wbContent = await this._readZipEntryContent('xl/workbook.xml');
                        if (wbContent) {
                            const sheetRegex = /<sheet[^>]*name="([^"]*)"[^>]*sheetId="([^"]*)"[^>]*r:id="([^"]*)"/g;
                            let match;
                            while ((match = sheetRegex.exec(wbContent)) !== null) {
                                const name = this._unescapeXml(match[1]);
                                const sheetId = match[2];
                                const rId = match[3];

                                let target = rIdToTarget[rId];
                                let fullPath = target;
                                if (!fullPath.startsWith('xl/')) {
                                    fullPath = 'xl/' + fullPath;
                                }

                                this.sheetNames.push(name);
                                this.sheets.push({ name, sheetId, rId, path: fullPath });
                            }
                        }

                        // 3. Read Shared Strings
                        if (readSharedStrings) {
                            const ssContent = await this._readZipEntryContent('xl/sharedStrings.xml');
                            if (ssContent) {
                                this._parseSharedStrings(ssContent);
                            }
                        }

                        // 4. Read Styles
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

                zipfile.readEntry(); // start reading
            });
        });
    }

    async close() {
        if (this.zipfile) {
            this.zipfile.close();
        }
    }

    // New helper to read entry content fully
    async _readZipEntryContent(path) {
        // yauzl paths are specific. AdmZip was forgiving.
        // We look up in our map
        const entry = this.entries.get(path);
        if (!entry) return null; // or empty string

        return new Promise((resolve, reject) => {
            this.zipfile.openReadStream(entry, (err, readStream) => {
                if (err) return reject(err);

                const chunks = [];
                readStream.on('data', chunk => chunks.push(chunk));
                readStream.on('end', () => {
                    resolve(Buffer.concat(chunks).toString('utf8'));
                });
                readStream.on('error', reject);
            });
        });
    }

    _unescapeXml(str) {
        if (!str) return "";
        if (str.indexOf('&') === -1) return str;
        return str.replace(/&amp;/g, '&')
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&quot;/g, '"')
            .replace(/&apos;/g, "'");
    }

    _parseSharedStrings(xml) {
        // Copy pasted logic from previous version, adapted for input string
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

    _parseStyles(xml) {
        // Copy pasted logic
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

    getSheetNames() {
        return this.sheetNames;
    }

    // NOTE: read() is now slightly awkward because it might need to load sheet data asynchronously.
    // However, the original interface requires synchronous read().
    // We can pre-load current sheet in a "prepareSheet" step or break the interface.
    // The user authorized "async open", but `read()` being sync implies data must be ready.
    // So `open()` reads metadata.
    // But `read()` iterates rows.
    // Best Approach for performance:
    // Make `read()` async? Or make `initSheet` async and user calls it?
    // Let's check Test Script. It loops `while(reader.read())`.
    // If we make `read()` async, it becomes `while(await reader.read())`.
    // This is acceptable given the task scope.

    async read() {
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

    async _initSheet(index) {
        if (index >= this.sheets.length) return;
        const sheet = this.sheets[index];
        this.actualSheetName = sheet.name;

        // Async read sheet content
        this._sheetXml = await this._readZipEntryContent(sheet.path);

        if (!this._sheetXml) {
            this._sheetXml = "";
        }

        this._xmlPos = 0;
        const sd = this._sheetXml.indexOf('<sheetData>');
        if (sd !== -1) this._xmlPos = sd + 11;
    }

    _readNextRow() {
        // Same optimized logic as before (sync)
        if (!this._sheetXml) return false;

        const rowStart = this._sheetXml.indexOf('<row', this._xmlPos);
        if (rowStart === -1) return false;

        // Safety / Optimization
        // const sdEnd = this._sheetXml.lastIndexOf('</sheetData>'); 

        const rowEnd = this._sheetXml.indexOf('</row>', rowStart);
        if (rowEnd === -1) return false;

        this._xmlPos = rowEnd + 6;
        this._parseRowByIndex(rowStart, rowEnd);
        return true;
    }

    _parseRowByIndex(rowStart, rowEnd) {
        // Same optimized parser
        this._currentRow = [];
        const xml = this._sheetXml;

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

            let val = null;
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

            let finalVal = null;
            if (val !== null) {
                if (type === 's') {
                    let idx = parseInt(val, 10);
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
                            } catch (e) { }
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

    _columnLetterToIndex(letter) {
        let column = 0;
        const length = letter.length;
        for (let i = 0; i < length; i++) {
            column += (letter.charCodeAt(i) - 64) * Math.pow(26, length - i - 1);
        }
        return column - 1;
    }

    getValue(i) {
        if (i < 0 || i >= this._currentRow.length) return null;
        return this._currentRow[i];
    }
}

module.exports = XlsxReader;
