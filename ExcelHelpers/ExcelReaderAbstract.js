class ExcelReaderAbstract {
    constructor() {
        this.fieldCount = 0;
        this.rowCount = 0;
        this.actualSheetName = '';
        this.resultsCount = 0;

        // OA Date Epoch: December 30, 1899
        this._oaEpoch = Date.UTC(1899, 11, 30);
    }

    /**
     * @param {string} path 
     * @param {boolean} readSharedStrings 
     * @param {boolean} updateMode 
     */
    async open(path, readSharedStrings = true, updateMode = false) {
        throw new Error("Method 'open' must be implemented.");
    }

    async close() {
        // cleanup if needed
    }

    /**
     * @returns {boolean} true if row read, false if end of sheet
     */
    read() {
        throw new Error("Method 'read' must be implemented.");
    }

    /**
     * @returns {string[]}
     */
    getSheetNames() {
        throw new Error("Method 'getSheetNames' must be implemented.");
    }

    /**
     * @param {number} i Column index
     * @returns {any}
     */
    getValue(i) {
        throw new Error("Method 'getValue' must be implemented.");
    }

    dispose() {
        // cleanup
    }

    /**
     * Convert OLE Automation date (double) to JS Date
     * @param {number} oaDate 
     * @returns {Date}
     */
    getDateTimeFromOaDate(oaDate) {
        const ms = oaDate * 86400000 + this._oaEpoch;
        return new Date(ms);
    }
}

module.exports = ExcelReaderAbstract;
