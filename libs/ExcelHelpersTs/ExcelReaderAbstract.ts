export abstract class ExcelReaderAbstract {
    fieldCount: number = 0;
    rowCount: number = 0;
    actualSheetName: string = '';
    resultsCount: number = 0;

    protected _oaEpoch: number;

    constructor() {
        this._oaEpoch = Date.UTC(1899, 11, 30);
    }

    abstract open(path: string, readSharedStrings?: boolean, updateMode?: boolean): Promise<void>;

    async close(): Promise<void> {
        // cleanup if needed
    }

    abstract read(): boolean | Promise<boolean>;

    abstract getSheetNames(): string[];

    abstract getValue(i: number): any;

    dispose(): void {
        // cleanup
    }

    getDateTimeFromOaDate(oaDate: number): Date {
        const ms = oaDate * 86400000 + this._oaEpoch;
        return new Date(ms);
    }
}

export default ExcelReaderAbstract;
