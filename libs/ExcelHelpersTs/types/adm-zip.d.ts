declare module 'adm-zip' {
    interface IZipEntry {
        entryName: string;
        getData(): Buffer;
    }

    class AdmZip {
        constructor(filePath?: string | Buffer);
        getEntries(): IZipEntry[];
        getEntry(name: string): IZipEntry | null;
        extractAllTo(targetPath: string, overwrite?: boolean): void;
        addFile(entryName: string, content: Buffer, comment?: string, attr?: number): void;
        writeZip(targetFileName?: string): void;
        toBuffer(): Buffer;
    }

    export = AdmZip;
}
