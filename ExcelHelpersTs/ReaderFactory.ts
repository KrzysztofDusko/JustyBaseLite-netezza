import * as path from 'path';
import { XlsxReader } from './XlsxReader';
import { XlsbReader } from './XlsbReader';
import { ExcelReaderAbstract } from './ExcelReaderAbstract';

export class ReaderFactory {
    static create(filePath: string): ExcelReaderAbstract {
        const ext = path.extname(filePath).toLowerCase();
        if (ext === '.xlsx') {
            return new XlsxReader();
        } else if (ext === '.xlsb') {
            return new XlsbReader();
        } else {
            throw new Error(`Unsupported extension: ${ext}`);
        }
    }
}

export default ReaderFactory;
