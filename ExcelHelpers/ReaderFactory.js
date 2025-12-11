const XlsxReader = require('./XlsxReader');
const XlsbReader = require('./XlsbReader');
const path = require('path');

class ReaderFactory {
    static create(filePath) {
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

module.exports = ReaderFactory;
