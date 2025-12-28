const XlsxWriter = require('./libs/ExcelHelpersTs/XlsxWriter');
const path = require('path');

async function test() {
    console.log('Testing XlsxWriter...');
    try {
        const filePath = path.resolve(__dirname, 'test_debug.xlsx');
        console.log('Path:', filePath);
        const writer = new XlsxWriter(filePath);
        console.log('Instance created');
        await writer.open(); // Wait, open() method?
        // XlsxWriter.js DOES NOT HAVE open() method in the code I viewed !!!
        // It has constructor that sets up the stream. 
        // It has addSheet(), writeSheet(), finalize().

        // Wait, did I hallucinate open()?
        // Checking XlsxWriter.js view output from Step 80.
        // Lines 25-43: Constructor sets `this.output = fs.createWriteStream(filePath);`
        // There is NO `open()` method.

        console.log('Skipping open() as it does not exist (my mistake in previous script)');

        writer.addSheet('Sheet1');
        console.log('Sheet added');

        const headers = ['A', 'B'];
        writer.writeSheet([], headers); // writeSheet takes rows and headers.

        // Wait, writeSheet WRITES THE WHOLE SHEET AT ONCE.
        // It does NOT support `writeRow`.
        // Line 149: `writeSheet(rows, headers = null, doAutofilter = true)`
        // Line 149 implementation creates BigBuffer and writes XML.

        // So `XlsxWriter` is a "Write All at Once" writer, unlike `XlsxReader` which is streaming?
        // Or did I miss `writeRow`?
        // Scanning Step 80 output...
        // Methods: constructor, _getColumnLetter, _sanitizeSheetName, ..., addSheet, _needsEscape, _escape, writeSheet, ..., finalize.
        // There is NO `writeRow`.

        // So my usage in `test_import_rewrite.ts` was completely wrong for this class.
        // `writer.open()` undefined -> crash?
        // `writer.writeRow()` undefined -> crash.

        // I need to fix the test script to usage:
        // writer.addSheet('Sheet1');
        // writer.writeSheet(rows, headers);
        // await writer.finalize();

        console.log('Calling finalize...');
        await writer.finalize();
        console.log('Done.');

    } catch (e) {
        console.error('Error:', e);
    }
}

test();
