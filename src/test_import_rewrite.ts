
const path = require('path');
const fs = require('fs');
const XlsxWriter = require('../ExcelHelpers/XlsxWriter');

import { NetezzaImporter } from './dataImporter';

async function runTest() {
    console.log('Starting verification test...');

    // Use __dirname which should resolve to src/ if ts-node runs there, 
    // or d:\... if project root.
    // Note: XlsxWriter in '../ExcelHelpers/' implies we are in 'src/'.
    // If we run `npx ts-node src/test_import_rewrite.ts`, __dirname is likely `.../src`.
    const testFile = path.resolve(__dirname, 'test_gen.xlsx');

    // 1. Generate Test File
    console.log('Generating test file:', testFile);
    try {
        const writer = new XlsxWriter(testFile);
        console.log('Writer created');

        writer.addSheet('Sheet1');
        console.log('Sheet added');

        const headers = ['ID', 'Name', 'DateVal', 'NumVal'];
        const rows = [
            [1, 'Alice', new Date(2023, 0, 15, 12, 0, 0), 123.45],
            [2, 'Bob', new Date(2023, 5, 20), 678.90]
        ];

        writer.writeSheet(rows, headers);
        console.log('Sheet written');

        await writer.finalize();
        console.log('Test file generated.');
    } catch (e) {
        console.error('Error generating file:', e);
        return;
    }

    // 2. Analyze with NetezzaImporter
    console.log('Analyzing with NetezzaImporter...');
    const importer = new NetezzaImporter(testFile, 'TEST_TABLE', 'dummy_connection');

    try {
        const types = await importer.analyzeDataTypes((msg) => console.log('Progress:', msg));

        console.log('Analysis complete. Types detected:');
        const sqlHeaders = importer.getSqlHeaders();
        console.log('Columns:', sqlHeaders);

        const typeNames = types.map(t => t.currentType.dbType);
        console.log('Detected Types:', typeNames);

        if (typeNames[0] !== 'BIGINT') console.error('FAIL: Column ID should be BIGINT');
        if (typeNames[1] !== 'NVARCHAR') console.error('FAIL: Column Name should be NVARCHAR');
        if (!['DATE', 'DATETIME', 'NVARCHAR'].includes(typeNames[2])) console.error('FAIL: Column DateVal unexpected type:', typeNames[2]);
        if (typeNames[3] !== 'NUMERIC') console.error('FAIL: Column NumVal should be NUMERIC');

        if (typeNames[0] === 'BIGINT' && typeNames[1] === 'NVARCHAR' && ['DATE', 'DATETIME'].includes(typeNames[2]) && typeNames[3] === 'NUMERIC') {
            console.log('SUCCESS: All types matched expectations.');
        }

        console.log('Verification Finished.');

    } catch (e) {
        console.error('Error during analysis:', e);
        if (e instanceof Error && e.stack) console.error(e.stack);
    }
}

runTest().catch(console.error);
