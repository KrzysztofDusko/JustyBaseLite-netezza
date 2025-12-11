const fs = require('fs');
const archiver = require('archiver');
const { Readable } = require('stream');
const BigBuffer = require('./BigBuffer');

// Invalid characters for Excel sheet names
const INVALID_SHEET_NAME_CHARS = /[\\\/*?\[\]:]/g;

class XlsbWriter {
    constructor(filePath) {
        this.filePath = filePath;
        this.output = fs.createWriteStream(filePath);
        this.archive = archiver('zip');

        this.archive.pipe(this.output);

        this.sheetCount = 0;
        this.sheetList = [];
        this.sstDic = new Map();
        this.sstCntUnique = 0;
        this.sstCntAll = 0;
        this.colWidths = [];
        this._autofilterIsOn = false;

        // Cache OA date epoch for performance
        this._oaEpoch = Date.UTC(1899, 11, 30);

        // Constants similar to C# implementation
        this._sheet1Bytes = Buffer.from([
            //sheet1Bytes[0..84]
            0x81, 0x01, 0x00, 0x93, 0x01, 0x17, 0xCB, 0x04, //0 ..7
            0x02, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, //8 ..15
            0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, //16 ..23
            0xFF, 0x00, 0x00, 0x00, 0x00, 0x94, 0x01, 0x10, //24 ..31
            0x00, 0x00, 0x00, 0x00,//start row //32 (not requied)
            0x00, 0x00, 0x00, 0x00,//last row  //36 (not requied)
            0x00, 0x00, 0x00, 0x00,//start col //40 (not requied)
            0x00, 0x00, 0x00, 0x00,//last col  //44 - 47 (not requied)
            0x85, 0x01, 0x00, 0x89, 0x01, 0x1E, 0xDC, 0x03,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
            0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,

            //sheet1Bytes[84..]
            0x98, 0x01, 0x24, 0x03,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x8A, 0x01, 0x00, 0x86, 0x01,
            0x00, 0x25, 0x06, 0x01, 0x00, 0x02, 0x0E, 0x00,
            0x80, 0x95, 0x08, 0x02, 0x05, 0x00, 0x26, 0x00,
            0xE5, 0x03, 0x0C, 0xFF, 0xFF, 0xFF, 0xFF, 0x08,
            0x00, 0x2C, 0x01, 0x00, 0x00, 0x00, 0x00, 0x91,
            0x01, 0x00, 0x25, 0x06, 0x01, 0x00, 0x02, 0x0E,
            0x00, 0x80, 0x80, 0x08, 0x02, 0x05, 0x00, 0x26,
            0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x2C, 0x01, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x07, 0x0C, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x92, 0x01, 0x00, 0x97, 0x04, 0x42,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x00,
            //autofilter goes here
            /*sheet1Bytes[290] = */
            0xDD, 0x03, 0x02, 0x10, 0x00, 0xDC, 0x03, 0x30,
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xE6, 0x3F,
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xE6, 0x3F,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xE8, 0x3F,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xE8, 0x3F,
            0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0xD3, 0x3F,
            0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0xD3, 0x3F,
            0x25, 0x06, 0x01, 0x00, 0x00, 0x10, 0x00, 0x80,
            0x80, 0x18, 0x10, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x26, 0x00, 0x82, 0x01, 0x00
        ]);

        this._workbookBinStart = Buffer.from([
            0x83, 0x01, 0x00, 0x80, 0x01, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x78, 0x00, 0x6C, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x37, 0x00, 0x01, 0x00, 0x00, 0x00, 0x36, 0x00, 0x05, 0x00, 0x00, 0x00, 0x32, 0x00, 0x34, 0x00, 0x33, 0x00, 0x32, 0x00, 0x36, 0x00, 0x99, 0x01, 0x0C, 0x20, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x25, 0x06, 0x01, 0x00, 0x03, 0x0F, 0x00, 0x80, 0x97, 0x10, 0x34, 0x18, 0x00, 0x00, 0x00, 0x43, 0x00, 0x3A, 0x00, 0x5C, 0x00, 0x73, 0x00, 0x71, 0x00, 0x6C, 0x00, 0x73, 0x00, 0x5C, 0x00,
            0x54, 0x00, 0x65, 0x00, 0x73, 0x00, 0x74, 0x00, 0x79, 0x00, 0x5A, 0x00, 0x61, 0x00, 0x70, 0x00, 0x69, 0x00, 0x73, 0x00, 0x75, 0x00, 0x58, 0x00, 0x6C, 0x00, 0x73, 0x00, 0x62, 0x00, 0x5C, 0x00, 0x26, 0x00,
            0x25, 0x06, 0x01, 0x00, 0x00, 0x10, 0x00, 0x80, 0x81, 0x18, 0x82, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2F, 0x00, 0x00, 0x00, 0x31, 0x00, 0x33, 0x00, 0x5F, 0x00, 0x6E, 0x00, 0x63, 0x00,
            0x72, 0x00, 0x3A, 0x00, 0x31, 0x00, 0x5F, 0x00, 0x7B, 0x00, 0x31, 0x00, 0x36, 0x00, 0x35, 0x00, 0x30, 0x00, 0x38, 0x00, 0x44, 0x00, 0x36, 0x00, 0x39, 0x00, 0x2D, 0x00, 0x43, 0x00, 0x46, 0x00, 0x38, 0x00,
            0x37, 0x00, 0x2D, 0x00, 0x34, 0x00, 0x37, 0x00, 0x36, 0x00, 0x39, 0x00, 0x2D, 0x00, 0x38, 0x00, 0x34, 0x00, 0x35, 0x00, 0x36, 0x00, 0x2D, 0x00, 0x44, 0x00, 0x34, 0x00, 0x41, 0x00, 0x34, 0x00, 0x30, 0x00,
            0x31, 0x00, 0x31, 0x00, 0x33, 0x00, 0x31, 0x00, 0x35, 0x00, 0x36, 0x00, 0x37, 0x00, 0x7D, 0x00, 0x2F, 0x00, 0x00, 0x00, 0x2F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x00, 0x87, 0x01, 0x00, 0x25, 0x06, 0x01, 0x00, 0x02, 0x10, 0x00, 0x80, 0x80, 0x18, 0x10, 0x00, 0x00, 0x00, 0x00, 0x0D, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
            0x00, 0x00, 0x00, 0x00, 0x26, 0x00, 0x9E, 0x01, 0x1D, 0x00, 0x00, 0x00, 0x00, 0x9E, 0x16, 0x00, 0x00, 0xB4, 0x69, 0x00, 0x00, 0xE8, 0x26, 0x00, 0x00, 0x58, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x78, 0x88, 0x01, 0x00,
            0x8F, 0x01, 0x00
        ]);

        this._workbookBinMiddle = Buffer.from([0x90, 0x01, 0x00]);

        this._workbookBinEnd = Buffer.from([
            0x9D, 0x01, 0x1A, 0x35, 0xEA, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0xFC, 0xA9, 0xF1, 0xD2, 0x4D, 0x62, 0x50, 0x3F, 0x01,
            0x00, 0x00, 0x00, 0x6A, 0x00, 0x9B, 0x01, 0x01, 0x00, 0x23, 0x04, 0x03, 0x0F, 0x00, 0x00, 0xAB, 0x10, 0x01, 0x01, 0x24, 0x00, 0x84, 0x01, 0x00
        ]);

        this._stylesBin = Buffer.from([
            0x96, 0x02, 0x00, 0xE7, 0x04, 0x04, 0x02,
            0x00, 0x00, 0x00, 0x2C, 0x2C, 0xA4, 0x00, 0x13,
            0x00, 0x00, 0x00, 0x79, 0x00, 0x79, 0x00, 0x79,
            0x00, 0x79, 0x00, 0x5C, 0x00, 0x2D, 0x00, 0x6D,
            0x00, 0x6D, 0x00, 0x5C, 0x00, 0x2D, 0x00, 0x64,
            0x00, 0x64, 0x00, 0x5C, 0x00, 0x20, 0x00, 0x68,
            0x00, 0x68, 0x00, 0x3A, 0x00, 0x6D, 0x00, 0x6D,
            0x00, 0x2C, 0x1E, 0xA6, 0x00, 0x0C, 0x00, 0x00,
            0x00, 0x79, 0x00, 0x79, 0x00, 0x79, 0x00, 0x79,
            0x00, 0x5C, 0x00, 0x2D, 0x00, 0x6D, 0x00, 0x6D,
            0x00, 0x5C, 0x00, 0x2D, 0x00, 0x64, 0x00, 0x64,
            0x00, 0xE8, 0x04, 0x00, 0xE3, 0x04, 0x04, 0x01,
            0x00, 0x00, 0x00,
            //standard font ?
            0x2B, 0x27, 0xDC, 0x00, 0x00, 0x00, 0x90, 0x01,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x07, 0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0x02, 0x07,
            0x00, 0x00, 0x00, 0x43, 0x00, 0x61, 0x00, 0x6C,
            0x00, 0x69, 0x00, 0x62, 0x00, 0x72, 0x00, 0x69,
            0x00,
            //bolded font?
            0x2B, 0x27, 0xDC, 0x00, 0x01, 0x00, 0xBC, 0x02,
            0x00, 0x00, 0x00, 0x02, 0xEE, 0x00, 0x07, 0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0x02, 0x07,
            0x00, 0x00, 0x00, 0x43, 0x00, 0x61, 0x00, 0x6C,
            0x00, 0x69, 0x00, 0x62, 0x00, 0x72, 0x00, 0x69,
            0x00,
            0x25, 0x06, 0x01, 0x00, 0x02, 0x0E, 0x00, 0x80, 0x81, 0x08, 0x00, 0x26, 0x00, 0xE4, 0x04, 0x00, 0xDB, 0x04, 0x04, 0x02, 0x00, 0x00, 0x00,
            0x2D, 0x44, 0x00, 0x00, 0x00, 0x00, 0x03, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0x03, 0x41, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x2D, 0x44, 0x11, 0x00, 0x00, 0x00, 0x03, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0x03, 0x41, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0xDC, 0x04, 0x00, 0xE5, 0x04, 0x04, 0x01, 0x00, 0x00, 0x00, 0x2E, 0x33, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xE6, 0x04, 0x00, 0xF2,
            0x04, 0x04, 0x01, 0x00, 0x00, 0x00, 0x2F, 0x10, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x10, 0x00,
            0x00, 0xF3, 0x04, 0x00, 0xE9, 0x04, 0x04,
            0x04,
            0x00, 0x00, 0x00,
            //(#font)
            0x2F, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x10, 0x00, 0x00,// standard 
            0x2F, 0x10, 0x00, 0x00, 0xA4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x10, 0x01, 0x00,// datetime
            0x2F, 0x10, 0x00, 0x00, 0xA6, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x10, 0x01, 0x00,//date
            0x2F, 0x10, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x10, 0x00, 0x00,//standard bolded

            0xEA, 0x04, 0x00, 0xEB, 0x04, 0x04, 0x01, 0x00,
            0x00, 0x00, 0x25, 0x06, 0x01, 0x00, 0x02, 0x11, 0x00, 0x80, 0x80, 0x18, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x00, 0x30, 0x1C, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x4E,
            0x00, 0x6F, 0x00, 0x72, 0x00, 0x6D, 0x00, 0x61, 0x00, 0x6C, 0x00, 0x6E, 0x00, 0x79, 0x00, 0xEC, 0x04, 0x00, 0xF9, 0x03, 0x04, 0x00, 0x00,
            0x00, 0x00, 0xFA, 0x03, 0x00, 0xFC, 0x03, 0x50, 0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x54, 0x00, 0x61, 0x00, 0x62, 0x00, 0x6C,
            0x00, 0x65, 0x00, 0x53, 0x00, 0x74, 0x00, 0x79, 0x00, 0x6C, 0x00, 0x65, 0x00, 0x4D, 0x00, 0x65, 0x00, 0x64, 0x00, 0x69, 0x00, 0x75, 0x00,
            0x6D, 0x00, 0x32, 0x00, 0x11, 0x00, 0x00, 0x00, 0x50, 0x00, 0x69, 0x00, 0x76, 0x00, 0x6F, 0x00, 0x74, 0x00, 0x53, 0x00, 0x74, 0x00, 0x79,
            0x00, 0x6C, 0x00, 0x65, 0x00, 0x4C, 0x00, 0x69, 0x00, 0x67, 0x00, 0x68, 0x00, 0x74, 0x00, 0x31, 0x00, 0x36, 0x00, 0xFD, 0x03, 0x00, 0x23,
            0x04, 0x02, 0x0E, 0x00, 0x00, 0xEB, 0x08, 0x00, 0xF6, 0x08, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x53, 0x00, 0x6C, 0x00,
            0x69, 0x00, 0x63, 0x00, 0x65, 0x00, 0x72, 0x00, 0x53, 0x00, 0x74, 0x00, 0x79, 0x00, 0x6C, 0x00, 0x65, 0x00, 0x4C, 0x00, 0x69, 0x00, 0x67,
            0x00, 0x68, 0x00, 0x74, 0x00, 0x31, 0x00, 0xF7, 0x08, 0x00, 0xEC, 0x08, 0x00, 0x24, 0x00, 0x23, 0x04, 0x03, 0x0F, 0x00, 0x00, 0xB0, 0x10,
            0x00, 0xB2, 0x10, 0x32, 0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x54, 0x00, 0x69, 0x00, 0x6D, 0x00, 0x65, 0x00, 0x53, 0x00, 0x6C,
            0x00, 0x69, 0x00, 0x63, 0x00, 0x65, 0x00, 0x72, 0x00, 0x53, 0x00, 0x74, 0x00, 0x79, 0x00, 0x6C, 0x00, 0x65, 0x00, 0x4C, 0x00, 0x69, 0x00,
            0x67, 0x00, 0x68, 0x00, 0x74, 0x00, 0x31, 0x00, 0xB3, 0x10, 0x00, 0xB1, 0x10, 0x00, 0x24, 0x00, 0x97, 0x02, 0x00
        ]);

        this._binaryIndexBin = Buffer.from([
            0x2A, 0x18, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x95,
            0x02, 0x00
        ]);

        this._rRkIntegerLowerLimit = -1 << 29;
        this._rRkIntegerUpperLimit = (1 << 29) - 1;

        // Autofilter bytes - exactly matching C#:
        // _autoFilterStartBytes = [0xA1, 0x01, 0x10]
        // _autoFilterEndBytes = [0xA2, 0x01, 0x00]
        this._autoFilterStartBytes = Buffer.from([0xA1, 0x01, 0x10]);
        this._autoFilterEndBytes = Buffer.from([0xA2, 0x01, 0x00]);

        // StickHeader for Autofilter (Frozen Pane)
        this._stickHeaderA1bytes = Buffer.from([
            0x97, 0x01, 0x1D, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0xF0, 0x3F, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03
        ]);

        // Magic Filter Fix Bytes for Excel 2016+
        this._magicFilterExcel2016Fix0 = Buffer.from([0xE1, 0x02, 0x00, 0xE5, 0x02, 0x00, 0xEA, 0x02]);
        this._magicFilterExcel2016Fix1 = Buffer.from([
            0x27, 0x46, 0x21, 0x00, 0x00, 0x00, 0x00, 0xFF, 0x00, 0x00, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x5F,
            0x00, 0x46, 0x00, 0x69, 0x00, 0x6C, 0x00, 0x74, 0x00, 0x65, 0x00, 0x72, 0x00, 0x44, 0x00, 0x61,
            0x00, 0x74, 0x00, 0x61, 0x00, 0x62, 0x00, 0x61, 0x00, 0x73, 0x00, 0x65, 0x00, 0x0F, 0x00, 0x00,
            0x00, 0x3B, 0xFF, 0x00
        ]);
        this._magicFilterExcel2016Fix2 = Buffer.from([0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF]);
    }

    /**
     * Validate and sanitize sheet name according to Excel rules.
     * @param {string} name - Original sheet name
     * @returns {string} - Sanitized sheet name
     */
    _sanitizeSheetName(name) {
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

    /**
     * Add a new sheet to the workbook.
     * @param {string} sheetName - Name of the sheet
     * @param {boolean} [hidden=false] - Whether the sheet is hidden
     */
    addSheet(sheetName, hidden = false) {
        const sanitizedName = this._sanitizeSheetName(sheetName);
        this.sheetCount++;
        this.sheetList.push({
            name: sanitizedName,
            pathInArchive: `xl/worksheets/sheet${this.sheetCount}.bin`,
            hidden: hidden,
            nameInArchive: `sheet${this.sheetCount}.bin`,
            sheetId: this.sheetCount,
            filterHeaderRange: null
        });
    }

    /**
     * Writes a sheet with data.
     * @param {Array<Array<any>>} rows - Array of rows, where each row is an array of values.
     * @param {Array<string>} [headers] - Optional array of header strings.
     * @param {boolean} [doAutofilter=true] - Enable autofilter.
     */
    writeSheet(rows, headers = null, doAutofilter = true) {
        let bigBuf = new BigBuffer();
        let columnCount = 0;

        if (rows.length > 0) {
            columnCount = rows[0].length;
        } else if (headers) {
            columnCount = headers.length;
        }

        // Initialize Column Widths and Types
        this.colWidths = new Array(columnCount).fill(-1.0);

        // --- Header Handling ---
        if (headers) {
            for (let i = 0; i < columnCount; i++) {
                let len = headers[i] ? headers[i].length : 0;
                let width = 1.25 * len + 2;
                if (width > 80) width = 80;
                if (this.colWidths[i] < width) this.colWidths[i] = width;
            }
        }

        // Scan data for column widths (subset)
        for (let r = 0; r < Math.min(rows.length, 100); r++) {
            let row = rows[r];
            for (let c = 0; c < row.length; c++) {
                let val = row[c];
                let len = val ? val.toString().length : 0;
                let width = 1.25 * len + 2;
                if (width > 80) width = 80;
                if (this.colWidths[c] < width) this.colWidths[c] = width;
            }
        }

        // --- Prepare Sheet Stream/Buffer ---

        // Init Sheet Header
        let sheetHeader = Buffer.from(this._sheet1Bytes);
        // Set update Start/End Col in sheetHeader
        let startCol = 0;
        let endCol = columnCount; // C# uses 1-based or inclusive/exclusive? C# writes loop < _endCol.

        // Directly modify the buffer instance we copied
        sheetHeader.writeInt32LE(startCol, 40);
        sheetHeader.writeInt32LE(endCol, 44);

        // Only select first sheet (fix at pos 54)
        if (this.sheetCount !== 1) {
            sheetHeader[54] = 0x9C;
        }

        // 1. Write Start 0..84
        bigBuf.write(sheetHeader.subarray(0, 84));

        // 2. Write StickyHeader (Autofilter Frozen Pane) if enabled
        if (doAutofilter && headers) {
            bigBuf.write(this._stickHeaderA1bytes);
        }

        // 3. Write Middle 84..159 (BrtWsDim, BrtWindowProtection...)
        bigBuf.write(sheetHeader.subarray(84, 159));

        // 4. Write Columns (BrtColInfo) - exactly matching C# WriteColsWidth()
        bigBuf.writeByte(134);
        bigBuf.writeByte(3);

        for (let i = startCol; i < endCol; i++) {
            // Column definition header: 0, 60, 18
            bigBuf.writeByte(0);
            bigBuf.writeByte(60);
            bigBuf.writeByte(18);

            // Column min (4 bytes)
            bigBuf.writeInt32LE(i);
            // Column max (4 bytes)
            bigBuf.writeInt32LE(i);

            // Width (4 bytes: 0, val, 0, 0)
            let width = this.colWidths[i] > 0 ? Math.floor(this.colWidths[i]) : 10;
            bigBuf.writeByte(0);
            bigBuf.writeByte(Math.max(0, Math.min(255, width)));
            bigBuf.writeByte(0);
            bigBuf.writeByte(0);

            // Reserved (4 bytes: 0, 0, 0, 0)
            bigBuf.writeByte(0);
            bigBuf.writeByte(0);
            bigBuf.writeByte(0);
            bigBuf.writeByte(0);

            // Column properties (1 byte: 2 = normal)
            bigBuf.writeByte(2);
            // Total: 4+4+4+4+1 = 17 bytes
        }

        // End columns: 0, 135, 3, 0
        bigBuf.writeByte(0);
        bigBuf.writeByte(135);
        bigBuf.writeByte(3);
        bigBuf.writeByte(0);

        // 5. Write BrtACBegin (159..175 of sheetHeader)
        bigBuf.write(sheetHeader.subarray(159, 175));

        // 6. Write BrtACEnd (Type 38)
        bigBuf.write(Buffer.from([38, 0]));

        // Write Rows
        let rowNum = 0;

        // Headers Row
        if (headers) {
            this.createRowHeader(bigBuf, rowNum, startCol, endCol);
            for (let c = 0; c < headers.length; c++) {
                this.writeString(bigBuf, headers[c], c, true);
            }
            rowNum++;
        }

        // Data Rows
        for (let r = 0; r < rows.length; r++) {
            this.createRowHeader(bigBuf, rowNum, startCol, endCol);
            let row = rows[r];
            for (let c = 0; c < row.length; c++) {
                let val = row[c];
                if (val === null || val === undefined) continue;

                if (typeof val === 'number') {
                    if (Number.isInteger(val)) {
                        if (val >= this._rRkIntegerLowerLimit && val <= this._rRkIntegerUpperLimit) {
                            this.writeRkNumberInteger(bigBuf, val, c);
                        } else {
                            this.writeDouble(bigBuf, val, c);
                        }
                    } else {
                        this.writeDouble(bigBuf, val, c);
                    }
                } else if (typeof val === 'bigint') {
                    // BigInt - convert to string to preserve precision
                    this.writeString(bigBuf, val.toString(), c);
                } else if (typeof val === 'boolean') {
                    this.writeBool(bigBuf, val, c);
                } else if (val instanceof Date) {
                    this.writeDateTime(bigBuf, val, c);
                } else {
                    // String
                    this.writeString(bigBuf, val.toString(), c);
                }
            }
            rowNum++;
        }

        // Sheet End (218..290)
        // Wait, C# uses 218..290. 
        // 218 is END of Sheet Data (after BrtRowHdr... loop).
        // My sheetHeader[218..] contains: 0x91 0x01 (BrtBeginSheetData ?? No).

        // Let's trust sheetHeader[218..290] matches C# footer logic BEFORE autofilter
        bigBuf.write(sheetHeader.subarray(218, 290));

        // Add autofilter if enabled
        if (doAutofilter && headers) {
            this._autofilterIsOn = true;
            const startRow = 0;
            const endRow = rows.length + 1; // +1 for header
            // C# autofilter: startRow, endRow.

            bigBuf.write(this._autoFilterStartBytes);

            // Write row range (start row, end row)
            const rowBuf = Buffer.alloc(8);
            rowBuf.writeInt32LE(0, 0); // Start Row (0-based)
            rowBuf.writeInt32LE(endRow - 1, 4); // End Row (inclusive 0-based)
            bigBuf.write(rowBuf);

            // Write column range (start col, end col)
            const colBuf = Buffer.alloc(8);
            colBuf.writeInt32LE(startCol, 0);
            colBuf.writeInt32LE(endCol - 1, 4);
            bigBuf.write(colBuf);

            bigBuf.write(this._autoFilterEndBytes);

            // Store filter data for workbook definedNames
            const sheet = this.sheetList[this.sheetCount - 1];
            sheet.filterData = {
                startRow: 0,
                endRow: rows.length, // C# uses _rowsCount which is rowNum - 1 after loop = total data rows
                startColumn: startCol,
                endColumn: endCol - 1
            };
        }

        // Final Footer (290..)
        bigBuf.write(sheetHeader.subarray(290));

        // Use stream to avoid Buffer.concat memory copy overhead
        this.archive.append(Readable.from(bigBuf.getChunks()), { name: this.sheetList[this.sheetCount - 1].pathInArchive });
    }

    _getColumnLetter(colIndex) {
        // Simple A-Z, AA-ZZ conversion
        if (colIndex < 26) {
            return String.fromCharCode(65 + colIndex);
        } else if (colIndex < 702) {
            const first = Math.floor(colIndex / 26) - 1;
            const second = colIndex % 26;
            return String.fromCharCode(65 + first) + String.fromCharCode(65 + second);
        }
        return 'A';
    }

    createRowHeader(bigBuf, rowNumber, startCol, endCol) {
        // C#: InitRow ...
        // We write directly to bigBuf now
        bigBuf.writeByte(0);
        bigBuf.writeByte(25);
        bigBuf.writeInt32LE(rowNumber);

        // writing 44, 1 into bytes 10, 11 (0-indexed relative to start of this record)
        // Previous wrote 27 bytes total
        // buf[10] = 44; buf[11] = 1;
        // The previous alloc was 27 bytes initialized to 0.
        // So we need to write carefully.
        // Bytes written so far: 1+1+4 = 6.
        // We need padding checks.

        // Let's reconstruct exact 27 bytes layout:
        // 0: 0
        // 1: 25
        // 2-5: rowNumber
        // 6-9: 0 (padding)
        // 10: 44
        // 11: 1
        // 12-14: 0
        // 15: 1
        // 16-18: 0
        // 19-22: startCol
        // 23-26: endCol

        bigBuf.writeInt32LE(0); // 6-9

        // 10, 11
        bigBuf.writeByte(44);
        bigBuf.writeByte(1);

        // 12-14
        bigBuf.writeByte(0);
        bigBuf.writeByte(0);
        bigBuf.writeByte(0);

        // 15
        bigBuf.writeByte(1);

        // 16-18
        bigBuf.writeByte(0);
        bigBuf.writeByte(0);
        bigBuf.writeByte(0);

        bigBuf.writeInt32LE(startCol);
        bigBuf.writeInt32LE(endCol);
    }

    writeRkNumberInteger(bigBuf, val, colNum, styleNum = 0) {
        // buf[0] = 2; // Record Type
        // buf[1] = 12; // Length 8+4
        // buf.writeInt32LE(colNum, 2);
        // buf[6] = styleNum;
        // rkVal at 10

        bigBuf.writeByte(2);
        bigBuf.writeByte(12);
        bigBuf.writeInt32LE(colNum);
        bigBuf.writeByte(styleNum);

        // Padding/zeros?
        // In previous code: buf = alloc(14). 
        // 0, 1 written. 2-5 colNum. 6 style.
        // 7, 8, 9 are 0.
        bigBuf.writeByte(0);
        bigBuf.writeByte(0);
        bigBuf.writeByte(0);

        let rkVal = (val << 2) | 2;
        bigBuf.writeInt32LE(rkVal);
    }

    writeDouble(bigBuf, val, colNum, styleNum = 0) {
        // buf = alloc(18)
        // 0: 5
        // 1: 16
        // 2-5: colNum
        // 6: styleNum
        // 7-9: 0 (unwritten in prev code, defaults to 0)
        // 10-17: double

        bigBuf.writeByte(5);
        bigBuf.writeByte(16);
        bigBuf.writeInt32LE(colNum);
        bigBuf.writeByte(styleNum);
        bigBuf.writeByte(0);
        bigBuf.writeByte(0);
        bigBuf.writeByte(0);
        bigBuf.writeDoubleLE(val);
    }

    writeBool(bigBuf, val, colNum) {
        // alloc(11)
        // 0: 0x04
        // 1: 9
        // 2-5: colNum
        // 6-9: 0 (implied)
        // 10: val

        bigBuf.writeByte(0x04);
        bigBuf.writeByte(9);
        bigBuf.writeInt32LE(colNum);
        bigBuf.writeInt32LE(0); // 6-9
        bigBuf.writeByte(val ? 1 : 0);
    }

    /**
     * Write a Date value as OLE Automation date.
     * @param {BigBuffer} bigBuf - Buffer to write to
     * @param {Date} date - Date value
     * @param {number} colNum - Column number
     */
    writeDateTime(bigBuf, date, colNum) {
        const timezoneOffset = date.getTimezoneOffset() * 60000;
        const oaDate = (date.getTime() - timezoneOffset - this._oaEpoch) / 86400000;
        this.writeDouble(bigBuf, oaDate, colNum, 1);
    }

    writeString(bigBuf, val, colNum, bolded = false) {
        let index;
        if (this.sstDic.has(val)) {
            index = this.sstDic.get(val);
        } else {
            index = this.sstCntUnique;
            this.sstDic.set(val, index);
            this.sstCntUnique++;
        }
        this.sstCntAll++;

        // alloc(14)
        // 0: 7
        // 1: 12
        // 2-5: colNum
        // 6: bolded ? 3 : 0
        // 7-9: 0
        // 10-13: index

        bigBuf.writeByte(7);
        bigBuf.writeByte(12);
        bigBuf.writeInt32LE(colNum);
        bigBuf.writeByte(bolded ? 3 : 0);
        bigBuf.writeByte(0);
        bigBuf.writeByte(0);
        bigBuf.writeByte(0);
        bigBuf.writeInt32LE(index);
    }

    saveSst() {
        let bigBuf = new BigBuffer();

        // buffers.push(Buffer.from([159, 1, 8])); // Header
        bigBuf.writeByte(159);
        bigBuf.writeByte(1);
        bigBuf.writeByte(8);

        // counts = Buffer.alloc(8);
        bigBuf.writeInt32LE(this.sstCntUnique);
        bigBuf.writeInt32LE(this.sstCntAll);

        for (let [txt, idx] of this.sstDic) {
            let txtLen = txt.length;
            // let buf = Buffer.alloc(5 + 2 * txtLen); 

            // Record ID
            // buf[pos++] = 19;
            bigBuf.writeByte(19);

            let recLen = 5 + 2 * txtLen;

            // C# varint logic re-implementation
            if (recLen >= 128) {
                bigBuf.writeByte(128 + (recLen % 128));
                let tmp = recLen >> 7;
                if (tmp >= 256) {
                    bigBuf.writeByte(128 + (tmp % 128));
                } else {
                    bigBuf.writeByte(tmp);
                }
                bigBuf.writeByte(recLen >> 14);
                if ((recLen >> 14) > 0) {
                    bigBuf.writeByte(0);
                }
            } else {
                bigBuf.writeByte(recLen & 0xFF);
                bigBuf.writeByte((recLen >> 8) & 0xFF);
            }

            // Length of string (4 bytes)
            bigBuf.writeInt32LE(txtLen);

            // String chars - use optimized UTF-16LE writing
            bigBuf.writeUtf16LE(txt);
        }

        // End
        // buffers.push(Buffer.from([160, 1, 0]));
        bigBuf.writeByte(160);
        bigBuf.writeByte(1);
        bigBuf.writeByte(0);

        this.archive.append(Readable.from(bigBuf.getChunks()), { name: 'xl/sharedStrings.bin' });
    }

    _writeFilterDefinedName(wbBuffers, sheet, sheetNum) {
        const filterData = sheet.filterData;
        const sheetIndex = sheet.sheetId - 1;

        // Create Fix1 buffer and inject sheet indices
        const fix1 = Buffer.alloc(this._magicFilterExcel2016Fix1.length);
        this._magicFilterExcel2016Fix1.copy(fix1);

        // C# uses [^2] which is second-to-last = length - 2
        const lastIdx = this._magicFilterExcel2016Fix1.length - 2;

        fix1[7] = sheetIndex;
        fix1[lastIdx] = sheetNum;

        wbBuffers.push(fix1);

        // Write row range (startRow, endRow as Int32)
        const rowBuf = Buffer.alloc(8);
        rowBuf.writeInt32LE(filterData.startRow, 0);
        rowBuf.writeInt32LE(filterData.endRow, 4);
        wbBuffers.push(rowBuf);

        // Write column range (startColumn, endColumn as Int16!)
        const colBuf = Buffer.alloc(4);
        colBuf.writeInt16LE(filterData.startColumn, 0);
        colBuf.writeInt16LE(filterData.endColumn, 2);
        wbBuffers.push(colBuf);

        // Write Fix2
        wbBuffers.push(this._magicFilterExcel2016Fix2);
    }

    /**
     * Finalize the workbook and write all remaining data.
     * @returns {Promise<void>} - Resolves when the file is written
     */
    finalize() {
        return new Promise((resolve, reject) => {
            try {
                this.saveSst();

                this.archive.append(this._stylesBin, { name: 'xl/styles.bin' });

                // Workbook.bin
                let wbBuffers = [];
                wbBuffers.push(this._workbookBinStart);

                for (let sheet of this.sheetList) {
                    let rId = `rId${sheet.sheetId}`;
                    // C# logic:
                    // sw.Write((byte)156); sw.Write((byte)1); sw.Write((byte)(4 + 3 * 4 + name.Length * 2 + rId.Length * 2));
                    // if (isHidden) ... else ...
                    // sw.Write(BitConverter.GetBytes(sheetId));
                    // sw.Write(BitConverter.GetBytes(rId.Length));
                    // write rId chars
                    // sw.Write(BitConverter.GetBytes(name.Length));
                    // write name chars

                    let recLen = 4 + 12 + sheet.name.length * 2 + rId.length * 2;
                    let buf = Buffer.alloc(3 + recLen);
                    buf[0] = 156; buf[1] = 1; buf[2] = recLen;
                    let pos = 3;

                    buf.writeInt32LE(sheet.hidden ? 1 : 0, pos); pos += 4;
                    buf.writeInt32LE(sheet.sheetId, pos); pos += 4;

                    buf.writeInt32LE(rId.length, pos); pos += 4;
                    for (let i = 0; i < rId.length; i++) {
                        let c = rId.charCodeAt(i);
                        buf[pos++] = c & 0xFF; buf[pos++] = (c >> 8) & 0xFF;
                    }

                    buf.writeInt32LE(sheet.name.length, pos); pos += 4;
                    for (let i = 0; i < sheet.name.length; i++) {
                        let c = sheet.name.charCodeAt(i);
                        buf[pos++] = c & 0xFF; buf[pos++] = (c >> 8) & 0xFF;
                    }
                    wbBuffers.push(buf);
                }

                wbBuffers.push(this._workbookBinMiddle);

                // WriteFilterDefinedNames - matching C# exactly
                if (this._autofilterIsOn) {
                    const filteredSheets = this.sheetList.filter(s => s.filterData);
                    const cnt = filteredSheets.length;

                    if (cnt > 0) {
                        // 1. Write Fix0 (Global header)
                        wbBuffers.push(this._magicFilterExcel2016Fix0);

                        // 2. Write count header - C# logic for cnt <= 10
                        const firstByte = 0x10 + (cnt - 1) * 0x0C;
                        const countBuf = Buffer.from([firstByte, cnt, 0x00, 0x00, 0x00]);
                        wbBuffers.push(countBuf);

                        // 3. Write sheet index bytes for each filtered sheet
                        for (let nm = 0; nm < cnt; nm++) {
                            const sheetIndex = filteredSheets[nm].sheetId - 1;
                            const idxBuf = Buffer.alloc(12);
                            // [0x00, 0x00, 0x00, 0x00]
                            // [sheetIndex, 0x00, 0x00, 0x00]
                            // [sheetIndex, 0x00, 0x00, 0x00]
                            idxBuf.writeInt32LE(0, 0);
                            idxBuf[4] = sheetIndex;
                            idxBuf[8] = sheetIndex;
                            wbBuffers.push(idxBuf);
                        }

                        // 4. Write separator 0xE2, 0x02, 0x00
                        wbBuffers.push(Buffer.from([0xE2, 0x02, 0x00]));

                        // 5. Write per-sheet filter records
                        for (let sheetNum = 0; sheetNum < cnt; sheetNum++) {
                            const sheet = filteredSheets[sheetNum];
                            this._writeFilterDefinedName(wbBuffers, sheet, sheetNum);
                        }
                    }
                }

                wbBuffers.push(this._workbookBinEnd);

                this.archive.append(Buffer.concat(wbBuffers), { name: 'xl/workbook.bin' });

                // BinaryIndices
                for (let sheet of this.sheetList) {
                    this.archive.append(this._binaryIndexBin, { name: `xl/worksheets/binaryIndex${sheet.sheetId}.bin` });
                }

                // [Content_Types].xml
                let contentTypes = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="bin" ContentType="application/vnd.ms-excel.sheet.binary.macroEnabled.main"/>
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>`;
                for (let sheet of this.sheetList) {
                    contentTypes += `<Override PartName="/${sheet.pathInArchive}" ContentType="application/vnd.ms-excel.worksheet"/>`;
                    contentTypes += `<Override PartName="/xl/worksheets/binaryIndex${sheet.sheetId}.bin" ContentType="application/vnd.ms-excel.binIndexWs"/>`;
                }
                contentTypes += `<Override PartName="/xl/styles.bin" ContentType="application/vnd.ms-excel.styles"/>
<Override PartName="/xl/sharedStrings.bin" ContentType="application/vnd.ms-excel.sharedStrings"/>
</Types>`;
                this.archive.append(contentTypes, { name: '[Content_Types].xml' });

                // xl/_rels/workbook.bin.rels
                let wbRels = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`;
                for (let sheet of this.sheetList) {
                    let rId = `rId${sheet.sheetId}`;
                    wbRels += `<Relationship Id="${rId}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/${sheet.nameInArchive}"/>`;
                }
                wbRels += `<Relationship Id="rId${this.sheetList.length + 2}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.bin"/>
<Relationship Id="rId${this.sheetList.length + 3}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.bin"/>
</Relationships>`;
                this.archive.append(wbRels, { name: 'xl/_rels/workbook.bin.rels' });

                // _rels/.rels
                let globalRels = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.bin"/>
</Relationships>`;
                this.archive.append(globalRels, { name: '_rels/.rels' });

                // Worksheet rels
                for (let sheet of this.sheetList) {
                    let wsRels = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.microsoft.com/office/2006/relationships/xlBinaryIndex" Target="binaryIndex${sheet.sheetId}.bin"/>
</Relationships>`;
                    this.archive.append(wsRels, { name: `xl/worksheets/_rels/${sheet.nameInArchive}.rels` });
                }

                this.output.on('close', () => resolve());
                this.archive.on('error', (err) => reject(err));
                this.archive.finalize();
            } catch (err) {
                reject(err);
            }
        });
    }
}

module.exports = XlsbWriter;
