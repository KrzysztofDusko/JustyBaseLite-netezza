/**
 * Helper functions for parsing Netezza data types
 * Port of C# NzConnectionHelpers.cs
 */

const CLIENT_ENCODING = 'utf8';
const CHAR_VARCHAR_ENCODING = 'latin1';

/**
 * Parse text from buffer
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {string}
 */
function textRecv(data, offset, length) {
    if (length + offset > data.length) {
        length = data.length - offset;
    }
    return data.toString(CLIENT_ENCODING, offset, offset + length);
}

/**
 * Parse Latin1/ASCII text from buffer
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {string}
 */
function textRecvLatin1(data, offset, length) {
    if (length + offset > data.length) {
        length = data.length - offset;
    }
    return data.toString(CHAR_VARCHAR_ENCODING, offset, offset + length);
}

/**
 * Parse bytea/binary data as hex string
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {string}
 */
function byteaRecv(data, offset, length) {
    return data.toString('ascii', offset, offset + length);
}

/**
 * Parse boolean from text ('t' or 'f')
 * @param {Buffer} data 
 * @param {number} offset 
 * @returns {boolean}
 */
function boolRecv(data, offset) {
    return data[offset] === 0x74; // 't'
}

/**
 * Parse 1-byte signed integer (BYTEINT)
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {number}
 */
function byteRecv(data, offset, length) {
    const str = data.toString('ascii', offset, offset + length);
    return parseInt(str, 10);
}

/**
 * Parse 2-byte integer from text
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {number}
 */
function int2Recv(data, offset, length) {
    const str = data.toString('ascii', offset, offset + length);
    return parseInt(str, 10);
}

/**
 * Parse 4-byte integer from text
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {number}
 */
function int4Recv(data, offset, length) {
    const str = data.toString('ascii', offset, offset + length);
    return parseInt(str, 10);
}

/**
 * Parse 8-byte integer from text
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {bigint|number}
 */
function int8Recv(data, offset, length) {
    const str = data.toString('ascii', offset, offset + length);
    const num = parseInt(str, 10);
    // Return as BigInt only if outside safe integer range
    if (num > Number.MAX_SAFE_INTEGER || num < Number.MIN_SAFE_INTEGER) {
        return BigInt(str);
    }
    return num;
}

/**
 * Parse 4-byte float from text
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {number}
 */
function float4Recv(data, offset, length) {
    const str = data.toString('ascii', offset, offset + length);
    return parseFloat(str);
}

/**
 * Parse 8-byte float from text
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {number}
 */
function float8Recv(data, offset, length) {
    const str = data.toString('ascii', offset, offset + length);
    return parseFloat(str);
}

/**
 * Parse numeric/decimal from text
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {number|string}
 */
function numericIn(data, offset, length) {
    const str = data.toString('ascii', offset, offset + length);
    const num = parseFloat(str);
    // Return as string if precision would be lost
    if (str.includes('.') && str.length > 15) {
        return str;
    }
    return num;
}

/**
 * Parse interval from text
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {string}
 */
function intervalRecv(data, offset, length) {
    return data.toString('ascii', offset, offset + length);
}

/**
 * Parse date from text (YYYY-MM-DD)
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {Date}
 */
function dateIn(data, offset, length) {
    const str = data.toString('ascii', offset, offset + length);
    try {
        const year = parseInt(str.substring(0, 4), 10);
        const month = parseInt(str.substring(5, 7), 10) - 1;
        const day = parseInt(str.substring(8, 10), 10);
        return new Date(Date.UTC(year, month, day));
    } catch (e) {
        return new Date(0);
    }
}

/**
 * Parse time from text (HH:MM:SS.ffffff)
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {{hours: number, minutes: number, seconds: number, microseconds: number, toString: Function}}
 */
function timeIn(data, offset, length) {
    const str = data.toString('ascii', offset, offset + length);
    const hour = parseInt(str.substring(0, 2), 10);
    const minute = parseInt(str.substring(3, 5), 10);
    const secStr = str.substring(6);
    const secParts = secStr.split('.');
    const seconds = parseInt(secParts[0], 10);
    const microseconds = secParts.length > 1 ?
        parseInt(secParts[1].padEnd(6, '0').substring(0, 6), 10) : 0;

    return {
        hours: hour,
        minutes: minute,
        seconds,
        microseconds,
        toString() {
            const hh = String(hour).padStart(2, '0');
            const mm = String(minute).padStart(2, '0');
            const ss = String(seconds).padStart(2, '0');
            if (microseconds > 0) {
                const us = String(microseconds).padStart(6, '0');
                return `${hh}:${mm}:${ss}.${us}`;
            }
            return `${hh}:${mm}:${ss}`;
        }
    };
}

/**
 * Parse timestamp from text
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {Date}
 */
function timestampIn(data, offset, length) {
    const str = data.toString('ascii', offset, offset + length);
    return new Date(str);
}

/**
 * Parse timestamp from 8-byte float (seconds since 2000-01-01)
 * @param {Buffer} data 
 * @param {number} offset 
 * @returns {Date}
 */
function timestampRecvFloat(data, offset) {
    const EPOCH_SECONDS = 946684800.0; // 2000-01-01 UTC in Unix seconds
    const seconds = data.readDoubleLE(offset);
    return new Date((EPOCH_SECONDS + seconds) * 1000);
}

/**
 * Parse UUID from 16 bytes
 * @param {Buffer} data 
 * @param {number} offset 
 * @param {number} length 
 * @returns {string}
 */
function uuidRecv(data, offset, length) {
    if (length !== 16) {
        throw new Error('UUID must be exactly 16 bytes');
    }
    const hex = data.toString('hex', offset, offset + 16);
    return `${hex.substr(0, 8)}-${hex.substr(8, 4)}-${hex.substr(12, 4)}-${hex.substr(16, 4)}-${hex.substr(20, 12)}`;
}

module.exports = {
    CLIENT_ENCODING,
    CHAR_VARCHAR_ENCODING,
    textRecv,
    textRecvLatin1,
    byteaRecv,
    boolRecv,
    byteRecv,
    int2Recv,
    int4Recv,
    int8Recv,
    float4Recv,
    float8Recv,
    numericIn,
    intervalRecv,
    dateIn,
    timeIn,
    timestampIn,
    timestampRecvFloat,
    uuidRecv
};
