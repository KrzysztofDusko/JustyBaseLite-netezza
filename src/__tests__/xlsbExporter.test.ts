/**
 * Unit tests for export/xlsbExporter.ts
 * Tests the pure utility functions for numeric string conversion
 */

// Recreate the functions for testing (same logic as in xlsbExporter.ts)
// Using unique names to avoid TypeScript duplicate function errors across test files
function _convertToNumberIfNumericString(val: unknown): unknown {
    if (typeof val === 'string' && val.length > 0) {
        // PRECISION SAFETY: Don't convert very long strings to numbers
        if (val.length > 15) {
            return val;
        }

        // LEADING ZERO SAFETY: Don't convert strings with leading zeros (e.g. "0123")
        // unless it is exactly "0" or a decimal starting with "0." (e.g. "0.123")
        if (/^-?0\d+/.test(val)) {
            return val;
        }

        // Check if it's a numeric string (including negatives and decimals)
        if (/^-?\d+(\.\d+)?$/.test(val)) {
            const num = parseFloat(val);
            if (Number.isFinite(num)) {
                return num;
            }
        }
    }
    return val;
}

function _convertRowNumericStrings(row: unknown[]): unknown[] {
    return row.map(_convertToNumberIfNumericString);
}

function _xlsb_parseConnectionString(connStr: string): Record<string, unknown> {
    const parts = connStr.split(';');
    const config: Record<string, unknown> = {};
    for (const part of parts) {
        const idx = part.indexOf('=');
        if (idx > 0) {
            const key = part.substring(0, idx).trim().toUpperCase();
            const value = part.substring(idx + 1).trim();
            if (key === 'SERVER') config.host = value;
            else if (key === 'PORT') config.port = parseInt(value);
            else if (key === 'DATABASE') config.database = value;
            else if (key === 'UID') config.user = value;
            else if (key === 'PWD') config.password = value;
        }
    }
    return config;
}

describe('export/xlsbExporter', () => {
    describe('convertToNumberIfNumericString', () => {
        describe('integer strings', () => {
            it('should convert positive integer string to number', () => {
                expect(_convertToNumberIfNumericString('123')).toBe(123);
            });

            it('should convert negative integer string to number', () => {
                expect(_convertToNumberIfNumericString('-456')).toBe(-456);
            });

            it('should convert zero string to number', () => {
                expect(_convertToNumberIfNumericString('0')).toBe(0);
            });

            it('should convert large integer string to number', () => {
                expect(_convertToNumberIfNumericString('9999999')).toBe(9999999);
            });

            it('should NOT convert very long integer string (precision safety)', () => {
                const longNum = '11111111111111111111'; // 20 chars
                expect(_convertToNumberIfNumericString(longNum)).toBe(longNum);
            });

            it('should NOT convert string with leading zeros (leading zero safety)', () => {
                expect(_convertToNumberIfNumericString('0123')).toBe('0123');
                expect(_convertToNumberIfNumericString('-0123')).toBe('-0123');
            });
        });

        describe('decimal strings', () => {
            it('should convert positive decimal string to number', () => {
                expect(_convertToNumberIfNumericString('123.45')).toBe(123.45);
            });

            it('should convert negative decimal string to number', () => {
                expect(_convertToNumberIfNumericString('-123.45')).toBe(-123.45);
            });

            it('should convert decimal starting with zero to number', () => {
                expect(_convertToNumberIfNumericString('0.123')).toBe(0.123);
            });
        });

        describe('non-numeric values', () => {
            it('should return non-numeric string unchanged', () => {
                expect(_convertToNumberIfNumericString('hello')).toBe('hello');
            });

            it('should return mixed alphanumeric string unchanged', () => {
                expect(_convertToNumberIfNumericString('abc123')).toBe('abc123');
                expect(_convertToNumberIfNumericString('123abc')).toBe('123abc');
            });

            it('should return string with spaces unchanged', () => {
                expect(_convertToNumberIfNumericString(' 123 ')).toBe(' 123 ');
            });

            it('should return empty string unchanged', () => {
                expect(_convertToNumberIfNumericString('')).toBe('');
            });

            it('should return number unchanged', () => {
                expect(_convertToNumberIfNumericString(42)).toBe(42);
            });

            it('should return null unchanged', () => {
                expect(_convertToNumberIfNumericString(null)).toBe(null);
            });

            it('should return undefined unchanged', () => {
                expect(_convertToNumberIfNumericString(undefined)).toBe(undefined);
            });

            it('should return object unchanged', () => {
                const obj = { a: 1 };
                expect(_convertToNumberIfNumericString(obj)).toBe(obj);
            });

            it('should return array unchanged', () => {
                const arr = [1, 2, 3];
                expect(_convertToNumberIfNumericString(arr)).toBe(arr);
            });

            it('should return boolean unchanged', () => {
                expect(_convertToNumberIfNumericString(true)).toBe(true);
                expect(_convertToNumberIfNumericString(false)).toBe(false);
            });
        });

        describe('edge cases', () => {
            it('should not convert string with leading zeros (except after decimal)', () => {
                // '0123' should be preserved as string to keep leading zero
                const result = _convertToNumberIfNumericString('0123');
                expect(result).toBe('0123');
            });

            it('should not convert string with plus sign', () => {
                expect(_convertToNumberIfNumericString('+123')).toBe('+123');
            });

            it('should not convert scientific notation', () => {
                expect(_convertToNumberIfNumericString('1e5')).toBe('1e5');
            });

            it('should not convert string with only minus sign', () => {
                expect(_convertToNumberIfNumericString('-')).toBe('-');
            });

            it('should not convert string with only decimal point', () => {
                expect(_convertToNumberIfNumericString('.')).toBe('.');
            });

            it('should not convert string without digits before decimal', () => {
                expect(_convertToNumberIfNumericString('.5')).toBe('.5');
            });
        });
    });

    describe('_convertRowNumericStrings', () => {
        it('should convert all numeric strings in a row', () => {
            const row = ['123', '45.67', 'hello', null, 42];
            const result = _convertRowNumericStrings(row);
            expect(result).toEqual([123, 45.67, 'hello', null, 42]);
        });

        it('should handle empty row', () => {
            expect(_convertRowNumericStrings([])).toEqual([]);
        });

        it('should handle row with no numeric strings', () => {
            const row = ['hello', 'world', null, true];
            const result = _convertRowNumericStrings(row);
            expect(result).toEqual(['hello', 'world', null, true]);
        });

        it('should handle row with all numeric strings', () => {
            const row = ['1', '2', '3.14', '-5'];
            const result = _convertRowNumericStrings(row);
            expect(result).toEqual([1, 2, 3.14, -5]);
        });
    });

    describe('_xlsb_parseConnectionString', () => {
        it('should parse complete connection string', () => {
            const connStr = 'SERVER=db.example.com;PORT=5480;DATABASE=mydb;UID=admin;PWD=secret';
            const result = _xlsb_parseConnectionString(connStr);
            expect(result).toEqual({
                host: 'db.example.com',
                port: 5480,
                database: 'mydb',
                user: 'admin',
                password: 'secret'
            });
        });

        it('should handle IP address as server', () => {
            const connStr = 'SERVER=192.168.1.100;PORT=5480;DATABASE=test';
            const result = _xlsb_parseConnectionString(connStr);
            expect(result.host).toBe('192.168.1.100');
        });

        it('should handle missing components', () => {
            const connStr = 'SERVER=localhost';
            const result = _xlsb_parseConnectionString(connStr);
            expect(result.host).toBe('localhost');
            expect(result.port).toBeUndefined();
            expect(result.database).toBeUndefined();
        });
    });
});
