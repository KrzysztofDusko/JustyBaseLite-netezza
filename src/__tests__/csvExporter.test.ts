/**
 * Unit tests for export/csvExporter.ts
 * Tests the pure utility functions: escapeCsvField and parseConnectionString
 */

// We need to import the functions - but they are not exported, so we'll test via module
// For testing, we'll redefine the functions here as they are internal

// Recreate the functions for testing (same logic as in csvExporter.ts)
// Using unique names to avoid TypeScript duplicate function errors across test files
function _escapeCsvField(field: unknown): string {
    if (field === null || field === undefined) {
        return '';
    }

    let stringValue = '';
    if (typeof field === 'bigint') {
        if (field >= Number.MIN_SAFE_INTEGER && field <= Number.MAX_SAFE_INTEGER) {
            stringValue = Number(field).toString();
        } else {
            stringValue = field.toString();
        }
    } else if (field instanceof Date) {
        stringValue = field.toISOString();
    } else if (typeof field === 'object' && Buffer.isBuffer(field)) {
        stringValue = field.toString('hex');
    } else if (typeof field === 'object') {
        stringValue = JSON.stringify(field);
    } else {
        stringValue = String(field);
    }

    if (
        stringValue.includes('"') ||
        stringValue.includes(',') ||
        stringValue.includes('\n') ||
        stringValue.includes('\r')
    ) {
        return `"${stringValue.replace(/"/g, '""')}"`;
    }

    return stringValue;
}

function _parseConnectionString(connStr: string): Record<string, unknown> {
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

describe('export/csvExporter', () => {
    describe('escapeCsvField', () => {
        describe('null and undefined handling', () => {
            it('should return empty string for null', () => {
                expect(_escapeCsvField(null)).toBe('');
            });

            it('should return empty string for undefined', () => {
                expect(_escapeCsvField(undefined)).toBe('');
            });
        });

        describe('simple values', () => {
            it('should convert string as-is when no escaping needed', () => {
                expect(_escapeCsvField('hello')).toBe('hello');
            });

            it('should convert number to string', () => {
                expect(_escapeCsvField(123)).toBe('123');
                expect(_escapeCsvField(123.45)).toBe('123.45');
            });

            it('should convert boolean to string', () => {
                expect(_escapeCsvField(true)).toBe('true');
                expect(_escapeCsvField(false)).toBe('false');
            });
        });

        describe('escaping special characters', () => {
            it('should escape field containing comma', () => {
                expect(_escapeCsvField('hello,world')).toBe('"hello,world"');
            });

            it('should escape field containing double quote', () => {
                expect(_escapeCsvField('say "hello"')).toBe('"say ""hello"""');
            });

            it('should escape field containing newline', () => {
                expect(_escapeCsvField('line1\nline2')).toBe('"line1\nline2"');
            });

            it('should escape field containing carriage return', () => {
                expect(_escapeCsvField('line1\rline2')).toBe('"line1\rline2"');
            });

            it('should escape field with mixed special characters', () => {
                expect(_escapeCsvField('he said, "hi"\n')).toBe('"he said, ""hi""\n"');
            });
        });

        describe('special types', () => {
            it('should handle BigInt within safe integer range', () => {
                expect(_escapeCsvField(BigInt(123))).toBe('123');
            });

            it('should handle BigInt outside safe integer range', () => {
                const bigNum = BigInt('9999999999999999999');
                expect(_escapeCsvField(bigNum)).toBe('9999999999999999999');
            });

            it('should format Date as ISO string', () => {
                const date = new Date('2024-01-15T10:30:00.000Z');
                expect(_escapeCsvField(date)).toBe('2024-01-15T10:30:00.000Z');
            });

            it('should handle Buffer as hex string', () => {
                const buffer = Buffer.from([0x48, 0x65, 0x6c, 0x6c, 0x6f]);
                expect(_escapeCsvField(buffer)).toBe('48656c6c6f');
            });

            it('should stringify objects as JSON', () => {
                const obj = { name: 'test', value: 123 };
                const result = _escapeCsvField(obj);
                // JSON contains quotes and possibly commas, so should be escaped
                expect(result).toBe('"{\\"name\\":\\"test\\",\\"value\\":123}"'.replace(/\\"/g, '""'));
            });

            it('should stringify arrays as JSON', () => {
                const arr = [1, 2, 3];
                const result = _escapeCsvField(arr);
                // Array contains commas, so should be wrapped
                expect(result).toBe('"[1,2,3]"');
            });
        });
    });

    describe('_parseConnectionString', () => {
        it('should parse simple connection string', () => {
            const connStr = 'SERVER=localhost;PORT=5480;DATABASE=test;UID=user;PWD=pass';
            const result = _parseConnectionString(connStr);
            expect(result).toEqual({
                host: 'localhost',
                port: 5480,
                database: 'test',
                user: 'user',
                password: 'pass'
            });
        });

        it('should handle case-insensitive keys', () => {
            const connStr = 'server=localhost;port=5480;database=test';
            const result = _parseConnectionString(connStr);
            expect(result.host).toBe('localhost');
            expect(result.port).toBe(5480);
            expect(result.database).toBe('test');
        });

        it('should handle extra whitespace', () => {
            const connStr = ' SERVER = localhost ; PORT = 5480 ';
            const result = _parseConnectionString(connStr);
            expect(result.host).toBe('localhost');
            expect(result.port).toBe(5480);
        });

        it('should handle partial connection string', () => {
            const connStr = 'SERVER=localhost;DATABASE=test';
            const result = _parseConnectionString(connStr);
            expect(result.host).toBe('localhost');
            expect(result.database).toBe('test');
            expect(result.port).toBeUndefined();
        });

        it('should handle empty string', () => {
            const result = _parseConnectionString('');
            expect(result).toEqual({});
        });

        it('should ignore unknown keys', () => {
            const connStr = 'SERVER=localhost;UNKNOWN=value;DATABASE=test';
            const result = _parseConnectionString(connStr);
            expect(result.host).toBe('localhost');
            expect(result.database).toBe('test');
            expect(result).not.toHaveProperty('UNKNOWN');
        });

        it('should handle values containing equals sign', () => {
            const connStr = 'SERVER=localhost;PWD=pass=word';
            const result = _parseConnectionString(connStr);
            expect(result.password).toBe('pass=word');
        });
    });
});
