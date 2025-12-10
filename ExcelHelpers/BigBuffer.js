const { Buffer } = require('buffer');

class BigBuffer {
    constructor(chunkSize = 65536) {
        this.chunkSize = chunkSize;
        this.chunks = [];
        this.currentBuffer = Buffer.alloc(chunkSize);
        this.cursor = 0;
    }

    _ensureCapacity(size) {
        if (this.cursor + size > this.chunkSize) {
            this._flush();
        }
    }

    _flush() {
        if (this.cursor > 0) {
            this.chunks.push(this.currentBuffer.subarray(0, this.cursor));
            this.currentBuffer = Buffer.alloc(this.chunkSize);
            this.cursor = 0;
        }
    }

    write(buffer) {
        let len = buffer.length;
        let offset = 0;

        while (len > 0) {
            let available = this.chunkSize - this.cursor;
            if (available === 0) {
                this._flush();
                available = this.chunkSize;
            }

            let toWrite = Math.min(len, available);
            buffer.copy(this.currentBuffer, this.cursor, offset, offset + toWrite);

            this.cursor += toWrite;
            offset += toWrite;
            len -= toWrite;
        }
    }

    writeByte(val) {
        this._ensureCapacity(1);
        this.currentBuffer[this.cursor] = val;
        this.cursor++;
    }

    writeInt32LE(val) {
        this._ensureCapacity(4);
        this.currentBuffer.writeInt32LE(val, this.cursor);
        this.cursor += 4;
    }

    writeDoubleLE(val) {
        this._ensureCapacity(8);
        this.currentBuffer.writeDoubleLE(val, this.cursor);
        this.cursor += 8;
    }

    /**
     * Write a string directly to buffer without creating intermediate Buffer object.
     * More efficient for hot loops where many small strings are written.
     */
    writeString(str) {
        const byteLength = Buffer.byteLength(str, 'utf8');

        // If it fits in current buffer, write directly
        if (this.cursor + byteLength <= this.chunkSize) {
            this.cursor += this.currentBuffer.write(str, this.cursor, 'utf8');
            return;
        }

        // Otherwise, flush and handle potentially large string
        this._flush();

        // If string is larger than chunk size, write as separate chunk
        if (byteLength > this.chunkSize) {
            this.chunks.push(Buffer.from(str, 'utf8'));
        } else {
            this.cursor += this.currentBuffer.write(str, this.cursor, 'utf8');
        }
    }

    /**
     * Write a string as UTF-16LE (2 bytes per character).
     * Used for XLSB binary format strings.
     */
    writeUtf16LE(str) {
        const byteLength = str.length * 2;

        // If it fits in current buffer, write directly
        if (this.cursor + byteLength <= this.chunkSize) {
            for (let i = 0; i < str.length; i++) {
                const code = str.charCodeAt(i);
                this.currentBuffer[this.cursor++] = code & 0xFF;
                this.currentBuffer[this.cursor++] = (code >> 8) & 0xFF;
            }
            return;
        }

        // Otherwise use Buffer.from for large strings
        this._flush();
        if (byteLength > this.chunkSize) {
            this.chunks.push(Buffer.from(str, 'utf16le'));
        } else {
            for (let i = 0; i < str.length; i++) {
                const code = str.charCodeAt(i);
                this.currentBuffer[this.cursor++] = code & 0xFF;
                this.currentBuffer[this.cursor++] = (code >> 8) & 0xFF;
            }
        }
    }

    getChunks() {
        if (this.cursor > 0) {
            this.chunks.push(this.currentBuffer.subarray(0, this.cursor));
            // Reset so we don't push again if called multiple times, or create new buffer if we plan to continue
            this.currentBuffer = Buffer.alloc(this.chunkSize);
            this.cursor = 0;
        }
        return this.chunks;
    }

    // Clear for reuse
    reset() {
        this.chunks = [];
        this.cursor = 0;
        // reuse current buffer
    }
}

module.exports = BigBuffer;
