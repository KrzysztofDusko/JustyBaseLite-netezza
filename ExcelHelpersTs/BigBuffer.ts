import { Buffer } from 'buffer';

export class BigBuffer {
    private chunkSize: number;
    private chunks: Buffer[];
    private currentBuffer: Buffer;
    private cursor: number;

    constructor(chunkSize: number = 65536) {
        this.chunkSize = chunkSize;
        this.chunks = [];
        this.currentBuffer = Buffer.alloc(chunkSize);
        this.cursor = 0;
    }

    private _ensureCapacity(size: number): void {
        if (this.cursor + size > this.chunkSize) {
            this._flush();
        }
    }

    private _flush(): void {
        if (this.cursor > 0) {
            this.chunks.push(this.currentBuffer.subarray(0, this.cursor));
            this.currentBuffer = Buffer.alloc(this.chunkSize);
            this.cursor = 0;
        }
    }

    write(buffer: Buffer): void {
        let len = buffer.length;
        let offset = 0;

        while (len > 0) {
            let available = this.chunkSize - this.cursor;
            if (available === 0) {
                this._flush();
                available = this.chunkSize;
            }

            const toWrite = Math.min(len, available);
            buffer.copy(this.currentBuffer, this.cursor, offset, offset + toWrite);

            this.cursor += toWrite;
            offset += toWrite;
            len -= toWrite;
        }
    }

    writeByte(val: number): void {
        this._ensureCapacity(1);
        this.currentBuffer[this.cursor] = val;
        this.cursor++;
    }

    writeInt32LE(val: number): void {
        this._ensureCapacity(4);
        this.currentBuffer.writeInt32LE(val, this.cursor);
        this.cursor += 4;
    }

    writeDoubleLE(val: number): void {
        this._ensureCapacity(8);
        this.currentBuffer.writeDoubleLE(val, this.cursor);
        this.cursor += 8;
    }

    writeString(str: string): void {
        const byteLength = Buffer.byteLength(str, 'utf8');

        if (this.cursor + byteLength <= this.chunkSize) {
            this.cursor += this.currentBuffer.write(str, this.cursor, 'utf8');
            return;
        }

        this._flush();

        if (byteLength > this.chunkSize) {
            this.chunks.push(Buffer.from(str, 'utf8'));
        } else {
            this.cursor += this.currentBuffer.write(str, this.cursor, 'utf8');
        }
    }

    writeUtf16LE(str: string): void {
        const byteLength = str.length * 2;

        if (this.cursor + byteLength <= this.chunkSize) {
            for (let i = 0; i < str.length; i++) {
                const code = str.charCodeAt(i);
                this.currentBuffer[this.cursor++] = code & 0xFF;
                this.currentBuffer[this.cursor++] = (code >> 8) & 0xFF;
            }
            return;
        }

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

    getChunks(): Buffer[] {
        if (this.cursor > 0) {
            this.chunks.push(this.currentBuffer.subarray(0, this.cursor));
            this.currentBuffer = Buffer.alloc(this.chunkSize);
            this.cursor = 0;
        }
        return this.chunks;
    }

    reset(): void {
        this.chunks = [];
        this.cursor = 0;
    }
}

export default BigBuffer;
