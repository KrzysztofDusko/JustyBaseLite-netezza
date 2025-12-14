const net = require('net');
const EventEmitter = require('events');
const Handshake = require('./Handshake');
const PGUtil = require('./utils/PGUtil');
const NzCommand = require('./NzCommand');
const NzDataReader = require('./NzDataReader');
const DbosTupleDesc = require('./DbosTupleDesc');
const { BackendMessageCode, NzType, ExtabSock } = require('./protocol/constants');
const TypeConversions = require('./types/TypeConversions');
const NzConnectionHelpers = require('./utils/NzConnectionHelpers');
const debug = require('debug')('nz:connection');
const fs = require('fs');

class NzConnection extends EventEmitter {
    constructor(config) {
        super();
        this.config = config;
        this._socket = null;
        this._stream = null;
        this._backendProcessId = 0;
        this._backendSecretKey = 0;
        this._commandNumber = -1;
        this._connected = false;
        this._rowDescription = null;
        this._rows = [];
        this._tupdesc = new DbosTupleDesc();
        this._tmpBuffer = Buffer.alloc(65536);

        // BUFFERED READ OPTIMIZATION
        // We keep a 64KB internal buffer to satisfy small reads (ints, headers) continuously
        // without awaiting promises/stream events for every byte.
        this._intBuf = Buffer.allocUnsafe(65536);
        this._intBufStart = 0;
        this._intBufEnd = 0;

        this.commandTimeout = 30; // Seconds
        this._executing = false;
    }

    async connect() {
        return new Promise((resolve, reject) => {
            if (!this.config.host) return reject(new Error("Host is required"));
            this._socket = new net.Socket();
            this._socket.connect(this.config.port || 5480, this.config.host, async () => {
                debug('Socket connected');
                this._socket.setNoDelay(true);
                this._stream = this._socket;
                const handshake = new Handshake(this._socket, this._stream, this.config.host, this.config);
                try {
                    this._stream = await handshake.startup(this.config.database, this.config.user, this.config.password);
                    this._connected = true;
                    this._backendProcessId = handshake.backendProcessId;
                    this._backendSecretKey = handshake.backendSecretKey;
                    resolve();
                } catch (err) {
                    this._socket.destroy();
                    reject(err);
                }
            });
            this._socket.on('error', (err) => {
                debug('Socket error', err);
                if (!this._connected) reject(err);
                else this.emit('error', err);
            });
            this._socket.on('close', () => {
                debug('Socket closed');
                this._connected = false;
                this.emit('close');
            });
        });
    }

    /**
     * Cancel the current running query
     * Opens a new connection to send CancelRequest
     */
    async cancel() {
        if (!this._backendProcessId || !this._backendSecretKey) return;

        return new Promise((resolve, reject) => {
            const socket = new net.Socket();
            socket.on('error', (err) => {
                debug('Cancel socket error', err);
                reject(err);
            });
            socket.connect(this.config.port || 5480, this.config.host, () => {
                const buf = Buffer.alloc(16);
                PGUtil.writeInt32(buf, 16, 0); // Length
                PGUtil.writeInt32(buf, 80877102, 4); // CancelRequest code (1234.5678)
                PGUtil.writeInt32(buf, this._backendProcessId, 8);
                PGUtil.writeInt32(buf, this._backendSecretKey, 12);

                socket.write(buf, () => {
                    // Wait for server to close connection (EOF) to ensure request is processed
                });

                socket.on('close', resolve);
                socket.on('end', resolve);
                // Also handle error if connection drops unexpectedly
                socket.on('error', (e) => {
                    debug('Cancel socket error during wait', e);
                    resolve();
                });
            });
        });
    }

    close() {
        if (this._socket) {
            this._socket.end();
            this._socket.destroy();
        }
    }

    createCommand(sql) {
        const cmd = new NzCommand(this);
        cmd.commandText = sql;
        return cmd;
    }

    async beginTransaction() {
        const cmd = this.createCommand("BEGIN");
        await this.execute(cmd);
    }

    async commit() {
        const cmd = this.createCommand("COMMIT");
        await this.execute(cmd);
    }

    async rollback() {
        const cmd = this.createCommand("ROLLBACK");
        await this.execute(cmd);
    }

    // New Optimized Read with Buffering
    async _readBytes(n) {
        // Fast path: we have enough data in buffer
        if (this._intBufEnd - this._intBufStart >= n) {
            const result = Buffer.from(this._intBuf.subarray(this._intBufStart, this._intBufStart + n));
            this._intBufStart += n;
            return result;
        }

        // Slow path: Need more data
        return this._readBytesSlow(n);
    }

    async _readBytesSlow(n) {
        // If request is larger than our entire buffer, bypass buffer to avoid copy overhead
        // But first, flush whatever we have in buffer
        if (n > this._intBuf.length) {
            const chunks = [];
            const available = this._intBufEnd - this._intBufStart;
            if (available > 0) {
                chunks.push(Buffer.from(this._intBuf.subarray(this._intBufStart, this._intBufEnd)));
                this._intBufStart = 0;
                this._intBufEnd = 0;
            }

            let remaining = n - available;
            while (remaining > 0) {
                const chunk = this._stream.read(); // Read whatever is available
                if (chunk !== null) {
                    if (chunk.length <= remaining) {
                        chunks.push(chunk);
                        remaining -= chunk.length;
                    } else {
                        // Got more than needed
                        chunks.push(chunk.slice(0, remaining));
                        // Put extra back
                        this._feedBuffer(chunk.slice(remaining));
                        remaining = 0;
                    }
                } else {
                    await this._waitForReadable();
                }
            }
            return Buffer.concat(chunks, n);
        }

        // Normal case: n fits in buffer.
        // Compact buffer if needed
        if (this._intBuf.length - this._intBufStart < n) {
            // Not enough space at end, compact
            const len = this._intBufEnd - this._intBufStart;
            this._intBuf.copy(this._intBuf, 0, this._intBufStart, this._intBufEnd);
            this._intBufStart = 0;
            this._intBufEnd = len;
        }

        // Fill buffer until we have enough
        while (this._intBufEnd - this._intBufStart < n) {
            const chunk = this._stream.read(); // Read whatever is available
            if (chunk !== null) {
                // Determine how much we can copy
                const space = this._intBuf.length - this._intBufEnd;
                if (chunk.length <= space) {
                    chunk.copy(this._intBuf, this._intBufEnd);
                    this._intBufEnd += chunk.length;
                } else {
                    // Chunk bigger than space? (Should be rare due to compaction/size check above, but loop logic safety)
                    chunk.copy(this._intBuf, this._intBufEnd, 0, space);
                    this._intBufEnd += space;
                    // What to do with the REST of the chunk? 
                    // We unshift it back to the stream so next read picks it up.
                    // IMPORTANT: socket.unshift puts it back to internal stream buffer.
                    this._stream.unshift(chunk.slice(space));
                }
            } else {
                await this._waitForReadable();
            }
        }

        const result = Buffer.from(this._intBuf.subarray(this._intBufStart, this._intBufStart + n));
        this._intBufStart += n;
        return result;
    }

    async _waitForReadable() {
        if (!this._socket || this._socket.destroyed) throw new Error('Socket closed or destroyed during read');
        return new Promise((resolve, reject) => {
            const cleanup = () => {
                this._stream.removeListener('readable', onReadable);
                this._stream.removeListener('close', onClose);
                this._stream.removeListener('error', onError);
                this._stream.removeListener('end', onClose);
            };
            const onReadable = () => { cleanup(); resolve(); };
            const onClose = () => { cleanup(); reject(new Error('Socket closed/ended during read')); };
            const onError = (err) => { cleanup(); reject(err); };

            this._stream.once('readable', onReadable);
            this._stream.once('close', onClose);
            this._stream.once('end', onClose);
            this._stream.once('error', onError);
        });
    }

    _feedBuffer(buf) {
        // Helper to put data back into our buffer if we over-read in "bypass" mode
        // Simplest is to copy into start? Or just unshift to stream.
        // Unshifting to stream preserves order for subsequent reads.
        this._stream.unshift(buf);
    }

    // INLINE INTEGER READING - avoids buffer allocation for each int
    async _readInt32() {
        await this._ensureBufferData(4);
        const val = this._intBuf.readInt32BE(this._intBufStart);
        this._intBufStart += 4;
        return val;
    }

    async _readInt16() {
        await this._ensureBufferData(2);
        const val = this._intBuf.readInt16BE(this._intBufStart);
        this._intBufStart += 2;
        return val;
    }

    async _readByte() {
        await this._ensureBufferData(1);
        const val = this._intBuf[this._intBufStart];
        this._intBufStart += 1;
        return val;
    }

    async _ensureBufferData(n) {
        if (this._intBufEnd - this._intBufStart >= n) return; // Already have enough

        // Need more data - compact and fill
        if (this._intBuf.length - this._intBufStart < n) {
            const len = this._intBufEnd - this._intBufStart;
            this._intBuf.copy(this._intBuf, 0, this._intBufStart, this._intBufEnd);
            this._intBufStart = 0;
            this._intBufEnd = len;
        }

        while (this._intBufEnd - this._intBufStart < n) {
            const chunk = this._stream.read();
            if (chunk !== null) {
                const space = this._intBuf.length - this._intBufEnd;
                if (chunk.length <= space) {
                    chunk.copy(this._intBuf, this._intBufEnd);
                    this._intBufEnd += chunk.length;
                } else {
                    chunk.copy(this._intBuf, this._intBufEnd, 0, space);
                    this._intBufEnd += space;
                    this._stream.unshift(chunk.slice(space));
                }
            } else {
                await this._waitForReadable();
            }
        }
    }

    async execute(command, bufferOnly = false) {
        const timeoutSeconds = command.commandTimeout;
        if (!timeoutSeconds || timeoutSeconds <= 0) {
            return this._doExecute(command, bufferOnly);
        }

        let timer;
        const execPromise = this._doExecute(command, bufferOnly);

        const timeoutPromise = new Promise((resolve, reject) => {
            timer = setTimeout(async () => {
                debug('Command timeout triggered');
                try {
                    await this.cancel();
                    reject(new Error('Command execution timeout'));
                } catch (e) {
                    reject(new Error('Command execution timeout (Cancel failed: ' + e.message + ')'));
                }
            }, timeoutSeconds * 1000);
        });

        try {
            return await Promise.race([execPromise, timeoutPromise]);
        } catch (err) {
            if (err.message && err.message.includes('Command execution timeout')) {
                execPromise.catch(() => { });
            }
            throw err;
        } finally {
            clearTimeout(timer);
        }
    }

    async _doExecute(command) {
        if (this._executing) {
            throw new Error("Connection is already executing a command");
        }
        this._executing = true;
        try {
            debug('Executing:', command.commandText);
            this._preExecution(command.commandText);

            // Consume everything
            let error = null;
            for await (const msg of this._responseGenerator(command)) {
                if (msg.type === 'ErrorResponse') {
                    error = new Error('Netezza Error: ' + msg.message);
                }
                // Continue draining
            }
            if (error) {
                throw error;
            }
            return true;
        } finally {
            this._executing = false;
        }
    }

    async executeReader(command) {
        const timeoutSeconds = command.commandTimeout || this.commandTimeout;
        if (!timeoutSeconds || timeoutSeconds <= 0) {
            return this._doExecuteReader(command);
        }

        let timer;
        const execPromise = this._doExecuteReader(command);

        const timeoutPromise = new Promise((resolve, reject) => {
            timer = setTimeout(async () => {
                debug('Command timeout triggered');
                try {
                    await this.cancel();
                    reject(new Error('Command execution timeout'));
                } catch (e) {
                    reject(new Error('Command execution timeout (Cancel failed: ' + e.message + ')'));
                }
            }, timeoutSeconds * 1000);
        });

        try {
            return await Promise.race([execPromise, timeoutPromise]);
        } catch (err) {
            if (err.message && err.message.includes('Command execution timeout')) {
                execPromise.catch(() => { });
            }
            throw err;
        } finally {
            clearTimeout(timer);
        }
    }

    async _doExecuteReader(command) {
        if (this._executing) {
            throw new Error("Connection is already executing a command");
        }
        this._executing = true;

        try {
            debug('Executing Reader:', command.commandText);
            this._preExecution(command.commandText);

            const generator = this._responseGenerator(command);
            let columns = [];

            // Pre-fetch until RowDescription or completion
            // We need to loop because we might get Notices or other ignored messages (though generator filters some)
            let item = await generator.next();
            let error = null;
            let initialNextItem = null;

            while (!item.done) {
                const val = item.value;
                // Handle text format RowDescription - has full column metadata
                if (val.type === 'RowDescription') {
                    columns = val.columns;
                    // Don't break yet - continue to see if RowDescriptionStandard follows
                } else if (val.type === 'RowDescriptionStandard') {
                    // For binary format, get columns from command's PreparedStatement if we don't have them
                    const desc = val.desc;
                    if (desc && desc.numFields > 0 && columns.length === 0) {
                        const ps = command._preparedStatement;
                        if (ps && ps.description) {
                            columns = ps.description;
                        }
                    }
                    // Don't break - keep going to find actual DataRow or CommandComplete
                } else if (val.type === 'DataRow' || val.type === 'CommandComplete' || val.type === 'ReadyForQuery') {
                    // We've hit actual row data or end-of-result marker
                    // Put this back as the initial next item
                    if (!error && columns.length > 0) {
                        // Set initialItem to current val
                        initialNextItem = val;
                        break;
                    }
                } else if (val.type === 'ErrorResponse') {
                    // Capture error, continue draining until ReadyForQuery implies done
                    error = new Error('Netezza Error: ' + val.message);
                }

                item = await generator.next();
            }

            // If there was an error during pre-fetch, throw it now
            if (error) {
                this._executing = false;
                throw error;
            }

            return new NzDataReader(command, generator, columns, async () => {
                // Release callback
                this._executing = false;
            }, initialNextItem);
        } catch (e) {
            this._executing = false;
            throw e;
        }
    }

    _preExecution(query) {
        const queryBytes = Buffer.from(query, 'utf8');
        const buf = Buffer.allocUnsafe(1 + 4 + queryBytes.length + 1);
        buf[0] = 'P'.charCodeAt(0);
        if (this._commandNumber !== -1) {
            this._commandNumber++;
            buf.writeInt32BE(this._commandNumber, 1);
        } else {
            buf[1] = 0xFF; buf[2] = 0xFF; buf[3] = 0xFF; buf[4] = 0xFF;
        }
        if (this._commandNumber > 100000) this._commandNumber = 1;
        queryBytes.copy(buf, 5);
        buf[5 + queryBytes.length] = 0;
        this._stream.write(buf);
    }

    async * _responseGenerator(command) {
        this._rows = []; // Legacy property, discourage use
        this._rowDescription = null; // We might need to track this for parsing DataRow

        let completed = false;

        while (!completed) {
            let type = await this._readByte();
            // debug('Resp:', String.fromCharCode(type), '0x' + type.toString(16));

            while (type === 0) {
                // debug('Skipping 0 byte');
                type = await this._readByte();
            }

            // External Table handlers
            if (type === 'u'.charCodeAt(0)) { // 0x75 Export Start
                await this._handleExternalTableExportStart();
                continue;
            }

            if (type === 'U'.charCodeAt(0)) { // 0x55 Export Data
                await this._handleExternalTableExportData();
                continue;
            }

            if (type === 'l'.charCodeAt(0)) { // 0x6C Import Start
                await this._handleExternalTableImport();
                continue;
            }

            if (type === 'x'.charCodeAt(0)) { // 0x78 Abort
                await this._readBytes(4);
                debug("Error operation cancel (Ext Tbl)");
                continue;
            }

            if (type === 'e'.charCodeAt(0)) { // 0x65 Logs
                await this._readBytes(4);
                const len = PGUtil.readInt32(await this._readBytes(4));
                const logDir = (await this._readBytes(len - 1)).toString('utf8');
                await this._readBytes(1);

                const filenameBuf = [];
                let b = (await this._readBytes(1))[0];
                filenameBuf.push(b);
                while (true) {
                    b = (await this._readBytes(1))[0];
                    if (b === 0) break;
                    filenameBuf.push(b);
                }
                // const filename = Buffer.from(filenameBuf).toString('utf8');
                const logType = PGUtil.readInt32(await this._readBytes(4));
                // debug('ExtLog: Dir:', logDir, 'File:', filename, 'Type:', logType);

                await this._consumeExternalTableLogData();
                continue;
            }

            // Skip 4 bytes
            const skip4 = await this._readBytes(4);
            // debug('Skip4:', skip4.toString('hex'));

            if (type === BackendMessageCode.CommandComplete) {
                const len = await this._readInt32();
                const data = await this._readBytes(len);
                debug('CommandComplete:', data.toString('utf8'));
                yield { type: 'CommandComplete', text: data.toString('utf8') };
                continue;
            }

            if (type === BackendMessageCode.ReadyForQuery) {
                completed = true;
                yield { type: 'ReadyForQuery' };
                continue;
            }

            if (type === 0x4C) { completed = true; continue; }
            if (type === 0x30 || type === 0x41) { continue; }

            if (type === 0x50) {
                const len = await this._readInt32();
                await this._readBytes(len);
                continue;
            }

            if (type === BackendMessageCode.ErrorResponse) {
                const len = await this._readInt32();
                const data = await this._readBytes(len);
                // Yield error instead of throw to allow consumer to drain
                yield { type: 'ErrorResponse', message: data.toString('utf8') };
                continue;
            }

            if (type === BackendMessageCode.RowDescription) {
                const len = await this._readInt32();
                const data = await this._readBytes(len);
                this._parseRowDescription(data, command);
                // debug('RowDescription:', this._rowDescription.length, 'columns');
                yield { type: 'RowDescription', columns: this._rowDescription };
                continue;
            }

            if (type === BackendMessageCode.DataRow) {
                const len = await this._readInt32();
                const data = await this._readBytes(len);
                const row = this._parseDataRow(data);
                yield { type: 'DataRow', row };
                continue;
            }

            if (type === BackendMessageCode.RowDescriptionStandard) {
                const len = await this._readInt32();
                const data = await this._readBytes(len);
                this._tupdesc.parse(data, command._preparedStatement);
                // debug('RowDescriptionStandard:', this._tupdesc.numFields, 'fields');
                // We might need to yield this too if we support standard rows
                yield { type: 'RowDescriptionStandard', desc: this._tupdesc };
                continue;
            }

            if (type === BackendMessageCode.RowStandard) {
                const row = await this._resReadDbosTuple(command);
                yield { type: 'DataRow', row };
                continue;
            }

            if (type === BackendMessageCode.NoticeResponse) {
                const len = await this._readInt32();
                const data = await this._readBytes(len);
                const message = data.toString('utf8').replace(/\0/g, '').trim();
                // debug('Notice:', message);
                this.emit('notice', { message });
                // We don't yield notices usually, just emit event
                continue;
            }

            debug('Unknown message:', '0x' + type.toString(16));
            try {
                const len = await this._readInt32();
                if (len > 0 && len < 10000000) await this._readBytes(len);
            } catch (e) { /* ignore */ }
        }
    }

    async _consumeExternalTableLogData() {
        while (true) {
            const lenBuf = await this._readBytes(4);
            const len = PGUtil.readInt32(lenBuf);
            if (len === 0) return; // EOF

            const data = await this._readBytes(len);
            debug('ExtLog Content:', data.toString('utf8'));
        }
    }

    async _handleExternalTableExportStart() {
        // Skip 4 bytes first (like C# IntepretReturnedByte does)
        await this._readBytes(4);
        // Skip 10 bytes (clientVersion, etc) + 16 bytes (Reserved)
        await this._readBytes(10);
        await this._readBytes(16);
        const len = PGUtil.readInt32(await this._readBytes(4));
        const filenameBuf = await this._readBytes(len);
        const filename = filenameBuf.toString('utf8');
        debug('ExternalTable Export Start. File:', filename);

        try {
            this._exportStream = fs.createWriteStream(filename);
            this._exportStream.on('error', (err) => {
                debug('Export Stream Error:', err);
                // We can't easily signal error to server here if protocol expects us to listen?
                // But we can close connection.
            });
            // Send status back: [0,0,0,0] (Success)
            const buf = Buffer.alloc(4);
            await new Promise(resolve => this._stream.write(buf, resolve));
        } catch (e) {
            debug('Error opening export file:', e);
            const buf = Buffer.alloc(4);
            PGUtil.writeInt32(buf, 1, 0); // Error?
            this._stream.write(buf);
        }
    }

    async _handleExternalTableExportData() {
        debug('Handle Export Data: Skipping 8 bytes...');
        // Skip 8 bytes (two 4-byte headers: generic + internal)
        const skip1 = await this._readBytes(4);
        debug('Skipped 4:', skip1.toString('hex'));
        const skip2 = await this._readBytes(4);
        debug('Skipped 4 (2):', skip2.toString('hex'));

        await this._consumeExternalTableData(this._exportStream);
        this._exportStream = null;
    }

    async _consumeExternalTableData(writeStream) {
        debug('Entering Consume Loop');
        while (true) {
            debug('Reading status...');
            const statusBuf = await this._readBytes(4);
            const status = PGUtil.readInt32(statusBuf);
            debug('ExtTab Status:', status);

            if (status === ExtabSock.DATA) {
                const numBytes = PGUtil.readInt32(await this._readBytes(4));
                debug('Block Length:', numBytes);
                const data = await this._readBytes(numBytes);
                if (writeStream) {
                    writeStream.write(data);
                }
            } else if (status === ExtabSock.DONE) {
                debug('ExternalTable Data Done');
                if (writeStream) {
                    await new Promise(resolve => {
                        debug('Waiting for writeStream finish...');
                        if (writeStream.writableFinished) {
                            debug('Stream already finished');
                            return resolve();
                        }
                        const timeout = setTimeout(() => {
                            debug('Stream finish timeout! Destroying...');
                            writeStream.destroy();
                            resolve();
                        }, 5000);

                        const onFinish = () => {
                            debug('Stream finished event');
                            clearTimeout(timeout);
                            cleanup();
                            resolve();
                        };
                        const onError = (err) => {
                            debug('Stream error on end:', err);
                            clearTimeout(timeout);
                            cleanup();
                            resolve();
                        };
                        const cleanup = () => {
                            writeStream.removeListener('finish', onFinish);
                            writeStream.removeListener('error', onError);
                        };
                        writeStream.on('finish', onFinish);
                        writeStream.on('error', onError);
                        writeStream.end();
                    });
                }
                return;
            } else if (status === ExtabSock.ERROR) {
                const len = PGUtil.readInt16(await this._readBytes(2));
                const msg = (await this._readBytes(len)).toString('utf8');
                debug('ExternalTable Data Error:', msg);
                if (writeStream) writeStream.end();
                // Should we throw? If we throw, we might break connection state
                // But protocol is at ERROR state.
                return;
            } else {
                debug('Unknown ExtTab Status:', status);
                if (writeStream) writeStream.end();
                return;
            }
        }
    }

    async _handleExternalTableImport() {
        // 'l' (Import Start)
        await this._readBytes(8); // Skip 4 bytes (message length) + 4 bytes (internal header)

        // Read filename (null terminated)
        const filenameBuf = [];
        let b = (await this._readBytes(1))[0];
        filenameBuf.push(b);
        while (b !== 0) {
            b = (await this._readBytes(1))[0];
            if (b !== 0) filenameBuf.push(b);
        }
        const filename = Buffer.from(filenameBuf).toString('utf8');
        debug('ExternalTable Import Start. File:', filename);

        const hostVersion = PGUtil.readInt32(await this._readBytes(4));
        debug('Host Version:', hostVersion);
        // Send clientVersion=1
        const clientVerBuf = Buffer.alloc(4);
        PGUtil.writeInt32(clientVerBuf, 1, 0);
        await new Promise(resolve => this._stream.write(clientVerBuf, resolve));
        debug('Sent Client Version');

        const format = PGUtil.readInt32(await this._readBytes(4));
        const bufSize = PGUtil.readInt32(await this._readBytes(4));

        debug('ExtTab Import Config:', { hostVersion, format, bufSize });

        // Stream file to server
        if (!fs.existsSync(filename)) {
            debug('Import file not found:', filename);
            // Protocol likely expects data or error.
            // If we send ERROR status?
            // Protocol flow: Client writes DATA blocks.
            // Send ERROR block?
            const errBuf = Buffer.alloc(4);
            PGUtil.writeInt32(errBuf, ExtabSock.ERROR, 0); // 2
            this._stream.write(errBuf);
            // Send error msg len + msg? 
            // C# XferTable catches Exception and logs. But protocol?
            // C# Send EXTAB_SOCK_DONE if error in loop?
            // I'll send DONE to be safe.
            // PGUtil.writeInt32(errBuf, ExtabSock.DONE, 0);
            return;
        }

        const readStream = fs.createReadStream(filename, { highWaterMark: bufSize || 65536 });

        // We must await the streaming to ensure loop waits
        await new Promise((resolve, reject) => {
            readStream.on('data', (chunk) => {
                const header = Buffer.alloc(8);
                PGUtil.writeInt32(header, ExtabSock.DATA, 0); // 1
                PGUtil.writeInt32(header, chunk.length, 4);
                this._stream.write(header);
                this._stream.write(chunk);
            });

            readStream.on('end', () => {
                debug('Import Stream End');
                // Send DONE
                const doneBuf = Buffer.alloc(4);
                PGUtil.writeInt32(doneBuf, ExtabSock.DONE, 0); // 3
                this._stream.write(doneBuf);
                resolve();
            });

            readStream.on('error', (err) => {
                debug('Import Stream Error:', err);
                const errBuf = Buffer.alloc(4);
                PGUtil.writeInt32(errBuf, ExtabSock.ERROR, 0); // 2
                this._stream.write(errBuf);
                // Also Send Len(2) + Msg?
                // Assuming standard Error structure
                const msg = err.message || 'Error';
                const lenBuf = Buffer.alloc(2);
                lenBuf.writeInt16BE(msg.length);
                this._stream.write(lenBuf);
                this._stream.write(Buffer.from(msg, 'utf8'));

                reject(err);
            });
        });
    }

    _parseRowDescription(data, command) {
        // Netezza format differs from PostgreSQL!
        // After name: typeOid(4) + typeSize(2) + typeMod(4) + format(1) = 11 bytes
        debug('parseRowDescription data length:', data.length, 'hex:', data.toString('hex').substring(0, 100));
        let offset = 0;
        if (data.length < 2) {
            debug('Data too short for row description');
            return;
        }
        const count = data.readInt16BE(offset); offset += 2;
        debug('Column count:', count);
        this._rowDescription = [];

        for (let i = 0; i < count && offset < data.length; i++) {
            // Find null terminator for column name
            const nameStart = offset;
            while (offset < data.length && data[offset] !== 0) offset++;
            const name = data.toString('utf8', nameStart, offset);
            offset++; // Skip null terminator

            if (offset + 11 > data.length) {
                debug('Not enough data for column', i, 'at offset', offset, 'need 11 more, have', data.length - offset);
                break;
            }

            // Netezza IHICUnpack format: typeOid(4) + typeSize(2) + typeMod(4) + format(1) = 11 bytes
            const typeOid = data.readInt32BE(offset); offset += 4;
            const typeLen = data.readInt16BE(offset); offset += 2;
            const typeMod = data.readInt32BE(offset); offset += 4;
            const format = data[offset]; offset += 1;

            this._rowDescription.push({ name, typeOid, typeLen, typeMod, format });
            debug('Column', i, ':', name, 'typeOid:', typeOid, 'typeLen:', typeLen);
        }

        if (command) command._preparedStatement = { description: this._rowDescription };
    }

    _parseDataRow(data) {
        // Netezza format: bitmap (numCols/8 bytes) + for each: vlen(4) + data(vlen-4)
        const numberOfCol = this._rowDescription.length;
        const bitmapLen = Math.ceil(numberOfCol / 8);
        let dataIdx = bitmapLen;
        const row = [];

        for (let columnNumber = 0; columnNumber < numberOfCol; columnNumber++) {
            const byteToTest = data[Math.floor(columnNumber / 8)];
            const positionInByte = 7 - (columnNumber % 8);
            const hasValue = (byteToTest & (1 << positionInByte)) !== 0;

            if (!hasValue) {
                row.push(null);
                continue;
            }

            const vlen = data.readInt32BE(dataIdx);
            dataIdx += 4;
            const actualLen = vlen - 4;

            if (actualLen <= 0) {
                row.push(null);
                continue;
            }

            const colDesc = this._rowDescription[columnNumber];
            const typeOid = colDesc?.typeOid;

            // Parse as string by default, can add type conversion later
            const value = data.toString('utf8', dataIdx, dataIdx + actualLen);

            if (typeOid === 1083) { // TIME
                row.push(TypeConversions.parseTimeString(value));
            } else {
                row.push(value);
            }
            dataIdx += actualLen;
        }
        return row;
    }

    async _resReadDbosTuple(command) {
        const numFields = this._tupdesc.numFields;
        await this._readBytes(4);
        let rowLength = PGUtil.readInt32(await this._readBytes(4));
        const data = await this._readBytes(rowLength);
        const row = new Array(numFields);
        for (let i = 0; i < numFields; i++) {
            if (this._columnIsNull(data, i)) { row[i] = null; continue; }
            const fieldData = this._cTableFieldAt(data, i);
            const fldType = this._tupdesc.fieldType[i];
            const fldLen = this._tupdesc.fieldSize[i];
            row[i] = this._parseFieldByType(fieldData, fldType, fldLen, i);
        }
        return row;
    }

    _parseFieldByType(fieldData, fldType, fldLen, fieldIdx) {
        switch (fldType) {
            case NzType.NzTypeChar: return fieldData.toString('latin1', 0, fldLen).trimEnd();
            case NzType.NzTypeNChar:
            case NzType.NzTypeNVarChar: {
                const cursize = fieldData.readInt16LE(0) - 2;
                return fieldData.toString('utf8', 2, 2 + cursize);
            }

            case NzType.NzTypeVarChar:
            case NzType.NzTypeVarFixedChar: {
                const s = fieldData.readInt16LE(0) - 2;
                return fieldData.toString('latin1', 2, 2 + s);
            }

            case NzType.NzTypeInt8: return fieldData.readBigInt64LE(0);
            case NzType.NzTypeInt: return fieldData.readInt32LE(0);
            case NzType.NzTypeInt2: return fieldData.readInt16LE(0);
            case NzType.NzTypeInt1: return fieldData.readInt8(0);
            case NzType.NzTypeDouble: return fieldData.readDoubleLE(0);
            case NzType.NzTypeFloat: return fieldData.readFloatLE(0);
            case NzType.NzTypeDate: return TypeConversions.toDateTimeFrom4Bytes(fieldData);
            case NzType.NzTypeTime: return TypeConversions.timeRecvFloat(fieldData);
            case NzType.NzTypeInterval: return TypeConversions.intervalRecvFloat(fieldData);
            case NzType.NzTypeTimeTz: return TypeConversions.timetzOutput(fieldData, fldLen);
            case NzType.NzTypeTimestamp: return TypeConversions.toDateTimeFrom8Bytes(fieldData);
            case NzType.NzTypeBool: return fieldData[0] === 0x01;
            case NzType.NzTypeNumeric: {
                const p = this._tupdesc.getFieldPrecision(fieldIdx);
                const s = this._tupdesc.getFieldScale(fieldIdx);
                const c = this._tupdesc.getNumericDigitCount(fieldIdx);
                return TypeConversions.getCsNumeric(fieldData, p, s, c);
            }
            default: return fieldData.toString('utf8', 0, fldLen);
        }
    }

    _columnIsNull(data, fieldLf) {
        if (!this._tupdesc.nullsAllowed) return false;
        const col = this._tupdesc.fieldPhysField[fieldLf];
        const byte = data[2 + Math.floor(col / 8)];
        return (byte & (1 << (col % 8))) !== 0;
    }

    _cTableFieldAt(data, i) {
        if (this._tupdesc.fieldFixedSize[i] !== 0)
            return data.slice(this._tupdesc.fieldOffset[i]);
        let p = data.slice(this._tupdesc.fixedFieldsSize);
        for (let j = 0; j < this._tupdesc.fieldOffset[i]; j++) {
            const l = p.readInt16LE(0);
            p = p.slice(l % 2 === 0 ? l : l + 1);
        }
        return p;
    }
}

module.exports = NzConnection;
