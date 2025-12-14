
/**
 * @typedef {import('stream').Readable} Readable
 * @typedef {import('stream').Writable} Writable
 */

class PGUtil {
  /**
   * Reads a 32-bit integer from the buffer at the given offset.
   * Big-endian (network byte order).
   * @param {Buffer} buffer 
   * @param {number} offset 
   * @returns {number}
   */
  static readInt32(buffer, offset = 0) {
    return buffer.readInt32BE(offset);
  }

  /**
   * Reads a 16-bit integer from the buffer at the given offset.
   * Big-endian.
   * @param {Buffer} buffer 
   * @param {number} offset 
   * @returns {number}
   */
  static readInt16(buffer, offset = 0) {
    return buffer.readInt16BE(offset);
  }

  /**
   * Writes a 32-bit integer to the stream or buffer.
   * @param {Writable|Buffer} target 
   * @param {number} value 
   * @param {number} [offset=0] used only if target is Buffer
   */
  static writeInt32(target, value, offset = 0) {
    if (Buffer.isBuffer(target)) {
      target.writeInt32BE(value, offset);
    } else {
      const buf = Buffer.allocUnsafe(4);
      buf.writeInt32BE(value, 0);
      target.write(buf);
    }
  }

  /**
   * Writes a 16-bit integer to the stream or buffer.
   * @param {Writable|Buffer} target 
   * @param {number} value 
   * @param {number} [offset=0] used only if target is Buffer
   */
  static writeInt16(target, value, offset = 0) {
    if (Buffer.isBuffer(target)) {
      target.writeInt16BE(value, offset);
    } else {
      const buf = Buffer.allocUnsafe(2);
      buf.writeInt16BE(value, 0);
      target.write(buf);
    }
  }
}

module.exports = PGUtil;
