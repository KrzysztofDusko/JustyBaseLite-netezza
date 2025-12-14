/**
 * Tuple descriptor for binary row format
 * Port of C# DbosTupleDesc.cs
 */

class DbosTupleDesc {
    constructor() {
        this.version = null;
        this.nullsAllowed = null;
        this.sizeWord = null;
        this.sizeWordSize = null;
        this.numFixedFields = null;
        this.numVaryingFields = null;
        this.fixedFieldsSize = 0;
        this.maxRecordSize = null;
        this.numFields = 0;

        // Per-field arrays
        this.fieldType = [];
        this.fieldSize = [];
        this.fieldTrueSize = [];
        this.fieldOffset = [];
        this.fieldPhysField = [];
        this.fieldLogField = [];
        this.fieldNullAllowed = [];
        this.fieldFixedSize = [];
        this.fieldSpringField = [];

        // Date/time settings
        this.dateStyle = null;
        this.euroDates = null;
    }

    /**
     * Reset all arrays for reuse
     */
    clear() {
        this.fieldType = [];
        this.fieldSize = [];
        this.fieldTrueSize = [];
        this.fieldOffset = [];
        this.fieldPhysField = [];
        this.fieldLogField = [];
        this.fieldNullAllowed = [];
        this.fieldFixedSize = [];
        this.fieldSpringField = [];
    }

    /**
     * Parse tuple description from binary data
     * @param {Buffer} data - raw data from backend
     * @param {Object} preparedStatement - prepared statement with description
     */
    parse(data, preparedStatement) {
        this.clear();

        let idx = 0;
        this.version = data.readInt32BE(idx); idx += 4;
        this.nullsAllowed = data.readInt32BE(idx); idx += 4;
        this.sizeWord = data.readInt32BE(idx); idx += 4;
        this.sizeWordSize = data.readInt32BE(idx); idx += 4;
        this.numFixedFields = data.readInt32BE(idx); idx += 4;
        this.numVaryingFields = data.readInt32BE(idx); idx += 4;
        this.fixedFieldsSize = data.readInt32BE(idx); idx += 4;
        this.maxRecordSize = data.readInt32BE(idx); idx += 4;
        this.numFields = data.readInt32BE(idx); idx += 4;

        const NzTypeInt = 3;
        const NzTypeIntvsAbsTimeFIX = 39;

        for (let ix = 0; ix < this.numFields; ix++) {
            let ft = data.readInt32BE(idx);

            // Fix for abstime type (OID 702) being returned as int
            // https://github.com/IBM/nzpy/issues/61
            if (ft === NzTypeInt && preparedStatement?.description?.[ix]?.typeOid === 702) {
                ft = NzTypeIntvsAbsTimeFIX;
            }

            this.fieldType.push(ft);
            this.fieldSize.push(data.readInt32BE(idx + 4));
            this.fieldTrueSize.push(data.readInt32BE(idx + 8));
            this.fieldOffset.push(data.readInt32BE(idx + 12));
            this.fieldPhysField.push(data.readInt32BE(idx + 16));
            this.fieldLogField.push(data.readInt32BE(idx + 20));
            this.fieldNullAllowed.push(data.readInt32BE(idx + 24) !== 0);
            this.fieldFixedSize.push(data.readInt32BE(idx + 28));
            this.fieldSpringField.push(data.readInt32BE(idx + 32));
            idx += 36;
        }

        this.dateStyle = data.readInt32BE(idx); idx += 4;
        this.euroDates = data.readInt32BE(idx);
    }

    /**
     * Get field precision for numeric types
     * @param {number} coldex - column index
     * @returns {number}
     */
    getFieldPrecision(coldex) {
        return (this.fieldSize[coldex] >> 8) & 0x7F;
    }

    /**
     * Get field scale for numeric types
     * @param {number} coldex - column index
     * @returns {number}
     */
    getFieldScale(coldex) {
        return this.fieldSize[coldex] & 0x00FF;
    }

    /**
     * Get numeric digit count (32-bit parts)
     * @param {number} coldex - column index
     * @returns {number}
     */
    getNumericDigitCount(coldex) {
        return Math.floor(this.fieldTrueSize[coldex] / 4);
    }
}

module.exports = DbosTupleDesc;
