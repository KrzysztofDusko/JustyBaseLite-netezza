/**
 * Netezza DDL Generator
 * 
 * This file has been refactored into smaller modules in ./ddl/
 * It now re-exports from the new location for backward compatibility.
 *
 * Module structure:
 * - ddl/types.ts - Type definitions (ColumnInfo, KeyInfo, DDLResult, etc.)
 * - ddl/helpers.ts - Utility functions (quoteNameIfNeeded, executeQueryHelper)
 * - ddl/metadata.ts - Metadata queries (getColumns, getDistributionInfo, etc.)
 * - ddl/tableDDL.ts - Table DDL generation
 * - ddl/viewDDL.ts - View DDL generation
 * - ddl/procedureDDL.ts - Procedure DDL generation
 * - ddl/externalTableDDL.ts - External table DDL generation
 * - ddl/synonymDDL.ts - Synonym DDL generation
 * - ddl/batchDDL.ts - Batch DDL generation
 * - ddl/ddlGenerator.ts - Main entry point
 * - ddl/index.ts - Module re-exports
 */

export {
    // Types
    DDLResult,
    BatchDDLOptions,
    BatchDDLResult,
    ColumnInfo,
    KeyInfo,
    ProcedureInfo,
    ExternalTableInfo,
    // Helpers
    quoteNameIfNeeded,
    // Metadata
    getColumns,
    getDistributionInfo,
    getOrganizeInfo,
    getKeysInfo,
    getTableComment,
    getTableOwner,
    // DDL Generators
    generateTableDDL,
    buildTableDDLFromCache,
    generateViewDDL,
    generateProcedureDDL,
    generateExternalTableDDL,
    generateSynonymDDL,
    generateBatchDDL,
    generateDDL
} from './ddl';
