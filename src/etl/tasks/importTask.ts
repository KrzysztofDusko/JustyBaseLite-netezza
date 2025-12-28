/**
 * Import Task Executor
 * Imports data from CSV or XLSB files into the database
 */

import { EtlNode, EtlNodeExecutionResult, ImportNodeConfig } from '../etlTypes';
import { ExecutionContext, TaskExecutor } from '../etlExecutionEngine';
import * as fs from 'fs';

// Import the importDataToNetezza function type
import { ImportResult } from '../../import/dataImporter';

// Define the function type for dynamic loading
let importDataToNetezza: (
    filePath: string,
    targetTable: string,
    connectionDetails: import('../../types').ConnectionDetails,
    progressCallback?: (message: string, increment?: number) => void,
    timeout?: number
) => Promise<ImportResult>;

try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const importerModule = require('../../import/dataImporter');
    importDataToNetezza = importerModule.importDataToNetezza;
} catch {
    // Will be handled at execution time
}

export class ImportTaskExecutor implements TaskExecutor {
    async execute(
        node: EtlNode,
        context: ExecutionContext
    ): Promise<EtlNodeExecutionResult> {
        const config = node.config as ImportNodeConfig;
        const startTime = new Date();

        // Validate configuration
        if (!config.inputPath) {
            return {
                nodeId: node.id,
                status: 'error',
                startTime,
                endTime: new Date(),
                error: 'Input path is required'
            };
        }

        if (!config.targetTable) {
            return {
                nodeId: node.id,
                status: 'error',
                startTime,
                endTime: new Date(),
                error: 'Target table is required'
            };
        }

        try {
            // Resolve variables in paths
            let inputPath = config.inputPath;
            let targetTable = config.targetTable;
            let targetSchema = config.targetSchema || '';

            for (const [key, value] of Object.entries(context.variables)) {
                inputPath = inputPath.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value);
                targetTable = targetTable.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value);
                targetSchema = targetSchema.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value);
            }

            // Check if file exists
            if (!fs.existsSync(inputPath)) {
                return {
                    nodeId: node.id,
                    status: 'error',
                    startTime,
                    endTime: new Date(),
                    error: `Input file not found: ${inputPath}`
                };
            }

            context.onProgress?.(`Importing from ${config.format.toUpperCase()}: ${inputPath}`);
            context.onProgress?.(`Target table: ${targetSchema ? targetSchema + '.' : ''}${targetTable}`);

            if (!importDataToNetezza) {
                return {
                    nodeId: node.id,
                    status: 'error',
                    startTime,
                    endTime: new Date(),
                    error: 'importDataToNetezza function not available'
                };
            }

            const result = await importDataToNetezza(
                inputPath,
                targetTable,
                context.connectionDetails,
                (message, _percent) => {
                    // percent is optional in the callback signature from ImportTaskExecutor's perspective, 
                    // but dataImporter passes it? 
                    // importDataToNetezza signature for progressCallback is (message: string, increment?: number, logToOutput?: boolean)
                    // Wait, I need to check dataImporter signature again. 
                    // export type ProgressCallback = (message: string, increment?: number, logToOutput?: boolean) => void;
                    // So I should adapt it.
                    context.onProgress?.(`[Import] ${message}`);
                },
                config.timeout
            );

            if (result.success) {
                return {
                    nodeId: node.id,
                    status: 'success',
                    startTime,
                    endTime: new Date(),
                    rowsAffected: result.details?.rowsInserted,
                    output: {
                        filePath: inputPath,
                        targetTable: `${targetSchema ? targetSchema + '.' : ''}${targetTable}`,
                        rowsProcessed: result.details?.rowsProcessed,
                        rowsInserted: result.details?.rowsInserted
                    }
                };
            } else {
                return {
                    nodeId: node.id,
                    status: 'error',
                    startTime,
                    endTime: new Date(),
                    error: result.message
                };
            }

        } catch (error) {
            return {
                nodeId: node.id,
                status: 'error',
                startTime,
                endTime: new Date(),
                error: String(error)
            };
        }
    }
}
