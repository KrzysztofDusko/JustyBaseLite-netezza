/**
 * Export Task Executor
 * Exports query results to CSV or XLSB files
 */

import { EtlNode, EtlNodeExecutionResult, ExportNodeConfig } from '../etlTypes';
import { ExecutionContext, TaskExecutor } from '../etlExecutionEngine';
import { exportToCsv } from '../../export/csvExporter';
import { exportQueryToXlsb } from '../../export/xlsbExporter';

export class ExportTaskExecutor implements TaskExecutor {
    async execute(
        node: EtlNode,
        context: ExecutionContext
    ): Promise<EtlNodeExecutionResult> {
        const config = node.config as ExportNodeConfig;
        const startTime = new Date();

        // Validate configuration
        if (!config.outputPath) {
            return {
                nodeId: node.id,
                status: 'error',
                startTime,
                endTime: new Date(),
                error: 'Output path is required'
            };
        }

        try {
            // Resolve variables in paths and query
            let outputPath = config.outputPath;
            let query = config.query || '';

            for (const [key, value] of Object.entries(context.variables)) {
                outputPath = outputPath.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value);
                query = query.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value);
            }

            // If no query, try to get from previous node
            if (!query && config.sourceNodeId) {
                const prevOutput = context.nodeOutputs.get(config.sourceNodeId) as {
                    columns?: string[];
                    rows?: unknown[][];
                } | undefined;

                if (prevOutput && prevOutput.rows) {
                    // Convert in-memory data to export
                    // For now, we'll need the query to execute
                    return {
                        nodeId: node.id,
                        status: 'error',
                        startTime,
                        endTime: new Date(),
                        error: 'In-memory data export is not yet supported. Please specify a query.'
                    };
                }
            }

            if (!query) {
                return {
                    nodeId: node.id,
                    status: 'error',
                    startTime,
                    endTime: new Date(),
                    error: 'No query or source node specified for export'
                };
            }

            context.onProgress?.(`Exporting to ${config.format.toUpperCase()}: ${outputPath}`);

            if (config.format === 'csv') {
                await exportToCsv(
                    context.extensionContext,
                    context.connectionDetails,
                    query,
                    outputPath,
                    undefined, // progress is not passed here but handled via onProgress if we refactor exportToCsv signature slightly, wait, exportToCsv calls progress.report.
                    // But exportToCsv takes a vscode.Progress object, not a simple callback.
                    // I will pass undefined for now for progress object if I cannot easily bridge it, 
                    // OR I can create a fake progress object.
                    // Actually, let's keep it simple and just pass timeout.
                    // Wait, lines 73-78 call exportToCsv. I need to match the signature.
                    // exportToCsv signature: (context, connectionDetails, query, filePath, progress?, timeout?)
                    config.timeout
                );
            } else if (config.format === 'xlsb') {
                const result = await exportQueryToXlsb(
                    context.connectionDetails,
                    query,
                    outputPath,
                    false, // copyToClipboard
                    (message) => {
                        context.onProgress?.(`[Export] ${message}`);
                    },
                    config.timeout
                );

                if (!result.success) {
                    return {
                        nodeId: node.id,
                        status: 'error',
                        startTime,
                        endTime: new Date(),
                        error: result.message
                    };
                }

                return {
                    nodeId: node.id,
                    status: 'success',
                    startTime,
                    endTime: new Date(),
                    rowsAffected: result.details?.rows_exported,
                    output: {
                        filePath: outputPath,
                        format: config.format,
                        rowsExported: result.details?.rows_exported
                    }
                };
            }

            return {
                nodeId: node.id,
                status: 'success',
                startTime,
                endTime: new Date(),
                output: {
                    filePath: outputPath,
                    format: config.format
                }
            };

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
