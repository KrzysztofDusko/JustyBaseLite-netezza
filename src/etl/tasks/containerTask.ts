/**
 * Container Task Executor
 * Executes a group of nested tasks as a single unit
 */

import { EtlNode, EtlNodeExecutionResult, ContainerNodeConfig, EtlProject } from '../etlTypes';
import { ExecutionContext, TaskExecutor, EtlExecutionEngine } from '../etlExecutionEngine';

export class ContainerTaskExecutor implements TaskExecutor {
    private engine: EtlExecutionEngine;

    constructor(engine: EtlExecutionEngine) {
        this.engine = engine;
    }

    async execute(
        node: EtlNode,
        context: ExecutionContext
    ): Promise<EtlNodeExecutionResult> {
        const config = node.config as ContainerNodeConfig;
        const startTime = new Date();

        // Validate configuration
        if (!config.nodes || config.nodes.length === 0) {
            return {
                nodeId: node.id,
                status: 'success',
                startTime,
                endTime: new Date(),
                output: 'Empty container - nothing to execute'
            };
        }

        try {
            context.onProgress?.(`Entering container: ${node.name} (${config.nodes.length} tasks)`);

            // Create a mini-project from the container's nodes
            const containerProject: EtlProject = {
                name: `Container: ${node.name}`,
                version: '1.0.0',
                nodes: config.nodes,
                connections: config.connections || []
            };

            // Execute the container's tasks using the same engine
            const result = await this.engine.execute(containerProject, context);

            context.onProgress?.(`Exiting container: ${node.name}`);

            if (result.status === 'completed') {
                // Count successful tasks
                let successCount = 0;
                let totalRows = 0;

                for (const nodeResult of result.nodeResults.values()) {
                    if (nodeResult.status === 'success') {
                        successCount++;
                        if (nodeResult.rowsAffected) {
                            totalRows += nodeResult.rowsAffected;
                        }
                    }
                }

                return {
                    nodeId: node.id,
                    status: 'success',
                    startTime,
                    endTime: new Date(),
                    rowsAffected: totalRows,
                    output: {
                        tasksExecuted: config.nodes.length,
                        tasksSucceeded: successCount,
                        nestedResults: Array.from(result.nodeResults.entries())
                    }
                };
            } else if (result.status === 'cancelled') {
                return {
                    nodeId: node.id,
                    status: 'skipped',
                    startTime,
                    endTime: new Date(),
                    error: 'Container execution was cancelled'
                };
            } else {
                // Find the first error
                let errorMessage = 'Container execution failed';
                for (const nodeResult of result.nodeResults.values()) {
                    if (nodeResult.status === 'error' && nodeResult.error) {
                        errorMessage = nodeResult.error;
                        break;
                    }
                }

                return {
                    nodeId: node.id,
                    status: 'error',
                    startTime,
                    endTime: new Date(),
                    error: errorMessage
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
