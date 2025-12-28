/**
 * ETL Execution Engine
 * Handles executing ETL projects with proper dependency resolution
 * and parallel/sequential execution based on connections
 */

import * as vscode from 'vscode';
import {
    EtlProject,
    EtlNode,
    EtlExecutionResult,
    EtlNodeExecutionResult,
    EtlNodeStatus
} from './etlTypes';
import { ConnectionDetails } from '../types';

/**
 * Execution context passed to task executors
 */
export interface ExecutionContext {
    extensionContext: vscode.ExtensionContext;
    variables: Record<string, string>;
    nodeOutputs: Map<string, unknown>;
    connectionDetails: ConnectionDetails;
    connectionManager?: import('./etlProjectManager').EtlProjectManager | unknown; // Optional connection manager
    cancellationToken?: vscode.CancellationToken;
    onProgress?: (message: string) => void;
}

/**
 * Base interface for task executors
 */
export interface TaskExecutor {
    execute(node: EtlNode, context: ExecutionContext): Promise<EtlNodeExecutionResult>;
}

/**
 * ETL Execution Engine
 */
export class EtlExecutionEngine {
    private statusCallback?: (nodeId: string, status: EtlNodeStatus, message?: string) => void;
    private executors: Map<string, TaskExecutor> = new Map();

    /**
     * Register a task executor for a node type
     */
    registerExecutor(nodeType: string, executor: TaskExecutor): void {
        this.executors.set(nodeType, executor);
    }

    /**
     * Set callback for status updates
     */
    onStatusChange(callback: (nodeId: string, status: EtlNodeStatus, message?: string) => void): void {
        this.statusCallback = callback;
    }

    /**
     * Execute an ETL project
     */
    async execute(
        project: EtlProject,
        context: ExecutionContext
    ): Promise<EtlExecutionResult> {
        const result: EtlExecutionResult = {
            projectName: project.name,
            startTime: new Date(),
            status: 'running',
            nodeResults: new Map()
        };

        // Initialize all nodes as pending
        for (const node of project.nodes) {
            this.statusCallback?.(node.id, 'pending');
        }

        try {
            // Build execution order (batches of nodes that can run in parallel)
            const executionOrder = this.buildExecutionOrder(project);

            context.onProgress?.(`Starting ETL project: ${project.name}`);
            context.onProgress?.(`Found ${project.nodes.length} tasks in ${executionOrder.length} execution batches`);

            // Execute each batch
            for (let batchIndex = 0; batchIndex < executionOrder.length; batchIndex++) {
                const batch = executionOrder[batchIndex];

                // Check for cancellation
                if (context.cancellationToken?.isCancellationRequested) {
                    result.status = 'cancelled';
                    result.endTime = new Date();
                    return result;
                }

                context.onProgress?.(`Executing batch ${batchIndex + 1}/${executionOrder.length} (${batch.length} tasks)`);

                // Execute nodes in this batch in parallel
                const batchResults = await Promise.all(
                    batch.map(node => this.executeNode(node, context))
                );

                // Store results and check for errors
                for (const nodeResult of batchResults) {
                    result.nodeResults.set(nodeResult.nodeId, nodeResult);

                    // Store output for downstream nodes
                    if (nodeResult.output !== undefined) {
                        context.nodeOutputs.set(nodeResult.nodeId, nodeResult.output);
                    }

                    // Stop if any node failed
                    if (nodeResult.status === 'error') {
                        context.onProgress?.(`Task ${nodeResult.nodeId} failed: ${nodeResult.error}`);
                        result.status = 'failed';
                        result.endTime = new Date();

                        // Mark remaining nodes as skipped
                        this.markRemainingAsSkipped(project, result, batchIndex, executionOrder);

                        return result;
                    }
                }
            }

            result.status = 'completed';
            result.endTime = new Date();
            context.onProgress?.(`ETL project completed successfully`);

        } catch (error) {
            result.status = 'failed';
            result.endTime = new Date();
            context.onProgress?.(`ETL project failed: ${String(error)}`);
        }

        return result;
    }

    /**
     * Build execution order using topological sort with batching
     * Nodes in the same batch have no dependencies on each other
     * and can be executed in parallel
     */
    private buildExecutionOrder(project: EtlProject): EtlNode[][] {
        // Build adjacency list and in-degree map
        const graph = new Map<string, string[]>();
        const inDegree = new Map<string, number>();

        // Initialize all nodes
        for (const node of project.nodes) {
            graph.set(node.id, []);
            inDegree.set(node.id, 0);
        }

        // Build graph from connections
        for (const conn of project.connections) {
            const neighbors = graph.get(conn.from);
            if (neighbors) {
                neighbors.push(conn.to);
            }
            inDegree.set(conn.to, (inDegree.get(conn.to) || 0) + 1);
        }

        // Kahn's algorithm for topological sort with batching
        const batches: EtlNode[][] = [];
        const nodeMap = new Map(project.nodes.map(n => [n.id, n]));
        const processedNodes = new Set<string>();

        while (processedNodes.size < project.nodes.length) {
            // Find all nodes with in-degree 0 that haven't been processed
            const batch: EtlNode[] = [];

            for (const [nodeId, degree] of inDegree) {
                if (degree === 0 && !processedNodes.has(nodeId)) {
                    const node = nodeMap.get(nodeId);
                    if (node) {
                        batch.push(node);
                    }
                }
            }

            if (batch.length === 0) {
                // This shouldn't happen if we validated for cycles
                throw new Error('Cycle detected in ETL project');
            }

            batches.push(batch);

            // Mark batch nodes as processed and update in-degrees
            for (const node of batch) {
                processedNodes.add(node.id);

                // Decrease in-degree of neighbors
                for (const neighbor of graph.get(node.id) || []) {
                    inDegree.set(neighbor, (inDegree.get(neighbor) || 1) - 1);
                }
            }
        }

        return batches;
    }

    /**
     * Execute a single node
     */
    private async executeNode(
        node: EtlNode,
        context: ExecutionContext
    ): Promise<EtlNodeExecutionResult> {
        this.statusCallback?.(node.id, 'running');
        context.onProgress?.(`Running task: ${node.name}`);

        const executor = this.executors.get(node.type);
        if (!executor) {
            const result: EtlNodeExecutionResult = {
                nodeId: node.id,
                status: 'error',
                startTime: new Date(),
                endTime: new Date(),
                error: `No executor registered for node type: ${node.type}`
            };
            this.statusCallback?.(node.id, 'error', result.error);
            return result;
        }

        try {
            const result = await executor.execute(node, context);
            this.statusCallback?.(node.id, result.status, result.error);

            if (result.status === 'success') {
                context.onProgress?.(`Task ${node.name} completed successfully`);
                if (result.rowsAffected !== undefined) {
                    context.onProgress?.(`  Rows affected: ${result.rowsAffected}`);
                }
            }

            return result;
        } catch (error) {
            const result: EtlNodeExecutionResult = {
                nodeId: node.id,
                status: 'error',
                startTime: new Date(),
                endTime: new Date(),
                error: String(error)
            };
            this.statusCallback?.(node.id, 'error', result.error);
            return result;
        }
    }

    /**
     * Mark remaining unprocessed nodes as skipped
     */
    private markRemainingAsSkipped(
        _project: EtlProject,
        result: EtlExecutionResult,
        currentBatchIndex: number,
        executionOrder: EtlNode[][]
    ): void {
        // Mark remaining nodes in current batch as skipped
        const currentBatch = executionOrder[currentBatchIndex];
        for (const node of currentBatch) {
            if (!result.nodeResults.has(node.id)) {
                result.nodeResults.set(node.id, {
                    nodeId: node.id,
                    status: 'skipped',
                    startTime: new Date(),
                    endTime: new Date()
                });
                this.statusCallback?.(node.id, 'skipped');
            }
        }

        // Mark all nodes in remaining batches as skipped
        for (let i = currentBatchIndex + 1; i < executionOrder.length; i++) {
            for (const node of executionOrder[i]) {
                result.nodeResults.set(node.id, {
                    nodeId: node.id,
                    status: 'skipped',
                    startTime: new Date(),
                    endTime: new Date()
                });
                this.statusCallback?.(node.id, 'skipped');
            }
        }
    }

    /**
     * Get previous node output (for nodes that depend on results)
     */
    getPreviousNodeOutput(context: ExecutionContext, nodeId: string): unknown {
        // Find the incoming connections and get the first one with output
        return context.nodeOutputs.get(nodeId);
    }
}
