/**
 * ETL Designer Types
 * Defines the data model for ETL projects, nodes, and connections
 */

// ETL Node Types
export type EtlNodeType = 'sql' | 'python' | 'container' | 'export' | 'import';

// Position in the canvas
export interface Position {
    x: number;
    y: number;
}

// Base ETL Node
export interface EtlNode {
    id: string;
    type: EtlNodeType;
    name: string;
    description?: string;
    position: Position;
    config: EtlNodeConfig;
}

// Node Configuration Types
export interface SqlNodeConfig {
    type: 'sql';
    query: string;
    connection?: string;  // Connection name or default
    timeout?: number;
}

export interface PythonNodeConfig {
    type: 'python';
    script: string;       // Python script content or path
    scriptPath?: string;  // Path to Python script file
    interpreter?: string; // Python interpreter path
    arguments?: string[];
    timeout?: number;
}

export interface ContainerNodeConfig {
    type: 'container';
    nodes: EtlNode[];
    connections: EtlConnection[];
}

export interface ExportNodeConfig {
    type: 'export';
    format: 'csv' | 'xlsb';
    outputPath: string;
    query?: string;       // If SQL is embedded
    sourceNodeId?: string; // Or use result from previous node
    delimiter?: string;
    encoding?: string;
    timeout?: number;
}

export interface ImportNodeConfig {
    type: 'import';
    format: 'csv' | 'xlsb';
    inputPath: string;
    targetTable: string;
    targetSchema?: string;
    delimiter?: string;
    skipRows?: number;
    createTable?: boolean;
    timeout?: number;
}

export type EtlNodeConfig =
    | SqlNodeConfig
    | PythonNodeConfig
    | ContainerNodeConfig
    | ExportNodeConfig
    | ImportNodeConfig;

// ETL Connection (Arrow)
export interface EtlConnection {
    id: string;
    from: string;  // Source node ID
    to: string;    // Target node ID
    label?: string;
    condition?: string;  // Optional condition expression
}

// ETL Project
export interface EtlProject {
    name: string;
    version: string;
    description?: string;
    variables?: Record<string, string>;
    nodes: EtlNode[];
    connections: EtlConnection[];
}

// Execution Status
export type EtlNodeStatus = 'pending' | 'running' | 'success' | 'error' | 'skipped';

export interface EtlNodeExecutionResult {
    nodeId: string;
    status: EtlNodeStatus;
    startTime: Date;
    endTime?: Date;
    error?: string;
    output?: unknown;
    rowsAffected?: number;
}

export interface EtlExecutionResult {
    projectName: string;
    startTime: Date;
    endTime?: Date;
    status: 'running' | 'completed' | 'failed' | 'cancelled';
    nodeResults: Map<string, EtlNodeExecutionResult>;
}

// Default configurations for new nodes
export function getDefaultConfig(type: EtlNodeType): EtlNodeConfig {
    switch (type) {
        case 'sql':
            return { type: 'sql', query: '', connection: 'default' };
        case 'python':
            return { type: 'python', script: '' };
        case 'container':
            return { type: 'container', nodes: [], connections: [] };
        case 'export':
            return { type: 'export', format: 'csv', outputPath: '' };
        case 'import':
            return { type: 'import', format: 'csv', inputPath: '', targetTable: '' };
    }
}

// Generate unique node ID
export function generateNodeId(): string {
    return 'node-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
}

// Generate unique connection ID
export function generateConnectionId(): string {
    return 'conn-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
}
