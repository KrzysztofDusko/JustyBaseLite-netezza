/**
 * ETL Project Manager
 * Handles loading, saving, and validating ETL projects
 */

import * as fs from 'fs';
import {
    EtlProject,
    EtlNode,
    EtlConnection,
    generateNodeId,
    generateConnectionId
} from './etlTypes';

export class EtlProjectManager {
    private static instance: EtlProjectManager;
    private currentProject: EtlProject | null = null;
    private projectPath: string | null = null;
    private isDirty: boolean = false;

    static getInstance(): EtlProjectManager {
        if (!EtlProjectManager.instance) {
            EtlProjectManager.instance = new EtlProjectManager();
        }
        return EtlProjectManager.instance;
    }

    /**
     * Create a new empty project
     */
    createProject(name: string): EtlProject {
        this.currentProject = {
            name,
            version: '1.0.0',
            description: '',
            variables: {},
            nodes: [],
            connections: []
        };
        this.projectPath = null;
        this.isDirty = true;
        return this.currentProject;
    }

    /**
     * Load project from file
     */
    async loadProject(filePath: string): Promise<EtlProject> {
        const content = await fs.promises.readFile(filePath, 'utf-8');
        const project = JSON.parse(content) as EtlProject;

        // Validate basic structure
        const errors = this.validateProject(project);
        if (errors.length > 0) {
            throw new Error(`Invalid project: ${errors.join(', ')}`);
        }

        this.currentProject = project;
        this.projectPath = filePath;
        this.isDirty = false;
        return this.currentProject;
    }

    /**
     * Save project to file
     */
    async saveProject(filePath?: string): Promise<void> {
        const targetPath = filePath || this.projectPath;
        if (!targetPath) {
            throw new Error('No file path specified');
        }
        if (!this.currentProject) {
            throw new Error('No project to save');
        }

        const content = JSON.stringify(this.currentProject, null, 2);
        await fs.promises.writeFile(targetPath, content, 'utf-8');
        this.projectPath = targetPath;
        this.isDirty = false;
    }

    /**
     * Validate project structure
     */
    validateProject(project: EtlProject): string[] {
        const errors: string[] = [];

        if (!project.name) {
            errors.push('Project name is required');
        }

        if (!project.version) {
            errors.push('Project version is required');
        }

        if (!Array.isArray(project.nodes)) {
            errors.push('Nodes must be an array');
        }

        if (!Array.isArray(project.connections)) {
            errors.push('Connections must be an array');
        }

        // Validate nodes
        const nodeIds = new Set<string>();
        for (const node of project.nodes || []) {
            if (!node.id) {
                errors.push('Node missing ID');
            } else if (nodeIds.has(node.id)) {
                errors.push(`Duplicate node ID: ${node.id}`);
            } else {
                nodeIds.add(node.id);
            }

            if (!node.type) {
                errors.push(`Node ${node.id} missing type`);
            }

            if (!node.position || typeof node.position.x !== 'number' || typeof node.position.y !== 'number') {
                errors.push(`Node ${node.id} has invalid position`);
            }
        }

        // Validate connections
        for (const conn of project.connections || []) {
            if (!conn.from || !nodeIds.has(conn.from)) {
                errors.push(`Connection ${conn.id} has invalid 'from' node: ${conn.from}`);
            }
            if (!conn.to || !nodeIds.has(conn.to)) {
                errors.push(`Connection ${conn.id} has invalid 'to' node: ${conn.to}`);
            }
            if (conn.from === conn.to) {
                errors.push(`Connection ${conn.id} cannot connect node to itself`);
            }
        }

        // Check for cycles
        const cycleErrors = this.detectCycles(project);
        errors.push(...cycleErrors);

        return errors;
    }

    /**
     * Detect cycles in the connection graph
     */
    private detectCycles(project: EtlProject): string[] {
        const errors: string[] = [];
        const visited = new Set<string>();
        const recStack = new Set<string>();

        // Build adjacency list
        const adj = new Map<string, string[]>();
        for (const node of project.nodes) {
            adj.set(node.id, []);
        }
        for (const conn of project.connections) {
            adj.get(conn.from)?.push(conn.to);
        }

        const dfs = (nodeId: string): boolean => {
            visited.add(nodeId);
            recStack.add(nodeId);

            for (const neighbor of adj.get(nodeId) || []) {
                if (!visited.has(neighbor)) {
                    if (dfs(neighbor)) {
                        return true;
                    }
                } else if (recStack.has(neighbor)) {
                    return true;
                }
            }

            recStack.delete(nodeId);
            return false;
        };

        for (const node of project.nodes) {
            if (!visited.has(node.id)) {
                if (dfs(node.id)) {
                    errors.push('Project contains circular dependencies');
                    break;
                }
            }
        }

        return errors;
    }

    /**
     * Add a node to the current project
     */
    addNode(node: EtlNode): void {
        if (!this.currentProject) {
            throw new Error('No project loaded');
        }
        if (!node.id) {
            node.id = generateNodeId();
        }
        this.currentProject.nodes.push(node);
        this.isDirty = true;
    }

    /**
     * Update an existing node
     */
    updateNode(nodeId: string, updates: Partial<EtlNode>): void {
        if (!this.currentProject) {
            throw new Error('No project loaded');
        }
        const index = this.currentProject.nodes.findIndex(n => n.id === nodeId);
        if (index === -1) {
            throw new Error(`Node not found: ${nodeId}`);
        }
        this.currentProject.nodes[index] = {
            ...this.currentProject.nodes[index],
            ...updates
        };
        this.isDirty = true;
    }

    /**
     * Remove a node and its connections
     */
    removeNode(nodeId: string): void {
        if (!this.currentProject) {
            throw new Error('No project loaded');
        }
        this.currentProject.nodes = this.currentProject.nodes.filter(n => n.id !== nodeId);
        this.currentProject.connections = this.currentProject.connections.filter(
            c => c.from !== nodeId && c.to !== nodeId
        );
        this.isDirty = true;
    }

    /**
     * Add a connection between nodes
     */
    addConnection(connection: EtlConnection): void {
        if (!this.currentProject) {
            throw new Error('No project loaded');
        }
        if (!connection.id) {
            connection.id = generateConnectionId();
        }

        // Check if connection already exists
        const exists = this.currentProject.connections.some(
            c => c.from === connection.from && c.to === connection.to
        );
        if (exists) {
            throw new Error('Connection already exists');
        }

        this.currentProject.connections.push(connection);
        this.isDirty = true;

        // Validate no cycles
        const errors = this.detectCycles(this.currentProject);
        if (errors.length > 0) {
            // Rollback
            this.currentProject.connections.pop();
            throw new Error('Connection would create a cycle');
        }
    }

    /**
     * Remove a connection
     */
    removeConnection(connectionId: string): void {
        if (!this.currentProject) {
            throw new Error('No project loaded');
        }
        this.currentProject.connections = this.currentProject.connections.filter(
            c => c.id !== connectionId
        );
        this.isDirty = true;
    }

    /**
     * Get the current project
     */
    getCurrentProject(): EtlProject | null {
        return this.currentProject;
    }

    /**
     * Get the current project path
     */
    getProjectPath(): string | null {
        return this.projectPath;
    }

    /**
     * Check if project has unsaved changes
     */
    hasUnsavedChanges(): boolean {
        return this.isDirty;
    }

    /**
     * Get node by ID
     */
    getNode(nodeId: string): EtlNode | undefined {
        return this.currentProject?.nodes.find(n => n.id === nodeId);
    }

    /**
     * Get all connections from a node
     */
    getOutgoingConnections(nodeId: string): EtlConnection[] {
        return this.currentProject?.connections.filter(c => c.from === nodeId) || [];
    }

    /**
     * Get all connections to a node
     */
    getIncomingConnections(nodeId: string): EtlConnection[] {
        return this.currentProject?.connections.filter(c => c.to === nodeId) || [];
    }
}
