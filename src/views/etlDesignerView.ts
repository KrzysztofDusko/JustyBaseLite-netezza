/**
 * ETL Designer View
 * Webview panel for visual ETL workflow design
 */

import * as vscode from 'vscode';
import {
    EtlProject,
    EtlNode,
    EtlNodeType,
    generateNodeId,
    generateConnectionId,
    getDefaultConfig
} from '../etl/etlTypes';
import { EtlProjectManager } from '../etl/etlProjectManager';
import { EtlExecutionEngine, ExecutionContext } from '../etl/etlExecutionEngine';
import { SqlTaskExecutor } from '../etl/tasks/sqlTask';
import { PythonTaskExecutor } from '../etl/tasks/pythonTask';
import { ExportTaskExecutor } from '../etl/tasks/exportTask';
import { ImportTaskExecutor } from '../etl/tasks/importTask';
import { ContainerTaskExecutor } from '../etl/tasks/containerTask';
import { ConnectionManager } from '../core/connectionManager';

export class EtlDesignerView {
    public static currentPanel: EtlDesignerView | undefined;
    public static readonly viewType = 'netezza.etlDesigner';

    private readonly _panel: vscode.WebviewPanel;
    private readonly _context: vscode.ExtensionContext;
    private _disposables: vscode.Disposable[] = [];
    private _projectManager: EtlProjectManager;
    private _executionEngine: EtlExecutionEngine;
    private static _connectionManager: ConnectionManager | undefined;
    private _cancellationTokenSource: vscode.CancellationTokenSource | undefined;

    public static setConnectionManager(connManager: ConnectionManager): void {
        EtlDesignerView._connectionManager = connManager;
    }

    public static createOrShow(
        context: vscode.ExtensionContext,
        project?: EtlProject
    ): void {
        const column = vscode.window.activeTextEditor
            ? vscode.window.activeTextEditor.viewColumn
            : undefined;

        if (EtlDesignerView.currentPanel) {
            EtlDesignerView.currentPanel._panel.reveal(column);
            if (project) {
                EtlDesignerView.currentPanel._updateProject(project);
            }
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            EtlDesignerView.viewType,
            'ETL Designer',
            column || vscode.ViewColumn.One,
            {
                enableScripts: true,
                retainContextWhenHidden: true,
                localResourceRoots: [context.extensionUri]
            }
        );

        EtlDesignerView.currentPanel = new EtlDesignerView(
            panel,
            context,
            project
        );
    }

    private constructor(
        panel: vscode.WebviewPanel,
        context: vscode.ExtensionContext,
        project?: EtlProject
    ) {
        this._panel = panel;
        this._context = context;
        this._projectManager = EtlProjectManager.getInstance();

        // Initialize execution engine with all task executors
        this._executionEngine = new EtlExecutionEngine();
        this._executionEngine.registerExecutor('sql', new SqlTaskExecutor());
        this._executionEngine.registerExecutor('python', new PythonTaskExecutor());
        this._executionEngine.registerExecutor('export', new ExportTaskExecutor());
        this._executionEngine.registerExecutor('import', new ImportTaskExecutor());
        this._executionEngine.registerExecutor('container', new ContainerTaskExecutor(this._executionEngine));

        // Initialize project
        const initialProject = project || this._projectManager.createProject('New ETL Project');
        this._updateWebview(initialProject);

        // Handle panel disposal
        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);

        // Handle messages from webview
        this._panel.webview.onDidReceiveMessage(
            message => this._handleMessage(message),
            null,
            this._disposables
        );
    }

    private async _handleMessage(message: { type: string; payload?: unknown }) {
        const project = this._projectManager.getCurrentProject();

        switch (message.type) {
            case 'addNode': {
                const payload = message.payload as { type: EtlNodeType; position: { x: number; y: number } };
                const node: EtlNode = {
                    id: generateNodeId(),
                    type: payload.type,
                    name: this._getDefaultNodeName(payload.type),
                    position: payload.position,
                    config: getDefaultConfig(payload.type)
                };
                this._projectManager.addNode(node);
                this._sendProjectUpdate();
                break;
            }

            case 'confirmRemoveNode': {
                const nodeId = message.payload as string;
                const confirm = await vscode.window.showWarningMessage(
                    'Delete this task?',
                    { modal: true },
                    'Delete'
                );
                if (confirm === 'Delete') {
                    this._projectManager.removeNode(nodeId);
                    this._sendProjectUpdate();
                }
                break;
            }

            case 'removeNode': {
                const nodeId = message.payload as string;
                this._projectManager.removeNode(nodeId);
                this._sendProjectUpdate();
                break;
            }

            case 'updateNodePosition': {
                const { nodeId, position } = message.payload as { nodeId: string; position: { x: number; y: number } };
                this._projectManager.updateNode(nodeId, { position });
                break;
            }

            case 'updateNodeName': {
                const { nodeId, name } = message.payload as { nodeId: string; name: string };
                this._projectManager.updateNode(nodeId, { name });
                this._sendProjectUpdate();
                break;
            }

            case 'addConnection': {
                const { from, to } = message.payload as { from: string; to: string };
                try {
                    this._projectManager.addConnection({
                        id: generateConnectionId(),
                        from,
                        to
                    });
                    this._sendProjectUpdate();
                } catch (error) {
                    vscode.window.showErrorMessage(String(error));
                }
                break;
            }

            case 'confirmRemoveConnection': {
                const connectionId = message.payload as string;
                const confirm = await vscode.window.showWarningMessage(
                    'Delete this connection?',
                    { modal: true },
                    'Delete'
                );
                if (confirm === 'Delete') {
                    this._projectManager.removeConnection(connectionId);
                    this._sendProjectUpdate();
                }
                break;
            }

            case 'removeConnection': {
                const connectionId = message.payload as string;
                this._projectManager.removeConnection(connectionId);
                this._sendProjectUpdate();
                break;
            }

            case 'configureNode': {
                const nodeId = message.payload as string;
                await this._configureNode(nodeId);
                break;
            }

            case 'saveProject': {
                await this._saveProject();
                break;
            }

            case 'loadProject': {
                await this._loadProject();
                break;
            }

            case 'newProject': {
                const name = await vscode.window.showInputBox({
                    prompt: 'Enter project name',
                    value: 'New ETL Project'
                });
                if (name) {
                    this._projectManager.createProject(name);
                    this._sendProjectUpdate();
                }
                break;
            }

            case 'runProject': {
                if (project) {
                    await this._runProject(project);
                }
                break;
            }

            case 'stopProject': {
                this._stopExecution();
                break;
            }

            case 'getProject': {
                this._sendProjectUpdate();
                break;
            }
        }
    }

    private _getDefaultNodeName(type: EtlNodeType): string {
        const names: Record<EtlNodeType, string> = {
            sql: 'SQL Task',
            python: 'Python Script',
            container: 'Container',
            export: 'Export Data',
            import: 'Import Data'
        };
        return names[type] || 'New Task';
    }

    private async _configureNode(nodeId: string) {
        const node = this._projectManager.getNode(nodeId);
        if (!node) {
            return;
        }

        switch (node.type) {
            case 'sql': {
                const config = node.config as { type: 'sql'; query: string; connection?: string; timeout?: number };

                // Ask for task name first
                const name = await vscode.window.showInputBox({
                    prompt: 'Enter task name',
                    value: node.name,
                    placeHolder: 'e.g. Load Customer Data'
                });
                if (name === undefined) break;

                const query = await vscode.window.showInputBox({
                    prompt: 'Enter SQL query',
                    value: config.query,
                    placeHolder: 'SELECT * FROM table_name'
                });
                if (query !== undefined) {
                    const timeoutStr = await vscode.window.showInputBox({
                        prompt: 'Enter execution timeout in seconds (optional)',
                        value: config.timeout ? String(config.timeout) : '',
                        placeHolder: 'e.g. 60'
                    });

                    const timeout = timeoutStr && !isNaN(parseInt(timeoutStr)) ? parseInt(timeoutStr) : undefined;

                    this._projectManager.updateNode(nodeId, {
                        name,
                        config: { ...config, query, timeout }
                    });
                    this._sendProjectUpdate();
                }
                break;
            }

            case 'python': {
                const config = node.config as { type: 'python'; script: string; scriptPath?: string; timeout?: number };

                // Ask for task name first
                const name = await vscode.window.showInputBox({
                    prompt: 'Enter task name',
                    value: node.name,
                    placeHolder: 'e.g. Transform Data'
                });
                if (name === undefined) break;

                const choice = await vscode.window.showQuickPick(
                    ['Enter script inline', 'Select script file'],
                    { placeHolder: 'How to provide the Python script?' }
                );

                if (choice === 'Enter script inline') {
                    const script = await vscode.window.showInputBox({
                        prompt: 'Enter Python script',
                        value: config.script,
                        placeHolder: 'print("Hello ETL")'
                    });
                    if (script !== undefined) {
                        const timeoutStr = await vscode.window.showInputBox({
                            prompt: 'Enter execution timeout in seconds (optional)',
                            value: config.timeout ? String(config.timeout) : '',
                            placeHolder: 'e.g. 60'
                        });
                        const timeout = timeoutStr && !isNaN(parseInt(timeoutStr)) ? parseInt(timeoutStr) : undefined;

                        this._projectManager.updateNode(nodeId, {
                            name,
                            config: { ...config, script, scriptPath: undefined, timeout }
                        });
                        this._sendProjectUpdate();
                    }
                } else if (choice === 'Select script file') {
                    const files = await vscode.window.showOpenDialog({
                        filters: { 'Python': ['py'] },
                        canSelectMany: false
                    });
                    if (files && files[0]) {
                        const timeoutStr = await vscode.window.showInputBox({
                            prompt: 'Enter execution timeout in seconds (optional)',
                            value: config.timeout ? String(config.timeout) : '',
                            placeHolder: 'e.g. 60'
                        });
                        const timeout = timeoutStr && !isNaN(parseInt(timeoutStr)) ? parseInt(timeoutStr) : undefined;

                        this._projectManager.updateNode(nodeId, {
                            name,
                            config: { ...config, scriptPath: files[0].fsPath, script: '', timeout }
                        });
                        this._sendProjectUpdate();
                    }
                }
                break;
            }

            case 'export': {
                const config = node.config as { type: 'export'; format: 'csv' | 'xlsb'; outputPath: string; query?: string; timeout?: number };

                // Ask for task name first
                const name = await vscode.window.showInputBox({
                    prompt: 'Enter task name',
                    value: node.name,
                    placeHolder: 'e.g. Export to CSV'
                });
                if (name === undefined) break;

                const format = await vscode.window.showQuickPick(
                    ['csv', 'xlsb'],
                    { placeHolder: 'Select output format' }
                ) as 'csv' | 'xlsb' | undefined;

                if (format) {
                    const outputPath = await vscode.window.showSaveDialog({
                        filters: format === 'csv' ? { 'CSV': ['csv'] } : { 'Excel Binary': ['xlsb'] }
                    });

                    if (outputPath) {
                        const query = await vscode.window.showInputBox({
                            prompt: 'Enter SQL query for export',
                            value: config.query || '',
                            placeHolder: 'SELECT * FROM table_name'
                        });

                        if (query !== undefined) {
                            const timeoutStr = await vscode.window.showInputBox({
                                prompt: 'Enter execution timeout in seconds (optional)',
                                value: config.timeout ? String(config.timeout) : '',
                                placeHolder: 'e.g. 300'
                            });
                            const timeout = timeoutStr && !isNaN(parseInt(timeoutStr)) ? parseInt(timeoutStr) : undefined;

                            this._projectManager.updateNode(nodeId, {
                                name,
                                config: { ...config, format, outputPath: outputPath.fsPath, query, timeout }
                            });
                            this._sendProjectUpdate();
                        }
                    }
                }
                break;
            }

            case 'import': {
                const config = node.config as { type: 'import'; format: 'csv' | 'xlsb'; inputPath: string; targetTable: string; timeout?: number };

                // Ask for task name first
                const name = await vscode.window.showInputBox({
                    prompt: 'Enter task name',
                    value: node.name,
                    placeHolder: 'e.g. Import Products'
                });
                if (name === undefined) break;

                const files = await vscode.window.showOpenDialog({
                    filters: {
                        'Data Files': ['csv', 'xlsb'],
                        'CSV': ['csv'],
                        'Excel Binary': ['xlsb']
                    },
                    canSelectMany: false
                });

                if (files && files[0]) {
                    const inputPath = files[0].fsPath;
                    const format = inputPath.endsWith('.xlsb') ? 'xlsb' : 'csv';

                    const targetTable = await vscode.window.showInputBox({
                        prompt: 'Enter target table name',
                        value: config.targetTable,
                        placeHolder: 'SCHEMA.TABLE_NAME'
                    });

                    if (targetTable) {
                        const timeoutStr = await vscode.window.showInputBox({
                            prompt: 'Enter execution timeout in seconds (optional)',
                            value: config.timeout ? String(config.timeout) : '',
                            placeHolder: 'e.g. 300'
                        });
                        const timeout = timeoutStr && !isNaN(parseInt(timeoutStr)) ? parseInt(timeoutStr) : undefined;

                        this._projectManager.updateNode(nodeId, {
                            name,
                            config: { ...config, format, inputPath, targetTable, timeout }
                        });
                        this._sendProjectUpdate();
                    }
                }
                break;
            }
        }
    }

    private async _saveProject() {
        const project = this._projectManager.getCurrentProject();
        if (!project) {
            return;
        }

        const uri = await vscode.window.showSaveDialog({
            filters: { 'ETL Project': ['etl.json'] },
            defaultUri: this._projectManager.getProjectPath()
                ? vscode.Uri.file(this._projectManager.getProjectPath()!)
                : undefined
        });

        if (uri) {
            try {
                await this._projectManager.saveProject(uri.fsPath);
                vscode.window.showInformationMessage(`Project saved to ${uri.fsPath}`);
            } catch (error) {
                vscode.window.showErrorMessage(`Failed to save project: ${error}`);
            }
        }
    }

    private async _loadProject() {
        const files = await vscode.window.showOpenDialog({
            filters: { 'ETL Project': ['etl.json'] },
            canSelectMany: false
        });

        if (files && files[0]) {
            try {
                const project = await this._projectManager.loadProject(files[0].fsPath);
                this._updateWebview(project);
                vscode.window.showInformationMessage(`Project loaded: ${project.name}`);
            } catch (error) {
                vscode.window.showErrorMessage(`Failed to load project: ${error}`);
            }
        }
    }

    private async _runProject(project: EtlProject) {
        // Validate project first
        const errors = this._projectManager.validateProject(project);
        if (errors.length > 0) {
            vscode.window.showErrorMessage(`Project validation failed:\n${errors.join('\n')}`);
            return;
        }

        // Get connection details
        const connManager = EtlDesignerView._connectionManager;
        if (!connManager) {
            vscode.window.showErrorMessage('Connection manager not initialized. Please reload the extension.');
            return;
        }

        const activeConnName = connManager.getActiveConnectionName();

        if (!activeConnName) {
            vscode.window.showErrorMessage('No active connection. Please connect to a database first.');
            return;
        }

        const connDetails = await connManager.getConnection(activeConnName);
        if (!connDetails) {
            vscode.window.showErrorMessage(`Connection not found: ${activeConnName}`);
            return;
        }

        // Create output channel for logging
        const outputChannel = vscode.window.createOutputChannel('ETL Execution');
        outputChannel.show();

        // Create cancellation token source
        this._cancellationTokenSource = new vscode.CancellationTokenSource();

        // Create execution context
        const context: ExecutionContext = {
            extensionContext: this._context,
            variables: project.variables || {},
            nodeOutputs: new Map(),
            connectionDetails: connDetails,
            cancellationToken: this._cancellationTokenSource.token,
            onProgress: (message) => {
                outputChannel.appendLine(`[${new Date().toLocaleTimeString()}] ${message}`);
            }
        };

        // Update UI when node status changes
        this._executionEngine.onStatusChange((nodeId, status, message) => {
            this._panel.webview.postMessage({
                type: 'nodeStatusUpdate',
                payload: { nodeId, status, message }
            });
        });

        // Notify webview that execution started
        this._panel.webview.postMessage({ type: 'executionStarted' });

        try {
            outputChannel.appendLine(`Starting ETL Project: ${project.name}`);
            outputChannel.appendLine(`Connection: ${activeConnName}`);
            outputChannel.appendLine('---');

            const result = await this._executionEngine.execute(project, context);

            outputChannel.appendLine('---');
            outputChannel.appendLine(`Execution ${result.status}`);
            outputChannel.appendLine(`Duration: ${result.endTime
                ? ((result.endTime.getTime() - result.startTime.getTime()) / 1000).toFixed(2)
                : 'N/A'
                } seconds`);

            // Notify webview that execution ended
            this._panel.webview.postMessage({
                type: 'executionEnded',
                payload: { status: result.status === 'completed' ? 'Completed ✓' : 'Failed ✗' }
            });

            if (result.status === 'completed') {
                vscode.window.showInformationMessage('ETL project completed successfully!');
            } else if (result.status === 'failed') {
                vscode.window.showErrorMessage('ETL project failed. Check output for details.');
            }

        } catch (error) {
            outputChannel.appendLine(`Error: ${error}`);
            vscode.window.showErrorMessage(`ETL execution error: ${error}`);

            // Notify webview that execution ended
            this._panel.webview.postMessage({
                type: 'executionEnded',
                payload: { status: 'Error ✗' }
            });
        } finally {
            this._cancellationTokenSource?.dispose();
            this._cancellationTokenSource = undefined;
        }
    }

    private _stopExecution(): void {
        if (this._cancellationTokenSource) {
            this._cancellationTokenSource.cancel();
            vscode.window.showInformationMessage('ETL execution cancellation requested...');
        }
    }

    private _updateProject(project: EtlProject) {
        // Update the project manager with the new project
        this._projectManager.createProject(project.name);
        for (const node of project.nodes) {
            this._projectManager.addNode(node);
        }
        for (const conn of project.connections) {
            try {
                this._projectManager.addConnection(conn);
            } catch {
                // Ignore connection errors during load
            }
        }
        this._updateWebview(project);
    }

    private _updateWebview(project: EtlProject) {
        this._panel.webview.html = this._getHtml(project);
    }

    private _sendProjectUpdate() {
        const project = this._projectManager.getCurrentProject();
        if (project) {
            this._panel.webview.postMessage({
                type: 'projectUpdate',
                payload: project
            });
        }
    }

    private _getHtml(project: EtlProject): string {
        const nonce = this._getNonce();

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-${nonce}';">
    <title>ETL Designer - ${project.name}</title>
    <style>${this._getStyles()}</style>
</head>
<body>
    <div class="etl-designer">
        <!-- Toolbar -->
        <header class="toolbar">
            <div class="toolbar-left">
                <button id="btn-new" title="New Project">📄 New</button>
                <button id="btn-open" title="Open Project">📂 Open</button>
                <button id="btn-save" title="Save Project">💾 Save</button>
                <span class="separator"></span>
                <button id="btn-run" class="btn-primary" title="Run Project">▶️ Run</button>
                <button id="btn-stop" class="btn-danger" title="Stop Execution" style="display:none;">⏹️ Stop</button>
            </div>
            <div class="toolbar-center">
                <span class="project-name">${project.name}</span>
            </div>
            <div class="toolbar-right">
                <span class="status" id="status"></span>
            </div>
        </header>

        <div class="main-content">
            <!-- Toolbox -->
            <aside class="toolbox">
                <h3>📦 Tasks</h3>
                <div class="toolbox-items">
                    <div class="toolbox-item" data-type="sql" draggable="true">
                        <span class="icon">📜</span>
                        <span class="label">SQL Task</span>
                    </div>
                    <div class="toolbox-item" data-type="python" draggable="true">
                        <span class="icon">🐍</span>
                        <span class="label">Python Script</span>
                    </div>
                    <div class="toolbox-item" data-type="container" draggable="true">
                        <span class="icon">📦</span>
                        <span class="label">Container</span>
                    </div>
                    <div class="toolbox-item" data-type="export" draggable="true">
                        <span class="icon">📤</span>
                        <span class="label">Export (CSV/XLSB)</span>
                    </div>
                    <div class="toolbox-item" data-type="import" draggable="true">
                        <span class="icon">📥</span>
                        <span class="label">Import (CSV/XLSB)</span>
                    </div>
                </div>
                
                <h3>ℹ️ Help</h3>
                <div class="help-text">
                    <p>Drag tasks to canvas</p>
                    <p>Connect outputs to inputs</p>
                    <p>Double-click to configure</p>
                    <p>Connected = Sequential</p>
                    <p>Unconnected = Parallel</p>
                </div>
            </aside>

            <!-- Canvas -->
            <main class="canvas-container" id="canvas">
                <div id="zoom-wrapper" class="zoom-wrapper">
                    <svg class="connections-layer" id="connections-svg">
                        <defs>
                            <marker id="arrowhead" markerWidth="10" markerHeight="7" 
                                refX="9" refY="3.5" orient="auto">
                                <polygon points="0 0, 10 3.5, 0 7" />
                            </marker>
                        </defs>
                    </svg>
                    <div class="nodes-layer" id="nodes-container"></div>
                </div>
            </main>

            <!-- Properties Panel -->
            <aside class="properties-panel" id="properties">
                <h3>⚙️ Properties</h3>
                <div id="properties-content">
                    <p class="placeholder">Select a task to view properties</p>
                </div>
            </aside>
        </div>
    </div>

    <script nonce="${nonce}">
        ${this._getScript(project)}
    </script>
</body>
</html>`;
    }

    private _getStyles(): string {
        return `
            :root {
                --bg-primary: var(--vscode-editor-background);
                --bg-secondary: var(--vscode-sideBar-background);
                --bg-hover: var(--vscode-list-hoverBackground);
                --border-color: var(--vscode-panel-border);
                --text-color: var(--vscode-editor-foreground);
                --text-muted: var(--vscode-descriptionForeground);
                --accent-color: var(--vscode-button-background);
                --accent-hover: var(--vscode-button-hoverBackground);
                --success-color: #4CAF50;
                --error-color: #f44336;
                --warning-color: #ff9800;
            }

            * {
                box-sizing: border-box;
            }

            body {
                margin: 0;
                padding: 0;
                height: 100vh;
                overflow: hidden;
                font-family: var(--vscode-font-family);
                font-size: var(--vscode-font-size);
                color: var(--text-color);
                background: var(--bg-primary);
            }

            .etl-designer {
                display: flex;
                flex-direction: column;
                height: 100vh;
            }

            /* Toolbar */
            .toolbar {
                display: flex;
                align-items: center;
                justify-content: space-between;
                padding: 8px 16px;
                background: var(--bg-secondary);
                border-bottom: 1px solid var(--border-color);
            }

            .toolbar button {
                padding: 6px 12px;
                margin-right: 4px;
                border: 1px solid var(--border-color);
                background: var(--bg-primary);
                color: var(--text-color);
                cursor: pointer;
                border-radius: 4px;
            }

            .toolbar button:hover {
                background: var(--bg-hover);
            }

            .toolbar .btn-primary {
                background: var(--accent-color);
                border-color: var(--accent-color);
            }

            .toolbar .btn-primary:hover {
                background: var(--accent-hover);
            }

            .toolbar .btn-danger {
                background: var(--error-color);
                border-color: var(--error-color);
                color: white;
            }

            .toolbar .btn-danger:hover {
                opacity: 0.8;
            }

            .toolbar .separator {
                display: inline-block;
                width: 1px;
                height: 20px;
                background: var(--border-color);
                margin: 0 8px;
            }

            .project-name {
                font-weight: bold;
                font-size: 1.1em;
            }

            .status {
                color: var(--text-muted);
                font-size: 0.9em;
            }

            /* Main Content */
            .main-content {
                display: flex;
                flex: 1;
                overflow: hidden;
            }

            /* Toolbox */
            .toolbox {
                width: 200px;
                background: var(--bg-secondary);
                border-right: 1px solid var(--border-color);
                padding: 12px;
                overflow-y: auto;
            }

            .toolbox h3 {
                margin: 0 0 12px 0;
                font-size: 0.9em;
                color: var(--text-muted);
                text-transform: uppercase;
            }

            .toolbox-item {
                display: flex;
                align-items: center;
                gap: 8px;
                padding: 10px 12px;
                margin-bottom: 6px;
                background: var(--bg-primary);
                border: 1px solid var(--border-color);
                border-radius: 6px;
                cursor: grab;
                transition: all 0.15s ease;
            }

            .toolbox-item:hover {
                border-color: var(--accent-color);
                transform: translateX(3px);
            }

            .toolbox-item:active {
                cursor: grabbing;
            }

            .toolbox-item .icon {
                font-size: 1.2em;
            }

            .toolbox-item .label {
                font-size: 0.85em;
            }

            .help-text {
                font-size: 0.8em;
                color: var(--text-muted);
                line-height: 1.6;
            }

            .help-text p {
                margin: 4px 0;
            }

            /* Canvas */
            .canvas-container {
                flex: 1;
                position: relative;
                overflow: hidden; /* Changed for zoom/pan */
                background: var(--bg-primary);
                cursor: grab;
            }
            
            .canvas-container:active {
                cursor: grabbing;
            }

            .zoom-wrapper {
                position: absolute;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                transform-origin: 0 0;
                background: 
                    linear-gradient(90deg, rgba(128,128,128,0.1) 1px, transparent 1px),
                    linear-gradient(rgba(128,128,128,0.1) 1px, transparent 1px);
                background-size: 20px 20px;
            }

            .connections-layer {
                position: absolute;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                z-index: 1;
            }

            .connections-layer path.connection-line {
                stroke: var(--accent-color);
                stroke-width: 2;
                fill: none;
                pointer-events: none;
            }
            
            .connections-layer path.connection-hit {
                stroke: transparent;
                stroke-width: 15px;
                fill: none;
                pointer-events: stroke;
                cursor: pointer;
            }

            .connections-layer path.connection-hit:hover + .connection-line {
                stroke-width: 3;
                filter: drop-shadow(0 0 2px var(--accent-color));
            }

            /* Removed previous hover effect as we use connection-hit now */

            .connections-layer marker polygon {
                fill: var(--accent-color);
            }

            .nodes-layer {
                position: absolute;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                z-index: 2;
                pointer-events: none; /* Let clicks pass to canvas/nodes */
            }

            .etl-node {
                pointer-events: all; /* Re-enable for nodes */
            }

            /* Nodes */
            .etl-node {
                position: absolute;
                min-width: 160px;
                background: var(--bg-secondary);
                border: 2px solid var(--border-color);
                border-radius: 8px;
                cursor: move;
                user-select: none;
                box-shadow: 0 2px 8px rgba(0,0,0,0.15);
            }

            .etl-node:hover {
                box-shadow: 0 4px 12px rgba(0,0,0,0.25);
            }

            .etl-node.selected {
                border-color: var(--accent-color);
            }

            .etl-node.running {
                border-color: var(--warning-color);
                animation: pulse 1s infinite;
            }

            .etl-node.success {
                border-color: var(--success-color);
            }

            .etl-node.error {
                border-color: var(--error-color);
            }

            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.7; }
            }

            .node-type-indicator {
                height: 4px;
                border-radius: 6px 6px 0 0;
            }

            .etl-node.sql .node-type-indicator { background: #4CAF50; }
            .etl-node.python .node-type-indicator { background: #3776AB; }
            .etl-node.container .node-type-indicator { background: #FF9800; }
            .etl-node.export .node-type-indicator { background: #2196F3; }
            .etl-node.import .node-type-indicator { background: #9C27B0; }

            .node-content {
                padding: 10px 12px;
            }

            .node-header {
                display: flex;
                align-items: center;
                gap: 8px;
                margin-bottom: 4px;
            }

            .node-icon {
                font-size: 1.2em;
            }

            .node-name {
                font-weight: 500;
                font-size: 0.9em;
            }

            .node-type {
                font-size: 0.75em;
                color: var(--text-muted);
                text-transform: uppercase;
            }

            .node-connectors {
                position: relative;
            }

            .node-actions {
                position: absolute;
                top: -8px;
                right: -8px;
                display: none;
            }

            .etl-node:hover .node-actions,
            .etl-node.selected .node-actions {
                display: block;
            }

            .node-delete-btn {
                width: 20px;
                height: 20px;
                border-radius: 50%;
                background: var(--error-color);
                color: white;
                border: 2px solid var(--bg-secondary);
                font-size: 12px;
                line-height: 16px;
                cursor: pointer;
                display: flex;
                align-items: center;
                justify-content: center;
            }

            .node-delete-btn:hover {
                transform: scale(1.2);
            }

            .connector {
                width: 14px;
                height: 14px;
                background: var(--accent-color);
                border: 2px solid var(--bg-secondary);
                border-radius: 50%;
                position: absolute;
                cursor: crosshair;
                z-index: 10;
            }

            .connector.input {
                left: -9px;
                top: 50%;
                transform: translateY(-50%);
            }

            .connector.output {
                right: -9px;
                top: 50%;
                transform: translateY(-50%);
            }

            .connector:hover {
                transform: translateY(-50%) scale(1.3);
            }

            /* Properties Panel */
            .properties-panel {
                width: 250px;
                background: var(--bg-secondary);
                border-left: 1px solid var(--border-color);
                padding: 12px;
                overflow-y: auto;
            }

            .properties-panel h3 {
                margin: 0 0 12px 0;
                font-size: 0.9em;
                color: var(--text-muted);
                text-transform: uppercase;
            }

            .properties-panel .placeholder {
                color: var(--text-muted);
                font-style: italic;
            }

            .property-group {
                margin-bottom: 16px;
            }

            .property-label {
                display: block;
                font-size: 0.8em;
                color: var(--text-muted);
                margin-bottom: 4px;
            }

            .property-value {
                font-size: 0.9em;
                word-break: break-word;
            }

            .property-code {
                font-family: monospace;
                font-size: 0.8em;
                background: var(--bg-primary);
                border: 1px solid var(--border-color);
                border-radius: 4px;
                padding: 6px 8px;
                margin: 4px 0 0 0;
                white-space: pre-wrap;
                word-break: break-all;
                max-height: 80px;
                overflow-y: auto;
            }

            .configure-btn {
                width: 100%;
                margin-top: 16px;
                padding: 8px 12px;
                background: var(--accent-color);
                color: white;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 0.85em;
            }

            .configure-btn:hover {
                background: var(--accent-hover);
            }

            /* Connection line being drawn */
            .temp-connection {
                stroke: var(--accent-color);
                stroke-width: 2;
                stroke-dasharray: 5,5;
                fill: none;
                pointer-events: none;
            }
        `;
    }

    private _getScript(project: EtlProject): string {
        return `
            const vscode = acquireVsCodeApi();
            
            // State
            let project = ${JSON.stringify(project)};
            let selectedNodeId = null;
            let isDragging = false;
            let dragOffset = { x: 0, y: 0 };
            let isConnecting = false;
            let connectionStart = null;
            let tempLine = null;
            
            // Zoom & Pan state
            let scale = 1;
            let pan = { x: 0, y: 0 };
            let isPanning = false;
            let panStart = { x: 0, y: 0 };

            // Node icons
            const nodeIcons = {
                sql: '📜',
                python: '🐍',
                container: '📦',
                export: '📤',
                import: '📥'
            };

            // Initialize
            // Initialize
            function init() {
                renderNodes();
                renderConnections();
                setupToolboxDrag();
                setupCanvasDrop();
                setupToolbarButtons();
                setupKeyboardEvents();
                setupZoomPan();
                
                // Request current project state
                vscode.postMessage({ type: 'getProject' });
            }

            function updateTransform() {
                const wrapper = document.getElementById('zoom-wrapper');
                if (wrapper) {
                    wrapper.style.transform = \`translate(\${pan.x}px, \${pan.y}px) scale(\${scale})\`;
                }
            }

            function setupZoomPan() {
                const container = document.getElementById('canvas');
                
                // Wheel zoom
                container.addEventListener('wheel', (e) => {
                    // Zoom with Ctrl+Wheel or just Wheel (standard for node editors often)
                    if (e.ctrlKey || true) { 
                        e.preventDefault();
                        const zoomSensitivity = 0.001;
                        const delta = -e.deltaY * zoomSensitivity;
                        const newScale = Math.min(Math.max(0.1, scale + delta), 5);
                        scale = newScale;
                        updateTransform();
                    }
                });

                // Pan
                container.addEventListener('mousedown', (e) => {
                    // Start pan if clicking purely on background/container
                    // Check against container or the wrapper or the SVG definition layer
                    if (e.target === container || e.target.id === 'nodes-container' || e.target.id === 'zoom-wrapper' || e.target.id === 'connections-svg') {
                        isPanning = true;
                        panStart = { x: e.clientX - pan.x, y: e.clientY - pan.y };
                        container.style.cursor = 'grabbing';
                    }
                });

                window.addEventListener('mousemove', (e) => {
                    if (isPanning) {
                        pan.x = e.clientX - panStart.x;
                        pan.y = e.clientY - panStart.y;
                        updateTransform();
                    }
                });

                window.addEventListener('mouseup', () => {
                    if (isPanning) {
                        isPanning = false;
                        container.style.cursor = 'grab';
                    }
                });
            }

            // Keyboard events for Delete key
            function setupKeyboardEvents() {
                document.addEventListener('keydown', (e) => {
                    if (e.key === 'Delete' && selectedNodeId) {
                        vscode.postMessage({ type: 'confirmRemoveNode', payload: selectedNodeId });
                    }
                });
            }

            // Handle messages from extension
            window.addEventListener('message', (event) => {
                const message = event.data;
                switch (message.type) {
                    case 'projectUpdate':
                        project = message.payload;
                        renderNodes();
                        renderConnections();
                        break;
                    case 'nodeStatusUpdate':
                        updateNodeStatus(message.payload.nodeId, message.payload.status);
                        break;
                    case 'executionStarted':
                        document.getElementById('btn-run').style.display = 'none';
                        document.getElementById('btn-stop').style.display = 'inline-block';
                        document.getElementById('status').textContent = 'Running...';
                        break;
                    case 'executionEnded':
                        document.getElementById('btn-run').style.display = 'inline-block';
                        document.getElementById('btn-stop').style.display = 'none';
                        document.getElementById('status').textContent = message.payload?.status || '';
                        break;
                }
            });

            function renderNodes() {
                const container = document.getElementById('nodes-container');
                container.innerHTML = '';

                for (const node of project.nodes) {
                    const el = createNodeElement(node);
                    container.appendChild(el);
                }
            }

            function createNodeElement(node) {
                const el = document.createElement('div');
                el.className = 'etl-node ' + node.type + (selectedNodeId === node.id ? ' selected' : '');
                el.id = 'node-' + node.id;
                el.style.left = node.position.x + 'px';
                el.style.top = node.position.y + 'px';

                el.innerHTML = \`
                    <div class="node-actions">
                        <button class="node-delete-btn" data-node="\${node.id}" title="Delete task">×</button>
                    </div>
                    <div class="node-type-indicator"></div>
                    <div class="node-content">
                        <div class="node-header">
                            <span class="node-icon">\${nodeIcons[node.type] || '📋'}</span>
                            <span class="node-name">\${escapeHtml(node.name)}</span>
                        </div>
                        <div class="node-type">\${node.type}</div>
                    </div>
                    <div class="node-connectors">
                        <div class="connector input" data-node="\${node.id}" data-type="input"></div>
                        <div class="connector output" data-node="\${node.id}" data-type="output"></div>
                    </div>
                \`;

                setupNodeEvents(el, node);
                return el;
            }

            function escapeHtml(text) {
                const div = document.createElement('div');
                div.textContent = text;
                return div.innerHTML;
            }

            function setupNodeEvents(el, node) {
                // Delete button click
                const deleteBtn = el.querySelector('.node-delete-btn');
                if (deleteBtn) {
                    deleteBtn.addEventListener('click', (e) => {
                        e.stopPropagation();
                        vscode.postMessage({ type: 'confirmRemoveNode', payload: node.id });
                    });
                }

                // Drag
                el.addEventListener('mousedown', (e) => {
                    if (e.target.classList.contains('connector')) return;
                    if (e.target.classList.contains('node-delete-btn')) return;
                    
                    selectedNodeId = node.id;
                    isDragging = true;
                    
                    const rect = el.getBoundingClientRect();
                    dragOffset.x = (e.clientX - rect.left) / scale;
                    dragOffset.y = (e.clientY - rect.top) / scale;
                    
                    // Update selection
                    document.querySelectorAll('.etl-node').forEach(n => n.classList.remove('selected'));
                    el.classList.add('selected');
                    
                    updatePropertiesPanel(node);
                });

                // Double-click to configure
                el.addEventListener('dblclick', () => {
                    vscode.postMessage({ type: 'configureNode', payload: node.id });
                });

                // Connector events
                el.querySelectorAll('.connector').forEach(conn => {
                    conn.addEventListener('mousedown', (e) => {
                        e.stopPropagation();
                        if (conn.dataset.type === 'output') {
                            isConnecting = true;
                            connectionStart = node.id;
                            createTempLine(e);
                        }
                    });

                    conn.addEventListener('mouseup', (e) => {
                        if (isConnecting && conn.dataset.type === 'input' && connectionStart !== node.id) {
                            vscode.postMessage({
                                type: 'addConnection',
                                payload: { from: connectionStart, to: node.id }
                            });
                        }
                        endConnection();
                    });
                });

                // Context menu for delete
                el.addEventListener('contextmenu', (e) => {
                    e.preventDefault();
                    vscode.postMessage({ type: 'confirmRemoveNode', payload: node.id });
                });
            }

            function renderConnections() {
                const svg = document.getElementById('connections-svg');
                // Clear existing paths but keep defs
                svg.querySelectorAll('path').forEach(p => p.remove());

                for (const conn of project.connections) {
                    const fromNode = project.nodes.find(n => n.id === conn.from);
                    const toNode = project.nodes.find(n => n.id === conn.to);
                    
                    if (fromNode && toNode) {
                        const path = createConnectionPath(fromNode, toNode, conn.id);
                        svg.appendChild(path);
                    }
                }
            }

            function createConnectionPath(from, to, connId) {
                const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
                
                // Calculate positions
                const x1 = from.position.x + 160; // Right side of node
                const y1 = from.position.y + 35;  // Middle of node
                const x2 = to.position.x;          // Left side of node
                const y2 = to.position.y + 35;    // Middle of node

                // Create bezier curve
                const midX = (x1 + x2) / 2;
                const d = 'M ' + x1 + ' ' + y1 + ' C ' + midX + ' ' + y1 + ', ' + midX + ' ' + y2 + ', ' + x2 + ' ' + y2;
                
                // 1. Hit path (invisible, wider)
                const hitPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                hitPath.setAttribute('d', d);
                hitPath.setAttribute('class', 'connection-hit');
                hitPath.dataset.connectionId = connId; // For debugging
                
                hitPath.addEventListener('click', (e) => {
                    e.stopPropagation();
                    vscode.postMessage({ type: 'confirmRemoveConnection', payload: connId });
                });

                // 2. Visible path
                const visiblePath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                visiblePath.setAttribute('d', d);
                visiblePath.setAttribute('marker-end', 'url(#arrowhead)');
                visiblePath.setAttribute('class', 'connection-line');

                group.appendChild(hitPath);
                group.appendChild(visiblePath);
                
                return group;
            }

            function setupToolboxDrag() {
                document.querySelectorAll('.toolbox-item').forEach(item => {
                    item.addEventListener('dragstart', (e) => {
                        e.dataTransfer.setData('nodeType', item.dataset.type);
                    });
                });
            }

            function setupCanvasDrop() {
                const canvas = document.getElementById('canvas');
                
                canvas.addEventListener('dragover', (e) => {
                    e.preventDefault();
                });
                
                canvas.addEventListener('drop', (e) => {
                    e.preventDefault();
                    const nodeType = e.dataTransfer.getData('nodeType');
                    if (nodeType) {
                        const rect = canvas.getBoundingClientRect();
                        const x = (e.clientX - rect.left - pan.x) / scale;
                        const y = (e.clientY - rect.top - pan.y) / scale;
                        
                        vscode.postMessage({
                            type: 'addNode',
                            payload: { type: nodeType, position: { x, y } }
                        });
                    }
                });

                // Mouse move for dragging and connecting
                document.addEventListener('mousemove', (e) => {
                    if (isDragging && selectedNodeId) {
                        const canvas = document.getElementById('canvas');
                        const rect = canvas.getBoundingClientRect();
                        const x = (e.clientX - rect.left - pan.x) / scale - dragOffset.x;
                        const y = (e.clientY - rect.top - pan.y) / scale - dragOffset.y;
                        
                        // Update node position in DOM
                        const nodeEl = document.getElementById('node-' + selectedNodeId);
                        if (nodeEl) {
                            nodeEl.style.left = Math.max(0, x) + 'px';
                            nodeEl.style.top = Math.max(0, y) + 'px';
                        }
                        
                        // Update connections
                        const node = project.nodes.find(n => n.id === selectedNodeId);
                        if (node) {
                            node.position = { x: Math.max(0, x), y: Math.max(0, y) };
                            renderConnections();
                        }
                    }
                    
                    if (isConnecting && tempLine) {
                        updateTempLine(e);
                    }
                });

                document.addEventListener('mouseup', (e) => {
                    if (isDragging && selectedNodeId) {
                        // Save position
                        const node = project.nodes.find(n => n.id === selectedNodeId);
                        if (node) {
                            vscode.postMessage({
                                type: 'updateNodePosition',
                                payload: { nodeId: selectedNodeId, position: node.position }
                            });
                        }
                    }
                    
                    isDragging = false;
                    endConnection();
                });

                // Deselect on canvas click
                canvas.addEventListener('click', (e) => {
                    if (e.target === canvas || e.target.classList.contains('nodes-layer')) {
                        selectedNodeId = null;
                        document.querySelectorAll('.etl-node').forEach(n => n.classList.remove('selected'));
                        updatePropertiesPanel(null);
                    }
                });
            }

            function createTempLine(e) {
                const svg = document.getElementById('connections-svg');
                tempLine = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                tempLine.classList.add('temp-connection');
                svg.appendChild(tempLine);
                updateTempLine(e);
            }

            function updateTempLine(e) {
                if (!tempLine || !connectionStart) return;
                
                const fromNode = project.nodes.find(n => n.id === connectionStart);
                if (!fromNode) return;
                
                const canvas = document.getElementById('canvas');
                const rect = canvas.getBoundingClientRect();
                
                const x1 = fromNode.position.x + 160;
                const y1 = fromNode.position.y + 35;
                const x2 = (e.clientX - rect.left - pan.x) / scale;
                const y2 = (e.clientY - rect.top - pan.y) / scale;
                
                const midX = (x1 + x2) / 2;
                tempLine.setAttribute('d', 'M ' + x1 + ' ' + y1 + ' C ' + midX + ' ' + y1 + ', ' + midX + ' ' + y2 + ', ' + x2 + ' ' + y2);
            }

            function endConnection() {
                isConnecting = false;
                connectionStart = null;
                if (tempLine) {
                    tempLine.remove();
                    tempLine = null;
                }
            }

            function setupToolbarButtons() {
                document.getElementById('btn-new').addEventListener('click', () => {
                    vscode.postMessage({ type: 'newProject' });
                });
                
                document.getElementById('btn-open').addEventListener('click', () => {
                    vscode.postMessage({ type: 'loadProject' });
                });
                
                document.getElementById('btn-save').addEventListener('click', () => {
                    vscode.postMessage({ type: 'saveProject' });
                });
                
                document.getElementById('btn-run').addEventListener('click', () => {
                    vscode.postMessage({ type: 'runProject' });
                });
                
                document.getElementById('btn-stop').addEventListener('click', () => {
                    vscode.postMessage({ type: 'stopProject' });
                });
            }

            function updatePropertiesPanel(node) {
                const content = document.getElementById('properties-content');
                
                if (!node) {
                    content.innerHTML = '<p class="placeholder">Select a task to view properties</p>';
                    return;
                }
                
                let configHtml = '';
                const config = node.config;
                
                switch (node.type) {
                    case 'sql':
                        const queryPreview = config.query 
                            ? (config.query.length > 100 ? config.query.substring(0, 100) + '...' : config.query)
                            : '(not configured)';
                        configHtml = \`
                            <div class="property-group">
                                <span class="property-label">Connection</span>
                                <div class="property-value">\${escapeHtml(config.connection || 'Active connection')}</div>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Query</span>
                                <pre class="property-code">\${escapeHtml(queryPreview)}</pre>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Timeout</span>
                                <div class="property-value">\${config.timeout ? config.timeout + 's' : '(default)'}</div>
                            </div>
                        \`;
                        break;
                    case 'python':
                        let scriptInfo = '(not configured)';
                        if (config.scriptPath) {
                            const pathParts = config.scriptPath.split(/[/\\\\]/);
                            scriptInfo = '📁 ' + pathParts[pathParts.length - 1];
                        } else if (config.script) {
                            const preview = config.script.length > 50 ? config.script.substring(0, 50) + '...' : config.script;
                            scriptInfo = preview;
                        }
                        configHtml = \`
                            <div class="property-group">
                                <span class="property-label">Script Source</span>
                                <div class="property-value">\${config.scriptPath ? 'File' : (config.script ? 'Inline' : 'Not set')}</div>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Script</span>
                                <pre class="property-code">\${escapeHtml(scriptInfo)}</pre>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Interpreter</span>
                                <div class="property-value">\${escapeHtml(config.pythonPath || 'Auto-detect')}</div>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Timeout</span>
                                <div class="property-value">\${config.timeout ? config.timeout + 's' : '(default)'}</div>
                            </div>
                        \`;
                        break;
                    case 'export':
                        const outPath = config.outputPath 
                            ? config.outputPath.split(/[/\\\\]/).pop() 
                            : '(not configured)';
                        const exportQueryPreview = config.query 
                            ? (config.query.length > 50 ? config.query.substring(0, 50) + '...' : config.query)
                            : '(uses previous output)';
                        configHtml = \`
                            <div class="property-group">
                                <span class="property-label">Format</span>
                                <div class="property-value">\${config.format?.toUpperCase() || '(not set)'}</div>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Output File</span>
                                <div class="property-value">📄 \${escapeHtml(outPath)}</div>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Query</span>
                                <pre class="property-code">\${escapeHtml(exportQueryPreview)}</pre>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Timeout</span>
                                <div class="property-value">\${config.timeout ? config.timeout + 's' : '(default)'}</div>
                            </div>
                        \`;
                        break;
                    case 'import':
                        const inPath = config.inputPath 
                            ? config.inputPath.split(/[/\\\\]/).pop() 
                            : '(not configured)';
                        configHtml = \`
                            <div class="property-group">
                                <span class="property-label">Format</span>
                                <div class="property-value">\${config.format?.toUpperCase() || 'Auto-detect'}</div>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Input File</span>
                                <div class="property-value">📄 \${escapeHtml(inPath)}</div>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Target Table</span>
                                <div class="property-value">\${escapeHtml(config.targetTable || '(not set)')}</div>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Create Table</span>
                                <div class="property-value">\${config.createTable !== false ? 'Yes (if needed)' : 'No'}</div>
                            </div>
                            <div class="property-group">
                                <span class="property-label">Timeout</span>
                                <div class="property-value">\${config.timeout ? config.timeout + 's' : '(default)'}</div>
                            </div>
                        \`;
                        break;
                    case 'container':
                        const childCount = (config.childNodes || []).length;
                        configHtml = \`
                            <div class="property-group">
                                <span class="property-label">Child Tasks</span>
                                <div class="property-value">\${childCount} task(s)</div>
                            </div>
                        \`;
                        break;
                }
                
                content.innerHTML = \`
                    <div class="property-group">
                        <span class="property-label">ID</span>
                        <div class="property-value" style="font-family: monospace; font-size: 0.75em;">\${node.id}</div>
                    </div>
                    <div class="property-group">
                        <span class="property-label">Name</span>
                        <div class="property-value">\${escapeHtml(node.name)}</div>
                    </div>
                    <div class="property-group">
                        <span class="property-label">Type</span>
                        <div class="property-value">\${node.type.toUpperCase()}</div>
                    </div>
                    <div class="property-group">
                        <span class="property-label">Position</span>
                        <div class="property-value">(\${node.position.x}, \${node.position.y})</div>
                    </div>
                    <hr style="border: none; border-top: 1px solid var(--border-color); margin: 12px 0;">
                    <h4 style="margin: 8px 0; font-size: 0.85em; color: var(--text-muted);">⚙️ Configuration</h4>
                    \${configHtml}
                    <button class="configure-btn" data-node-id="\${node.id}">
                        ✏️ Edit Configuration
                    </button>
                \`;
                
                // Attach event listener to configure button (CSP-safe)
                const configBtn = content.querySelector('.configure-btn');
                if (configBtn) {
                    configBtn.addEventListener('click', () => {
                        vscode.postMessage({ type: 'configureNode', payload: node.id });
                    });
                }
            }

            function updateNodeStatus(nodeId, status) {
                const nodeEl = document.getElementById('node-' + nodeId);
                if (nodeEl) {
                    nodeEl.classList.remove('running', 'success', 'error', 'pending', 'skipped');
                    nodeEl.classList.add(status);
                }
                
                document.getElementById('status').textContent = status === 'running' 
                    ? 'Running...' 
                    : (status === 'error' ? 'Error!' : '');
            }

            // Start
            init();
        `;
    }

    private _getNonce(): string {
        let text = '';
        const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        for (let i = 0; i < 32; i++) {
            text += possible.charAt(Math.floor(Math.random() * possible.length));
        }
        return text;
    }

    public dispose(): void {
        EtlDesignerView.currentPanel = undefined;
        this._panel.dispose();
        while (this._disposables.length) {
            const d = this._disposables.pop();
            if (d) {
                d.dispose();
            }
        }
    }
}
