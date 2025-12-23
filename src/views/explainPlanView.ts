import * as vscode from 'vscode';

/**
 * Parsed node from EXPLAIN output
 */
interface ExplainNode {
    id: number;
    type: string; // e.g., "Sequential Scan", "Nested Loop", "Aggregate"
    table?: string; // table name if applicable
    alias?: string; // table alias
    cost: { start: number; end: number };
    rows: number;
    width: number;
    confidence: number;
    indent: number; // tree depth
    children: ExplainNode[];
    raw: string; // original line
    warnings: PlanWarning[];
    // VERBOSE specific
    projections?: string[];
    restrictions?: string[];
    nodeDetails?: string[]; // Additional details from VERBOSE output
}

interface PlanWarning {
    type: 'critical' | 'warning' | 'info';
    message: string;
    icon: string;
}

interface ParsedPlan {
    nodes: ExplainNode[];
    rawText: string;
    isVerbose: boolean;
    totalCost: number;
    warnings: PlanWarning[];
}

/**
 * Parse EXPLAIN output text into structured data
 */
export function parseExplainOutput(text: string): ParsedPlan {
    const lines = text.split('\n').filter(l => l.trim());
    const isVerbose = text.includes('QUERY VERBOSE PLAN:') || text.includes('Node 1.');

    // Remove NOTICE prefix and headers
    const cleanLines = lines
        .map(l => l.replace(/^NOTICE:\s*/i, '').trim())
        .filter(l => l && !l.startsWith('QUERY PLAN') && !l.startsWith('QUERY VERBOSE'));

    const nodes: ExplainNode[] = [];
    const warnings: PlanWarning[] = [];
    let nodeId = 0;
    let totalCost = 0;

    // Parse tree-structured PLANTEXT
    const planTextStart = cleanLines.findIndex(l => l.includes('PLANTEXT:'));
    const planLines =
        planTextStart >= 0
            ? cleanLines.slice(planTextStart + 1).filter(l => !l.startsWith('(') && !l.includes('locus='))
            : cleanLines;

    const nodeStack: ExplainNode[] = [];

    for (const line of planLines) {
        if (!line.trim()) continue;

        // Skip metadata lines
        if (line.startsWith('(xpath_') || line.includes('locus=')) continue;

        // Calculate indent level
        const leadingSpaces = line.match(/^(\s*)/)?.[1]?.length || 0;
        const indent = Math.floor(leadingSpaces / 3);

        // Parse node type and details
        // Pattern: "l: NodeType (cost=X..Y rows=N width=W conf=C)"
        // or just "NodeType (cost=X..Y rows=N width=W conf=C)"
        const nodeMatch = line.match(
            /^[\s]*(?:[lr]:\s*)?([\w\s-]+?)(?:\s+table\s+"(\w+)")?(?:\s+{[^}]*})?\s*\(cost=([\d.]+)\.\.([\d.]+)\s+rows=(\d+)\s+width=(\d+)\s+conf=([\d.]+)\)/i
        );

        if (nodeMatch) {
            const [, typeRaw, tableName, costStart, costEnd, rows, width, conf] = nodeMatch;
            const type = typeRaw.trim();

            const node: ExplainNode = {
                id: ++nodeId,
                type,
                table: tableName,
                cost: { start: parseFloat(costStart), end: parseFloat(costEnd) },
                rows: parseInt(rows),
                width: parseInt(width),
                confidence: parseFloat(conf),
                indent,
                children: [],
                raw: line.trim(),
                warnings: []
            };

            // Detect performance issues
            detectWarnings(node);
            warnings.push(...node.warnings);

            // Track total cost
            if (node.cost.end > totalCost) {
                totalCost = node.cost.end;
            }

            // Build tree structure
            while (nodeStack.length > 0 && nodeStack[nodeStack.length - 1].indent >= indent) {
                nodeStack.pop();
            }

            if (nodeStack.length > 0) {
                nodeStack[nodeStack.length - 1].children.push(node);
            } else {
                nodes.push(node);
            }
            nodeStack.push(node);
        }
    }

    return {
        nodes,
        rawText: text,
        isVerbose,
        totalCost,
        warnings
    };
}

/**
 * Detect performance warnings for a node
 */
function detectWarnings(node: ExplainNode): void {
    // Sequential Scan on large table (> 500k rows)
    if (node.type.includes('Sequential Scan') && node.rows > 500000) {
        node.warnings.push({
            type: 'warning',
            message: `Sequential Scan on ${node.table || 'table'} (${node.rows.toLocaleString()} rows) - consider adding an index`,
            icon: '⚠️'
        });
    }

    // Nested Loop with high row count
    if (node.type.includes('Nested Loop') && node.rows > 100000) {
        node.warnings.push({
            type: 'critical',
            message: `Nested Loop producing ${node.rows.toLocaleString()} rows - potential performance issue`,
            icon: '🔴'
        });
    }

    // Very high cost (> 10,000,000,000)
    if (node.cost.end > 10000000000) {
        node.warnings.push({
            type: 'critical',
            message: `Very high estimated cost: ${node.cost.end.toExponential(2)}`,
            icon: '🔴'
        });
    } else if (node.cost.end > 10000000) {
        // High cost (> 10,000,000)
        node.warnings.push({
            type: 'warning',
            message: `High estimated cost: ${node.cost.end.toLocaleString()}`,
            icon: '⚠️'
        });
    }

    // Low confidence
    if (node.confidence === 0) {
        node.warnings.push({
            type: 'info',
            message: 'Low confidence estimate (conf=0) - statistics may be outdated',
            icon: 'ℹ️'
        });
    }
}

/**
 * Get node type color for visualization
 */
function getNodeColor(type: string): string {
    if (type.includes('Sequential Scan')) return '#e74c3c'; // Red
    if (type.includes('Index Scan')) return '#27ae60'; // Green
    if (type.includes('Nested Loop')) return '#e67e22'; // Orange
    if (type.includes('Hash Join')) return '#3498db'; // Blue
    if (type.includes('Merge Join')) return '#9b59b6'; // Purple
    if (type.includes('Aggregate')) return '#1abc9c'; // Teal
    if (type.includes('Group')) return '#16a085'; // Dark Teal
    if (type.includes('Sort')) return '#f39c12'; // Yellow
    if (type.includes('Limit')) return '#95a5a6'; // Gray
    if (type.includes('Sub-query')) return '#34495e'; // Dark Gray
    return '#7f8c8d'; // Default gray
}

/**
 * WebviewPanel for Explain Plan visualization
 */
export class ExplainPlanView {
    public static readonly viewType = 'netezza.explainPlan';
    private _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private _disposables: vscode.Disposable[] = [];

    private constructor(panel: vscode.WebviewPanel, extensionUri: vscode.Uri, plan: ParsedPlan, query: string) {
        this._panel = panel;
        this._extensionUri = extensionUri;

        this._update(plan, query);

        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);

        this._panel.webview.onDidReceiveMessage(
            message => {
                switch (message.command) {
                    case 'copyPlan':
                        vscode.env.clipboard.writeText(plan.rawText);
                        vscode.window.showInformationMessage('Plan copied to clipboard');
                        return;
                }
            },
            null,
            this._disposables
        );
    }

    public static createOrShow(extensionUri: vscode.Uri, plan: ParsedPlan, query: string) {
        const column = vscode.window.activeTextEditor ? vscode.ViewColumn.Beside : undefined;

        const panel = vscode.window.createWebviewPanel(
            ExplainPlanView.viewType,
            'Query Execution Plan',
            column || vscode.ViewColumn.One,
            {
                enableScripts: true,
                localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'media')],
                retainContextWhenHidden: true
            }
        );

        new ExplainPlanView(panel, extensionUri, plan, query);
    }

    public dispose() {
        this._panel.dispose();
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) {
                x.dispose();
            }
        }
    }

    private _update(plan: ParsedPlan, query: string) {
        this._panel.title = 'Query Execution Plan';
        this._panel.webview.html = this._getHtmlForWebview(plan, query);
    }

    private _getHtmlForWebview(plan: ParsedPlan, query: string): string {
        const styleUri = this._panel.webview.asWebviewUri(
            vscode.Uri.joinPath(this._extensionUri, 'media', 'explainPlan.css')
        );

        const nonce = getNonce();
        const queryEscaped = escapeHtml(query);

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${this._panel.webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}';">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="${styleUri}" rel="stylesheet">
    <title>Query Execution Plan</title>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>Query Execution Plan</h2>
            <div class="header-actions">
                <button id="copyBtn" class="btn">📋 Copy Plan</button>
                <button id="expandAllBtn" class="btn">➕ Expand All</button>
                <button id="collapseAllBtn" class="btn">➖ Collapse All</button>
            </div>
        </div>

        ${plan.warnings.length > 0
                ? `
        <div class="warnings-summary">
            <h3>⚠️ Performance Issues Detected (${plan.warnings.length})</h3>
            <ul>
                ${plan.warnings.map(w => `<li class="warning-${w.type}">${w.icon} ${w.message}</li>`).join('')}
            </ul>
        </div>
        `
                : `
        <div class="no-warnings">
            <span>✅ No obvious performance issues detected</span>
        </div>
        `
            }

        <div class="stats-bar">
            <div class="stat">
                <span class="stat-label">Total Cost:</span>
                <span class="stat-value">${plan.totalCost > 1000000 ? plan.totalCost.toExponential(2) : plan.totalCost.toLocaleString()}</span>
            </div>
            <div class="stat">
                <span class="stat-label">Nodes:</span>
                <span class="stat-value">${countNodes(plan.nodes)}</span>
            </div>
        </div>

        <div class="query-section">
            <h3>Query</h3>
            <pre class="query-text">${queryEscaped}</pre>
        </div>

        <div class="plan-section">
            <h3>Execution Plan</h3>
            <div id="planTree" class="plan-tree">
                ${renderPlanTree(plan.nodes)}
            </div>
        </div>

        <div class="raw-section">
            <details>
                <summary>Raw Plan Output</summary>
                <pre class="raw-plan">${escapeHtml(plan.rawText)}</pre>
            </details>
        </div>
    </div>

    <script nonce="${nonce}">
        const vscode = acquireVsCodeApi();
        
        document.getElementById('copyBtn').addEventListener('click', () => {
            vscode.postMessage({ command: 'copyPlan' });
        });

        document.getElementById('expandAllBtn').addEventListener('click', () => {
            document.querySelectorAll('.node-children').forEach(el => el.style.display = 'block');
            document.querySelectorAll('.toggle-btn').forEach(el => el.textContent = '▼');
        });

        document.getElementById('collapseAllBtn').addEventListener('click', () => {
            document.querySelectorAll('.node-children').forEach(el => el.style.display = 'none');
            document.querySelectorAll('.toggle-btn').forEach(el => el.textContent = '▶');
        });

        document.querySelectorAll('.toggle-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const node = e.target.closest('.plan-node');
                const children = node.querySelector('.node-children');
                if (children) {
                    const isHidden = children.style.display === 'none';
                    children.style.display = isHidden ? 'block' : 'none';
                    e.target.textContent = isHidden ? '▼' : '▶';
                }
            });
        });
    </script>
</body>
</html>`;
    }
}

function renderPlanTree(nodes: ExplainNode[], depth: number = 0): string {
    return nodes
        .map(node => {
            const hasChildren = node.children.length > 0;
            const color = getNodeColor(node.type);
            const warningBadges = node.warnings
                .map(w => `<span class="warning-badge warning-${w.type}" title="${w.message}">${w.icon}</span>`)
                .join('');

            return `
        <div class="plan-node" style="--node-color: ${color}">
            <div class="node-header">
                ${hasChildren ? '<span class="toggle-btn">▼</span>' : '<span class="toggle-spacer"></span>'}
                <span class="node-type" style="background-color: ${color}">${node.type}</span>
                ${node.table ? `<span class="node-table">${node.table}${node.alias ? ` (${node.alias})` : ''}</span>` : ''}
                ${warningBadges}
            </div>
            <div class="node-metrics">
                <span class="metric"><b>Cost:</b> ${formatCost(node.cost.start)}..${formatCost(node.cost.end)}</span>
                <span class="metric"><b>Rows:</b> ${node.rows.toLocaleString()}</span>
                <span class="metric"><b>Width:</b> ${node.width}</span>
                <span class="metric ${node.confidence === 0 ? 'low-conf' : ''}"><b>Conf:</b> ${node.confidence}%</span>
            </div>
            ${hasChildren ? `<div class="node-children">${renderPlanTree(node.children, depth + 1)}</div>` : ''}
        </div>`;
        })
        .join('');
}

function formatCost(cost: number): string {
    if (cost > 1000000) {
        return cost.toExponential(1);
    }
    return cost.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

function countNodes(nodes: ExplainNode[]): number {
    return nodes.reduce((sum, node) => sum + 1 + countNodes(node.children), 0);
}

function escapeHtml(text: string): string {
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function getNonce(): string {
    let text = '';
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    for (let i = 0; i < 32; i++) {
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
}
