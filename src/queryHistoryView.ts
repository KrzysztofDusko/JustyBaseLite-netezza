import * as vscode from 'vscode';
import { QueryHistoryManager, QueryHistoryEntry } from './queryHistoryManager';

export class QueryHistoryView implements vscode.WebviewViewProvider {
    public static readonly viewType = 'netezza.queryHistory';
    private _view?: vscode.WebviewView;

    constructor(
        private readonly _extensionUri: vscode.Uri,
        private readonly _context: vscode.ExtensionContext
    ) { }

    public resolveWebviewView(
        webviewView: vscode.WebviewView,
        context: vscode.WebviewViewResolveContext,
        _token: vscode.CancellationToken
    ) {
        this._view = webviewView;

        webviewView.webview.options = {
            enableScripts: true,
            localResourceRoots: [this._extensionUri]
        };

        webviewView.webview.html = this._getHtmlForWebview(webviewView.webview);

        // Send initial history to webview immediately (in case the webview load message is missed)
        this.sendHistoryToWebview();

        // Handle messages from the webview
        webviewView.webview.onDidReceiveMessage(async (data) => {
            switch (data.type) {
                case 'refresh':
                    this.refresh();
                    break;
                case 'clearAll':
                    await this.clearAllHistory();
                    break;
                case 'deleteEntry':
                    await this.deleteEntry(data.id, data.query);
                    break;
                case 'copyQuery':
                    await vscode.env.clipboard.writeText(data.query);
                    vscode.window.showInformationMessage('Query copied to clipboard');
                    break;
                case 'executeQuery':
                    await this.executeQuery(data.query);
                    break;
                case 'getHistory':
                    await this.sendHistoryToWebview();
                    break;
                case 'toggleFavorite':
                    await this.toggleFavorite(data.id);
                    break;
                case 'updateEntry':
                    await this.updateEntry(data.id, data.tags, data.description);
                    break;
                case 'requestEdit':
                    await this.requestEdit(data.id);
                    break;
                case 'requestTagFilter':
                    await this.requestTagFilter(data.tags);
                    break;
                case 'showFavoritesOnly':
                    await this.sendFavoritesToWebview();
                    break;
                case 'filterByTag':
                    await this.sendFilteredByTagToWebview(data.tag);
                    break;
            }
        });
    }

    public refresh() {
        if (this._view) {
            this.sendHistoryToWebview();
        }
    }

    private async sendHistoryToWebview() {
        if (!this._view) {
            return;
        }

        const historyManager = new QueryHistoryManager(this._context);
        const history = await historyManager.getHistory();
        const stats = await historyManager.getStats();

        console.log('QueryHistoryView: sending history to webview, entries=', history.length);
        this._view.webview.postMessage({
            type: 'historyData',
            history: history,
            stats: stats
        });
    }

    private async clearAllHistory() {
        const confirm = await vscode.window.showWarningMessage(
            'Are you sure you want to clear all query history?',
            { modal: true },
            'Clear All'
        );

        if (confirm === 'Clear All') {
            const historyManager = new QueryHistoryManager(this._context);
            await historyManager.clearHistory();
            this.refresh();
            vscode.window.showInformationMessage('Query history cleared');
        }
    }

    private async deleteEntry(id: string, query?: string) {
        const queryText = query ? `: ${query.substring(0, 50)}${query.length > 50 ? '...' : ''}` : '';
        const answer = await vscode.window.showWarningMessage(
            `Are you sure you want to delete this query${queryText}?`,
            { modal: true },
            'Delete'
        );

        if (answer === 'Delete') {
            const historyManager = new QueryHistoryManager(this._context);
            await historyManager.deleteEntry(id);
            this.refresh();
        }
    }

    private async executeQuery(query: string) {
        // Create a new untitled document with the query
        const doc = await vscode.workspace.openTextDocument({
            content: query,
            language: 'sql'
        });
        await vscode.window.showTextDocument(doc);

        // Optionally execute it immediately
        // vscode.commands.executeCommand('netezza.runQuery');
    }

    private async toggleFavorite(id: string) {
        const historyManager = new QueryHistoryManager(this._context);
        await historyManager.toggleFavorite(id);
        this.refresh();
    }

    private async updateEntry(id: string, tags?: string, description?: string) {
        const historyManager = new QueryHistoryManager(this._context);
        await historyManager.updateEntry(id, tags, description);
        this.refresh();
        vscode.window.showInformationMessage('Entry updated successfully');
    }

    private async requestEdit(id: string) {
        const historyManager = new QueryHistoryManager(this._context);
        const history = await historyManager.getHistory();
        const entry = history.find(e => e.id === id);
        
        if (!entry) {
            vscode.window.showErrorMessage('Entry not found');
            return;
        }

        // Get tags from user
        const tags = await vscode.window.showInputBox({
            prompt: 'Enter tags (comma separated)',
            value: entry.tags || '',
            placeHolder: 'tag1, tag2, tag3'
        });

        if (tags === undefined) {
            return; // User cancelled
        }

        // Get description from user
        const description = await vscode.window.showInputBox({
            prompt: 'Enter description',
            value: entry.description || '',
            placeHolder: 'Description for this query'
        });

        if (description === undefined) {
            return; // User cancelled
        }

        await this.updateEntry(id, tags, description);
    }

    private async requestTagFilter(tags: string[]) {
        if (tags.length === 1) {
            // Only one tag, filter by it directly
            await this.sendFilteredByTagToWebview(tags[0]);
        } else if (tags.length > 1) {
            // Multiple tags, let user choose
            const selectedTag = await vscode.window.showQuickPick(tags, {
                placeHolder: 'Filter by which tag?'
            });
            
            if (selectedTag) {
                await this.sendFilteredByTagToWebview(selectedTag);
            }
        }
    }

    private async sendFavoritesToWebview() {
        if (!this._view) {
            return;
        }

        const historyManager = new QueryHistoryManager(this._context);
        const favorites = await historyManager.getFavorites();
        const stats = await historyManager.getStats();

        this._view.webview.postMessage({
            type: 'historyData',
            history: favorites,
            stats: stats,
            filter: 'favorites'
        });
    }

    private async sendFilteredByTagToWebview(tag: string) {
        if (!this._view) {
            return;
        }

        const historyManager = new QueryHistoryManager(this._context);
        const entries = await historyManager.getByTag(tag);
        const stats = await historyManager.getStats();

        this._view.webview.postMessage({
            type: 'historyData',
            history: entries,
            stats: stats,
            filter: `tag: ${tag}`
        });
    }

    private _getHtmlForWebview(webview: vscode.Webview): string {
        const nonce = getNonce();

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}';">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Query History</title>
    <style>
        body {
            padding: 0;
            margin: 0;
            font-family: var(--vscode-font-family);
            font-size: var(--vscode-font-size);
            color: var(--vscode-foreground);
            background-color: var(--vscode-editor-background);
        }

        .toolbar {
            display: flex;
            flex-direction: column;
            gap: 8px;
            padding: 8px;
            border-bottom: 1px solid var(--vscode-panel-border);
            background-color: var(--vscode-sideBar-background);
        }

        .toolbar-top {
            display: flex;
            align-items: center;
            gap: 8px;
            width: 100%;
        }

        .toolbar-buttons {
            display: flex;
            gap: 3px;
            flex-wrap: wrap;
        }
        
        .toolbar-buttons button {
            padding: 2px 5px;
            font-size: 10px;
            line-height: 14px;
        }

        .stats {
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
        }

        input[type="search"] {
            background: var(--vscode-input-background);
            color: var(--vscode-input-foreground);
            border: 1px solid var(--vscode-input-border);
            padding: 4px 8px;
            font-size: 12px;
            flex: 1;
            min-width: 150px;
        }

        button {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 4px;
            background-color: var(--vscode-button-secondaryBackground);
            color: var(--vscode-button-secondaryForeground);
            border: 1px solid var(--vscode-contrastBorder, transparent);
            padding: 2px 6px;
            cursor: pointer;
            border-radius: 2px;
            font-family: var(--vscode-font-family);
            font-size: 11px;
            line-height: 16px;
        }

        button:hover {
            background-color: var(--vscode-button-secondaryHoverBackground);
        }

        /* If we need a primary button override */
        button.primary {
            background-color: var(--vscode-button-background);
            color: var(--vscode-button-foreground);
        }

        button.primary:hover {
            background-color: var(--vscode-button-hoverBackground);
        }

        .history-container {
            overflow-y: auto;
            height: calc(100vh - 50px);
        }

        .history-item {
            padding: 12px;
            border-bottom: 1px solid var(--vscode-panel-border);
            cursor: pointer;
        }

        .history-item:hover {
            background-color: var(--vscode-list-hoverBackground);
        }

        .history-item:hover .action-btn {
            opacity: 0.7;
        }

        .history-item-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }

        .history-item-time {
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
        }

        .history-item-actions {
            display: flex;
            gap: 4px;
        }

        .action-btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 2px 6px;
            font-size: 11px;
            background-color: transparent;
            color: var(--vscode-foreground);
            border: none;
            border-radius: 2px;
            cursor: pointer;
            font-family: var(--vscode-font-family);
            opacity: 0.8;
            transition: opacity 0.2s, background-color 0.2s;
        }

        .action-btn:hover {
            opacity: 1;
            background-color: var(--vscode-button-secondaryBackground);
        }

        .action-btn.delete {
            /* Optional: keep it same as others, or give it a slight redness on hover? */
            /* For consistency, let's keep it same, maybe just specific hover if needed. */
            /* Using standard secondary colors for now to match request "not fitting". */
        }

        .action-btn.delete:hover {
            background-color: var(--vscode-button-destructiveHoverBackground);
            color: var(--vscode-button-destructiveForeground);
        }

        .action-btn.favorite {
            color: gold;
            opacity: 1;
        }

        .tags {
            background: var(--vscode-badge-background);
            color: var(--vscode-badge-foreground);
            padding: 2px 6px;
            border-radius: 10px;
            font-size: 10px;
            cursor: pointer;
        }

        .history-item-description {
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
            margin: 4px 0;
            font-style: italic;
        }

        .history-item-meta {
            font-size: 12px;
            color: var(--vscode-descriptionForeground);
            margin-bottom: 4px;
        }

        .history-item-meta span {
            margin-right: 12px;
        }

        .history-item-query {
            font-family: var(--vscode-editor-font-family);
            font-size: 12px;
            background: var(--vscode-textCodeBlock-background);
            padding: 6px;
            border-radius: 3px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .empty-state {
            text-align: center;
            padding: 40px 20px;
            color: var(--vscode-descriptionForeground);
        }

        .empty-state-icon {
            font-size: 48px;
            margin-bottom: 16px;
        }
    </style>
</head>
<body>
    <div class="toolbar">
        <div class="toolbar-top">
            <input type="search" id="searchInput" placeholder="Search queries..." />
            <span class="stats" id="stats">Loading...</span>
        </div>
        <div class="toolbar-buttons">
            <button class="secondary" id="showAllBtn">📜 All</button>
            <button class="secondary" id="showFavoritesBtn">⭐ Favorites</button>
            <button class="secondary" id="refreshBtn">↻ Refresh</button>
            <button class="secondary" id="clearAllBtn">🗑️ Clear All</button>
        </div>
    </div>
    <div class="history-container" id="historyContainer">
        <div class="empty-state">
            <div class="empty-state-icon">📜</div>
            <div>No query history yet</div>
        </div>
    </div>

    <script nonce="${nonce}">
        const vscode = acquireVsCodeApi();
        let allHistory = [];

        // Request history on load
        window.addEventListener('load', () => {
            console.log('queryHistory webview: load -> requesting history');
            vscode.postMessage({ type: 'getHistory' });
        });

        // Listen for messages from extension
        window.addEventListener('message', event => {
            const message = event.data;
            console.log('queryHistory webview: received message', message);
            switch (message.type) {
                case 'historyData':
                    allHistory = message.history;
                    updateStats(message.stats);
                    renderHistory(allHistory);
                    break;
                case 'debug':
                    console.log('queryHistory debug:', message.msg, message);
                    break;
            }
        });

        // Search functionality
        document.getElementById('searchInput').addEventListener('input', (e) => {
            const searchTerm = e.target.value.toLowerCase();
            if (!searchTerm) {
                renderHistory(allHistory);
                return;
            }

            const filtered = allHistory.filter(entry => 
                entry.query.toLowerCase().includes(searchTerm) ||
                entry.host.toLowerCase().includes(searchTerm) ||
                entry.database.toLowerCase().includes(searchTerm) ||
                entry.schema.toLowerCase().includes(searchTerm)
            );
            renderHistory(filtered);
        });

        function updateStats(stats) {
            const statsEl = document.getElementById('stats');
            statsEl.textContent = \`\${stats.totalEntries} entries · \${stats.totalFileSizeMB} MB\`;
        }

        function renderHistory(history) {
            const container = document.getElementById('historyContainer');
            
            if (history.length === 0) {
                container.innerHTML = \`
                    <div class="empty-state">
                        <div class="empty-state-icon">📜</div>
                        <div>No query history found</div>
                    </div>
                \`;
                return;
            }

            container.innerHTML = history.map(entry => \`
                <div class="history-item">
                    <div class="history-item-header">
                        <div class="history-item-time">\${formatTimestamp(entry.timestamp)}</div>
                        <div class="history-item-actions">
                            <button class="action-btn \${entry.is_favorite ? 'favorite' : ''}" data-action="favorite" data-id="\${escapeHtml(entry.id)}">\${entry.is_favorite ? '⭐' : '☆'}</button>
                            <button class="action-btn" data-action="edit" data-id="\${escapeHtml(entry.id)}">✏️</button>
                            <button class="action-btn" data-action="execute" data-id="\${escapeHtml(entry.id)}">▶️ Run</button>
                            <button class="action-btn" data-action="copy" data-id="\${escapeHtml(entry.id)}">📋 Copy</button>
                            <button class="action-btn delete" data-action="delete" data-id="\${escapeHtml(entry.id)}">🗑️</button>
                        </div>
                    </div>
                    <div class="history-item-meta">
                        <span>🖥️ \${escapeHtml(entry.host)}</span>
                        <span>🗃️ \${escapeHtml(entry.database)}</span>
                        <span>📁 \${escapeHtml(entry.schema)}</span>
                        \${entry.tags ? \`<span class="tags">🏷️ \${escapeHtml(entry.tags)}</span>\` : ''}
                    </div>
                    \${entry.description ? \`<div class="history-item-description">\${escapeHtml(entry.description)}</div>\` : ''}
                    <div class="history-item-query" title="\${escapeHtml(entry.query)}">\${escapeHtml(entry.query)}</div>
                </div>
            \`).join('');
        }

        function formatTimestamp(timestamp) {
            const date = new Date(timestamp);
            return date.toLocaleString();
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        function refreshHistory() {
            vscode.postMessage({ type: 'getHistory' });
        }

        function clearAllHistory() {
            vscode.postMessage({ type: 'clearAll' });
        }

        function deleteEntry(id) {
            vscode.postMessage({ type: 'deleteEntry', id: id });
        }

        function copyQuery(id) {
            const entry = allHistory.find(e => e.id === id);
            if (entry) {
                vscode.postMessage({ type: 'copyQuery', query: entry.query });
            }
        }

        function executeQuery(id) {
            const entry = allHistory.find(e => e.id === id);
            if (entry) {
                vscode.postMessage({ type: 'executeQuery', query: entry.query });
            }
        }

        function showFavorites() {
            vscode.postMessage({ type: 'showFavoritesOnly' });
        }

        function showAll() {
            vscode.postMessage({ type: 'getHistory' });
        }

        function toggleFavorite(id) {
            vscode.postMessage({ type: 'toggleFavorite', id: id });
        }

        function editEntry(id) {
            const entry = allHistory.find(e => e.id === id);
            if (entry) {
                vscode.postMessage({ 
                    type: 'requestEdit', 
                    id: id
                });
            }
        }

        function filterByTag(tag) {
            vscode.postMessage({ type: 'filterByTag', tag: tag });
        }

        // Attach event listeners (no inline handlers to satisfy CSP)
        window.addEventListener('load', () => {
            const refreshBtn = document.getElementById('refreshBtn');
            const clearBtn = document.getElementById('clearAllBtn');
            const showAllBtn = document.getElementById('showAllBtn');
            const showFavoritesBtn = document.getElementById('showFavoritesBtn');
            const container = document.getElementById('historyContainer');

            if (refreshBtn) {
                refreshBtn.addEventListener('click', (e) => { e.preventDefault(); refreshHistory(); });
            }
            if (clearBtn) {
                clearBtn.addEventListener('click', (e) => { e.preventDefault(); clearAllHistory(); });
            }
            if (showAllBtn) {
                showAllBtn.addEventListener('click', (e) => { e.preventDefault(); showAll(); });
            }
            if (showFavoritesBtn) {
                showFavoritesBtn.addEventListener('click', (e) => { e.preventDefault(); showFavorites(); });
            }

            if (container) {
                container.addEventListener('click', (e) => {
                    let target = e.target;
                    // Handle text nodes (e.g. clicking on the emoji)
                    if (!(target instanceof Element)) {
                        target = target.parentElement;
                    }
                    if (!target) return;
                    
                    const btn = target.closest('button');
                    if (!btn) return;
                    const action = btn.getAttribute('data-action');
                    const id = btn.getAttribute('data-id');
                    if (!action || !id) return;

                    if (action === 'execute') {
                        executeQuery(id);
                    } else if (action === 'copy') {
                        copyQuery(id);
                    } else if (action === 'delete') {
                        // Find entry to pass query text for confirmation
                        const entry = allHistory.find(e => e.id === id);
                        if (entry) {
                            vscode.postMessage({ type: 'deleteEntry', id: id, query: entry.query });
                        }
                    } else if (action === 'favorite') {
                        toggleFavorite(id);
                    } else if (action === 'edit') {
                        editEntry(id);
                    }
                });

                // Handle tag clicks
                container.addEventListener('click', (e) => {
                    const tagElement = e.target.closest('.tags');
                    if (tagElement) {
                        const tagText = tagElement.textContent.replace('🏷️ ', '').trim();
                        const tags = tagText.split(',').map(t => t.trim());
                        vscode.postMessage({ 
                            type: 'requestTagFilter', 
                            tags: tags 
                        });
                    }
                });
            }
        });
    </script>
</body>
</html>`;
    }
}

function getNonce() {
    let text = '';
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    for (let i = 0; i < 32; i++) {
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
}
