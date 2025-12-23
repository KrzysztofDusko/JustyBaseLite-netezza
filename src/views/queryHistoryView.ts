import * as vscode from 'vscode';
import { QueryHistoryManager } from '../core/queryHistoryManager';

export class QueryHistoryView implements vscode.WebviewViewProvider {
    public static readonly viewType = 'netezza.queryHistory';
    private _view?: vscode.WebviewView;

    constructor(
        private readonly _extensionUri: vscode.Uri,
        private readonly _context: vscode.ExtensionContext
    ) { }

    public resolveWebviewView(
        webviewView: vscode.WebviewView,
        _context: vscode.WebviewViewResolveContext,
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
        webviewView.webview.onDidReceiveMessage(async data => {
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
        // Get URIs for external resources
        const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'queryHistory.css'));
        const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'queryHistory.js'));

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource}; script-src ${webview.cspSource};">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Query History</title>
    <link href="${styleUri}" rel="stylesheet">
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

    <script src="${scriptUri}"></script>
</body>
</html>`;
    }
}
