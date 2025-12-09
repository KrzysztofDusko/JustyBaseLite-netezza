// Query History View JavaScript
// @ts-check

/**
 * @typedef {{ postMessage: (msg: any) => void, setState: (state: any) => void, getState: () => any }} VsCodeApi
 */

/** @type {VsCodeApi} */
// @ts-ignore
const vscode = acquireVsCodeApi();

/** @type {Array<{id: string, query: string, host: string, database: string, schema: string, timestamp: string, is_favorite: boolean, tags?: string, description?: string}>} */
let allHistory = [];

/**
 * Initialize the query history view
 */
function init() {
    console.log('queryHistory webview: init -> requesting history');
    vscode.postMessage({ type: 'getHistory' });
    attachEventListeners();
}

/**
 * Update the stats display
 * @param {{totalEntries: number, totalFileSizeMB: string}} stats
 */
function updateStats(stats) {
    const statsEl = document.getElementById('stats');
    if (statsEl) {
        statsEl.textContent = `${stats.totalEntries} entries · ${stats.totalFileSizeMB} MB`;
    }
}

/**
 * Render the history list
 * @param {typeof allHistory} history
 */
function renderHistory(history) {
    const container = document.getElementById('historyContainer');
    if (!container) return;

    if (history.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📜</div>
                <div>No query history found</div>
            </div>
        `;
        return;
    }

    container.innerHTML = history.map(entry => `
        <div class="history-item">
            <div class="history-item-header">
                <div class="history-item-time">${formatTimestamp(entry.timestamp)}</div>
                <div class="history-item-actions">
                    <button class="action-btn ${entry.is_favorite ? 'favorite' : ''}" data-action="favorite" data-id="${escapeHtml(entry.id)}">${entry.is_favorite ? '⭐' : '☆'}</button>
                    <button class="action-btn" data-action="edit" data-id="${escapeHtml(entry.id)}">✏️</button>
                    <button class="action-btn" data-action="execute" data-id="${escapeHtml(entry.id)}">▶️ Run</button>
                    <button class="action-btn" data-action="copy" data-id="${escapeHtml(entry.id)}">📋 Copy</button>
                    <button class="action-btn delete" data-action="delete" data-id="${escapeHtml(entry.id)}">🗑️</button>
                </div>
            </div>
            <div class="history-item-meta">
                ${entry.connectionName ? `<span>🔌 ${escapeHtml(entry.connectionName)}</span>` : ''}
                <span>🖥️ ${escapeHtml(entry.host)}</span>
                <span>🗃️ ${escapeHtml(entry.database)}</span>
                <span>📁 ${escapeHtml(entry.schema)}</span>
                ${entry.tags ? `<span class="tags">🏷️ ${escapeHtml(entry.tags)}</span>` : ''}
            </div>
            ${entry.description ? `<div class="history-item-description">${escapeHtml(entry.description)}</div>` : ''}
            <div class="history-item-query" title="${escapeHtml(entry.query)}">${escapeHtml(entry.query)}</div>
        </div>
    `).join('');
}

/**
 * Format timestamp for display
 * @param {string} timestamp
 * @returns {string}
 */
function formatTimestamp(timestamp) {
    const date = new Date(timestamp);
    return date.toLocaleString();
}

/**
 * Escape HTML to prevent XSS
 * @param {string} text
 * @returns {string}
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Request history refresh
 */
function refreshHistory() {
    vscode.postMessage({ type: 'getHistory' });
}

/**
 * Request to clear all history
 */
function clearAllHistory() {
    vscode.postMessage({ type: 'clearAll' });
}

/**
 * Delete a specific entry
 * @param {string} id
 */
function deleteEntry(id) {
    vscode.postMessage({ type: 'deleteEntry', id: id });
}

/**
 * Copy query to clipboard
 * @param {string} id
 */
function copyQuery(id) {
    const entry = allHistory.find(e => e.id === id);
    if (entry) {
        vscode.postMessage({ type: 'copyQuery', query: entry.query });
    }
}

/**
 * Execute a query
 * @param {string} id
 */
function executeQuery(id) {
    const entry = allHistory.find(e => e.id === id);
    if (entry) {
        vscode.postMessage({ type: 'executeQuery', query: entry.query });
    }
}

/**
 * Show favorites only
 */
function showFavorites() {
    vscode.postMessage({ type: 'showFavoritesOnly' });
}

/**
 * Show all history
 */
function showAll() {
    vscode.postMessage({ type: 'getHistory' });
}

/**
 * Toggle favorite status
 * @param {string} id
 */
function toggleFavorite(id) {
    vscode.postMessage({ type: 'toggleFavorite', id: id });
}

/**
 * Edit an entry
 * @param {string} id
 */
function editEntry(id) {
    const entry = allHistory.find(e => e.id === id);
    if (entry) {
        vscode.postMessage({
            type: 'requestEdit',
            id: id
        });
    }
}

/**
 * Filter by a specific tag
 * @param {string} tag
 */
function filterByTag(tag) {
    vscode.postMessage({ type: 'filterByTag', tag: tag });
}

/**
 * Attach all event listeners
 */
function attachEventListeners() {
    const refreshBtn = document.getElementById('refreshBtn');
    const clearBtn = document.getElementById('clearAllBtn');
    const showAllBtn = document.getElementById('showAllBtn');
    const showFavoritesBtn = document.getElementById('showFavoritesBtn');
    const container = document.getElementById('historyContainer');
    const searchInput = document.getElementById('searchInput');

    // Search functionality
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            const target = /** @type {HTMLInputElement} */ (e.target);
            const searchTerm = target.value.toLowerCase();
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
    }

    // Toolbar buttons
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

    // History container - action buttons
    if (container) {
        container.addEventListener('click', (e) => {
            let target = e.target;
            // Handle text nodes (e.g. clicking on the emoji)
            if (!(target instanceof Element)) {
                target = /** @type {Node} */ (target).parentElement;
            }
            if (!target) return;

            const btn = /** @type {Element} */ (target).closest('button');
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
            const target = /** @type {Element} */ (e.target);
            const tagElement = target.closest('.tags');
            if (tagElement) {
                const tagText = (tagElement.textContent || '').replace('🏷️ ', '').trim();
                const tags = tagText.split(',').map(t => t.trim());
                vscode.postMessage({
                    type: 'requestTagFilter',
                    tags: tags
                });
            }
        });
    }
}

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

// Initialize on load
window.addEventListener('load', () => {
    init();
});
