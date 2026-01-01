// Query History View JavaScript
// @ts-check

/**
 * @typedef {{ postMessage: (msg: any) => void, setState: (state: any) => void, getState: () => any }} VsCodeApi
 */

/** @type {VsCodeApi} */
// @ts-ignore
const vscode = acquireVsCodeApi();

/** @type {Array<{id: string, query: string, host: string, database: string, schema: string, timestamp: string, is_favorite: boolean, connectionName?: string, tags?: string, description?: string}>} */
let allHistory = [];
let isLoading = false;
let isEndOfList = false;

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
 * @param {{totalEntries: number, totalFileSizeMB: string, activeEntries: number, archivedEntries: number}} stats
 */
function updateStats(stats) {
    const statsEl = document.getElementById('stats');
    if (statsEl) {
        // Show active + archive info
        let text = `${stats.activeEntries} active`;
        if (stats.archivedEntries > 0) {
            text += ` · ${stats.archivedEntries} archived`;
        }
        text += ` · ${stats.totalFileSizeMB} MB`;
        statsEl.textContent = text;
    }
}

/**
 * Render the history list
 * @param {typeof allHistory} history
 * @param {boolean} append
 */
function renderHistory(history, append = false) {
    const container = document.getElementById('historyContainer');
    if (!container) return;

    if (!append) {
        container.innerHTML = '';
        if (history.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">📜</div>
                    <div>No query history found</div>
                </div>
            `;
            return;
        }
    }

    const html = history.map(entry => `
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

    if (append) {
        container.insertAdjacentHTML('beforeend', html);
    } else {
        container.innerHTML = html;
        container.scrollTop = 0; // Reset scroll on full new render
    }
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
    if (!text) return '';
    if (typeof text !== 'string') text = String(text);
    return text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

/**
 * Request history refresh
 */
function refreshHistory() {
    vscode.postMessage({ type: 'refresh' });
}

function loadMore() {
    if (isLoading || isEndOfList) return;
    isLoading = true;
    const indicator = document.getElementById('loadingIndicator');
    if (indicator) indicator.style.display = 'block';
    vscode.postMessage({ type: 'loadMore' });
}

function searchArchive() {
    const searchInput = /** @type {HTMLInputElement} */ (document.getElementById('searchInput'));
    const term = searchInput.value.trim();
    if (!term) return;

    // Clear current view to show we are searching archive
    const container = document.getElementById('historyContainer');
    if (container) container.innerHTML = '<div class="empty-state"><div>Searching Archive...</div></div>';

    vscode.postMessage({ type: 'searchArchive', term: term });
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

    // Search functionality with Archive option
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            const target = /** @type {HTMLInputElement} */ (e.target);
            const searchTerm = target.value.toLowerCase();

            // If empty, reset
            if (!searchTerm) {
                renderHistory(allHistory);
                document.getElementById('searchArchiveBtn')?.remove();
                return;
            }

            // Local filter first
            const filtered = allHistory.filter(entry =>
                entry.query.toLowerCase().includes(searchTerm) ||
                entry.host.toLowerCase().includes(searchTerm) ||
                (entry.database && entry.database.toLowerCase().includes(searchTerm)) ||
                (entry.schema && entry.schema.toLowerCase().includes(searchTerm))
            );
            renderHistory(filtered);

            // Add "Search Archive" button check
            let archiveBtn = document.getElementById('searchArchiveBtn');
            if (!archiveBtn) {
                archiveBtn = document.createElement('div');
                archiveBtn.id = 'searchArchiveBtn';
                archiveBtn.className = 'search-archive-prompt';
                archiveBtn.innerHTML = `<button class="secondary">🔍 Search in Archive</button>`;
                archiveBtn.onclick = () => searchArchive();

                // insert after search input's container? Or just append to toolbar top
                const toolbarTop = document.querySelector('.toolbar-top');
                if (toolbarTop) toolbarTop.appendChild(archiveBtn);
            }
        });
    }

    // Infinite Scroll
    if (container) {
        container.addEventListener('scroll', () => {
            const { scrollTop, scrollHeight, clientHeight } = container;
            if (scrollTop + clientHeight >= scrollHeight - 50) {
                // Near bottom
                loadMore();
            }
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
            isLoading = false;
            document.getElementById('loadingIndicator').style.display = 'none';
            updateStats(message.stats);

            if (message.reset) {
                allHistory = message.history;
                isEndOfList = false; // reset end flag
                renderHistory(allHistory, false);
            } else {
                // Append
                if (message.history && message.history.length > 0) {
                    allHistory = [...allHistory, ...message.history];
                    renderHistory(message.history, true); // Render only new items? No, renderHistory supports append.
                } else {
                    isEndOfList = true; // No more data
                }
            }
            break;

        case 'archiveSearchResults':
            isLoading = false;
            allHistory = message.history; // Replace current view with archive results
            updateStats(message.stats);
            renderHistory(allHistory, false);
            break;

        case 'entryDeleted':
            const id = message.id;
            allHistory = allHistory.filter(e => e.id !== id);
            renderHistory(allHistory, false);
            break;

        case 'updateStats':
            updateStats(message.stats);
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
