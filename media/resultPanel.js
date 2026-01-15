// Enhanced resultPanel.js with Excel-like column filtering
// Add CSS for Console View
const style = document.createElement('style');
style.textContent = `
    .console-wrapper {
        background-color: var(--vscode-editor-background);
        color: var(--vscode-editor-foreground);
        font-family: var(--vscode-editor-font-family), 'Consolas', 'Courier New', monospace;
        font-size: var(--vscode-editor-font-size, 13px);
        overflow-y: auto;
        padding: 10px;
        height: 100%;
        box-sizing: border-box;
    }
    .console-view {
        display: flex;
        flex-direction: column;
    }
    .console-line {
        line-height: 1.5;
        white-space: pre-wrap;
        border-bottom: 1px solid var(--vscode-panel-border);
    }
    .console-line.separator {
        color: var(--vscode-textLink-foreground);
        font-weight: bold;
        margin-top: 10px;
        margin-bottom: 5px;
        border-bottom: 1px solid var(--vscode-textLink-foreground);
    }
    .console-time {
        color: var(--vscode-descriptionForeground);
        margin-right: 8px;
        user-select: none;
    }
    .console-msg {
        color: var(--vscode-editor-foreground);
    }
    .error-wrapper {
        padding: 20px;
        background-color: var(--vscode-editor-background);
        height: 100%;
        overflow: auto;
        box-sizing: border-box;
    }
    .error-view {
        border-left: 4px solid var(--vscode-errorForeground);
        background-color: var(--vscode-input-background);
        padding: 15px;
        margin: 10px 0;
        white-space: pre-wrap;
        font-family: var(--vscode-editor-font-family);
        color: var(--vscode-errorForeground);
    }
    .error-title {
        font-weight: bold;
        margin-bottom: 8px;
        font-size: 1.1em;
    }
    .error-sql {
        margin-top: 15px;
        padding-top: 10px;
        border-top: 1px solid var(--vscode-panel-border);
        opacity: 0.7;
        font-size: 0.9em;
    }
`;
document.head.appendChild(style);

// Entry point for resultPanelView.ts
function init() {
    try {
        renderSourceTabs();
        renderResultSetTabs();
        renderGrids();
        updateLoadingState();

        // Setup global keyboard shortcuts
        setupGlobalKeyboardShortcuts();

        // Setup message handler for streaming updates
        setupStreamingMessageHandler();

        // Setup cancel button handler
        setupCancelButton();

        // Switch to the correct grid if it's not the default (0)
        // This handles the initial focus sent by extension in ViewData
        if (activeGridIndex !== 0) {
            switchToResultSet(activeGridIndex);
        }
    } catch (e) {
        showError('Initialization error: ' + e.message);
        console.error(e);
    }
}

function updateLoadingState() {
    const overlay = document.getElementById('loadingOverlay');
    if (!overlay) return;

    const isActiveExecuting = window.executingSources && window.executingSources.has(window.activeSource);
    if (isActiveExecuting) {
        overlay.classList.add('visible');
    } else {
        overlay.classList.remove('visible');
    }
}

function setupCancelButton() {
    const cancelBtn = document.getElementById('cancelQueryBtn');
    if (cancelBtn) {
        cancelBtn.addEventListener('click', () => {
            const currentRowCounts = window.resultSets ? window.resultSets.map(rs => rs.data.length) : [];
            vscode.postMessage({
                command: 'cancelQuery',
                sourceUri: window.activeSource,
                currentRowCounts: currentRowCounts
            });
            // Optimistically update UI
            handleCancelExecution({ sourceUri: window.activeSource });
        });
    }
}

// Handle messages from extension for streaming updates
function setupStreamingMessageHandler() {
    window.addEventListener('message', event => {
        const message = event.data;

        switch (message.command) {
            case 'cancelExecution':
                handleCancelExecution(message);
                break;
            case 'appendRows':
                handleAppendRows(message);
                break;
            case 'streamingComplete':
                handleStreamingComplete(message);
                break;
            case 'switchToResultSet':
                if (typeof message.resultSetIndex === 'number') {
                    switchToResultSet(message.resultSetIndex);
                }
                break;
            case 'copySelection':
                if (window.copySelection) {
                    window.copySelection(true); // Copy with headers
                }
                break;
        }
    });
}

function handleCancelExecution(message) {
    // If the cancelled source is the active one, mark current result sets as cancelled
    if (window.activeSource === message.sourceUri) {
        if (window.resultSets) {
            window.resultSets.forEach(rs => {
                rs.isCancelled = true;
                // Optional: Show visual indicator of cancellation?
                if (rs.limitReached === undefined) rs.limitReached = true;
            });
        }

        // Remove from executing Set if present
        if (window.executingSources && window.executingSources.has(message.sourceUri)) {
            window.executingSources.delete(message.sourceUri);

            // Update UI spinner in tab
            const tabs = document.querySelectorAll('.source-tab');
            tabs.forEach(tab => {
                const span = tab.querySelector('span');
                if (span && span.title === message.sourceUri) {
                    const spinner = tab.querySelector('.source-tab-spinner');
                    if (spinner) spinner.remove();
                }
            });
        }

        // Always ensure the spinner is removed and overlay hidden
        updateLoadingState();
    }
}

// Handle incremental row append during streaming
function handleAppendRows(message) {
    const { resultSetIndex, rows, totalRows, isLastChunk, limitReached } = message;

    // Update the data in window.resultSets
    if (window.resultSets && window.resultSets[resultSetIndex]) {
        const rs = window.resultSets[resultSetIndex];

        // Check for cancellation
        if (rs.isCancelled) {
            return;
        }

        rs.data.push(...rows);
        rs.limitReached = limitReached;

        // Update search worker with new data
        if (searchWorker) {
            searchWorker.postMessage({
                command: 'appendData',
                id: resultSetIndex,
                rows: rows
            });
        }

        // Update the grid virtualizer count
        const grid = grids[resultSetIndex];
        if (grid && grid.tanTable) {
            // Update TanStack Table's data
            grid.tanTable.options.data = rs.data;

            // Recreate virtualizer with new count
            if (grid.createVirtualizer) {
                grid.createVirtualizer();
            }

            // Render only the table rows (not full re-render)
            if (grid.renderTableRows) {
                grid.renderTableRows();
            }
        }

        // Update row count display
        updateRowCountInfo(resultSetIndex, totalRows, limitReached);
    }
}

// Handle streaming completion
function handleStreamingComplete(message) {
    const { resultSetIndex, totalRows, limitReached } = message;

    // Final update of row count
    updateRowCountInfo(resultSetIndex, totalRows, limitReached);

    // Update search worker with complete data for accurate search
    if (searchWorker && window.resultSets && window.resultSets[resultSetIndex]) {
        searchWorker.postMessage({
            command: 'setData',
            id: resultSetIndex,
            data: window.resultSets[resultSetIndex].data,
            columns: window.resultSets[resultSetIndex].columns
        });
    }
}

// Update the row count info display
function updateRowCountInfo(resultSetIndex, totalRows, limitReached) {
    if (resultSetIndex === activeGridIndex) {
        const rowCountInfo = document.getElementById('rowCountInfo');
        if (rowCountInfo) {
            let text = `${totalRows.toLocaleString()} rows`;
            if (limitReached) {
                text += ' (limit reached)';
            }
            rowCountInfo.textContent = text;
        }
    }
}

// Entry point for resultView.ts
function initializeResultView(data, columns) {
    try {
        // Adapt to resultSets format
        window.resultSets = [{
            data: data,
            columns: columns.map((c, i) => ({ name: c.header, index: i, accessorKey: c.accessorKey }))
        }];
        window.sources = ['Result'];
        window.activeSource = 'Result';

        // Hide source tabs for single result view
        const sourceTabs = document.getElementById('sourceTabs');
        if (sourceTabs) sourceTabs.style.display = 'none';

        renderGrids();

        // Setup global keyboard shortcuts
        setupGlobalKeyboardShortcuts();
        updateLoadingState();
    } catch (e) {
        showError('Initialization error: ' + e.message);
        console.error(e);
    }
}

function renderSourceTabs() {
    const container = document.getElementById('sourceTabs');
    if (!container) return;

    container.innerHTML = '';
    if (!window.sources) return;

    window.sources.forEach(source => {
        const tab = createSourceTab(source);
        container.appendChild(tab);
    });
}

function renderResultSetTabs() {
    const container = document.getElementById('resultSetTabs');
    if (!container) return;

    container.innerHTML = '';

    if (!window.resultSets || window.resultSets.length === 0) {
        container.style.display = 'none';
        return;
    }

    container.style.display = 'flex';

    window.resultSets.forEach((rs, index) => {
        const tab = document.createElement('div');
        tab.className = 'result-set-tab' + (index === activeGridIndex ? ' active' : '');

        // Tab Text
        // Use name if provided, otherwise generate a label
        // For non-log results, subtract 1 from index to get 1-based naming (since log is at index 0)
        const textSpan = document.createElement('span');
        const defaultLabel = rs.isLog ? 'Logs' : `Result ${index}`;
        textSpan.textContent = rs.name || defaultLabel;
        tab.appendChild(textSpan);

        // Pin Button
        const pinSpan = document.createElement('span');
        pinSpan.className = 'pin-icon codicon codicon-pin';
        pinSpan.title = 'Pin this result';

        // Check if this specific result (source + index) is pinned
        const isPinned = window.pinnedResults && window.pinnedResults.some(p =>
            p.sourceUri === window.activeSource && p.resultSetIndex === index
        );

        if (isPinned) {
            pinSpan.classList.add('pinned');
            pinSpan.title = 'Unpin this result';
        } else {
            pinSpan.innerHTML = '📌'; // Fallback if codicon font not loaded, will rely on CSS opacity for "unpinned" look
            // But usually we want just the icon class. Let's use text content as backup
        }
        // Use text content for now as CSS handles the icon content or use an emoji if no font
        pinSpan.textContent = '📌';

        pinSpan.onclick = (e) => {
            e.stopPropagation();
            // Send toggle pin message
            vscode.postMessage({
                command: 'toggleResultPin',
                sourceUri: window.activeSource,
                resultSetIndex: index
            });
        };
        tab.appendChild(pinSpan);

        // Analyze Button
        const analyzeSpan = document.createElement('span');
        analyzeSpan.className = 'codicon codicon-graph';
        analyzeSpan.style.marginLeft = '6px';
        analyzeSpan.style.cursor = 'pointer';
        analyzeSpan.title = 'Analyze Data (Pivot Table)';
        analyzeSpan.textContent = '📊';

        analyzeSpan.onclick = (e) => {
            e.stopPropagation();
            // Send analyze message
            const rs = window.resultSets[index];
            if (rs) {
                vscode.postMessage({
                    command: 'analyze',
                    data: {
                        data: rs.data,
                        columns: rs.columns
                    }
                });
            }
        };
        tab.appendChild(analyzeSpan);

        // Close Button (x) for individual result
        const closeSpan = document.createElement('span');
        closeSpan.className = 'result-set-close-btn';
        closeSpan.textContent = '×';
        closeSpan.title = 'Close this result';
        closeSpan.style.marginLeft = '8px';
        closeSpan.style.cursor = 'pointer';
        closeSpan.style.opacity = '0.6';
        closeSpan.style.fontWeight = 'bold';
        closeSpan.style.fontSize = '16px';
        closeSpan.onmouseover = () => closeSpan.style.opacity = '1';
        closeSpan.onmouseout = () => closeSpan.style.opacity = '0.6';

        closeSpan.onclick = (e) => {
            e.stopPropagation();
            vscode.postMessage({
                command: 'closeResult',
                sourceUri: window.activeSource,
                resultSetIndex: index
            });
        };
        tab.appendChild(closeSpan);

        // Add context menu (right-click) to individual tabs
        tab.addEventListener('contextmenu', (e) => {
            e.preventDefault();
            e.stopPropagation();

            // Create context menu
            const menu = document.createElement('div');
            menu.className = 'context-menu';
            menu.style.position = 'fixed';
            menu.style.top = e.clientY + 'px';
            menu.style.left = e.clientX + 'px';
            menu.style.backgroundColor = 'var(--vscode-menu-background)';
            menu.style.border = '1px solid var(--vscode-menu-border)';
            menu.style.borderRadius = '4px';
            menu.style.boxShadow = '0 2px 8px rgba(0, 0, 0, 0.3)';
            menu.style.zIndex = '10000';
            menu.style.minWidth = '150px';

            // Close Result option
            const closeResultItem = document.createElement('div');
            closeResultItem.className = 'context-menu-item';
            closeResultItem.textContent = 'Close This Result';
            closeResultItem.style.padding = '8px 12px';
            closeResultItem.style.cursor = 'pointer';
            closeResultItem.style.color = 'var(--vscode-menu-foreground)';
            closeResultItem.style.fontSize = '12px';
            closeResultItem.style.userSelect = 'none';

            closeResultItem.addEventListener('mouseover', () => {
                closeResultItem.style.backgroundColor = 'var(--vscode-menu-selectionBackground)';
            });
            closeResultItem.addEventListener('mouseout', () => {
                closeResultItem.style.backgroundColor = 'transparent';
            });

            closeResultItem.addEventListener('click', () => {
                vscode.postMessage({
                    command: 'closeResult',
                    sourceUri: window.activeSource,
                    resultSetIndex: index
                });
                document.body.removeChild(menu);
            });

            menu.appendChild(closeResultItem);

            // Close All Results option
            const closeAllItem = document.createElement('div');
            closeAllItem.className = 'context-menu-item';
            closeAllItem.textContent = 'Close All Results';
            closeAllItem.style.padding = '8px 12px';
            closeAllItem.style.cursor = 'pointer';
            closeAllItem.style.color = 'var(--vscode-menu-foreground)';
            closeAllItem.style.fontSize = '12px';
            closeAllItem.style.userSelect = 'none';

            closeAllItem.addEventListener('mouseover', () => {
                closeAllItem.style.backgroundColor = 'var(--vscode-menu-selectionBackground)';
            });
            closeAllItem.addEventListener('mouseout', () => {
                closeAllItem.style.backgroundColor = 'transparent';
            });

            closeAllItem.addEventListener('click', () => {
                vscode.postMessage({
                    command: 'closeAllResults',
                    sourceUri: window.activeSource
                });
                document.body.removeChild(menu);
            });

            menu.appendChild(closeAllItem);
            document.body.appendChild(menu);

            // Close menu when clicking elsewhere
            const closeMenu = () => {
                if (document.body.contains(menu)) {
                    document.body.removeChild(menu);
                }
                document.removeEventListener('click', closeMenu);
                document.removeEventListener('contextmenu', closeMenu);
            };

            document.addEventListener('click', closeMenu);
            document.addEventListener('contextmenu', closeMenu);
        });

        tab.onclick = () => switchToResultSet(index);
        container.appendChild(tab);
    });
}

function switchToResultSet(index) {
    if (index < 0 || index >= grids.length) return;

    // Save state of current grid before switching (including scroll position)
    saveAllGridStates();

    activeGridIndex = index;

    // Notify extension of manual tab switch
    vscode.postMessage({
        command: 'switchResultSet',
        sourceUri: window.activeSource,
        resultSetIndex: index
    });

    updateControlsVisibility(index);

    // Update tab styling
    const tabs = document.querySelectorAll('.result-set-tab');
    tabs.forEach((tab, i) => {
        if (i === index) {
            tab.classList.add('active');
        } else {
            tab.classList.remove('active');
        }
    });

    // Show/hide grids
    const gridWrappers = document.querySelectorAll('.grid-wrapper');
    gridWrappers.forEach((wrapper, i) => {
        if (i === index) {
            wrapper.style.display = 'block';
        } else {
            wrapper.style.display = 'none';
        }
    });

    // Restore scroll position of newly active grid
    if (grids[index] && grids[index].executionTimestamp) {
        // Force render to ensure visualization is correct (sorting arrows, etc.)
        if (grids[index].render) {
            grids[index].render();
        }

        const savedState = getSavedStateFor(index, grids[index].executionTimestamp);
        if (savedState) {
            const wrapper = gridWrappers[index];
            if (wrapper && (savedState.scrollTop !== undefined || savedState.scrollLeft !== undefined)) {
                requestAnimationFrame(() => {
                    wrapper.scrollTop = savedState.scrollTop || 0;
                    wrapper.scrollLeft = savedState.scrollLeft || 0;
                });
            }
        }
    }

    // Update row count for active grid
    if (grids[index] && grids[index].updateRowCount) {
        grids[index].updateRowCount();
    }
}

function updateControlsVisibility(index) {
    const rs = window.resultSets[index];
    const isLog = rs && rs.isLog;
    const controls = document.querySelector('.controls');

    if (controls) {
        // Hide most controls when viewing logs, except the Clear Logs button
        const children = controls.children;
        for (let i = 0; i < children.length; i++) {
            const child = children[i];
            if (child.id === 'clearLogsBtn') {
                child.style.display = isLog ? 'inline-flex' : 'none';
            } else {
                // Hide other controls when in log view
                child.style.display = isLog ? 'none' : '';
            }
        }
    }

    const groupingPanel = document.getElementById('groupingPanel');
    if (groupingPanel) groupingPanel.style.display = isLog ? 'none' : '';
}

function createSourceTab(source) {
    const tab = document.createElement('div');
    tab.className = 'source-tab' + (source === activeSource ? ' active' : '');
    tab.style.display = 'flex';
    tab.style.alignItems = 'center';

    const filename = document.createElement('span');
    const parts = source.split(/[\\/]/);
    filename.textContent = parts[parts.length - 1] || source;
    filename.title = source;
    tab.appendChild(filename);

    if (window.executingSources && window.executingSources.has(source)) {
        const spinner = document.createElement('div');
        spinner.className = 'source-tab-spinner';
        spinner.title = 'SQL Execution in progress...';
        tab.appendChild(spinner);
    }

    const closeBtn = document.createElement('span');
    closeBtn.textContent = '×';
    closeBtn.className = 'close-tab';
    closeBtn.style.marginLeft = '8px';
    closeBtn.style.cursor = 'pointer';
    closeBtn.style.opacity = '0.6';
    closeBtn.style.fontWeight = 'bold';
    closeBtn.onmouseover = () => closeBtn.style.opacity = '1';
    closeBtn.onmouseout = () => closeBtn.style.opacity = '0.6';
    closeBtn.onclick = (e) => {
        e.stopPropagation();
        vscode.postMessage({ command: 'closeSource', sourceUri: source });
    };
    tab.appendChild(closeBtn);

    tab.onclick = () => {
        vscode.postMessage({ command: 'switchSource', sourceUri: source });
    };

    return tab;
}

function renderGrids() {
    const container = document.getElementById('gridContainer');
    if (!container) return;

    container.innerHTML = '';
    grids = [];

    if (!window.resultSets || window.resultSets.length === 0) {
        container.innerHTML = '<div style="padding: 20px; text-align: center; opacity: 0.6;">No results</div>';
        return;
    }

    const libs = validateRequiredLibraries();
    if (!libs) {
        return;
    }

    const { createTable, getCoreRowModel, getSortedRowModel, getFilteredRowModel, getGroupedRowModel, getExpandedRowModel } = libs.TableCore;

    // Render all result sets
    window.resultSets.forEach((rs, index) => {
        try {
            if (rs.isLog) {
                createLogConsole(rs, index, container);
            } else if (rs.isError) {
                createErrorView(rs, index, container);
            } else {
                createResultSetGrid(rs, index, container, createTable, getCoreRowModel, getSortedRowModel, getFilteredRowModel, getGroupedRowModel, getExpandedRowModel);
            }
        } catch (e) {
            console.error(`Error rendering grid ${index}:`, e);
            const wrapper = document.createElement('div');
            wrapper.style.padding = '20px';
            wrapper.style.color = 'red';
            wrapper.textContent = `Error rendering result set ${index + 1}: ${e.message}`;
            container.appendChild(wrapper);
            grids.push(null);
        }
    });

    // Ensure controls are set correctly for the initial active grid
    updateControlsVisibility(activeGridIndex);
}

function createLogConsole(rs, rsIndex, container) {
    const wrapper = document.createElement('div');
    wrapper.className = 'grid-wrapper console-wrapper';
    wrapper.style.display = rsIndex === activeGridIndex ? 'block' : 'none';
    wrapper.dataset.index = rsIndex;

    const consoleView = document.createElement('div');
    consoleView.className = 'console-view';

    // Render existing logs
    // rs.data structure is [[Time, Message], ...] based on startExecution
    if (rs.data && Array.isArray(rs.data)) {
        rs.data.forEach(row => {
            const line = document.createElement('div');
            line.className = 'console-line';

            if (row[1] === '') {
                // Spacer/Separator Logic if handled by startExecution pushing empty
                line.innerHTML = '&nbsp;';
            } else if (row[1] && row[1].startsWith('---')) {
                line.className += ' separator';
                line.textContent = `${row[0]} ${row[1]}`;
            } else {
                const timeSpan = document.createElement('span');
                timeSpan.className = 'console-time';
                timeSpan.textContent = `[${row[0]}] `;

                const msgSpan = document.createElement('span');
                msgSpan.className = 'console-msg';
                msgSpan.textContent = row[1];

                line.appendChild(timeSpan);
                line.appendChild(msgSpan);
            }
            consoleView.appendChild(line);
        });
    }

    wrapper.appendChild(consoleView);
    container.appendChild(wrapper);

    // "Grid" interface mock so switching tabs works
    const mockGrid = {
        executionTimestamp: rs.executionTimestamp,
        // Custom update function for when new logs arrive (re-render or append?)
        // Since we re-render everything on updateResults call, this function mainly handles
        // initial render scrolling.
        // But if init() is called again, we are re-rendereding from scratch.
    };
    grids.push(mockGrid);

    // Auto-scroll to bottom
    setTimeout(() => {
        wrapper.scrollTop = wrapper.scrollHeight;
    }, 0);
}

function validateRequiredLibraries() {
    if (typeof TableCore === 'undefined') {
        showError('TableCore is not defined. The TanStack Table library might not have loaded.');
        return null;
    }

    if (typeof VirtualCore === 'undefined') {
        showError('VirtualCore is not defined. The TanStack Virtual library might not have loaded.');
        return null;
    }

    return { TableCore };
}

function showError(msg) {
    const container = document.getElementById('gridContainer');
    if (container) {
        container.innerHTML += `<div style="color: red; padding: 20px;">Error: ${msg}</div>`;
    }
    vscode.postMessage({ command: 'error', text: msg });
}

// Store for column filters per grid
let columnFilterStates = {};
// Store for aggregation selection per grid: { rsIndex: { columnId: 'sum'|'count'|'countDistinct'|null } }
let aggregationStates = {};

// Search Worker
let searchWorker = null;
let searchMatches = {}; // { [rsIndex]: Set<matchedIndex> | null } -> null means match all
let isSearching = false;

// Initialize Worker
if (typeof workerUri !== 'undefined') {
    try {
        fetch(workerUri)
            .then(response => response.text())
            .then(code => {
                const blob = new Blob([code], { type: 'application/javascript' });
                const blobUrl = URL.createObjectURL(blob);
                searchWorker = new Worker(blobUrl);

                searchWorker.onmessage = function (e) {
                    const { command, id, matchedIndices } = e.data;
                    if (command === 'searchResult') {
                        isSearching = false;

                        // Store results
                        if (matchedIndices === null) {
                            searchMatches[id] = null;
                        } else {
                            searchMatches[id] = new Set(matchedIndices);
                        }

                        // Trigger re-render of the specific grid
                        if (grids[id] && grids[id].tanTable) {
                            const currentGlobal = document.getElementById('globalFilter').value;
                            grids[id].tanTable.setGlobalFilter(currentGlobal);
                        }

                        // Update UI
                        const rowCountInfo = document.getElementById('rowCountInfo');
                        if (rowCountInfo) {
                            rowCountInfo.style.opacity = '0.8';
                        }
                    } else if (command === 'setDataDone') {
                        // Data loaded in worker
                    }
                };

                // Send initial data to worker if it wasn't ready during grid creation
                if (window.resultSets) {
                    window.resultSets.forEach((rs, index) => {
                        searchWorker.postMessage({
                            command: 'setData',
                            id: index,
                            data: rs.data,
                            columns: rs.columns // simplified columns
                        });
                    });
                }
            })
            .catch(err => {
                console.error('Failed to initialize search worker:', err);
            });
    } catch (e) {
        console.error('Error creating worker:', e);
    }
}

// Debounce utility
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Global drag state management
let globalDragState = {
    isDragging: false,
    dragType: null,
    draggedItem: null
};

function createResultSetGrid(rs, rsIndex, container, createTable, getCoreRowModel, getSortedRowModel, getFilteredRowModel, getGroupedRowModel, getExpandedRowModel) {
    const wrapper = document.createElement('div');
    wrapper.className = 'grid-wrapper' + (rsIndex === activeGridIndex ? ' active' : '');
    wrapper.style.height = '100%';
    wrapper.style.overflow = 'auto';
    wrapper.style.position = 'relative';
    wrapper.style.display = rsIndex === activeGridIndex ? 'block' : 'none';
    container.appendChild(wrapper);

    // If data is missing or not an array, show invalid message
    if (!rs.data || !Array.isArray(rs.data)) {
        wrapper.innerHTML = '<div style="padding: 20px; text-align: center; color: red;">Invalid result data</div>';
        grids.push(null);
        return;
    }

    // If there are no rows but columns are present, render the table with headers only
    const hasRows = rs.data.length > 0;

    // If there are no rows and no columns, prefer showing message/rowsAffected when available
    if (!hasRows && (!rs.columns || rs.columns.length === 0)) {
        if (rs.rowsAffected !== undefined || rs.message) {
            const rowsAffectedText = rs.rowsAffected !== undefined ? `${rs.rowsAffected} rows affected` : '';
            const messageText = rs.message || '';
            const displayText = [messageText, rowsAffectedText].filter(Boolean).join(' - ');

            wrapper.innerHTML = `<div style="padding: 20px; text-align: center; opacity: 0.8; font-size: 14px;">${displayText}</div>`;
        } else {
            wrapper.innerHTML = '<div style="padding: 20px; text-align: center; opacity: 0.6;">Empty result set</div>';
        }
        grids.push(null);
        return;
    }

    if (!rs.columns || !Array.isArray(rs.columns)) {
        wrapper.innerHTML = '<div style="padding: 20px; text-align: center; color: red;">Invalid columns definition</div>';
        grids.push(null);
        return;
    }

    // Initialize search matches state for this grid (null = match all)
    searchMatches[rsIndex] = null;

    // Send data to worker, but sanitize to ensure it can be cloned if needed, 
    // though structured clone handles most things. 
    // We send just data and simplified column info.
    if (searchWorker) {
        searchWorker.postMessage({
            command: 'setData',
            id: rsIndex,
            data: rs.data,
            columns: rs.columns
        });
    }

    // Initialize column filter state for this grid
    columnFilterStates[rsIndex] = {};

    // Initialize aggregation state for this grid
    aggregationStates[rsIndex] = {};

    // Create table element
    const table = document.createElement('table');
    table.style.width = '100%';
    table.style.borderCollapse = 'collapse';

    const thead = document.createElement('thead');
    thead.style.position = 'sticky';
    thead.style.top = '0';
    thead.style.zIndex = '10';
    thead.style.backgroundColor = 'var(--vscode-editor-background)';

    const tbody = document.createElement('tbody');
    const tfoot = document.createElement('tfoot');
    table.appendChild(thead);
    table.appendChild(tbody);
    table.appendChild(tfoot);
    wrapper.appendChild(table);

    // Prepare columns with unique values for filtering
    const columns = rs.columns.map((col, index) => {
        // Determine the correct accessor key (for resultView.ts which uses column names as keys)
        const accessorKey = col.accessorKey || String(index);

        // Get unique values for this column
        const uniqueValues = [...new Set(rs.data.map(row => {
            const value = Array.isArray(row) ? row[index] : (col.accessorKey ? row[col.accessorKey] : row[String(index)]);
            return value === null || value === undefined ? 'NULL' : String(value);
        }))].sort();

        // Define accessor function variable to be used in both accessorFn and filterFn
        // CRITICAL: Return empty string for null/undefined so TanStack Table infers string type
        // This fixes global filtering when first row has NULL values
        const accessorFn = (row) => {
            if (!row) return '';
            let value;
            // Support both array-based data and object-based data with column names as keys
            if (Array.isArray(row)) {
                value = row[index];
            } else if (col.accessorKey) {
                value = row[col.accessorKey];
            } else {
                value = row[String(index)];
            }
            // Return empty string for null/undefined to ensure string type inference
            return value === null || value === undefined ? '' : value;
        };

        return {
            id: String(index),
            accessorFn: accessorFn,
            header: col.name || `Col ${index}`,
            uniqueValues: uniqueValues,
            filterFn: (row, columnId, filterValue) => {
                if (!filterValue) return true;

                // Use accessorFn directly on original data
                const cellValue = accessorFn(row.original);
                const stringValue = cellValue === null || cellValue === undefined || cellValue === '' ? 'NULL' : String(cellValue);
                const numericValue = parseFloat(String(cellValue).replace(/,/g, ''));

                // Handle condition-based filters
                if (filterValue._isConditionFilter) {
                    const { conditions, logic } = filterValue;

                    const evaluateCondition = (cond) => {
                        const condValue = cond.value;
                        const condValue2 = cond.value2;
                        const isNull = stringValue === 'NULL';

                        switch (cond.type) {
                            case 'contains':
                                return !isNull && stringValue.toLowerCase().includes(condValue.toLowerCase());
                            case 'notContains':
                                return isNull || !stringValue.toLowerCase().includes(condValue.toLowerCase());
                            case 'equals':
                                if (!isNaN(numericValue) && !isNaN(parseFloat(condValue))) {
                                    return numericValue === parseFloat(condValue);
                                }
                                return stringValue.toLowerCase() === condValue.toLowerCase();
                            case 'notEquals':
                                if (!isNaN(numericValue) && !isNaN(parseFloat(condValue))) {
                                    return numericValue !== parseFloat(condValue);
                                }
                                return stringValue.toLowerCase() !== condValue.toLowerCase();
                            case 'startsWith':
                                return !isNull && stringValue.toLowerCase().startsWith(condValue.toLowerCase());
                            case 'endsWith':
                                return !isNull && stringValue.toLowerCase().endsWith(condValue.toLowerCase());
                            case 'isEmpty':
                                return isNull;
                            case 'isNotEmpty':
                                return !isNull;
                            case 'greaterThan':
                                return !isNull && !isNaN(numericValue) && numericValue > parseFloat(condValue);
                            case 'greaterThanOrEqual':
                                return !isNull && !isNaN(numericValue) && numericValue >= parseFloat(condValue);
                            case 'lessThan':
                                return !isNull && !isNaN(numericValue) && numericValue < parseFloat(condValue);
                            case 'lessThanOrEqual':
                                return !isNull && !isNaN(numericValue) && numericValue <= parseFloat(condValue);
                            case 'between':
                                const min = parseFloat(condValue);
                                const max = parseFloat(condValue2);
                                return !isNull && !isNaN(numericValue) && numericValue >= min && numericValue <= max;
                            default:
                                return true;
                        }
                    };

                    if (logic === 'and') {
                        return conditions.every(evaluateCondition);
                    } else {
                        return conditions.some(evaluateCondition);
                    }
                }

                // Handle value array filters (checkbox list)
                if (Array.isArray(filterValue)) {
                    if (filterValue.length === 0) return true;
                    return filterValue.includes(stringValue);
                }

                return true;
            }
        };
    });

    // State - Try to restore from saved state (using executionTimestamp in key)
    const savedState = getSavedStateFor(rsIndex, rs.executionTimestamp);

    // Initialize column widths map
    let columnWidths = new Map();
    if (savedState && savedState.columnWidths) {
        try {
            // Restore from array entry [key, value]
            columnWidths = new Map(savedState.columnWidths);
        } catch (e) { console.error('Error restoring column widths', e); }
    }

    // Calculate initial column widths if not fully populated
    try {
        const measureCanvas = document.createElement('canvas');
        const measureCtx = measureCanvas.getContext('2d');
        const computedStyle = window.getComputedStyle(document.body);
        const fontSize = computedStyle.getPropertyValue('--vscode-editor-font-size') || '13px';
        const fontFamily = computedStyle.getPropertyValue('--vscode-editor-font-family') || 'Consolas, monospace';
        measureCtx.font = `${fontSize} ${fontFamily}`;

        columns.forEach(col => {
            if (!columnWidths.has(col.id)) {
                // Start with header width + padding for sort/filter icons
                let maxWidth = measureCtx.measureText(col.header).width + 30;

                // Check first 100 rows
                const sampleSize = Math.min(rs.data.length, 100);
                for (let i = 0; i < sampleSize; i++) {
                    const val = col.accessorFn(rs.data[i]);
                    if (val !== null && val !== undefined) {
                        // Add padding (~16px for cell padding)
                        const w = measureCtx.measureText(String(val)).width + 16;
                        if (w > maxWidth) maxWidth = w;
                    }
                }

                // Apply 800px max width limit
                maxWidth = Math.min(maxWidth, 800);
                columnWidths.set(col.id, Math.ceil(maxWidth));
            }
        });
    } catch (e) {
        console.error('Error calculating column widths:', e);
    }

    // Set table layout to fixed
    table.style.tableLayout = 'fixed';

    // Helper to render colgroup
    function renderColGroup() {
        // Remove existing colgroup
        const existing = table.querySelector('colgroup');
        if (existing) existing.remove();

        const colGroup = document.createElement('colgroup');
        // Get visible columns in correct order
        const visibleCols = tanTable.getVisibleLeafColumns();

        visibleCols.forEach(col => {
            const colEl = document.createElement('col');
            const w = columnWidths.get(col.id) || 100; // Default fallback
            colEl.style.width = w + 'px';
            colGroup.appendChild(colEl);
        });

        table.insertBefore(colGroup, table.querySelector('thead'));
    }

    // Restore custom states
    if (savedState) {
        if (savedState.customColumnFilters) columnFilterStates[rsIndex] = savedState.customColumnFilters;
        if (savedState.aggregations) aggregationStates[rsIndex] = savedState.aggregations;
    }

    let sorting = savedState?.sorting || [];
    let globalFilter = savedState?.globalFilter || '';
    let grouping = savedState?.grouping || [];
    let expanded = savedState?.expanded || {};
    let columnOrder = savedState?.columnOrder || columns.map(c => c.id);
    let columnFilters = savedState?.columnFilters || [];
    let columnPinning = savedState?.columnPinning || { left: [], right: [] };
    let columnVisibility = savedState?.columnVisibility || {};

    // Create TanStack Table
    const tanTable = createTable({
        data: rs.data,
        columns,
        state: {
            get sorting() { return sorting; },
            get globalFilter() { return globalFilter; },
            get grouping() { return grouping; },
            get expanded() { return expanded; },
            get columnOrder() { return columnOrder; },
            get columnFilters() { return columnFilters; },
            get columnPinning() { return columnPinning; },
            get columnVisibility() { return columnVisibility; }
        },
        onSortingChange: (updater) => {
            sorting = typeof updater === 'function' ? updater(sorting) : updater;
            scheduleRender();
            savePinnedState();
        },
        onGlobalFilterChange: (updater) => {
            globalFilter = typeof updater === 'function' ? updater(globalFilter) : updater;
            scheduleRender();
            savePinnedState();
        },
        onColumnFiltersChange: (updater) => {
            columnFilters = typeof updater === 'function' ? updater(columnFilters) : updater;
            scheduleRender();
            savePinnedState();
        },
        onGroupingChange: (updater) => {
            grouping = typeof updater === 'function' ? updater(grouping) : updater;
            scheduleRender();
            renderGrouping();
            savePinnedState();
        },
        onExpandedChange: (updater) => {
            expanded = typeof updater === 'function' ? updater(expanded) : updater;
            scheduleRender();
            savePinnedState();
        },
        onColumnOrderChange: (updater) => {
            columnOrder = typeof updater === 'function' ? updater(columnOrder) : updater;
            scheduleRender();
            savePinnedState();
        },
        // We need to implement connection for these handlers too if we want them persisted
        onColumnPinningChange: (updater) => {
            columnPinning = typeof updater === 'function' ? updater(columnPinning) : updater;
            scheduleRender();
            savePinnedState();
        },
        onColumnVisibilityChange: (updater) => {
            columnVisibility = typeof updater === 'function' ? updater(columnVisibility) : updater;
            scheduleRender();
            savePinnedState();
        },
        globalFilterFn: (row, columnId, filterValue) => {
            // If unmodified logic needed for immediate small data, we could check size here.
            // But for consistency we use worker results.

            // If no filter value, show all.
            if (!filterValue || filterValue === '') return true;

            // Check if we have worker results for this grid
            const matches = searchMatches[rsIndex];

            // If matches is null, it means "match all" or "not ready yet" (but usually match all if filterValue is set)
            // However, since we trigger this via setGlobalFilter AFTER getting results, 
            // valid states are:
            // 1. filterValue empty -> returns true at top
            // 2. filterValue set, matches null -> means worker said "match all" (empty query) -> return true
            // 3. filterValue set, matches Set -> check index

            if (matches === undefined || matches === null) return true;

            // Check if this row index is in the matched set
            return matches.has(row.index);
        },
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getGroupedRowModel: getGroupedRowModel(),
        getExpandedRowModel: getExpandedRowModel(),
    });

    // Virtualization state
    let rowVirtualizer = null;
    let virtualizerCleanup = null;
    let renderScheduled = false;

    function createVirtualizer() {
        if (virtualizerCleanup) {
            virtualizerCleanup();
            virtualizerCleanup = null;
        }

        const rows = tanTable.getRowModel().rows;

        rowVirtualizer = new VirtualCore.Virtualizer({
            count: rows.length,
            getScrollElement: () => wrapper,
            estimateSize: () => 30,
            overscan: 15,
            scrollToFn: VirtualCore.elementScroll,
            observeElementRect: VirtualCore.observeElementRect,
            observeElementOffset: VirtualCore.observeElementOffset,
            onChange: () => {
                renderTableRows();
            }
        });

        virtualizerCleanup = rowVirtualizer._didMount();
        rowVirtualizer._willUpdate();
    }

    function scheduleRender() {
        if (renderScheduled) return;
        renderScheduled = true;
        requestAnimationFrame(() => {
            renderScheduled = false;
            render();
        });
    }

    // Create grid object with methods
    const gridObj = {
        tanTable,
        rsIndex,
        executionTimestamp: rs.executionTimestamp,
        renderGrouping: () => renderGrouping(),
        updateRowCount: () => updateRowCount(),
        render: () => render(),
        createVirtualizer: () => createVirtualizer(),
        renderTableRows: () => renderTableRows(),
        renderColGroup: () => renderColGroup(),
        columnWidths: columnWidths
    };
    grids.push(gridObj);

    function renderGrouping() {
        const panel = document.getElementById('groupingPanel');
        if (!panel) return;
        panel.innerHTML = '';

        if (grouping.length === 0) {
            panel.innerHTML = '<span style="opacity: 0.5;">Drag headers here to group</span>';
            return;
        }

        grouping.forEach((colId, index) => {
            const chip = document.createElement('div');
            chip.className = 'group-chip';
            chip.draggable = true;
            chip.dataset.colId = colId;
            chip.dataset.groupIndex = index;

            // Add drag & drop event handlers for reordering
            chip.ondragstart = (e) => {

                e.dataTransfer.setData('text/plain', colId);
                e.dataTransfer.setData('type', 'groupChip');
                e.dataTransfer.effectAllowed = 'move';
                chip.classList.add('dragging');

                // Set global drag state
                globalDragState.isDragging = true;
                globalDragState.dragType = 'groupChip';
                globalDragState.draggedItem = colId;
            };

            chip.ondragover = (e) => {
                e.preventDefault();
                e.stopPropagation();

                // Only allow drop if we're dragging a group chip and it's not this chip
                if (globalDragState.dragType === 'groupChip' && globalDragState.draggedItem !== colId) {
                    e.dataTransfer.dropEffect = 'move';
                    chip.classList.add('drag-over');
                } else if (globalDragState.dragType === 'column') {
                    // Allow dropping columns to create new groups
                    e.dataTransfer.dropEffect = 'copy';
                } else {
                    e.dataTransfer.dropEffect = 'none';
                }
            };

            chip.ondragleave = (e) => {
                // Only remove drag-over if we're actually leaving this element
                if (!chip.contains(e.relatedTarget)) {
                    chip.classList.remove('drag-over');
                }
            };

            chip.ondrop = (e) => {
                e.preventDefault();
                e.stopPropagation();
                chip.classList.remove('drag-over');

                // Handle the drop specifically for this chip
                if (globalDragState.dragType === 'groupChip') {
                    const draggedColId = globalDragState.draggedItem;
                    if (draggedColId && draggedColId !== colId && grids[activeGridIndex] && grids[activeGridIndex].tanTable) {
                        const currentGrouping = grids[activeGridIndex].tanTable.getState().grouping;
                        const newGrouping = [...currentGrouping];
                        const fromIndex = newGrouping.indexOf(draggedColId);
                        const toIndex = newGrouping.indexOf(colId);

                        if (fromIndex !== -1 && toIndex !== -1 && fromIndex !== toIndex) {
                            // Remove from old position
                            newGrouping.splice(fromIndex, 1);
                            // Insert at target position
                            newGrouping.splice(toIndex, 0, draggedColId);
                            grids[activeGridIndex].tanTable.setGrouping(newGrouping);
                        }
                    }
                }
            };

            chip.ondragend = () => {
                chip.classList.remove('dragging');

                // Clear global drag state
                globalDragState.isDragging = false;
                globalDragState.dragType = null;
                globalDragState.draggedItem = null;
            };

            const chipContent = document.createElement('span');
            chipContent.textContent = rs.columns[parseInt(colId)].name;
            chip.appendChild(chipContent);

            const removeBtn = document.createElement('span');
            removeBtn.className = 'remove-group';
            removeBtn.textContent = '×';
            removeBtn.onclick = (e) => {
                e.stopPropagation();
                removeGroup(colId);
            };
            chip.appendChild(removeBtn);

            panel.appendChild(chip);
        });
    }

    function updateRowCount() {
        const info = document.getElementById('rowCountInfo');
        if (!info) return;
        const rows = tanTable.getFilteredRowModel().rows;
        info.textContent = `${rows.length} row${rows.length !== 1 ? 's' : ''}`;

        if (rs.limitReached) {
            const warning = document.createElement('span');
            warning.style.color = 'var(--vscode-errorForeground)';
            warning.style.marginLeft = '10px';
            warning.style.fontWeight = 'bold';
            warning.title = 'Result limit of 200,000 rows reached.';
            warning.textContent = '⚠️ Limit Reached';
            info.appendChild(warning);
        }
    }

    function render() {
        try {
            createVirtualizer();
            renderColGroup(); // Update column widths (and order)
            renderTableHeaders();
            renderTableRows();
            updateRowCount();
            renderAggregations();
        } catch (e) {
            console.error('Render error:', e);
            wrapper.innerHTML = `<div style="color: red; padding: 20px;">Render error: ${e.message}</div>`;
        }
    }

    function renderAggregations() {
        try {
            if (!tfoot) return;
            tfoot.innerHTML = '';

            const aggRow = document.createElement('tr');
            aggRow.className = 'aggregation-row';

            const rows = tanTable.getFilteredRowModel().rows || [];
            // Use getVisibleLeafColumns to respect column order changes
            const visibleColumns = tanTable.getVisibleLeafColumns();

            visibleColumns.forEach(col => {
                const td = document.createElement('td');
                td.style.padding = '6px 8px';
                td.style.borderTop = '1px solid var(--vscode-panel-border)';
                td.style.fontWeight = '600';

                const agg = (aggregationStates[rsIndex] || {})[col.id];
                if (!agg) {
                    td.textContent = '';
                    aggRow.appendChild(td);
                    return;
                }

                if (agg === 'sum') {
                    let sum = 0;
                    let hasNumber = false;
                    rows.forEach(r => {
                        const v = r.getValue(col.id);
                        if (v !== null && v !== undefined && v !== '') {
                            const n = parseFloat(String(v).replace(/,/g, ''));
                            if (!isNaN(n)) { sum += n; hasNumber = true; }
                        }
                    });
                    td.textContent = hasNumber ? String(sum) : '';
                } else if (agg === 'count') {
                    const cnt = rows.filter(r => {
                        const v = r.getValue(col.id);
                        return v !== null && v !== undefined;
                    }).length;
                    td.textContent = String(cnt);
                } else if (agg === 'countDistinct') {
                    const s = new Set();
                    rows.forEach(r => {
                        const v = r.getValue(col.id);
                        if (v !== null && v !== undefined) s.add(String(v));
                    });
                    td.textContent = String(s.size);
                }

                aggRow.appendChild(td);
            });

            tfoot.appendChild(aggRow);
        } catch (e) {
            console.error('Aggregation render error:', e);
        }
    }

    function renderTableHeaders() {
        thead.innerHTML = '';
        tanTable.getHeaderGroups().forEach(headerGroup => {
            const tr = document.createElement('tr');
            headerGroup.headers.forEach(header => {
                const th = createHeaderCellWithFilter(header, rs, tanTable, rsIndex);
                tr.appendChild(th);
            });
            thead.appendChild(tr);
        });
    }

    function renderTableRows() {
        if (!rowVirtualizer) return;

        rowVirtualizer._willUpdate();

        const rows = tanTable.getRowModel().rows;
        const virtualItems = rowVirtualizer.getVirtualItems();
        const totalSize = rowVirtualizer.getTotalSize();

        const paddingTop = virtualItems.length > 0 ? virtualItems[0].start : 0;
        const paddingBottom = virtualItems.length > 0 ? totalSize - virtualItems[virtualItems.length - 1].end : 0;

        tbody.innerHTML = '';

        if (paddingTop > 0) {
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.style.height = `${paddingTop}px`;
            td.colSpan = columns.length;
            td.style.padding = '0';
            td.style.border = 'none';
            tr.appendChild(td);
            tbody.appendChild(tr);
        }

        // Track groups to insert footers
        const renderedGroupFooters = new Set();

        virtualItems.forEach((virtualRow, vIdx) => {
            const row = rows[virtualRow.index];
            if (!row) return;

            const tr = document.createElement('tr');
            tr.dataset.index = virtualRow.index;
            tr.className = virtualRow.index % 2 === 0 ? 'even' : 'odd';

            if (row.getIsGrouped?.()) {
                createGroupHeaderRow(tr, row, rs);
            } else {
                createDataRow(tr, row);
            }

            tbody.appendChild(tr);

            // Check if we need to insert a group footer after this row
            // A non-grouped row with depth > 0 is inside a group
            if (!row.getIsGrouped?.() && row.depth > 0) {
                const nextRow = rows[virtualRow.index + 1];

                // Group ends when:
                // - No next row exists
                // - Next row has different depth (going up or into another group)  
                // - Next row is a group header
                const isLastInGroup = !nextRow ||
                    nextRow.depth < row.depth ||
                    nextRow.getIsGrouped?.();

                if (isLastInGroup) {
                    // Get the parent group row using getParentRow or by searching
                    const parentRow = row.getParentRow?.() || rows.find(r =>
                        r.getIsGrouped?.() &&
                        r.subRows?.some(sr => sr.id === row.id)
                    );

                    if (parentRow && parentRow.getIsExpanded?.() && !renderedGroupFooters.has(parentRow.id)) {
                        const footerTr = createGroupFooterRow(parentRow, rs);
                        if (footerTr) {
                            tbody.appendChild(footerTr);
                            renderedGroupFooters.add(parentRow.id);
                        }
                    }
                }
            }
        });

        if (paddingBottom > 0) {
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.style.height = `${paddingBottom}px`;
            td.colSpan = columns.length;
            td.style.padding = '0';
            td.style.border = 'none';
            tr.appendChild(td);
            tbody.appendChild(tr);
        }
    }

    function createDataRow(tr, row) {
        row.getVisibleCells().forEach(cell => {
            const td = document.createElement('td');
            const value = cell.getValue();

            if (row.depth > 0) {
                const indent = document.createElement('span');
                indent.className = 'group-indent';
                indent.style.width = (row.depth * 20) + 'px';
                td.appendChild(indent);
            }

            // Check for null, undefined, or empty string (accessorFn returns '' for nulls)
            if (value === null || value === undefined || value === '') {
                const nullSpan = document.createElement('span');
                nullSpan.className = 'null-value';
                nullSpan.textContent = 'NULL';
                td.appendChild(nullSpan);
            } else {
                const valueSpan = document.createElement('span');
                valueSpan.textContent = String(value);
                td.appendChild(valueSpan);
            }

            tr.appendChild(td);
        });
    }

    function createGroupHeaderRow(tr, row, resultSet) {
        tr.className = 'group-header';
        const firstCell = document.createElement('td');
        firstCell.colSpan = row.getVisibleCells().length;

        const indent = document.createElement('span');
        indent.className = 'group-indent';
        indent.style.width = (row.depth * 20) + 'px';
        firstCell.appendChild(indent);

        const indicator = document.createElement('span');
        indicator.textContent = row.getIsExpanded() ? '▼' : '▶';
        indicator.style.cursor = 'pointer';
        indicator.onclick = () => row.toggleExpanded();
        firstCell.appendChild(indicator);

        const groupValue = row.getGroupingValue(row.groupingColumnId);
        const groupText = document.createElement('span');
        groupText.textContent = ` ${resultSet.columns[parseInt(row.groupingColumnId)].name}: ${groupValue} (${row.subRows.length} row${row.subRows.length !== 1 ? 's' : ''})`;
        firstCell.appendChild(groupText);

        tr.appendChild(firstCell);
    }

    // Create group footer row with subtotals for each group
    function createGroupFooterRow(groupRow, resultSet) {
        const tr = document.createElement('tr');
        tr.className = 'group-footer';
        tr.dataset.groupFooter = 'true';

        const depth = groupRow.depth || 0;
        const subRows = groupRow.subRows || [];
        const currentAggs = aggregationStates[rsIndex] || {};

        // Check if any aggregation is set
        const hasAnyAggregation = Object.keys(currentAggs).length > 0;
        if (!hasAnyAggregation) {
            return null; // Don't show footer if no aggregations selected
        }

        // Use getVisibleLeafColumns to respect column order changes
        const visibleColumns = tanTable.getVisibleLeafColumns();

        visibleColumns.forEach((col, colIndex) => {
            const td = document.createElement('td');

            // Add indent for first column
            if (colIndex === 0 && depth > 0) {
                const indent = document.createElement('span');
                indent.className = 'group-indent';
                indent.style.width = (depth * 20) + 'px';
                td.appendChild(indent);
            }

            const agg = currentAggs[col.id];
            if (!agg) {
                td.textContent = '';
                tr.appendChild(td);
                return;
            }

            // Calculate aggregation for this group's subRows
            let result = '';
            if (agg === 'sum') {
                let sum = 0;
                let hasNumber = false;
                subRows.forEach(r => {
                    const v = r.getValue(col.id);
                    if (v !== null && v !== undefined && v !== '') {
                        const n = parseFloat(String(v).replace(/,/g, ''));
                        if (!isNaN(n)) { sum += n; hasNumber = true; }
                    }
                });
                result = hasNumber ? String(sum) : '';
            } else if (agg === 'count') {
                const cnt = subRows.filter(r => {
                    const v = r.getValue(col.id);
                    return v !== null && v !== undefined;
                }).length;
                result = String(cnt);
            } else if (agg === 'countDistinct') {
                const s = new Set();
                subRows.forEach(r => {
                    const v = r.getValue(col.id);
                    if (v !== null && v !== undefined) s.add(String(v));
                });
                result = String(s.size);
            }

            if (result) {
                const labelSpan = document.createElement('span');
                labelSpan.className = 'agg-label';
                labelSpan.textContent = agg === 'sum' ? 'Σ' : agg === 'count' ? '#' : '◇';
                td.appendChild(labelSpan);

                const valueSpan = document.createElement('span');
                valueSpan.textContent = result;
                td.appendChild(valueSpan);
            }

            tr.appendChild(td);
        });

        return tr;
    }

    render();

    // Restore scroll position and globalFilter input value after initial render
    if (savedState) {
        // Restore scroll position (use requestAnimationFrame to ensure DOM is ready)
        if (savedState.scrollTop || savedState.scrollLeft) {
            requestAnimationFrame(() => {
                wrapper.scrollTop = savedState.scrollTop || 0;
                wrapper.scrollLeft = savedState.scrollLeft || 0;
            });
        }
    }

    // Save state when scrolling (debounced)
    const debouncedSaveOnScroll = debounce(() => {
        saveAllGridStates();
    }, 200);
    wrapper.addEventListener('scroll', debouncedSaveOnScroll);

    // Setup cell selection events for rectangular selection with mouse
    const selectionHandlers = setupCellSelectionEvents(wrapper, tanTable, columns.length);
    Object.assign(gridObj, selectionHandlers);
}

function createErrorView(rs, rsIndex, container) {
    const wrapper = document.createElement('div');
    // Use grid-wrapper class so switchToResultSet can show/hide it correctly
    wrapper.className = 'grid-wrapper error-wrapper' + (rsIndex === activeGridIndex ? ' active' : '');
    wrapper.style.display = rsIndex === activeGridIndex ? 'block' : 'none';
    container.appendChild(wrapper);

    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-view';

    const title = document.createElement('div');
    title.className = 'error-title';
    title.textContent = 'SQL Execution Error';
    errorDiv.appendChild(title);

    const msg = document.createElement('div');
    msg.textContent = rs.message || 'Unknown error occurred.';
    errorDiv.appendChild(msg);

    if (rs.sql) {
        const sqlDiv = document.createElement('div');
        sqlDiv.className = 'error-sql';
        sqlDiv.innerHTML = `<strong>Executed SQL:</strong><br><pre style="margin-top: 5px;">${rs.sql}</pre>`;
        errorDiv.appendChild(sqlDiv);
    }

    wrapper.appendChild(errorDiv);
    grids.push(null); // Register placeholder grid to keep indices in sync
}

function clearLogs() {
    if (window.activeSource) {
        vscode.postMessage({
            command: 'clearLogs',
            sourceUri: window.activeSource
        });
    }
}

// State Management
// Save state for ALL grids using sourceUri:rsIndex:executionTimestamp key
// The executionTimestamp ensures state from previous SQL executions is not reused
function saveAllGridStates() {
    const stateToSave = vscode.getState() || {};

    grids.forEach((grid, rsIndex) => {
        if (!grid || !grid.tanTable) return;

        const timestamp = grid.executionTimestamp || '';
        const key = `${window.activeSource}:${rsIndex}:${timestamp}`;
        const tableState = grid.tanTable.getState();
        const wrapper = document.querySelectorAll('.grid-wrapper')[rsIndex];
        const isVisible = wrapper && wrapper.style.display !== 'none';

        let scrollTop = 0;
        let scrollLeft = 0;

        if (isVisible) {
            scrollTop = wrapper?.scrollTop || 0;
            scrollLeft = wrapper?.scrollLeft || 0;
        } else {
            // If hidden, try to preserve existing scroll from saved state
            // to avoid overwriting it with 0
            if (stateToSave[key]) {
                scrollTop = stateToSave[key].scrollTop || 0;
                scrollLeft = stateToSave[key].scrollLeft || 0;
            }
        }

        stateToSave[key] = {
            sorting: tableState.sorting,
            grouping: tableState.grouping,
            expanded: tableState.expanded,
            columnOrder: tableState.columnOrder,
            columnFilters: tableState.columnFilters,
            columnPinning: tableState.columnPinning,
            columnVisibility: tableState.columnVisibility,
            globalFilter: tableState.globalFilter,
            // Custom states
            customColumnFilters: columnFilterStates[rsIndex],
            aggregations: aggregationStates[rsIndex],
            columnWidths: Array.from(grid.columnWidths || []),
            // Scroll position
            scrollTop: scrollTop,
            scrollLeft: scrollLeft
        };
    });

    vscode.setState(stateToSave);
}

// Backward compatibility alias
function savePinnedState() {
    saveAllGridStates();
}

// Helper to get saved state for a result set using sourceUri:rsIndex:executionTimestamp key
function getSavedStateFor(rsIndex, executionTimestamp) {
    const savedState = vscode.getState();
    if (!savedState) return null;

    const timestamp = executionTimestamp || '';
    const key = `${window.activeSource}:${rsIndex}:${timestamp}`;
    return savedState[key] || null;
}

// Function to create header cell with Excel-like filter dropdown
function createHeaderCellWithFilter(header, resultSet, table, rsIndex) {
    const th = document.createElement('th');
    th.draggable = true;
    th.dataset.colId = header.column.id;
    th.style.position = 'relative';
    th.style.padding = '4px 8px';
    th.style.borderBottom = '1px solid var(--vscode-panel-border)';
    th.style.borderRight = '1px solid var(--vscode-panel-border)';

    const headerContent = document.createElement('div');
    headerContent.style.display = 'flex';
    headerContent.style.justifyContent = 'space-between';
    headerContent.style.alignItems = 'center';
    headerContent.style.minHeight = '20px';

    const headerText = document.createElement('span');
    headerText.innerHTML = header.column.columnDef.header;
    headerText.style.cursor = 'pointer';
    headerText.style.flex = '1';

    // Add sorting indicator
    const isSorted = header.column.getIsSorted();
    if (isSorted) {
        headerText.innerHTML += isSorted === 'asc' ? ' 🔼' : ' 🔽';
    }

    // Sort handler
    headerText.onclick = header.column.getToggleSortingHandler();

    headerContent.appendChild(headerText);

    // Filter dropdown button
    const filterBtn = document.createElement('span');
    filterBtn.innerHTML = '▼';
    filterBtn.style.cursor = 'pointer';
    filterBtn.style.fontSize = '12px';
    filterBtn.style.marginLeft = '4px';
    filterBtn.style.opacity = '0.6';
    filterBtn.style.userSelect = 'none';

    // Check if column has active filter (array or condition-based)
    const currentFilter = table.getColumn(header.column.id).getFilterValue();
    const hasActiveFilter = currentFilter && (
        (Array.isArray(currentFilter) && currentFilter.length > 0) ||
        (currentFilter._isConditionFilter && currentFilter.conditions && currentFilter.conditions.length > 0)
    );

    if (hasActiveFilter) {
        filterBtn.style.color = 'var(--vscode-charts-blue)';
        filterBtn.style.opacity = '1';
        filterBtn.innerHTML = '🔽';
        // Also style the header cell to indicate active filter
        th.style.backgroundColor = 'var(--vscode-list-activeSelectionBackground)';
        th.style.borderBottom = '2px solid var(--vscode-charts-blue)';
    }

    filterBtn.onclick = (e) => {
        e.stopPropagation();
        showColumnFilterDropdown(header.column, table, filterBtn, rsIndex);
    };

    // Aggregation button
    const aggBtn = document.createElement('span');
    aggBtn.innerHTML = 'Σ';
    aggBtn.title = 'Aggregation';
    aggBtn.style.cursor = 'pointer';
    aggBtn.style.fontSize = '12px';
    aggBtn.style.marginLeft = '6px';
    aggBtn.style.opacity = '0.6';
    aggBtn.style.userSelect = 'none';

    // Highlight if aggregation is active for this column
    const currentAgg = (aggregationStates[rsIndex] || {})[header.column.id];
    if (currentAgg) {
        aggBtn.style.color = 'var(--vscode-charts-green)';
        aggBtn.style.opacity = '1';
    }

    aggBtn.onclick = (e) => {
        e.stopPropagation();
        showAggregationDropdown(header.column, table, aggBtn, rsIndex);
    };

    headerContent.appendChild(aggBtn);
    headerContent.appendChild(filterBtn);

    // Drag and drop for column reordering and grouping
    th.ondragstart = (e) => {
        // Set column name as text for external drops (e.g., to editor)
        e.dataTransfer.setData('text/plain', header.column.columnDef.header);
        e.dataTransfer.setData('type', 'column');
        e.dataTransfer.setData('columnId', header.column.id);
        e.dataTransfer.setData('columnName', header.column.columnDef.header);
        e.dataTransfer.effectAllowed = 'copyMove';
        th.classList.add('dragging');

        // Set global drag state
        globalDragState.isDragging = true;
        globalDragState.dragType = 'column';
        globalDragState.draggedItem = header.column.id;
    };

    th.ondragover = (e) => {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        th.classList.add('drag-over');
    };

    th.ondragleave = () => {
        th.classList.remove('drag-over');
    };

    th.ondrop = (e) => {
        e.preventDefault();
        th.classList.remove('drag-over');

        const type = e.dataTransfer.getData('type');
        if (type === 'column') {
            // Use columnId for internal reordering (text/plain now has column name for external drops)
            const draggedColId = e.dataTransfer.getData('columnId');
            const targetColId = header.column.id;

            if (draggedColId && draggedColId !== targetColId) {
                const currentOrder = table.getState().columnOrder;
                const newOrder = [...currentOrder];
                const fromIndex = newOrder.indexOf(draggedColId);
                const toIndex = newOrder.indexOf(targetColId);

                if (fromIndex !== -1 && toIndex !== -1) {
                    newOrder.splice(fromIndex, 1);
                    newOrder.splice(toIndex, 0, draggedColId);
                    table.setColumnOrder(newOrder);
                }
            }
        }
    };

    th.ondragend = () => {
        th.classList.remove('dragging');

        // Clear global drag state
        globalDragState.isDragging = false;
        globalDragState.dragType = null;
        globalDragState.draggedItem = null;
    };

    th.appendChild(headerContent);

    // Resizer handle
    const resizer = document.createElement('div');
    resizer.className = 'col-resizer';
    resizer.style.position = 'absolute';
    resizer.style.right = '0';
    resizer.style.top = '0';
    resizer.style.width = '5px';
    resizer.style.height = '100%';
    resizer.style.cursor = 'col-resize';
    resizer.style.userSelect = 'none';
    resizer.style.touchAction = 'none';
    resizer.style.zIndex = '5'; // Higher than others

    // Drag handlers for resizing
    resizer.addEventListener('mousedown', (e) => {
        e.preventDefault();
        e.stopPropagation();

        const grid = grids[rsIndex];
        const colId = header.column.id;
        const startX = e.clientX;
        const startWidth = (grid && grid.columnWidths && grid.columnWidths.get(colId)) || th.getBoundingClientRect().width;

        const onMouseMove = (moveEvent) => {
            const currentX = moveEvent.clientX;
            const delta = currentX - startX;
            const newWidth = Math.max(50, startWidth + delta); // Min width 50

            if (grid && grid.columnWidths) {
                grid.columnWidths.set(colId, newWidth);
                // Update col element directly for performance
                // We need to find the correct col index
                if (grid.tanTable) {
                    const visibleCols = grid.tanTable.getVisibleLeafColumns();
                    const colIndex = visibleCols.findIndex(c => c.id === colId);
                    if (colIndex !== -1) {
                        const colGroup = grid.tanTable.options.meta?.colGroupElement ||
                            document.querySelectorAll('.grid-wrapper')[rsIndex]?.querySelector('colgroup');
                        if (colGroup && colGroup.children[colIndex]) {
                            colGroup.children[colIndex].style.width = newWidth + 'px';
                        }
                    }
                }
            }
        };

        const onMouseUp = () => {
            document.removeEventListener('mousemove', onMouseMove);
            document.removeEventListener('mouseup', onMouseUp);
            // Save state
            savePinnedState();
        };

        document.addEventListener('mousemove', onMouseMove);
        document.addEventListener('mouseup', onMouseUp);
    });

    // Prevent starting drag-reorder when clicking resizer
    resizer.draggable = true;
    resizer.ondragstart = (e) => {
        e.preventDefault();
        e.stopPropagation();
    };

    th.appendChild(resizer);

    return th;
}

// Function to show advanced Excel-like column filter dropdown
function showColumnFilterDropdown(column, table, anchorElement, rsIndex) {
    // Remove any existing dropdown
    const existingDropdown = document.querySelector('.column-filter-dropdown');
    if (existingDropdown) {
        existingDropdown.remove();
    }

    // Get unique values for this column with counts
    const uniqueValues = column.columnDef.uniqueValues || [];
    const currentFilter = column.getFilterValue();

    // Calculate value counts from the original data
    const valueCounts = new Map();
    const allRows = table.getCoreRowModel().rows;
    allRows.forEach(row => {
        const cellValue = column.columnDef.accessorFn(row.original);
        const stringValue = cellValue === null || cellValue === undefined || cellValue === '' ? 'NULL' : String(cellValue);
        valueCounts.set(stringValue, (valueCounts.get(stringValue) || 0) + 1);
    });

    // Detect if column is numeric
    const isNumericColumn = uniqueValues.some(v => {
        if (v === 'NULL') return false;
        const num = parseFloat(String(v).replace(/,/g, ''));
        return !isNaN(num);
    });

    // Create dropdown container
    const dropdown = document.createElement('div');
    dropdown.className = 'column-filter-dropdown';
    dropdown.style.position = 'fixed';
    dropdown.style.zIndex = '10000';
    dropdown.style.backgroundColor = 'var(--vscode-dropdown-background)';
    dropdown.style.border = '1px solid var(--vscode-dropdown-border)';
    dropdown.style.borderRadius = '4px';
    dropdown.style.width = '320px';
    dropdown.style.boxShadow = '0 8px 16px rgba(0,0,0,0.4)';
    dropdown.style.display = 'flex';
    dropdown.style.flexDirection = 'column';

    // Position dropdown below the filter button
    const rect = anchorElement.getBoundingClientRect();
    let top = rect.bottom + 5;
    let left = rect.left;

    // Calculate available height
    const availableHeightBelow = window.innerHeight - rect.bottom - 20;
    const availableHeightAbove = rect.top - 20;
    const desiredHeight = 400;

    let dropdownHeight;

    // Ensure dropdown doesn't go off-screen horizontally
    if (left + 320 > window.innerWidth) {
        left = window.innerWidth - 330;
    }
    if (left < 10) left = 10;

    // Decide whether to show above or below
    if (availableHeightBelow >= desiredHeight) {
        // Enough space below
        dropdownHeight = Math.min(desiredHeight, availableHeightBelow);
    } else if (availableHeightAbove > availableHeightBelow) {
        // More space above, show above
        dropdownHeight = Math.min(desiredHeight, availableHeightAbove);
        top = rect.top - dropdownHeight - 5;
    } else {
        // Show below but constrained
        dropdownHeight = Math.max(200, availableHeightBelow); // Minimum 200px
    }

    dropdown.style.top = Math.max(10, top) + 'px';
    dropdown.style.left = left + 'px';
    dropdown.style.maxHeight = dropdownHeight + 'px';

    // ========== CREATE TABS ==========
    const tabsContainer = document.createElement('div');
    tabsContainer.className = 'filter-tabs';

    const valuesTab = document.createElement('button');
    valuesTab.className = 'filter-tab active';
    valuesTab.textContent = '📋 Values';
    valuesTab.dataset.tab = 'values';

    const conditionsTab = document.createElement('button');
    conditionsTab.className = 'filter-tab';
    conditionsTab.textContent = '🔧 Conditions';
    conditionsTab.dataset.tab = 'conditions';

    tabsContainer.appendChild(valuesTab);
    tabsContainer.appendChild(conditionsTab);
    dropdown.appendChild(tabsContainer);

    // ========== TAB CONTENT CONTAINERS ==========
    const valuesContent = document.createElement('div');
    valuesContent.className = 'filter-tab-content active';
    valuesContent.dataset.tabContent = 'values';

    const conditionsContent = document.createElement('div');
    conditionsContent.className = 'filter-tab-content';
    conditionsContent.dataset.tabContent = 'conditions';

    // Tab switching logic
    const switchTab = (tabName) => {
        [valuesTab, conditionsTab].forEach(t => t.classList.remove('active'));
        [valuesContent, conditionsContent].forEach(c => c.classList.remove('active'));

        if (tabName === 'values') {
            valuesTab.classList.add('active');
            valuesContent.classList.add('active');
        } else {
            conditionsTab.classList.add('active');
            conditionsContent.classList.add('active');
        }
    };

    valuesTab.onclick = () => switchTab('values');
    conditionsTab.onclick = () => switchTab('conditions');

    // ========== VALUES TAB CONTENT ==========

    // Quick filter buttons
    const quickFilters = document.createElement('div');
    quickFilters.className = 'quick-filters';

    const blanksBtn = document.createElement('button');
    blanksBtn.className = 'quick-filter-btn';
    blanksBtn.textContent = 'Blanks';
    blanksBtn.onclick = () => {
        checkboxes.forEach((cb, val) => cb.checked = (val === 'NULL'));
    };

    const nonBlanksBtn = document.createElement('button');
    nonBlanksBtn.className = 'quick-filter-btn';
    nonBlanksBtn.textContent = 'Non-blanks';
    nonBlanksBtn.onclick = () => {
        checkboxes.forEach((cb, val) => cb.checked = (val !== 'NULL'));
    };

    quickFilters.appendChild(blanksBtn);
    quickFilters.appendChild(nonBlanksBtn);

    if (isNumericColumn) {
        const top10Btn = document.createElement('button');
        top10Btn.className = 'quick-filter-btn';
        top10Btn.textContent = 'Top 10';
        top10Btn.onclick = () => {
            const numericValues = uniqueValues
                .filter(v => v !== 'NULL')
                .map(v => ({ val: v, num: parseFloat(String(v).replace(/,/g, '')) }))
                .filter(x => !isNaN(x.num))
                .sort((a, b) => b.num - a.num)
                .slice(0, 10)
                .map(x => x.val);
            checkboxes.forEach((cb, val) => cb.checked = numericValues.includes(val));
        };

        const bottom10Btn = document.createElement('button');
        bottom10Btn.className = 'quick-filter-btn';
        bottom10Btn.textContent = 'Bottom 10';
        bottom10Btn.onclick = () => {
            const numericValues = uniqueValues
                .filter(v => v !== 'NULL')
                .map(v => ({ val: v, num: parseFloat(String(v).replace(/,/g, '')) }))
                .filter(x => !isNaN(x.num))
                .sort((a, b) => a.num - b.num)
                .slice(0, 10)
                .map(x => x.val);
            checkboxes.forEach((cb, val) => cb.checked = numericValues.includes(val));
        };

        quickFilters.appendChild(top10Btn);
        quickFilters.appendChild(bottom10Btn);
    }

    valuesContent.appendChild(quickFilters);

    // Search box with icon
    const searchWrapper = document.createElement('div');
    searchWrapper.className = 'filter-search-wrapper';

    const searchIcon = document.createElement('span');
    searchIcon.className = 'filter-search-icon';
    searchIcon.textContent = '🔍';

    const searchBox = document.createElement('input');
    searchBox.type = 'text';
    searchBox.placeholder = 'Search values...';
    searchBox.className = 'filter-search-input';

    searchWrapper.appendChild(searchIcon);
    searchWrapper.appendChild(searchBox);
    valuesContent.appendChild(searchWrapper);

    // Selection buttons
    const selectionButtons = document.createElement('div');
    selectionButtons.className = 'filter-selection-buttons';

    const selectAllBtn = document.createElement('button');
    selectAllBtn.className = 'filter-selection-btn';
    selectAllBtn.textContent = '✓ Select All';

    const clearAllBtn = document.createElement('button');
    clearAllBtn.className = 'filter-selection-btn';
    clearAllBtn.textContent = '✗ Clear All';

    const invertBtn = document.createElement('button');
    invertBtn.className = 'filter-selection-btn';
    invertBtn.textContent = '⇄ Invert';

    selectionButtons.appendChild(selectAllBtn);
    selectionButtons.appendChild(clearAllBtn);
    selectionButtons.appendChild(invertBtn);
    valuesContent.appendChild(selectionButtons);

    // Values list container
    const valuesContainer = document.createElement('div');
    valuesContainer.className = 'filter-values-container';

    // Parse current filter to determine checked values
    let checkedValues = new Set();
    if (currentFilter && Array.isArray(currentFilter) && currentFilter.length > 0) {
        currentFilter.forEach(v => checkedValues.add(v));
    } else {
        // No filter = all selected
        uniqueValues.forEach(v => checkedValues.add(v));
    }

    let filteredValues = [...uniqueValues];
    const checkboxes = new Map();

    function renderValuesList() {
        valuesContainer.innerHTML = '';
        filteredValues.forEach(value => {
            const item = document.createElement('div');
            item.className = 'filter-value-item';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = checkedValues.has(value);
            checkbox.onchange = () => {
                if (checkbox.checked) {
                    checkedValues.add(value);
                } else {
                    checkedValues.delete(value);
                }
            };

            const label = document.createElement('span');
            label.className = 'filter-value-label';
            label.textContent = value;
            label.title = value;

            const count = document.createElement('span');
            count.className = 'filter-value-count';
            count.textContent = valueCounts.get(value) || 0;

            item.onclick = (e) => {
                if (e.target !== checkbox) {
                    checkbox.checked = !checkbox.checked;
                    checkbox.dispatchEvent(new Event('change'));
                }
            };

            item.appendChild(checkbox);
            item.appendChild(label);
            item.appendChild(count);
            valuesContainer.appendChild(item);

            checkboxes.set(value, checkbox);
        });
    }

    // Search functionality
    searchBox.oninput = () => {
        const needle = searchBox.value.toLowerCase();
        filteredValues = uniqueValues.filter(value => {
            return String(value).toLowerCase().includes(needle);
        });
        renderValuesList();
    };

    // Selection button handlers
    selectAllBtn.onclick = () => {
        filteredValues.forEach(v => checkedValues.add(v));
        renderValuesList();
    };

    clearAllBtn.onclick = () => {
        filteredValues.forEach(v => checkedValues.delete(v));
        renderValuesList();
    };

    invertBtn.onclick = () => {
        filteredValues.forEach(v => {
            if (checkedValues.has(v)) {
                checkedValues.delete(v);
            } else {
                checkedValues.add(v);
            }
        });
        renderValuesList();
    };

    renderValuesList();
    valuesContent.appendChild(valuesContainer);

    // ========== CONDITIONS TAB CONTENT ==========

    // Filter type options
    const textFilterTypes = [
        { value: 'contains', label: 'Contains' },
        { value: 'notContains', label: 'Does not contain' },
        { value: 'equals', label: 'Equals' },
        { value: 'notEquals', label: 'Does not equal' },
        { value: 'startsWith', label: 'Starts with' },
        { value: 'endsWith', label: 'Ends with' },
        { value: 'isEmpty', label: 'Is empty' },
        { value: 'isNotEmpty', label: 'Is not empty' }
    ];

    const numericFilterTypes = [
        { value: 'equals', label: 'Equals' },
        { value: 'notEquals', label: 'Does not equal' },
        { value: 'greaterThan', label: 'Greater than' },
        { value: 'greaterThanOrEqual', label: 'Greater than or equal' },
        { value: 'lessThan', label: 'Less than' },
        { value: 'lessThanOrEqual', label: 'Less than or equal' },
        { value: 'between', label: 'Between' },
        { value: 'isEmpty', label: 'Is empty' },
        { value: 'isNotEmpty', label: 'Is not empty' }
    ];

    const filterTypes = isNumericColumn ? numericFilterTypes : textFilterTypes;

    // State for conditions - restore from current filter if it's a condition filter
    let conditions;
    let logicOperator;

    if (currentFilter && currentFilter._isConditionFilter) {
        // Restore saved conditions
        conditions = currentFilter.conditions.map(c => ({
            type: c.type,
            value: c.value || '',
            value2: c.value2 || ''
        }));
        logicOperator = currentFilter.logic || 'and';
    } else {
        // Default state
        conditions = [{ type: filterTypes[0].value, value: '', value2: '' }];
        logicOperator = 'and';
    }

    function createConditionRow(condition, index) {
        const row = document.createElement('div');
        row.className = 'filter-condition';

        const conditionRow = document.createElement('div');
        conditionRow.className = 'filter-condition-row';

        // Filter type select
        const typeSelect = document.createElement('select');
        typeSelect.className = 'filter-type-dropdown';
        filterTypes.forEach(ft => {
            const opt = document.createElement('option');
            opt.value = ft.value;
            opt.textContent = ft.label;
            if (ft.value === condition.type) opt.selected = true;
            typeSelect.appendChild(opt);
        });

        typeSelect.onchange = () => {
            condition.type = typeSelect.value;
            renderConditions();
        };

        conditionRow.appendChild(typeSelect);

        // Value input(s) based on filter type
        const needsNoInput = ['isEmpty', 'isNotEmpty'].includes(condition.type);
        const needsTwoInputs = condition.type === 'between';

        if (!needsNoInput) {
            if (needsTwoInputs) {
                const betweenContainer = document.createElement('div');
                betweenContainer.className = 'filter-between-inputs';

                const input1 = document.createElement('input');
                input1.type = isNumericColumn ? 'number' : 'text';
                input1.className = 'filter-value-input';
                input1.placeholder = 'From';
                input1.value = condition.value || '';
                input1.oninput = () => { condition.value = input1.value; };

                const sep = document.createElement('span');
                sep.className = 'filter-between-separator';
                sep.textContent = 'and';

                const input2 = document.createElement('input');
                input2.type = isNumericColumn ? 'number' : 'text';
                input2.className = 'filter-value-input';
                input2.placeholder = 'To';
                input2.value = condition.value2 || '';
                input2.oninput = () => { condition.value2 = input2.value; };

                betweenContainer.appendChild(input1);
                betweenContainer.appendChild(sep);
                betweenContainer.appendChild(input2);
                conditionRow.appendChild(betweenContainer);
            } else {
                const input = document.createElement('input');
                input.type = isNumericColumn && !['contains', 'notContains', 'startsWith', 'endsWith'].includes(condition.type) ? 'number' : 'text';
                input.className = 'filter-value-input';
                input.placeholder = 'Enter value...';
                input.value = condition.value || '';
                input.oninput = () => { condition.value = input.value; };
                conditionRow.appendChild(input);
            }
        }

        // Remove condition button (only if more than one condition)
        if (conditions.length > 1) {
            const removeBtn = document.createElement('button');
            removeBtn.className = 'remove-condition-btn';
            removeBtn.textContent = '×';
            removeBtn.title = 'Remove condition';
            removeBtn.onclick = (e) => {
                e.stopPropagation();
                conditions.splice(index, 1);
                renderConditions();
            };
            conditionRow.appendChild(removeBtn);
        }

        row.appendChild(conditionRow);
        return row;
    }

    function renderConditions() {
        conditionsContent.innerHTML = '';

        conditions.forEach((cond, idx) => {
            // Add logic operator between conditions
            if (idx > 0) {
                const logicToggle = document.createElement('div');
                logicToggle.className = 'filter-logic-toggle';
                logicToggle.style.margin = '8px 0';
                logicToggle.style.padding = '6px';
                logicToggle.style.backgroundColor = 'var(--vscode-editor-background)';
                logicToggle.style.borderRadius = '4px';
                logicToggle.style.display = 'flex';
                logicToggle.style.justifyContent = 'center';
                logicToggle.style.gap = '8px';

                const andBtn = document.createElement('button');
                andBtn.className = 'filter-logic-btn' + (logicOperator === 'and' ? ' active' : '');
                andBtn.textContent = '⋀ AND';
                andBtn.title = 'All conditions must match';
                andBtn.onclick = (e) => {
                    e.stopPropagation();
                    logicOperator = 'and';
                    renderConditions();
                };

                const orBtn = document.createElement('button');
                orBtn.className = 'filter-logic-btn' + (logicOperator === 'or' ? ' active' : '');
                orBtn.textContent = '⋁ OR';
                orBtn.title = 'Any condition must match';
                orBtn.onclick = (e) => {
                    e.stopPropagation();
                    logicOperator = 'or';
                    renderConditions();
                };

                logicToggle.appendChild(andBtn);
                logicToggle.appendChild(orBtn);
                conditionsContent.appendChild(logicToggle);
            }

            const condRow = createConditionRow(cond, idx);
            conditionsContent.appendChild(condRow);
        });

        // Add condition button
        if (conditions.length < 3) {
            const addBtn = document.createElement('button');
            addBtn.className = 'add-condition-btn';
            addBtn.textContent = conditions.length === 1 ? '+ Add another condition (for AND/OR)' : '+ Add condition';
            addBtn.onclick = (e) => {
                e.stopPropagation();
                e.preventDefault();
                conditions.push({ type: filterTypes[0].value, value: '', value2: '' });
                renderConditions();
            };
            conditionsContent.appendChild(addBtn);
        }
    }

    renderConditions();

    // ========== ACTION BUTTONS ==========
    const actionsContainer = document.createElement('div');
    actionsContainer.className = 'filter-actions';

    const clearFilterBtn = document.createElement('button');
    clearFilterBtn.className = 'filter-btn';
    clearFilterBtn.textContent = 'Clear Filter';
    clearFilterBtn.onclick = () => {
        column.setFilterValue(undefined);
        dropdown.remove();
    };

    const applyBtn = document.createElement('button');
    applyBtn.className = 'filter-btn primary';
    applyBtn.textContent = 'Apply';
    applyBtn.onclick = () => {
        const activeTab = valuesTab.classList.contains('active') ? 'values' : 'conditions';

        if (activeTab === 'values') {
            // Apply values filter
            const selectedValues = Array.from(checkedValues);
            if (selectedValues.length === uniqueValues.length || selectedValues.length === 0) {
                column.setFilterValue(undefined);
            } else {
                column.setFilterValue(selectedValues);
            }
        } else {
            // Apply conditions filter
            // Create a custom filter object that our filterFn can interpret
            const validConditions = conditions.filter(c => {
                if (['isEmpty', 'isNotEmpty'].includes(c.type)) return true;
                if (c.type === 'between') return c.value !== '' && c.value2 !== '';
                return c.value !== '';
            });

            if (validConditions.length === 0) {
                column.setFilterValue(undefined);
            } else {
                // Store conditions as a special object
                column.setFilterValue({
                    _isConditionFilter: true,
                    conditions: validConditions,
                    logic: logicOperator
                });
            }
        }
        savePinnedState();
        dropdown.remove();
    };

    actionsContainer.appendChild(clearFilterBtn);
    actionsContainer.appendChild(applyBtn);

    // ========== ASSEMBLE DROPDOWN ==========
    dropdown.appendChild(valuesContent);
    dropdown.appendChild(conditionsContent);
    dropdown.appendChild(actionsContainer);

    document.body.appendChild(dropdown);

    // Auto-switch to Conditions tab if there's an existing condition filter
    if (currentFilter && currentFilter._isConditionFilter) {
        switchTab('conditions');
    }

    // Focus search box (only if on Values tab)
    setTimeout(() => {
        if (valuesTab.classList.contains('active')) {
            searchBox.focus();
        }
    }, 50);

    // Close on outside click
    setTimeout(() => {
        const closeHandler = (e) => {
            if (!dropdown.contains(e.target) && !anchorElement.contains(e.target)) {
                dropdown.remove();
                document.removeEventListener('click', closeHandler);
            }
        };
        document.addEventListener('click', closeHandler);
    }, 100);

    // Close on Escape key
    const keyHandler = (e) => {
        if (e.key === 'Escape') {
            dropdown.remove();
            document.removeEventListener('keydown', keyHandler);
        }
    };
    document.addEventListener('keydown', keyHandler);
}

// Function to show aggregation selection dropdown for a column
function showAggregationDropdown(column, table, anchorElement, rsIndex) {
    const existing = document.querySelector('.column-aggregation-dropdown');
    if (existing) existing.remove();

    const dropdown = document.createElement('div');
    dropdown.className = 'column-aggregation-dropdown';
    dropdown.style.position = 'absolute';
    dropdown.style.zIndex = '1000';
    dropdown.style.backgroundColor = 'var(--vscode-dropdown-background)';
    dropdown.style.border = '1px solid var(--vscode-dropdown-border)';
    dropdown.style.borderRadius = '3px';
    dropdown.style.minWidth = '150px';
    dropdown.style.padding = '8px';
    dropdown.style.boxShadow = '0 4px 8px rgba(0,0,0,0.3)';
    dropdown.style.fontFamily = 'var(--vscode-font-family)';
    dropdown.style.fontSize = '13px';

    const rect = anchorElement.getBoundingClientRect();
    dropdown.style.top = (rect.bottom + 5) + 'px';
    dropdown.style.left = rect.left + 'px';

    const title = document.createElement('div');
    title.textContent = 'Aggregation';
    title.style.fontWeight = '600';
    title.style.marginBottom = '6px';
    dropdown.appendChild(title);

    const select = document.createElement('select');
    const opts = [['none', 'None'], ['sum', 'Sum'], ['count', 'Count'], ['countDistinct', 'Count Distinct']];
    opts.forEach(o => {
        const el = document.createElement('option');
        el.value = o[0];
        el.textContent = o[1];
        select.appendChild(el);
    });

    const current = (aggregationStates[rsIndex] || {})[column.id] || 'none';
    select.value = current;
    dropdown.appendChild(select);

    const btnRow = document.createElement('div');
    btnRow.style.marginTop = '8px';
    btnRow.style.display = 'flex';
    btnRow.style.gap = '6px';

    const applyBtn = document.createElement('button');
    applyBtn.textContent = 'Apply';
    applyBtn.className = 'filter-btn primary';
    applyBtn.onclick = () => {
        const val = select.value;
        if (!aggregationStates[rsIndex]) aggregationStates[rsIndex] = {};
        if (val === 'none') delete aggregationStates[rsIndex][column.id]; else aggregationStates[rsIndex][column.id] = val;
        savePinnedState();
        dropdown.remove();
        if (grids[rsIndex] && grids[rsIndex].render) grids[rsIndex].render();
    };

    const cancelBtn = document.createElement('button');
    cancelBtn.textContent = 'Cancel';
    cancelBtn.className = 'filter-btn clear';
    cancelBtn.onclick = () => { dropdown.remove(); };

    btnRow.appendChild(applyBtn);
    btnRow.appendChild(cancelBtn);
    dropdown.appendChild(btnRow);

    document.body.appendChild(dropdown);

    setTimeout(() => {
        const closeHandler = (e) => {
            if (!dropdown.contains(e.target) && !anchorElement.contains(e.target)) {
                dropdown.remove();
                document.removeEventListener('click', closeHandler);
            }
        };
        document.addEventListener('click', closeHandler);
    }, 100);
}

// Global filter handler
// Global filter handler with debounce
const debouncedSearch = debounce((value) => {
    if (grids[activeGridIndex]) {
        if (searchWorker) {
            isSearching = true;
            const rowCountInfo = document.getElementById('rowCountInfo');
            if (rowCountInfo) {
                rowCountInfo.textContent = 'Searching...';
                rowCountInfo.style.opacity = '1';
            }

            searchWorker.postMessage({
                command: 'search',
                id: activeGridIndex,
                query: value
            });
        } else {
            // Fallback if no worker (shouldn't happen given setup)
            grids[activeGridIndex].tanTable.setGlobalFilter(value);
        }
    } else {
        console.error('No grid at activeGridIndex:', activeGridIndex);
    }
}, 300);

function onFilterChanged() {
    const filterInput = document.getElementById('globalFilter');
    debouncedSearch(filterInput.value);
}

// Helper functions
function removeGroup(colId) {
    if (grids[activeGridIndex]) {
        const currentGrouping = grids[activeGridIndex].tanTable.getState().grouping;
        const newGrouping = currentGrouping.filter(id => id !== colId);
        grids[activeGridIndex].tanTable.setGrouping(newGrouping);
    }
}

function setupGlobalKeyboardShortcuts() {
    // Add any global keyboard shortcuts here
}

// Export functions (keeping existing functionality)


function getAllGridsExportData() {
    if (!window.resultSets || window.resultSets.length === 0) return [];

    const exportData = [];

    window.resultSets.forEach((rs, index) => {
        if (rs.isLog) return; // Skip logs

        const grid = grids[index];
        if (!grid || !grid.tanTable) return;

        const table = grid.tanTable;
        const filteredRows = table.getFilteredRowModel().rows;
        const visibleHeaders = table.getAllColumns().filter(col => col.getIsVisible());

        // Build column metadata with types from original resultSet
        // Map visible header IDs to original column definitions
        const columns = visibleHeaders.map(h => {
            const colIndex = parseInt(h.id);
            const originalCol = rs.columns[colIndex];
            return {
                name: h.columnDef.header,
                type: originalCol ? originalCol.type : undefined
            };
        });

        // Build rows as arrays of values (preserving original values)
        const rows = filteredRows.map(row =>
            visibleHeaders.map(header => row.getValue(header.id))
        );

        exportData.push({
            columns: columns,
            rows: rows,
            sql: rs.sql || '',
            name: rs.name || `Result ${index + 1}`,
            isActive: index === activeGridIndex
        });
    });

    return exportData;
}

function openInExcel() {
    const data = getAllGridsExportData();
    if (data.length === 0) return;

    vscode.postMessage({
        command: 'openInExcel',
        data: data
    });
}

function openInExcelXlsx() {
    const data = getAllGridsExportData();
    if (data.length === 0) return;

    vscode.postMessage({
        command: 'info',
        text: 'Starting Excel (XLSX) export...'
    });

    vscode.postMessage({
        command: 'openInExcelXlsx',
        data: data
    });
}

function copyAsExcel() {
    const data = getAllGridsExportData();
    if (data.length === 0) return;

    vscode.postMessage({
        command: 'copyAsExcel',
        data: data
    });
}

function escapeCsvValue(value) {
    if (value === null || value === undefined) return '';
    const str = String(value);
    if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
        return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
}

function exportToCsv() {
    if (!grids[activeGridIndex] || !grids[activeGridIndex].tanTable) return;

    const table = grids[activeGridIndex].tanTable;
    const rows = table.getFilteredRowModel().rows;
    const headers = table.getAllColumns().filter(col => col.getIsVisible());

    let csv = headers.map(h => escapeCsvValue(h.columnDef.header)).join(',') + '\n';

    rows.forEach(row => {
        const rowData = headers.map(header => {
            const cell = row.getValue(header.id);
            return escapeCsvValue(cell);
        });
        csv += rowData.join(',') + '\n';
    });

    vscode.postMessage({
        command: 'exportCsv',
        data: csv
    });
}



// Split Button Logic
let currentExportFormat = 'excel'; // default

function executeSplitExport() {
    switch (currentExportFormat) {
        case 'excel':
            openInExcel();
            break;
        case 'xlsx':
            openInExcelXlsx();
            break;
        case 'csv':
            exportToCsv();
            break;
        case 'json':
            exportToJson();
            break;
        case 'xml':
            exportToXml();
            break;
        case 'sql':
            exportToSqlInsert();
            break;
        case 'markdown':
            exportToMarkdown();
            break;
    }
}

function toggleExportMenu() {
    const menu = document.getElementById('exportMenu');
    if (menu.style.display === 'none') {
        menu.style.display = 'block';
    } else {
        menu.style.display = 'none';
    }
}

function selectExportFormat(format) {
    currentExportFormat = format;

    // Update main button UI
    // We can copy innerHTML from the selected menu item (icon + text)
    // We need to find the menu item that was clicked. 
    // Since we pass format string, we can find the element by querying for onclick attribute manually or just iterating.

    // Or easier: hardcode the mapping or use data-attributes. 
    // Let's rely on finding the menu item by its onclick attribute content for simplicity in this context.
    const items = document.querySelectorAll('.split-btn-menu-item');
    let selectedItem = null;
    items.forEach(item => {
        if (item.getAttribute('onclick').includes(`'${format}'`)) {
            selectedItem = item;
        }
    });

    if (selectedItem) {
        const btn = document.getElementById('exportMainBtn');
        // Copy icon and text
        // But we want to preserve "Excel" vs "Excel (XLSB)" distinction? 
        // The menu item says "Excel (XLSB)", the button said "Excel".
        // Let's just use what's in the menu item, it's fine.
        btn.innerHTML = selectedItem.innerHTML;
        // Trim format specifier for shorter button text
        if (format === 'excel') btn.innerHTML = btn.innerHTML.replace(' (XLSB)', '');
        if (format === 'xlsx') btn.innerHTML = btn.innerHTML.replace(' (XLSX)', '');
    }

    // Hide menu
    document.getElementById('exportMenu').style.display = 'none';
}

// Close menu when clicking outside
document.addEventListener('click', (e) => {
    const container = document.querySelector('.split-btn-container');
    const menu = document.getElementById('exportMenu');
    if (container && menu && menu.style.display !== 'none') {
        if (!container.contains(e.target)) {
            menu.style.display = 'none';
        }
    }
});

function getValueForExport(row, columnId, resultSetColumns) {
    // Helper to get raw value from row.original
    // columnId is usually the index string "0", "1", etc.
    const index = parseInt(columnId);
    if (row.original) {
        if (Array.isArray(row.original)) {
            if (!isNaN(index) && index >= 0 && index < row.original.length) {
                return row.original[index];
            }
        } else {
            // Object based
            // We need to know the key.
            // If column definition has accessorKey, use it.
            const colDef = resultSetColumns[index];
            if (colDef && colDef.accessorKey) {
                return row.original[colDef.accessorKey];
            }
            // Fallback to index if it's stored as "0", "1" etc in object
            return row.original[columnId];
        }
    }
    return null;
}

function exportToJson() {
    if (!grids[activeGridIndex] || !grids[activeGridIndex].tanTable) return;

    const table = grids[activeGridIndex].tanTable;
    const rows = table.getFilteredRowModel().rows;
    const headers = table.getAllColumns().filter(col => col.getIsVisible());
    const rsColumns = window.resultSets[activeGridIndex].columns;

    const data = rows.map(row => {
        const obj = {};
        headers.forEach(header => {
            const val = getValueForExport(row, header.id, rsColumns);
            // Use header text as key
            obj[header.columnDef.header] = val;
        });
        return obj;
    });

    vscode.postMessage({
        command: 'exportJson',
        data: JSON.stringify(data, null, 2)
    });
}

function exportToXml() {
    if (!grids[activeGridIndex] || !grids[activeGridIndex].tanTable) return;

    const table = grids[activeGridIndex].tanTable;
    const rows = table.getFilteredRowModel().rows;
    const headers = table.getAllColumns().filter(col => col.getIsVisible());
    const rsColumns = window.resultSets[activeGridIndex].columns;

    let xml = '<?xml version="1.0" encoding="UTF-8"?>\n<results>\n';

    rows.forEach(row => {
        xml += '  <row>\n';
        headers.forEach(header => {
            const val = getValueForExport(row, header.id, rsColumns);
            const tagName = header.columnDef.header.replace(/[^a-zA-Z0-9_-]/g, '_');

            let content = '';
            if (val !== null && val !== undefined) {
                // simple XML escaping
                content = String(val)
                    .replace(/&/g, '&amp;')
                    .replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;')
                    .replace(/"/g, '&quot;')
                    .replace(/'/g, '&apos;');
            }

            xml += `    <${tagName}>${content}</${tagName}>\n`;
        });
        xml += '  </row>\n';
    });
    xml += '</results>';

    vscode.postMessage({
        command: 'exportXml',
        data: xml
    });
}

function exportToSqlInsert() {
    if (!grids[activeGridIndex] || !grids[activeGridIndex].tanTable) return;

    const table = grids[activeGridIndex].tanTable;
    const rows = table.getFilteredRowModel().rows;
    const headers = table.getAllColumns().filter(col => col.getIsVisible());
    const rsColumns = window.resultSets[activeGridIndex].columns;

    const tableName = 'EXPORT_TABLE';
    const colNames = headers.map(h => h.columnDef.header.replace(/[^a-zA-Z0-9_]/g, '') || 'COL').join(', ');

    let sql = '';

    rows.forEach(row => {
        const values = headers.map(header => {
            const val = getValueForExport(row, header.id, rsColumns);

            if (val === null || val === undefined) {
                return 'NULL';
            }

            if (typeof val === 'number') {
                return val;
            }

            if (typeof val === 'boolean') {
                return val ? 'TRUE' : 'FALSE';
            }

            // Handle BigInt (might come as number or string depending on serialization)
            // But typeof val would catch it if it was supported as primitive, assuming serialization handled it.

            // String escaping for SQL
            const str = String(val);
            return `'${str.replace(/'/g, "''")}'`;
        });

        sql += `INSERT INTO ${tableName} (${colNames}) VALUES (${values.join(', ')});\n`;
    });

    vscode.postMessage({
        command: 'exportSqlInsert',
        data: sql
    });
}

function exportToMarkdown() {
    if (!grids[activeGridIndex] || !grids[activeGridIndex].tanTable) return;

    const table = grids[activeGridIndex].tanTable;
    const rows = table.getFilteredRowModel().rows;
    const headers = table.getAllColumns().filter(col => col.getIsVisible());
    const rsColumns = window.resultSets[activeGridIndex].columns;

    // Header
    let md = '| ' + headers.map(h => h.columnDef.header.replace(/\|/g, '\\|')).join(' | ') + ' |\n';

    // Separator
    md += '| ' + headers.map(() => '---').join(' | ') + ' |\n';

    // Rows
    rows.forEach(row => {
        const rowData = headers.map(header => {
            const val = getValueForExport(row, header.id, rsColumns);
            if (val === null || val === undefined) return '';
            // Escape pipes and newlines
            return String(val).replace(/\|/g, '\\|').replace(/\r?\n/g, ' ');
        });
        md += '| ' + rowData.join(' | ') + ' |\n';
    });

    vscode.postMessage({
        command: 'exportMarkdown',
        data: md
    });
}

// Function to handle drag over events on grouping panel
function onDragOverGroup(event) {
    event.preventDefault();

    const panel = document.getElementById('groupingPanel');
    if (panel) {
        // Set appropriate drop effect based on what's being dragged
        if (globalDragState.dragType === 'column') {
            event.dataTransfer.dropEffect = 'copy';
            panel.style.backgroundColor = 'var(--vscode-list-dropBackground)';
            panel.style.border = '2px dashed var(--vscode-charts-blue)';
        } else if (globalDragState.dragType === 'groupChip') {
            event.dataTransfer.dropEffect = 'move';
            panel.style.backgroundColor = 'var(--vscode-list-dropBackground)';
            panel.style.border = '2px dashed var(--vscode-charts-orange, orange)';
        } else {
            event.dataTransfer.dropEffect = 'none';
        }
    }
}

// Function to handle drag leave events on grouping panel  
function onDragLeaveGroup(event) {
    const panel = document.getElementById('groupingPanel');
    if (panel) {
        panel.style.backgroundColor = '';
        panel.style.border = '';
    }
}

// Function to handle drop events on grouping panel
function onDropGroup(event) {
    event.preventDefault();
    const panel = document.getElementById('groupingPanel');
    if (panel) {
        panel.style.backgroundColor = '';
        panel.style.border = '';
    }

    const type = event.dataTransfer.getData('type');

    if (type === 'column') {
        // Use columnId for internal operations (text/plain now has column name for external drops)
        const colId = event.dataTransfer.getData('columnId');
        if (colId && grids[activeGridIndex] && grids[activeGridIndex].tanTable) {
            const currentGrouping = grids[activeGridIndex].tanTable.getState().grouping;
            if (!currentGrouping.includes(colId)) {
                grids[activeGridIndex].tanTable.setGrouping([...currentGrouping, colId]);
            }
        }
    } else if (type === 'groupChip') {
        // Handle reordering of group chips
        const draggedColId = event.dataTransfer.getData('text/plain');

        // Find the target chip element
        let targetElement = event.target.closest('.group-chip');

        // If we dropped on the panel but not on a specific chip, add to end
        if (!targetElement) {
            // Find all chips and add at the end position
            const allChips = panel.querySelectorAll('.group-chip');
            if (allChips.length > 0) {
                targetElement = allChips[allChips.length - 1];
            }
        }

        if (targetElement && grids[activeGridIndex] && grids[activeGridIndex].tanTable) {
            const targetColId = targetElement.dataset.colId;
            const currentGrouping = grids[activeGridIndex].tanTable.getState().grouping;

            if (draggedColId !== targetColId) {
                const newGrouping = [...currentGrouping];
                const fromIndex = newGrouping.indexOf(draggedColId);
                const toIndex = newGrouping.indexOf(targetColId);

                if (fromIndex !== -1 && toIndex !== -1) {
                    // Remove from old position
                    newGrouping.splice(fromIndex, 1);
                    // Insert at new position (adjust index if we removed from earlier position)
                    const insertIndex = fromIndex < toIndex ? toIndex : toIndex + 1;
                    newGrouping.splice(insertIndex, 0, draggedColId);

                    grids[activeGridIndex].tanTable.setGrouping(newGrouping);
                }
            }
        }
    }
}

// Function to setup cell selection events for rectangular selection
function setupCellSelectionEvents(wrapper, table, columnCount) {
    let isSelecting = false;
    let startCell = null;
    let endCell = null;

    let selectedCells = new Set();
    let isAllSelected = false;

    function _internalClearSelection() {
        isAllSelected = false;
        selectedCells.forEach(cellId => {
            const cell = document.querySelector(`[data-cell-id="${cellId}"]`);
            if (cell) {
                cell.classList.remove('selected-cell');
            }
        });
        selectedCells.clear();
    }

    function clearSelection() {
        _internalClearSelection();
        vscode.postMessage({
            command: 'setContext',
            key: 'netezza.resultsHasSelection',
            value: false
        });
    }

    function getCellId(element) {
        // Find the closest td element
        const td = element.closest('td');
        if (!td) return null;

        const tr = td.closest('tr');
        if (!tr) return null;

        const rowIndex = tr.dataset.index;
        const cellIndex = Array.from(tr.children).indexOf(td);

        return `${rowIndex}-${cellIndex}`;
    }

    function selectRange(start, end) {
        if (!start || !end) return;

        const [startRow, startCol] = start.split('-').map(Number);
        const [endRow, endCol] = end.split('-').map(Number);

        const minRow = Math.min(startRow, endRow);
        const maxRow = Math.max(startRow, endRow);
        const minCol = Math.min(startCol, endCol);
        const maxCol = Math.max(startCol, endCol);

        _internalClearSelection();

        for (let row = minRow; row <= maxRow; row++) {
            for (let col = minCol; col <= maxCol; col++) {
                const cellId = `${row}-${col}`;
                selectedCells.add(cellId);

                const cell = document.querySelector(`[data-cell-id="${cellId}"]`);
                if (cell) {
                    cell.classList.add('selected-cell');
                }
            }
        }

        vscode.postMessage({
            command: 'setContext',
            key: 'netezza.resultsHasSelection',
            value: true
        });
    }

    function addCellIds() {
        const rows = wrapper.querySelectorAll('tbody tr');
        rows.forEach(tr => {
            const rowIndex = tr.dataset.index;
            if (rowIndex !== undefined) {
                const cells = tr.querySelectorAll('td');
                cells.forEach((td, cellIndex) => {
                    td.dataset.cellId = `${rowIndex}-${cellIndex}`;
                });
            }
        });
    }

    // Make wrapper focusable to receive keyboard events
    wrapper.tabIndex = 0;
    wrapper.style.outline = 'none';  // Remove focus outline

    wrapper.addEventListener('mousedown', (e) => {
        if (e.target.closest('td')) {
            isSelecting = true;
            startCell = getCellId(e.target);
            endCell = startCell;
            e.preventDefault();

            // Focus wrapper so keyboard events work (Ctrl+C)
            wrapper.focus();

            if (!e.ctrlKey && !e.metaKey) {
                _internalClearSelection();
            }

            if (startCell) {
                selectedCells.add(startCell);
                const cell = document.querySelector(`[data-cell-id="${startCell}"]`);
                if (cell) {
                    cell.classList.add('selected-cell');
                }
                renderRowView();



                vscode.postMessage({
                    command: 'setContext',
                    key: 'netezza.resultsHasSelection',
                    value: true
                });
            }
        }
    });

    wrapper.addEventListener('mousemove', (e) => {
        if (isSelecting && startCell) {
            const currentCell = getCellId(e.target);
            if (currentCell && currentCell !== endCell) {
                endCell = currentCell;
                selectRange(startCell, endCell);
            }
        }
    });

    wrapper.addEventListener('mouseup', () => {
        isSelecting = false;
        renderRowView();
    });

    // Update cell IDs when table re-renders
    const observer = new MutationObserver(() => {
        addCellIds();
    });

    observer.observe(wrapper.querySelector('tbody'), {
        childList: true,
        subtree: true
    });

    // Initial setup
    addCellIds();

    function performSelectAll() {
        _internalClearSelection();
        isAllSelected = true;
        const rows = wrapper.querySelectorAll('tbody tr[data-index]');

        rows.forEach(tr => {
            const cells = tr.querySelectorAll('td[data-cell-id]');
            cells.forEach((td, cellIndex) => {
                const cellId = td.dataset.cellId;
                if (cellId) {
                    selectedCells.add(cellId);
                    td.classList.add('selected-cell');
                }
            });
        });

        vscode.postMessage({
            command: 'setContext',
            key: 'netezza.resultsHasSelection',
            value: true
        });
    }

    // Copy selection functionality
    // Return handlers to be attached to the grid object
    return {
        copySelection: function (withHeaders = false) {
            // Auto-select all if nothing is selected
            if (!isAllSelected && selectedCells.size === 0) {
                performSelectAll();
            }

            if (isAllSelected) {
                const rows = table.getFilteredRowModel().rows;
                const columns = table.getAllColumns().filter(col => col.getIsVisible());

                let clipboardText = '';

                // Add headers
                if (withHeaders) {
                    const headerRow = columns.map(col => col.columnDef.header);
                    clipboardText += headerRow.join('\t') + '\n';
                }

                // Add rows
                const rowStrings = rows.map(row => {
                    return columns.map(col => {
                        const cellValue = row.getValue(col.id);
                        if (cellValue === null || cellValue === undefined || cellValue === '') {
                            return 'NULL';
                        }
                        return String(cellValue);
                    }).join('\t');
                });

                clipboardText += rowStrings.join('\n');

                navigator.clipboard.writeText(clipboardText).then(() => {
                    vscode.postMessage({
                        command: 'info',
                        text: `Copied ${rows.length} rows to clipboard`
                    });
                }).catch(() => {
                    const textArea = document.createElement('textarea');
                    textArea.value = clipboardText;
                    document.body.appendChild(textArea);
                    textArea.select();
                    document.execCommand('copy');
                    document.body.removeChild(textArea);

                    vscode.postMessage({
                        command: 'info',
                        text: `Copied ${rows.length} rows to clipboard`
                    });
                });
                return;
            }

            if (selectedCells.size === 0) return;

            const cellArray = Array.from(selectedCells).map(cellId => {
                const [row, col] = cellId.split('-').map(Number);
                return { row, col, cellId };
            }).sort((a, b) => a.row - b.row || a.col - b.col);

            if (cellArray.length === 0) return;

            const minRow = Math.min(...cellArray.map(c => c.row));
            const maxRow = Math.max(...cellArray.map(c => c.row));
            const minCol = Math.min(...cellArray.map(c => c.col));
            const maxCol = Math.max(...cellArray.map(c => c.col));

            let clipboardText = '';

            // Add headers if requested
            if (withHeaders) {
                const headers = table.getAllColumns().filter(col => col.getIsVisible());
                const headerRow = [];
                for (let col = minCol; col <= maxCol; col++) {
                    if (headers[col]) {
                        headerRow.push(headers[col].columnDef.header);
                    }
                }
                clipboardText += headerRow.join('\t') + '\n';
            }

            // Add data rows
            for (let row = minRow; row <= maxRow; row++) {
                const rowData = [];
                for (let col = minCol; col <= maxCol; col++) {
                    const cell = wrapper.querySelector(`[data-cell-id="${row}-${col}"]`);
                    const value = cell ? cell.textContent.trim() : '';
                    rowData.push(value);
                }
                clipboardText += rowData.join('\t') + '\n';
            }

            navigator.clipboard.writeText(clipboardText).then(() => {
                vscode.postMessage({
                    command: 'info',
                    text: `Copied ${selectedCells.size} cells to clipboard`
                });
            }).catch(() => {
                // Fallback for older browsers
                const textArea = document.createElement('textarea');
                textArea.value = clipboardText;
                document.body.appendChild(textArea);
                textArea.select();
                document.execCommand('copy');
                document.body.removeChild(textArea);

                vscode.postMessage({
                    command: 'info',
                    text: `Copied ${selectedCells.size} cells to clipboard`
                });
            });
        },

        selectAll: function () {
            performSelectAll();
        },

        clearSelection: clearSelection,

        hasSelection: function () {
            return selectedCells.size > 0;
        }
    };
}


// Row View State
let isRowViewOpen = false;

function toggleRowView() {
    isRowViewOpen = !isRowViewOpen;
    const panel = document.getElementById('rowViewPanel');

    if (isRowViewOpen) {
        panel.classList.add('visible');
        renderRowView();
    } else {
        panel.classList.remove('visible');
    }
}

function renderRowView() {
    if (!isRowViewOpen || !grids[activeGridIndex] || !grids[activeGridIndex].tanTable) return;

    const content = document.getElementById('rowViewContent');
    const table = grids[activeGridIndex].tanTable;

    // Get unique selected row indices
    const selectedRows = new Set();
    const selectedCells = document.querySelectorAll('.selected-cell'); // Access selection from DOM

    selectedCells.forEach(cell => {
        const [rowIdx] = cell.dataset.cellId.split('-').map(Number);
        selectedRows.add(rowIdx);
    });

    const rowIndices = Array.from(selectedRows).sort((a, b) => a - b);

    if (rowIndices.length === 0) {
        content.innerHTML = '<div class="row-view-placeholder">Select rows to view details</div>';
        return;
    }

    if (rowIndices.length > 10) {
        content.innerHTML = '<div class="row-view-placeholder">Select 1 to 10 rows to compare</div>';
        return;
    }

    // GetData
    const rows = table.getRowModel().rows;
    const columns = table.getAllColumns().filter(col => col.getIsVisible());

    let html = '<table class="row-view-table"><thead><tr><th>Column</th>';

    rowIndices.forEach((idx, i) => {
        html += `<th>Value ${i + 1}</th>`;
    });

    html += '</tr></thead><tbody>';

    columns.forEach(col => {
        // Collect values for this column across all selected rows
        const values = rowIndices.map(rowIndex => {
            const val = rows[rowIndex].getValue(col.id);
            return val;
        });

        // Check for diffs (only if more than 1 row selected)
        let isDiff = false;
        if (rowIndices.length > 1) {
            const firstVal = String(values[0]);
            isDiff = values.some(v => String(v) !== firstVal);
        }

        html += `<tr class="${isDiff ? 'diff-cell' : ''}">`;
        html += `<td><b>${col.columnDef.header}</b><br><small style="opacity:0.6">${col.columnDef.dataType || ''}</small></td>`;

        values.forEach(val => {
            html += `<td class="${isDiff ? 'diff-cell-highlight' : ''}">${val ?? '<span class="null-value">NULL</span>'}</td>`;
        });

        html += '</tr>';
    });

    html += '</tbody></table>';
    content.innerHTML = html;
}

// Global functions that are called from HTML templates
window.onDropGroup = onDropGroup;
window.onDragOverGroup = onDragOverGroup;
window.onDragLeaveGroup = onDragLeaveGroup;
window.removeGroup = removeGroup;

// Window functions for compatibility
window.toggleRowView = toggleRowView;
window.openInExcel = openInExcel;
window.copyAsExcel = copyAsExcel;
window.exportToCsv = exportToCsv;
window.onFilterChanged = onFilterChanged;
window.clearFilter = function () {
    const filter = document.getElementById('globalFilter');
    if (filter) filter.value = '';
};

// Clear all column filters
window.clearAllFilters = function () {
    // Clear global filter input
    const globalFilter = document.getElementById('globalFilter');
    if (globalFilter) globalFilter.value = '';

    // Clear column filters for the active grid
    if (grids && grids.length > 0 && typeof activeGridIndex !== 'undefined' && activeGridIndex >= 0) {
        const activeGrid = grids[activeGridIndex];
        if (activeGrid && activeGrid.tanTable) {
            // Reset all column filters
            activeGrid.tanTable.resetColumnFilters();

            // Clear global filter too
            activeGrid.tanTable.setGlobalFilter('');

            // Re-render to update UI
            if (activeGrid.render) {
                activeGrid.render();
            }
        }
    }
};

// Keyboard shortcuts - Ctrl+C to copy rectangular selection
// Keyboard shortcuts - Ctrl+C to copy rectangular selection
document.addEventListener('keydown', function (e) {
    // Ctrl+C (or Cmd+C on Mac) to copy selection
    if ((e.ctrlKey || e.metaKey) && e.key === 'c') {
        // Check if there's a rectangular selection in the active grid
        if (grids[activeGridIndex] && grids[activeGridIndex].hasSelection && grids[activeGridIndex].hasSelection()) {
            e.preventDefault();  // Prevent default (Monaco editor) from handling
            e.stopPropagation();
            grids[activeGridIndex].copySelection(true);  // Copy with headers by default for Ctrl+C
        }
    }
});

// Global functions acting on active grid
window.copySelection = function (withHeaders) {
    if (grids[activeGridIndex] && grids[activeGridIndex].copySelection) {
        grids[activeGridIndex].copySelection(withHeaders);
    }
};

window.selectAll = function () {
    if (grids[activeGridIndex] && grids[activeGridIndex].selectAll) {
        grids[activeGridIndex].selectAll();
    }
};
