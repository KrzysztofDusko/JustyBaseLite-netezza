export class SchemaSearchHtmlGenerator {

    constructor() {
    }

    public generateHtml(): string {
        return `<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Schema Search</title>
        <style>
            body {
                font-family: var(--vscode-font-family);
                padding: 0;
                margin: 0;
                color: var(--vscode-foreground);
                display: flex;
                flex-direction: column;
                height: 100vh;
                overflow: hidden;
            }
            .search-box {
                display: flex;
                gap: 5px;
                padding: 10px;
                flex-shrink: 0;
                border-bottom: 1px solid var(--vscode-panel-border);
            }
            input { flex-grow: 1; padding: 5px; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); }
            button {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 6px;
                background-color: var(--vscode-button-secondaryBackground);
                color: var(--vscode-button-secondaryForeground);
                border: 1px solid var(--vscode-contrastBorder, transparent);
                padding: 4px 10px;
                cursor: pointer;
                border-radius: 2px;
                font-family: var(--vscode-font-family);
                font-size: 12px;
                line-height: 18px;
            }
            button:hover { background-color: var(--vscode-button-secondaryHoverBackground); }
            button.primary { background-color: var(--vscode-button-background); color: var(--vscode-button-foreground); }
            button.primary:hover { background-color: var(--vscode-button-hoverBackground); }
            #status { padding: 5px 10px; flex-shrink: 0; }
            .results {
                list-style: none;
                padding: 0;
                margin: 0;
                flex-grow: 1;
                overflow-y: auto;
            }
            .result-item { padding: 8px 10px; border-bottom: 1px solid var(--vscode-panel-border); display: flex; flex-direction: column; cursor: pointer; position: relative; }
            .result-item:hover { background: var(--vscode-list-hoverBackground); }
            .group-header {
                padding: 10px 10px 5px 10px;
                font-weight: bold;
                background: var(--vscode-editor-background);
                border-bottom: 1px solid var(--vscode-panel-border);
                display: flex;
                justify-content: space-between;
                align-items: center;
                cursor: pointer;
                user-select: none;
                position: sticky;
                top: 0;
                z-index: 10;
            }
            .group-header:hover {
                background: var(--vscode-list-hoverBackground);
            }
            .group-count {
                background: var(--vscode-badge-background);
                color: var(--vscode-badge-foreground);
                padding: 2px 8px;
                border-radius: 12px;
                font-size: 0.85em;
                font-weight: normal;
            }
            .group-toggle {
                display: inline-block;
                width: 12px;
                height: 12px;
                margin-right: 6px;
                transition: transform 0.2s;
            }
            .group-toggle.collapsed {
                transform: rotate(-90deg);
            }
            .group-items {
                display: contents;
            }
            .group-items.collapsed {
                display: none;
            }
            .item-header { display: flex; justify-content: space-between; font-weight: bold; }
            .item-details { font-size: 0.9em; opacity: 0.8; display: flex; gap: 10px; }
            .type-badge { font-size: 0.8em; background: var(--vscode-badge-background); color: var(--vscode-badge-foreground); padding: 2px 5px; border-radius: 3px; }
            .tooltip { position: absolute; background: var(--vscode-editorHoverWidget-background); color: var(--vscode-editorHoverWidget-foreground); border: 1px solid var(--vscode-editorHoverWidget-border); padding: 8px; border-radius: 4px; font-size: 0.9em; max-width: 300px; word-wrap: break-word; z-index: 1000; opacity: 0; visibility: hidden; transition: opacity 0.2s, visibility 0.2s; pointer-events: none; }
            .result-item:hover .tooltip { opacity: 1; visibility: visible; }
            .tooltip.top { bottom: 100%; left: 0; margin-bottom: 5px; }
            .tooltip.bottom { top: 100%; left: 0; margin-top: 5px; }
            .cache-badge { background-color: var(--vscode-charts-green); color: white; padding: 1px 4px; border-radius: 2px; font-size: 0.7em; margin-left: 5px; }
            .spinner {
                border: 2px solid transparent;
                border-top: 2px solid var(--vscode-progressBar-background);
                border-radius: 50%;
                width: 14px;
                height: 14px;
                animation: spin 1s linear infinite;
                display: inline-block;
                vertical-align: middle;
                margin-right: 8px;
            }
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
            .options-row {
                display: flex;
                align-items: center;
                gap: 8px;
                padding: 5px 10px;
                font-size: 12px;
                border-bottom: 1px solid var(--vscode-panel-border);
            }
            .options-row label {
                display: flex;
                align-items: center;
                gap: 4px;
                cursor: pointer;
            }
            .options-row select {
                background: var(--vscode-dropdown-background);
                color: var(--vscode-dropdown-foreground);
                border: 1px solid var(--vscode-dropdown-border);
                padding: 3px 6px;
                border-radius: 2px;
                cursor: pointer;
                font-size: 12px;
            }
            .searching-indicator {
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 20px;
                color: var(--vscode-descriptionForeground);
            }
        </style>
    </head>
    <body>
        <div class="search-box">
            <input type="text" id="searchInput" placeholder="Search tables, columns, view definitions, procedure source..." />
            <button id="searchBtn" class="primary">Search</button>
            <button id="cancelBtn" style="display: none;" title="Cancel search">✕</button>
            <button id="resetBtn" title="Reset search">↺</button>
        </div>
        <div class="options-row">
            <label>
                Source search mode:
                <select id="sourceModeSelect">
                    <option value="">Objects Only</option>
                    <option value="raw">Source: Raw</option>
                    <option value="objectsRaw">Objects + Source Raw</option>
                    <option value="noComments">Source: No Comments</option>
                    <option value="noCommentsNoLiterals">Source: No Comments / Strings</option>
                </select>
            </label>
        </div>
        <div id="status"></div>
        <ul class="results" id="resultsList"></ul>

        <script>
            try {
                const vscode = acquireVsCodeApi();
                const searchInput = document.getElementById('searchInput');
                const searchBtn = document.getElementById('searchBtn');
                const cancelBtn = document.getElementById('cancelBtn');
                const resetBtn = document.getElementById('resetBtn');
                const sourceModeSelect = document.getElementById('sourceModeSelect');
                const resultsList = document.getElementById('resultsList');
                const status = document.getElementById('status');

                let isSearching = false;
                let allResults = [];

                function setSearchingState(searching) {
                    isSearching = searching;
                    cancelBtn.style.display = searching ? 'inline-flex' : 'none';
                    searchBtn.disabled = searching;
                }

                searchBtn.addEventListener('click', () => {
                    const term = searchInput.value;
                    if (term) {
                        allResults = [];
                        resultsList.innerHTML = '<li class="searching-indicator"><span class="spinner"></span> Searching...</li>';
                        status.textContent = '';
                        setSearchingState(true);

                        const sourceMode = sourceModeSelect.value;
                        if (sourceMode === 'objectsRaw') {
                            vscode.postMessage({ type: 'searchCombined', value: term, mode: 'raw' });
                        } else if (sourceMode && sourceMode !== '') {
                            vscode.postMessage({ type: 'searchSource', value: term, mode: sourceMode });
                        } else {
                            vscode.postMessage({ type: 'search', value: term });
                        }
                    }
                });

                searchInput.addEventListener('keyup', (e) => {
                    if (e.key === 'Enter') {
                        searchBtn.click();
                    }
                });

                cancelBtn.addEventListener('click', () => {
                    vscode.postMessage({ type: 'cancel' });
                });

                resetBtn.addEventListener('click', () => {
                    vscode.postMessage({ type: 'reset' });
                });

                window.addEventListener('message', event => {
                    const message = event.data;
                    switch (message.type) {
                        case 'results':
                            if (!message.append) {
                                allResults = [];
                                resultsList.innerHTML = '';
                            }
                            allResults = allResults.concat(message.data);
                            setSearchingState(false);
                            if (allResults.length === 0) {
                                if (!message.append) {
                                    resultsList.innerHTML = '<li style="padding: 10px; color: var(--vscode-descriptionForeground);">No results found.</li>';
                                }
                            } else {
                                renderResults(allResults);
                            }
                            break;
                        case 'searching':
                            status.textContent = message.message;
                            break;
                        case 'error':
                            setSearchingState(false);
                            status.textContent = 'Error: ' + message.message;
                            status.style.color = 'var(--vscode-errorForeground)';
                            break;
                        case 'cancelled':
                            setSearchingState(false);
                            status.textContent = 'Search cancelled.';
                            break;
                        case 'reset':
                            setSearchingState(false);
                            searchInput.value = '';
                            allResults = [];
                            resultsList.innerHTML = '';
                            status.textContent = '';
                            break;
                    }
                });

                function renderResults(results) {
                    resultsList.innerHTML = '';
                    const groups = {};
                    results.forEach(item => {
                        const groupKey = item.TYPE;
                        if (!groups[groupKey]) groups[groupKey] = [];
                        groups[groupKey].push(item);
                    });
                    const sortedGroups = Object.keys(groups).sort();
                    sortedGroups.forEach(type => {
                        const groupItems = groups[type];
                        const groupHeader = document.createElement('div');
                        groupHeader.className = 'group-header';
                        groupHeader.innerHTML = \`
                            <span><span class="group-toggle">▼</span>\${type}</span>
                            <span class="group-count">\${groupItems.length}</span>
                        \`;
                        const itemsContainer = document.createElement('div');
                        itemsContainer.className = 'group-items';
                        groupHeader.addEventListener('click', () => {
                            const toggle = groupHeader.querySelector('.group-toggle');
                            toggle.classList.toggle('collapsed');
                            itemsContainer.classList.toggle('collapsed');
                        });
                        resultsList.appendChild(groupHeader);
                        groupItems.forEach(item => {
                            const li = document.createElement('li');
                            li.className = 'result-item';
                            let tooltipText = \`Type: \${item.TYPE}\\nDatabase: \${item.DATABASE}\\nSchema: \${item.SCHEMA}\`;
                            if (item.PARENT) tooltipText += \`\\nParent: \${item.PARENT}\`;
                            if (item.DESCRIPTION) tooltipText += \`\\n\${item.DESCRIPTION}\`;
                            const tooltip = document.createElement('div');
                            tooltip.className = 'tooltip top';
                            tooltip.textContent = tooltipText;
                            let html = \`
                                <div class="item-header">
                                    <span>\${item.NAME}</span>
                                </div>
                                <div class="item-details">
                                    <span>\${item.SCHEMA}.\${item.DATABASE}</span>
                            \`;
                            if (item.PARENT) html += \`<span>Parent: \${item.PARENT}</span>\`;
                            if (item.DESCRIPTION && item.DESCRIPTION !== 'Result from Cache') {
                                html += \`<span style="opacity: 0.7; font-style: italic;">\${item.DESCRIPTION}</span>\`;
                            }
                            if (item.DESCRIPTION === 'Result from Cache') {
                                html += \`<span class="cache-badge">Cached</span>\`;
                            }
                            html += \`</div>\`;
                            li.innerHTML = html;
                            li.appendChild(tooltip);
                            // Send payload with 'type' reserved for message action.
                            // Use 'objType' for the item's schema/object type to avoid overwriting type.
                            li.onclick = () => {
                                vscode.postMessage({ 
                                    type: 'navigate', 
                                    database: item.DATABASE,
                                    schema: item.SCHEMA,
                                    name: item.NAME,
                                    objType: item.TYPE,
                                    parent: item.PARENT,
                                    connectionName: item.connectionName
                                });
                            };
                            itemsContainer.appendChild(li);
                        });
                        resultsList.appendChild(itemsContainer);
                    });
                }
            } catch (err) {
                const errDiv = document.createElement('div');
                errDiv.style.color = 'red';
                errDiv.innerText = 'Script Error: ' + err.message;
                document.body.appendChild(errDiv);
            }
        </script>
    </body>
    </html>`;
    }
}
