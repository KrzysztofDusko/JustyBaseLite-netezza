const vscode = acquireVsCodeApi();

// State
let tableData = {
    original: [],
    working: [],
    columns: [],
    metadata: null
};

// Tracking Changes
let changes = {
    updates: {},
    deletes: new Set(),
    inserts: []
};

let nextInsertId = -1;
let tanTable = null;
let rowVirtualizer = null;

// Initialize
window.addEventListener('message', event => {
    const message = event.data;
    switch (message.command) {
        case 'setData':
            initData(message.data, message.columns, message.metadata);
            break;
        case 'setLoading':
            setLoading(message.loading, message.message);
            break;
        case 'setError':
            showError(message.text);
            break;
    }
});

document.addEventListener('DOMContentLoaded', () => {
    // Top Toolbar
    document.getElementById('refreshBtn').onclick = () => {
        if (hasUnsavedChanges() && !confirm('You have unsaved changes. Discard them?')) {
            return;
        }
        vscode.postMessage({ command: 'refresh' });
    };

    document.getElementById('saveBtn').onclick = () => {
        saveChanges();
    };

    document.getElementById('addRowBtn').onclick = () => {
        addNewRow();
    };

    // Tab Switching
    document.querySelectorAll('.tab').forEach(tab => {
        tab.onclick = () => {
            const targetId = tab.dataset.target;
            setActiveTab(targetId);
        };
    });
});

function setActiveTab(tabId) {
    document.querySelectorAll('.tab').forEach(t => {
        if (t.dataset.target === tabId) t.classList.add('active');
        else t.classList.remove('active');
    });

    document.querySelectorAll('.tab-content').forEach(c => {
        if (c.id === tabId) c.classList.add('active');
        else c.classList.remove('active');
    });
}

function setLoading(isLoading, message) {
    const status = document.getElementById('status');
    if (message) {
        status.textContent = message;
    } else {
        status.textContent = isLoading ? 'Loading...' : (tableData.working ? `${tableData.working.length} rows` : '');
    }

    const btns = document.querySelectorAll('button');
    btns.forEach(b => b.disabled = isLoading);
}

function showError(msg) {
    const container = document.getElementById('gridContainer');
    if (container) {
        container.innerHTML = `<div style="color: var(--vscode-errorForeground); padding: 20px;">Error: ${msg}</div>`;
    }
}

function initData(data, columns, metadata) {
    console.log('[editDataPanel] initData received:', { dataRows: data ? data.length : 0, columns: columns ? columns.length : 0, metadata: !!metadata });
    console.log('[editDataPanel] Sample data (first row):', data && data[0] ? JSON.stringify(data[0]).substring(0, 200) : 'none');

    tableData.original = JSON.parse(JSON.stringify(data));
    tableData.working = JSON.parse(JSON.stringify(data));
    tableData.columns = columns;
    tableData.metadata = metadata;

    changes = {
        updates: {},
        deletes: new Set(),
        inserts: []
    };
    nextInsertId = -1;

    renderMetadataPanel();
    renderTable();
    setLoading(false);
}

function hasUnsavedChanges() {
    return Object.keys(changes.updates).length > 0 ||
        changes.deletes.size > 0 ||
        changes.inserts.length > 0;
}

// --- Metadata Panel ---
function renderMetadataPanel() {
    const container = document.getElementById('metadataContent');
    if (!container) return;

    container.innerHTML = '';

    if (!tableData.metadata) {
        container.innerHTML = `
            <div class="empty-state">
                <span style="font-size: 32px; margin-bottom: 12px;">📋</span>
                <span>No metadata available</span>
            </div>
        `;
        return;
    }

    const { tableComment, columns } = tableData.metadata;

    // 1. Table Comment Card
    const commentCard = document.createElement('div');
    commentCard.className = 'metadata-card';
    commentCard.innerHTML = `
        <div class="metadata-card-header">
            <h3><span class="icon">💬</span> Table Description</h3>
            <button id="saveTableCommentBtn" class="small-btn primary">Save</button>
        </div>
        <div class="metadata-card-body">
            <textarea id="tableCommentBox" class="comment-box" placeholder="Add a description for this table...">${tableComment || ''}</textarea>
        </div>
    `;
    container.appendChild(commentCard);

    document.getElementById('saveTableCommentBtn').onclick = () => {
        const newComment = document.getElementById('tableCommentBox').value;
        vscode.postMessage({ command: 'updateTableComment', comment: newComment });
    };

    // 2. Columns Card
    const columnsCard = document.createElement('div');
    columnsCard.className = 'metadata-card';

    const columnRows = columns.map(col => {
        const keyIndicators = [];
        if (col.IS_PK == 1) keyIndicators.push('<span class="key-indicator pk" title="Primary Key">🔑</span>');
        if (col.IS_FK == 1) keyIndicators.push('<span class="key-indicator fk" title="Foreign Key">🔗</span>');
        const keyCell = keyIndicators.length > 0 ? keyIndicators.join(' ') : '<span style="opacity:0.3">—</span>';

        const nullIndicator = col.IS_NOT_NULL == 1
            ? '<span class="null-indicator required" title="NOT NULL">✓</span>'
            : '<span class="null-indicator nullable" title="Nullable">○</span>';

        const defaultVal = col.COLDEFAULT
            ? `<span class="default-value">${escapeHtml(col.COLDEFAULT)}</span>`
            : '<span class="no-default">—</span>';

        return `
            <tr>
                <td style="text-align:center; width:50px;">${keyCell}</td>
                <td style="font-weight:500;">${escapeHtml(col.ATTNAME)}</td>
                <td><span class="type-badge">${escapeHtml(col.FORMAT_TYPE)}</span></td>
                <td style="text-align:center; width:60px;">${nullIndicator}</td>
                <td>${defaultVal}</td>
                <td style="padding:4px;">
                    <input type="text" 
                        class="inline-edit col-comment" 
                        data-col="${escapeHtml(col.ATTNAME)}" 
                        value="${escapeHtml(col.DESCRIPTION || '')}" 
                        placeholder="Add comment..."
                    >
                </td>
                <td style="text-align:center; width:40px;">
                    <button class="icon-btn delete-col-btn" data-col="${escapeHtml(col.ATTNAME)}" title="Drop Column">×</button>
                </td>
            </tr>
        `;
    }).join('');

    columnsCard.innerHTML = `
        <div class="metadata-card-header">
            <h3><span class="icon">📊</span> Columns <span style="opacity:0.6; font-weight:400; margin-left:8px;">(${columns.length})</span></h3>
        </div>
        <div class="metadata-grid-container">
            <table class="metadata-table">
                <thead>
                    <tr>
                        <th style="width:60px; text-align:center;">Key</th>
                        <th style="width:180px;">Column Name</th>
                        <th style="width:140px;">Type</th>
                        <th style="width:50px; text-align:center;">NN</th>
                        <th style="width:120px;">Default</th>
                        <th style="min-width:200px;">Comment</th>
                        <th style="width:40px;"></th>
                    </tr>
                </thead>
                <tbody>
                    ${columnRows}
                </tbody>
            </table>
        </div>
    `;

    container.appendChild(columnsCard);

    // Bind Metadata Events - Comment updates
    container.querySelectorAll('.col-comment').forEach(input => {
        input.onblur = (e) => {
            const colName = e.target.dataset.col;
            const newComment = e.target.value;
            const original = columns.find(c => c.ATTNAME === colName);
            if (original && (original.DESCRIPTION || '') !== newComment) {
                vscode.postMessage({ command: 'updateColumnComment', column: colName, comment: newComment });
            }
        };
        input.onkeydown = (e) => {
            if (e.key === 'Enter') e.target.blur();
        };
    });

    // Delete column buttons
    container.querySelectorAll('.delete-col-btn').forEach(btn => {
        btn.onclick = (e) => {
            const colName = e.target.dataset.col;
            if (confirm(`Are you sure you want to DROP column "${colName}"? This cannot be undone.`)) {
                vscode.postMessage({ command: 'dropColumn', column: colName });
            }
        };
    });

    // 3. Add Column Card
    const addCard = document.createElement('div');
    addCard.className = 'add-column-form';
    addCard.innerHTML = `
        <span class="form-label"><span class="icon">➕</span> Add Column</span>
        <input type="text" id="newColName" class="form-input" placeholder="Column name" style="width:140px;">
        <input type="text" id="newColType" class="form-input" placeholder="Type (e.g. INTEGER)" style="width:160px;">
        <button id="addColBtn" class="primary">Add Column</button>
    `;
    container.appendChild(addCard);

    document.getElementById('addColBtn').onclick = () => {
        const name = document.getElementById('newColName').value;
        const type = document.getElementById('newColType').value;
        if (!name || !type) {
            showError("Column Name and Type are required.");
            return;
        }
        vscode.postMessage({ command: 'addColumn', name, type });
    };
}

// Helper function to escape HTML
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}


// --- Main Data Table ---

function renderTable() {
    console.log('[editDataPanel] renderTable called. tableData.working:', tableData.working ? tableData.working.length : 'null');
    console.log('[editDataPanel] tableData.columns:', tableData.columns);

    const container = document.getElementById('gridContainer');
    console.log('[editDataPanel] gridContainer found:', !!container);
    container.innerHTML = '';

    if (!tableData.working || tableData.working.length === 0) {
        container.innerHTML = '<div class="empty-state">No data in table or loading failed</div>';
        console.log('[editDataPanel] renderTable: No working data, showing empty state');
        return;
    }

    if (typeof TableCore === 'undefined' || typeof VirtualCore === 'undefined') {
        container.innerHTML = 'Error: Libraries not loaded';
        console.log('[editDataPanel] renderTable: Libraries not loaded');
        return;
    }

    console.log('[editDataPanel] renderTable: Libraries loaded, building table...');

    const columnDefs = [
        {
            id: '__actions',
            header: '',
            size: 40,
            cell: info => {
                const row = info.row.original;
                const btn = document.createElement('span');
                btn.className = 'delete-btn';
                btn.textContent = '×';
                btn.title = 'Delete Row';
                btn.onclick = (e) => {
                    e.stopPropagation();
                    toggleDeleteRow(row);
                };
                return btn;
            }
        },
        ...tableData.columns.map(col => ({
            accessorKey: col,
            header: col,
            cell: info => {
                const value = info.getValue();
                const row = info.row.original;
                const rowId = row.ROWID || row.__tempId;
                const isReadOnly = col === 'ROWID';

                if (isReadOnly) {
                    return `<span class="readonly-val">${value !== null ? value : 'NULL'}</span>`;
                }

                const input = document.createElement('input');
                input.value = value !== null ? value : '';
                input.placeholder = 'NULL';

                input.onblur = () => {
                    if (isReadOnly) return;
                    const newValue = input.value;
                    updateCell(rowId, col, newValue, value);

                    const parentTd = input.parentElement;
                    if (isModified(rowId, col)) {
                        parentTd.classList.add('cell-modified');
                    } else {
                        parentTd.classList.remove('cell-modified');
                    }
                };
                return input;
            }
        }))
    ];

    try {
        tanTable = TableCore.createTable({
            data: tableData.working,
            columns: columnDefs,
            defaultColumn: {
                size: 150,
                minSize: 50,
                maxSize: 500
            },
            state: {
                columnPinning: { left: [], right: [] },
                columnSizing: { __actions: 40 },
            },
            getCoreRowModel: TableCore.getCoreRowModel(),
            getSortedRowModel: TableCore.getSortedRowModel(),
        });
        console.log('[editDataPanel] TanStack Table created successfully');
    } catch (e) {
        console.error('[editDataPanel] Error creating TanStack Table:', e);
        container.innerHTML = '<div class="empty-state">Error creating table: ' + e.message + '</div>';
        return;
    }

    const wrapper = document.createElement('div');
    wrapper.style.height = '100%';
    wrapper.style.overflow = 'auto';
    wrapper.style.position = 'relative';
    container.appendChild(wrapper);

    const tableEl = document.createElement('table');
    const thead = document.createElement('thead');
    const tbody = document.createElement('tbody');
    tableEl.appendChild(thead);
    tableEl.appendChild(tbody);
    wrapper.appendChild(tableEl);

    // Render header
    tanTable.getHeaderGroups().forEach(headerGroup => {
        const tr = document.createElement('tr');
        headerGroup.headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header.column.columnDef.header;
            th.style.width = `${header.getSize()}px`;

            if (tableData.metadata) {
                const colMeta = tableData.metadata.columns.find(c => c.ATTNAME === header.column.id);
                if (colMeta) {
                    if (colMeta.IS_PK == 1) th.textContent = '🔑 ' + th.textContent;
                    if (colMeta.IS_FK == 1) th.textContent = '🔗 ' + th.textContent;
                    th.title = colMeta.DESCRIPTION || '';
                }
            }
            tr.appendChild(th);
        });
        thead.appendChild(tr);
    });
    console.log('[editDataPanel] Header rendered');

    const { rows } = tanTable.getRowModel();
    console.log('[editDataPanel] Row model rows:', rows.length);

    // Simple non-virtualized render for now to debug
    // If more than 500 rows, just show first 500 to test
    const maxRowsToShow = Math.min(rows.length, 500);
    console.log('[editDataPanel] Rendering', maxRowsToShow, 'rows (non-virtualized for debug)');

    for (let i = 0; i < maxRowsToShow; i++) {
        const row = rows[i];
        const tr = document.createElement('tr');
        const rowData = row.original;
        const rowId = rowData.ROWID || rowData.__tempId;

        if (changes.deletes.has(rowId)) tr.classList.add('row-deleted');
        if (!rowData.ROWID) tr.classList.add('row-new');

        row.getVisibleCells().forEach(cell => {
            const td = document.createElement('td');
            if (isModified(rowId, cell.column.id)) td.classList.add('cell-modified');
            if (cell.column.id === 'ROWID') td.classList.add('readonly');

            const content = cell.column.columnDef.cell(cell.getContext());
            if (content instanceof Node) {
                td.appendChild(content);
            } else {
                td.innerHTML = content;
            }
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    }
    console.log('[editDataPanel] Rows rendered to tbody');
}

function renderRows(tbody, rows) {
    if (!rowVirtualizer) return;

    tbody.innerHTML = '';

    const virtualRows = rowVirtualizer.getVirtualItems();
    if (virtualRows.length === 0) return;

    const padTop = virtualRows[0].start;
    const totalHeight = rowVirtualizer.getTotalSize();
    const padBottom = totalHeight - virtualRows[virtualRows.length - 1].end;

    if (padTop > 0) {
        const tr = document.createElement('tr');
        tr.style.height = `${padTop}px`;
        tbody.appendChild(tr);
    }

    virtualRows.forEach(virtualRow => {
        const row = rows[virtualRow.index];
        const tr = document.createElement('tr');
        tr.dataset.index = virtualRow.index;
        tr.style.height = `${virtualRow.size}px`;

        const rowData = row.original;
        const rowId = rowData.ROWID || rowData.__tempId;

        if (changes.deletes.has(rowId)) tr.classList.add('row-deleted');
        if (!rowData.ROWID) tr.classList.add('row-new');

        row.getVisibleCells().forEach(cell => {
            const td = document.createElement('td');
            if (isModified(rowId, cell.column.id)) td.classList.add('cell-modified');
            if (cell.column.id === 'ROWID') td.classList.add('readonly');

            const content = cell.column.columnDef.cell(cell.getContext());
            if (content instanceof Node) {
                td.appendChild(content);
            } else {
                td.innerHTML = content;
            }
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });

    if (padBottom > 0) {
        const tr = document.createElement('tr');
        tr.style.height = `${padBottom}px`;
        tbody.appendChild(tr);
    }
}

function updateCell(rowId, col, newValue, originalValue) {
    if (col === 'ROWID' || col === '__actions') return;

    const row = tableData.working.find(r => (r.ROWID || r.__tempId) == rowId);
    if (row) {
        row[col] = newValue;
    }

    if (!row.ROWID) {
        const insertObj = changes.inserts.find(i => i.__tempId == rowId);
        if (insertObj) {
            insertObj[col] = newValue;
        }
        return;
    }

    if (!changes.updates[rowId]) {
        changes.updates[rowId] = { rowId, changes: {} };
    }

    const originalRow = tableData.original.find(r => r.ROWID == rowId);
    const origVal = originalRow ? originalRow[col] : null;

    const valEq = (a, b) => {
        if (a === b) return true;
        if ((a === null || a === '') && (b === null || b === '')) return true;
        return String(a) === String(b);
    };

    if (!valEq(newValue, origVal)) {
        changes.updates[rowId].changes[col] = newValue;
    } else {
        delete changes.updates[rowId].changes[col];
        if (Object.keys(changes.updates[rowId].changes).length === 0) {
            delete changes.updates[rowId];
        }
    }
}

function isModified(rowId, col) {
    if (!rowId) return false;
    if (changes.updates[rowId] && changes.updates[rowId].changes[col] !== undefined) {
        return true;
    }
    return false;
}

function toggleDeleteRow(row) {
    const rowId = row.ROWID || row.__tempId;

    if (!row.ROWID) {
        const idx = tableData.working.findIndex(r => r.__tempId == rowId);
        if (idx !== -1) tableData.working.splice(idx, 1);
        const insIdx = changes.inserts.findIndex(i => i.__tempId == rowId);
        if (insIdx !== -1) changes.inserts.splice(insIdx, 1);
        renderTable();
        return;
    }

    if (changes.deletes.has(rowId)) {
        changes.deletes.delete(rowId);
    } else {
        changes.deletes.add(rowId);
    }
    renderTable();
}

function addNewRow() {
    const tempId = nextInsertId--;
    const newRow = { __tempId: tempId };
    tableData.columns.forEach(c => {
        if (c !== 'ROWID') newRow[c] = null;
    });

    tableData.working.unshift(newRow);
    changes.inserts.push(newRow);
    renderTable();
}

function saveChanges() {
    const payload = {
        updates: Object.values(changes.updates),
        deletes: Array.from(changes.deletes),
        inserts: changes.inserts.map(i => {
            const { __tempId, ...rest } = i;
            return rest;
        })
    };

    if (payload.updates.length === 0 && payload.deletes.length === 0 && payload.inserts.length === 0) {
        vscode.postMessage({ command: 'info', text: 'No changes to save.' });
        return;
    }

    vscode.postMessage({ command: 'save', changes: payload });
}
