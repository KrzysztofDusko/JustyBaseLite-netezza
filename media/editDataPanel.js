const vscode = acquireVsCodeApi();

// State
let tableData = {
    original: [], // Original data for reference
    working: [],  // Current working data (including edits)
    columns: []
};

// Tracking Changes
let changes = {
    updates: {}, // Map<rowId, { col: newValue }>
    deletes: new Set(), // Set<rowId>
    inserts: [] // Array<newRowObj>
};

// Insert Counter for temporary IDs
let nextInsertId = -1;

let tanTable = null;
let rowVirtualizer = null;

// Initialize
window.addEventListener('message', event => {
    const message = event.data;
    switch (message.command) {
        case 'setData':
            initData(message.data, message.columns);
            break;
        case 'setLoading':
            setLoading(message.loading);
            break;
        case 'setError':
            showError(message.text);
            break;
    }
});

document.addEventListener('DOMContentLoaded', () => {
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
});

function setLoading(isLoading) {
    const status = document.getElementById('status');
    status.textContent = isLoading ? 'Loading...' : `${tableData.working.length} rows`;

    // Disable buttons while loading
    document.getElementById('saveBtn').disabled = isLoading;
    document.getElementById('refreshBtn').disabled = isLoading;
}

function showError(msg) {
    const container = document.getElementById('gridContainer');
    container.innerHTML = `<div style="color: red; padding: 20px;">Error: ${msg}</div>`;
}

function initData(data, columns) {
    // Reset state
    tableData.original = JSON.parse(JSON.stringify(data)); // Deep copy
    tableData.working = JSON.parse(JSON.stringify(data));  // Deep copy
    tableData.columns = columns;

    changes = {
        updates: {},
        deletes: new Set(),
        inserts: []
    };
    nextInsertId = -1;

    renderTable();
    setLoading(false);
}

function hasUnsavedChanges() {
    return Object.keys(changes.updates).length > 0 ||
        changes.deletes.size > 0 ||
        changes.inserts.length > 0;
}

function renderTable() {
    const container = document.getElementById('gridContainer');
    container.innerHTML = '';

    if (!tableData.working || tableData.working.length === 0) {
        container.innerHTML = '<div style="padding: 20px;">No data</div>';
        return;
    }

    if (!TableCore || !VirtualCore) {
        container.innerHTML = 'Error: Libraries not loaded';
        return;
    }

    // Column Definitions
    const columnDefs = [
        // Action Column
        {
            id: '__actions',
            header: '',
            size: 40,
            cell: info => {
                const row = info.row.original;
                const rowId = row.ROWID || row.__tempId;

                // If deleted, show undo? Or just nothing (since it's styled deleted)
                // Let's rely on row click or context menu, but a delete button is explicit.

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

                // Create Input
                const input = document.createElement('input');
                input.value = value !== null ? value : '';
                input.placeholder = 'NULL';

                // Track Changes
                input.onblur = () => {
                    if (isReadOnly) return;
                    const newValue = input.value;
                    updateCell(rowId, col, newValue, value);
                    // Check if value changed from original to highlight
                    // But we used working data to render, so we need to check if it differs from what's in updates
                    // Or compare with original if found.

                    // Re-render handled by state update usually, but for performance we might just toggle class
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

    // Create Table Instance
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
            sorting: [],
            columnVisibility: {},
            rowSelection: {}
        },
        onColumnPinningChange: () => { },
        onSortingChange: () => { },
        onColumnVisibilityChange: () => { },
        onColumnSizingChange: () => { },

        getCoreRowModel: TableCore.getCoreRowModel(),
        getSortedRowModel: TableCore.getSortedRowModel(),
        getFilteredRowModel: TableCore.getFilteredRowModel(),
        getExpandedRowModel: TableCore.getExpandedRowModel(),
    });

    // Virtualizer
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

    // Header
    tanTable.getHeaderGroups().forEach(headerGroup => {
        const tr = document.createElement('tr');
        headerGroup.headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header.column.columnDef.header;
            th.style.width = `${header.getSize()}px`;
            tr.appendChild(th);
        });
        thead.appendChild(tr);
    });

    // Virtualizer Logic
    const { rows } = tanTable.getRowModel();

    rowVirtualizer = new VirtualCore.Virtualizer({
        count: rows.length,
        getScrollElement: () => wrapper,
        estimateSize: () => 35, // Row height
        overscan: 10,
        scrollToFn: VirtualCore.elementScroll,
        observeElementRect: VirtualCore.observeElementRect,
        observeElementOffset: VirtualCore.observeElementOffset,
        onChange: () => {
            renderRows();
        }
    });

    const renderRows = () => {
        tbody.innerHTML = '';
        const virtualRows = rowVirtualizer.getVirtualItems();

        // Spacer top
        const padTop = virtualRows.length > 0 ? virtualRows[0].start : 0;
        // Spacer bottom
        const totalHeight = rowVirtualizer.getTotalSize();
        const padBottom = totalHeight - (virtualRows.length > 0 ? virtualRows[virtualRows.length - 1].end : 0);

        if (padTop > 0) {
            const tr = document.createElement('tr');
            tr.style.height = `${padTop}px`;
            tbody.appendChild(tr);
        }

        virtualRows.forEach(virtualRow => {
            const row = rows[virtualRow.index];
            const tr = document.createElement('tr');
            tr.dataset.index = virtualRow.index;

            // Row Styles (Deleted/New)
            const rowData = row.original;
            const rowId = rowData.ROWID || rowData.__tempId;

            if (changes.deletes.has(rowId)) {
                tr.classList.add('row-deleted');
            }
            if (!rowData.ROWID) {
                tr.classList.add('row-new');
            }

            row.getVisibleCells().forEach(cell => {
                const td = document.createElement('td');
                // Check modified
                if (isModified(rowId, cell.column.id)) {
                    td.classList.add('cell-modified');
                }
                if (cell.column.id === 'ROWID') {
                    td.classList.add('readonly');
                }

                // Render content
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
    };

    // Initial Render & Scroll listener
    rowVirtualizer._willUpdate();
    renderRows();

    wrapper.addEventListener('scroll', () => {
        // rowVirtualizer.scrollToFn ... handled by Virtualizer instance usually via observe?
        // VirtualCore vanilla adapter needs manual trigger on scroll or using the observer setup in 3.0?
        // Let's use the pattern from resultPanel.js if needed or standard.
        renderRows();
    });
}

function updateCell(rowId, col, newValue, originalValue) {
    if (col === 'ROWID' || col === '__actions') return;

    // Update in working data
    const row = tableData.working.find(r => (r.ROWID || r.__tempId) == rowId);
    if (row) {
        row[col] = newValue;
    }

    // Is it a temp row?
    if (!row.ROWID) {
        // Just update in inserts map logic
        // We find the insert obj
        const insertObj = changes.inserts.find(i => i.__tempId == rowId);
        if (insertObj) {
            insertObj[col] = newValue;
        }
        return;
    }

    // Existing row - track in updates
    if (!changes.updates[rowId]) {
        changes.updates[rowId] = { rowId, changes: {} };
    }

    // Check against original
    const originalRow = tableData.original.find(r => r.ROWID == rowId);
    const origVal = originalRow ? originalRow[col] : null;

    // Loose equality for numbers/strings overlap
    if (String(newValue) !== String(origVal)) {
        changes.updates[rowId].changes[col] = newValue;
    } else {
        // Reverted to original
        delete changes.updates[rowId].changes[col];
        if (Object.keys(changes.updates[rowId].changes).length === 0) {
            delete changes.updates[rowId];
        }
    }
}

function isModified(rowId, col) {
    if (!rowId) return false; // new row logic handled by row class
    if (changes.updates[rowId] && changes.updates[rowId].changes[col] !== undefined) {
        return true;
    }
    return false;
}

function toggleDeleteRow(row) {
    const rowId = row.ROWID || row.__tempId;

    // If it's a new row (temp), just remove it completely from working & inserts
    if (!row.ROWID) {
        const idx = tableData.working.findIndex(r => r.__tempId == rowId);
        if (idx !== -1) tableData.working.splice(idx, 1);

        const insIdx = changes.inserts.findIndex(i => i.__tempId == rowId);
        if (insIdx !== -1) changes.inserts.splice(insIdx, 1);

        // Re-render fully to update virtualizer count
        initData(tableData.working, tableData.columns); // naive re-init, preserves edits in 'working' but resets 'changes'?? 
        // WAIT: re-init wipes changes. We shouldn't do that.
        // We should just re-render table.
        // But tableData.working is modified.
        // We need to keep 'changes' intact.
        // Let's just create new Table instance with current working data.
        renderTable();
        return;
    }

    // Existing row
    if (changes.deletes.has(rowId)) {
        changes.deletes.delete(rowId);
    } else {
        changes.deletes.add(rowId);
    }
    // Re-render rows to update style
    renderTable();
}

function addNewRow() {
    const tempId = nextInsertId--;
    const newRow = { __tempId: tempId };

    // Init columns with null
    tableData.columns.forEach(c => {
        if (c !== 'ROWID') newRow[c] = null;
    });

    tableData.working.unshift(newRow); // Add to top
    changes.inserts.push(newRow);

    renderTable(); // Re-create table to catch new data
}

function saveChanges() {
    // Collect changes
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
