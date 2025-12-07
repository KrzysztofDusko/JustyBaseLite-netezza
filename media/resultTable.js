const vscode = acquireVsCodeApi();

window.onerror = function (message, source, lineno, colno, error) {
    vscode.postMessage({
        command: 'error',
        text: `Error: ${message} at ${source}:${lineno}:${colno}`
    });
};

// For resultView.ts
function initializeResultView(rowData, columnsData) {
    renderSimpleTable(rowData, columnsData);
}

// For resultPanelView.ts
function init() {
    renderSourceTabs();
    renderGrids();
}

function renderSourceTabs() {
    const container = document.getElementById('sourceTabs');
    if (!container) return;

    container.innerHTML = '';
    sources.forEach(source => {
        const tab = document.createElement('div');
        tab.className = 'source-tab' + (source === activeSource ? ' active' : '');

        const filename = document.createElement('span');
        const parts = source.split(/[\\/]/);
        filename.textContent = parts[parts.length - 1] || source;
        filename.title = source;
        tab.appendChild(filename);

        tab.onclick = () => {
            vscode.postMessage({ command: 'switchSource', sourceUri: source });
        };

        container.appendChild(tab);
    });
}

function renderGrids() {
    const container = document.getElementById('gridContainer');
    if (!container) return;

    container.innerHTML = '';

    if (!resultSets || resultSets.length === 0) {
        container.innerHTML = '<div style="padding: 20px; text-align: center; opacity: 0.6;">No results</div>';
        return;
    }

    const rs = resultSets[0];
    if (!rs.data || rs.data.length === 0) {
        if (rs.message) {
            container.innerHTML = '<div style="padding: 20px; text-align: center; opacity: 0.6;">' + rs.message + '</div>';
        } else {
            container.innerHTML = '<div style="padding: 20px; text-align: center; opacity: 0.6;">Empty result set</div>';
        }
        return;
    }

    renderSimpleTable(rs.data, rs.columns.map((col, idx) => ({
        accessorKey: String(idx),
        header: col.name
    })));

    updateRowCount();
}

function renderSimpleTable(data, columns) {
    let container = document.getElementById('gridContainer');
    let thead = document.querySelector('#myTable thead');
    let tbody = document.querySelector('#myTable tbody');

    // If we don't have table elements, create them
    if (!container) {
        container = document.getElementById('tableContainer')?.parentElement || document.body;
    }

    if (!thead || !tbody) {
        const table = document.createElement('table');
        table.id = 'myTable';
        table.style.width = '100%';
        table.style.borderCollapse = 'collapse';

        thead = document.createElement('thead');
        tbody = document.createElement('tbody');
        table.appendChild(thead);
        table.appendChild(tbody);

        if (container) {
            container.appendChild(table);
        }
    }

    // Render Headers
    thead.innerHTML = '';
    const headerRow = document.createElement('tr');
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col.header;
        th.style.padding = '8px';
        th.style.borderBottom = '1px solid var(--vscode-panel-border)';
        th.style.textAlign = 'left';
        th.style.fontWeight = 'bold';
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    // Render Rows (max 100)
    tbody.innerHTML = '';
    const maxRows = Math.min(data.length, 100);
    for (let i = 0; i < maxRows; i++) {
        const row = data[i];
        const tr = document.createElement('tr');

        columns.forEach((col, colIndex) => {
            const td = document.createElement('td');
            td.style.padding = '6px 8px';
            td.style.borderBottom = '1px solid var(--vscode-panel-border)';

            let val;
            if (Array.isArray(row)) {
                val = row[colIndex];
            } else {
                val = row[col.accessorKey];
            }

            if (val === null || val === undefined) {
                const nullSpan = document.createElement('span');
                nullSpan.style.opacity = '0.5';
                nullSpan.style.fontStyle = 'italic';
                nullSpan.textContent = 'NULL';
                td.appendChild(nullSpan);
            } else {
                td.textContent = String(val);
            }

            tr.appendChild(td);
        });

        tbody.appendChild(tr);
    }

    // Show info if there are more rows
    if (data.length > maxRows) {
        const info = document.createElement('div');
        info.style.padding = '10px';
        info.style.textAlign = 'center';
        info.style.opacity = '0.6';
        info.textContent = `Showing ${maxRows} of ${data.length} rows`;
        if (container) {
            container.appendChild(info);
        }
    }
}

function updateRowCount() {
    const info = document.getElementById('rowCountInfo');
    if (!info) return;

    if (resultSets && resultSets.length > 0 && resultSets[0].data) {
        const count = resultSets[0].data.length;
        info.textContent = `${count} row${count !== 1 ? 's' : ''}`;
    } else {
        info.textContent = '0 rows';
    }
}

// Placeholder functions
function onFilterChanged() { }
function removeGroup(colId) { }
function onDropGroup(event) { event.preventDefault(); }

function escapeCsvValue(value) {
    if (value === null || value === undefined) return '';
    const str = String(value);
    if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
        return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
}

function exportToCsv() {
    if (!resultSets || resultSets.length === 0 || !resultSets[0].data) return;

    const rs = resultSets[0];
    let csv = rs.columns.map(c => escapeCsvValue(c.name)).join(',') + '\n';

    rs.data.forEach(row => {
        const rowData = row.map(cell => escapeCsvValue(cell));
        csv += rowData.join(',') + '\n';
    });

    vscode.postMessage({
        command: 'exportCsv',
        data: csv
    });
}

function openInExcel() {
    if (!resultSets || resultSets.length === 0 || !resultSets[0].data) return;

    const rs = resultSets[0];
    let csv = rs.columns.map(c => escapeCsvValue(c.name)).join(',') + '\n';

    rs.data.forEach(row => {
        const rowData = row.map(cell => escapeCsvValue(cell));
        csv += rowData.join(',') + '\n';
    });

    vscode.postMessage({
        command: 'openInExcel',
        data: csv
    });
}

// For resultView.ts compatibility
window.copySelection = function (withHeaders) { };
window.selectAll = function () { };
window.exportToCsv = exportToCsv;
window.onFilterChanged = onFilterChanged;
window.clearFilter = function () {
    const filter = document.getElementById('globalFilter');
    if (filter) filter.value = '';
};
