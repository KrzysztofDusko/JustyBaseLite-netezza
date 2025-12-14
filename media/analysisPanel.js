// @ts-check

// Global state
const state = {
    allData: [],
    fields: [],
    config: {
        rows: [],   // Field IDs
        cols: [],   // Field IDs
        values: []  // { fieldId, agg: 'sum' | 'count' | 'min' | 'max' | 'avg' }
    }
};

let tanTable = null;

// Initialize
(function init() {
    const vscode = acquireVsCodeApi();

    // Initial data from HTML
    const initialData = window.initialData;
    if (initialData) {
        state.allData = initialData.data || [];
        // Map columns to fields
        state.fields = (initialData.columns || []).map((col, idx) => ({
            id: col.accessorKey || col.index || String(idx),
            name: col.name || col.header || `Col ${idx}`,
            type: inferType(state.allData, col.accessorKey || col.index || String(idx))
        }));
    }

    renderFieldList();
    setupDragAndDrop();

    // Chart type selector
    const chartSelect = document.getElementById('chartType');
    if (chartSelect) {
        chartSelect.addEventListener('change', () => {
            renderChart();
        });
    }

    // Filter buttons
    const applyFilterBtn = document.getElementById('applyFilter');
    const clearFilterBtn = document.getElementById('clearFilter');
    const dataFilterInput = document.getElementById('dataFilter');

    if (applyFilterBtn && dataFilterInput) {
        applyFilterBtn.addEventListener('click', () => {
            applyFilter(dataFilterInput.value);
        });
        dataFilterInput.addEventListener('keyup', (e) => {
            if (e.key === 'Enter') {
                applyFilter(dataFilterInput.value);
            }
        });
    }

    if (clearFilterBtn && dataFilterInput) {
        clearFilterBtn.addEventListener('click', () => {
            dataFilterInput.value = '';
            clearFilter();
        });
    }

    // Initial render (empty state)
    updatePivot();

    // Listen for messages
    window.addEventListener('message', event => {
        const message = event.data;
        switch (message.command) {
            case 'updateData':
                // handle updates
                break;
        }
    });

    console.log('Analysis Panel Initialized', state.fields.length, 'fields', state.allData.length, 'rows');
})();

// Filter state
let filteredData = null;

function applyFilter(filterText) {
    if (!filterText || filterText.trim() === '') {
        clearFilter();
        return;
    }

    const lowerFilter = filterText.toLowerCase();
    filteredData = state.allData.filter(row => {
        return state.fields.some(field => {
            const val = getValue(row, field.id);
            if (val === null || val === undefined) return false;
            return String(val).toLowerCase().includes(lowerFilter);
        });
    });

    console.log(`Filter applied: ${filteredData.length} of ${state.allData.length} rows match`);
    updatePivot();
}

function clearFilter() {
    filteredData = null;
    console.log('Filter cleared');
    updatePivot();
}

function getDataForPivot() {
    return filteredData !== null ? filteredData : state.allData;
}

function inferType(data, accessor) {
    // Simple inference based on first non-null value
    for (let i = 0; i < Math.min(data.length, 100); i++) {
        const row = data[i];
        const val = Array.isArray(row) ? row[accessor] : row[accessor];
        if (val !== null && val !== undefined) {
            return typeof val === 'number' ? 'number' : 'string';
        }
    }
    return 'string';
}

// --- UI Rendering ---

function renderFieldList() {
    const list = document.getElementById('fieldList');
    list.innerHTML = '';

    state.fields.forEach(field => {
        const div = document.createElement('div');
        div.className = 'field-item';
        div.textContent = field.name;
        div.draggable = true;
        div.dataset.id = field.id;

        div.addEventListener('dragstart', (e) => {
            e.dataTransfer.setData('text/plain', JSON.stringify({
                type: 'field',
                id: field.id
            }));
            e.dataTransfer.effectAllowed = 'copy';
        });

        list.appendChild(div);
    });
}

function renderConfigZones() {
    ['rows', 'cols', 'values'].forEach(zone => {
        const container = document.getElementById(`${zone}Zone`);
        container.innerHTML = '';

        const items = state.config[zone];
        items.forEach((item, idx) => {
            const fieldId = typeof item === 'object' ? item.fieldId : item;
            const field = state.fields.find(f => f.id == fieldId); // Loose equality for consistency
            if (!field) return;

            const chip = document.createElement('div');
            chip.className = 'chip';
            chip.draggable = true;

            // Allow reordering within zone
            chip.addEventListener('dragstart', (e) => {
                e.dataTransfer.setData('text/plain', JSON.stringify({
                    type: 'chip',
                    zone: zone,
                    index: idx,
                    data: item
                }));
                e.dataTransfer.effectAllowed = 'move';
                chip.classList.add('dragging');
            });

            chip.addEventListener('dragend', () => {
                chip.classList.remove('dragging');
            });

            // Enable dropping ON chips (propagate to zone)
            chip.addEventListener('dragover', (e) => {
                e.preventDefault();
                e.stopPropagation();
                container.classList.add('drag-over');
            });

            chip.addEventListener('dragleave', (e) => {
                container.classList.remove('drag-over');
            });

            chip.addEventListener('drop', (e) => {
                e.preventDefault();
                e.stopPropagation();
                container.classList.remove('drag-over');

                try {
                    const data = JSON.parse(e.dataTransfer.getData('text/plain'));
                    if (data.type === 'field') {
                        addToZone(zone, data.id);
                    } else if (data.type === 'chip') {
                        moveBetweenZones(data.zone, data.index, zone, data.data);
                    }
                } catch (err) {
                    console.error('Drop on chip error', err);
                }
            });

            const content = document.createElement('div');
            content.className = 'chip-content';
            content.textContent = field.name;

            if (zone === 'values') {
                const agg = item.agg || 'count';
                const aggSelect = document.createElement('span');
                aggSelect.className = 'agg-select';
                aggSelect.textContent = `(${agg})`;
                aggSelect.title = 'Click to change aggregation';
                aggSelect.onclick = (e) => {
                    e.stopPropagation();
                    cycleAggregation(idx);
                };
                content.appendChild(aggSelect);
            }

            chip.appendChild(content);

            // Move-to buttons for easier zone switching
            const moveContainer = document.createElement('div');
            moveContainer.className = 'move-btns';
            moveContainer.style.cssText = 'display: flex; gap: 2px; margin-left: 6px;';

            const otherZones = ['rows', 'cols', 'values'].filter(z => z !== zone);
            otherZones.forEach(targetZone => {
                const moveBtn = document.createElement('span');
                moveBtn.className = 'move-btn';
                moveBtn.style.cssText = 'cursor: pointer; padding: 2px 4px; font-size: 10px; opacity: 0.7; border: 1px solid var(--vscode-panel-border); border-radius: 2px;';
                moveBtn.textContent = targetZone === 'rows' ? 'R' : targetZone === 'cols' ? 'C' : 'V';
                moveBtn.title = `Move to ${targetZone}`;

                moveBtn.onmouseover = () => moveBtn.style.opacity = '1';
                moveBtn.onmouseout = () => moveBtn.style.opacity = '0.7';

                moveBtn.onclick = (e) => {
                    e.stopPropagation();
                    moveBetweenZones(zone, idx, targetZone, item);
                };
                moveContainer.appendChild(moveBtn);
            });
            chip.appendChild(moveContainer);

            const removeBtn = document.createElement('span');
            removeBtn.className = 'remove-btn';
            removeBtn.textContent = '×';
            removeBtn.onclick = (e) => {
                e.stopPropagation();
                removeFromZone(zone, idx);
            };
            chip.appendChild(removeBtn);

            container.appendChild(chip);
        });
    });
}

function setupDragAndDrop() {
    const zones = document.querySelectorAll('.drop-zone');
    zones.forEach(zone => {
        zone.addEventListener('dragover', (e) => {
            e.preventDefault();
            zone.classList.add('drag-over');
            e.dataTransfer.dropEffect = 'copy';
        });

        zone.addEventListener('dragleave', (e) => {
            zone.classList.remove('drag-over');
        });

        zone.addEventListener('drop', (e) => {
            e.preventDefault();
            zone.classList.remove('drag-over');

            try {
                const data = JSON.parse(e.dataTransfer.getData('text/plain'));
                const targetZone = zone.dataset.zone;

                if (data.type === 'field') {
                    addToZone(targetZone, data.id);
                } else if (data.type === 'chip') {
                    moveBetweenZones(data.zone, data.index, targetZone, data.data);
                }
            } catch (err) {
                console.error('Drop error', err);
            }
        });
    });

    // Also handle dropping back to field list (remove)
    const fieldList = document.getElementById('fieldList');
    fieldList.addEventListener('dragover', e => e.preventDefault());
    fieldList.addEventListener('drop', (e) => {
        e.preventDefault();
        try {
            const data = JSON.parse(e.dataTransfer.getData('text/plain'));
            if (data.type === 'chip') {
                removeFromZone(data.zone, data.index);
            }
        } catch (err) { }
    });
}

// --- State Management ---

function addToZone(zone, fieldId) {
    if (zone === 'values') {
        // Default aggregation
        const field = state.fields.find(f => f.id == fieldId);
        const defaultAgg = field && field.type === 'number' ? 'sum' : 'count';
        state.config.values.push({ fieldId, agg: defaultAgg });
    } else {
        // Avoid duplicates in rows/cols
        if (!state.config[zone].includes(fieldId)) {
            state.config[zone].push(fieldId);
        }
    }
    renderConfigZones();
    updatePivot();
}

function removeFromZone(zone, index) {
    state.config[zone].splice(index, 1);
    renderConfigZones();
    updatePivot();
}

function moveBetweenZones(fromZone, fromIndex, toZone, itemData) {
    // Remove from old
    state.config[fromZone].splice(fromIndex, 1);

    // Add to new
    if (toZone === 'values') {
        const fieldId = typeof itemData === 'object' ? itemData.fieldId : itemData;
        const field = state.fields.find(f => f.id == fieldId);
        const defaultAgg = field && field.type === 'number' ? 'sum' : 'count';
        // Check if already exists? Values can have duplicates with different aggs, but let's allow it
        state.config.values.push({ fieldId, agg: defaultAgg });
    } else {
        const fieldId = typeof itemData === 'object' ? itemData.fieldId : itemData;
        if (!state.config[toZone].includes(fieldId)) {
            state.config[toZone].push(fieldId);
        }
    }

    renderConfigZones();
    updatePivot();
}

function cycleAggregation(index) {
    const aggs = ['sum', 'count', 'min', 'max', 'avg', 'countDistinct'];
    const item = state.config.values[index];
    const currentIdx = aggs.indexOf(item.agg);
    item.agg = aggs[(currentIdx + 1) % aggs.length];
    renderConfigZones();
    updatePivot();
}

// --- Pivot Logic ---

function updatePivot() {
    const gridDiv = document.getElementById('pivotGrid');

    if (state.config.rows.length === 0 && state.config.cols.length === 0 && state.config.values.length === 0) {
        gridDiv.innerHTML = '<div class="empty-state">Drag fields to Rows, Columns, or Values to analyze data</div>';
        return;
    }

    gridDiv.innerHTML = '<div class="empty-state">Computing...</div>';

    // Defer computation to avoid blocking UI
    setTimeout(() => {
        try {
            const { columns, data } = computePivotData();
            lastChartData = { columns, data };
            renderTable(columns, data);
            renderChart();
        } catch (e) {
            console.error(e);
            gridDiv.innerHTML = `<div style="color:red; padding:20px;">Error: ${e.message}</div>`;
        }
    }, 10);
}

function computePivotData() {
    const rows = state.config.rows;
    const cols = state.config.cols;
    const values = state.config.values;

    if (cols.length === 0) {
        // Simple Grouping (No Pivot)
        return computeGroupByData(rows, values);
    }

    // Full Pivot
    return computeMatrixData(rows, cols, values);
}

function computeGroupByData(rowFields, valueFields) {
    // We will manually group data to flatten it for display
    // Key -> { ...rowFields, ...aggregations }
    const groups = new Map();

    getDataForPivot().forEach(row => {
        const groupKey = rowFields.map(f => getValue(row, f)).join('|||');

        if (!groups.has(groupKey)) {
            const groupObj = {};
            rowFields.forEach(f => {
                groupObj[f] = getValue(row, f);
            });
            // Initialize aggregators
            valueFields.forEach(v => {
                groupObj[`agg_${v.fieldId}_${v.agg}`] = {
                    values: [],
                    sum: 0,
                    count: 0,
                    min: Infinity,
                    max: -Infinity,
                    set: new Set()
                };
            });
            groups.set(groupKey, groupObj);
        }

        const group = groups.get(groupKey);
        // Accumulate
        valueFields.forEach(v => {
            const val = getValue(row, v.fieldId);
            const numVal = parseFloat(val);
            const accumulator = group[`agg_${v.fieldId}_${v.agg}`];

            accumulator.count++;
            if (val !== null && val !== undefined) {
                accumulator.set.add(val);
                if (!isNaN(numVal)) {
                    accumulator.sum += numVal;
                    if (numVal < accumulator.min) accumulator.min = numVal;
                    if (numVal > accumulator.max) accumulator.max = numVal;
                }
            }
        });
    });

    // Finalize aggregations
    const data = [];
    groups.forEach(group => {
        const row = {};
        rowFields.forEach(f => row[f] = group[f]);

        valueFields.forEach(v => {
            const acc = group[`agg_${v.fieldId}_${v.agg}`];
            let result = 0;
            switch (v.agg) {
                case 'sum': result = acc.sum; break;
                case 'count': result = acc.count; break;
                case 'min': result = acc.min === Infinity ? null : acc.min; break;
                case 'max': result = acc.max === -Infinity ? null : acc.max; break;
                case 'avg': result = acc.count > 0 ? acc.sum / acc.count : 0; break;
                case 'countDistinct': result = acc.set.size; break;
            }
            // Simple formatting
            if (typeof result === 'number' && !Number.isInteger(result)) {
                result = parseFloat(result.toFixed(2));
            }
            row[`val_${v.fieldId}`] = result;
        });
        data.push(row);
    });

    // Columns definition
    const columns = [
        ...rowFields.map(f => {
            const field = state.fields.find(field => field.id == f);
            return {
                accessorKey: f,
                header: field ? field.name : f
            };
        }),
        ...valueFields.map(v => {
            const field = state.fields.find(f => f.id == v.fieldId);
            return {
                accessorKey: `val_${v.fieldId}`,
                header: `${v.agg.toUpperCase()}(${field ? field.name : v.fieldId})`
            };
        })
    ];

    return { columns, data };
}

function computeMatrixData(rowFields, colFields, valueFields) {
    // 1. Identify all unique column keys
    const colKeys = new Set();
    // 2. Map RowKey -> { ColKey -> { Aggregators } }
    const matrix = new Map();

    getDataForPivot().forEach(row => {
        // Create Row Key
        const rowKey = rowFields.map(f => getValue(row, f)).join('|||');

        // Create Col Key
        const colKey = colFields.map(f => getValue(row, f)).join(' - ');
        colKeys.add(colKey);

        if (!matrix.has(rowKey)) {
            matrix.set(rowKey, {
                meta: {}, // store row field values
                cells: {} // map colKey -> agg values
            });
            // Store meta
            rowFields.forEach(f => {
                matrix.get(rowKey).meta[f] = getValue(row, f);
            });
        }

        const rowEntry = matrix.get(rowKey);

        if (!rowEntry.cells[colKey]) {
            rowEntry.cells[colKey] = {};
            valueFields.forEach(v => {
                rowEntry.cells[colKey][v.fieldId] = {
                    sum: 0, count: 0, min: Infinity, max: -Infinity, set: new Set()
                };
            });
        }

        // Accumulate
        valueFields.forEach(v => {
            const val = getValue(row, v.fieldId);
            const numVal = parseFloat(val);
            const acc = rowEntry.cells[colKey][v.fieldId];

            acc.count++;
            if (val !== null && val !== undefined) {
                acc.set.add(val);
                if (!isNaN(numVal)) {
                    acc.sum += numVal;
                    if (numVal < acc.min) acc.min = numVal;
                    if (numVal > acc.max) acc.max = numVal;
                }
            }
        });
    });

    const sortedColKeys = Array.from(colKeys).sort();

    // Flatten to table data
    const data = [];
    matrix.forEach(entry => {
        const rowData = { ...entry.meta };

        sortedColKeys.forEach(colKey => {
            if (entry.cells[colKey]) {
                valueFields.forEach(v => {
                    const acc = entry.cells[colKey][v.fieldId];
                    let result = 0;
                    switch (v.agg) {
                        case 'sum': result = acc.sum; break;
                        case 'count': result = acc.count; break;
                        case 'min': result = acc.min === Infinity ? null : acc.min; break;
                        case 'max': result = acc.max === -Infinity ? null : acc.max; break;
                        case 'avg': result = acc.count > 0 ? acc.sum / acc.count : 0; break;
                        case 'countDistinct': result = acc.set.size; break;
                    }
                    if (typeof result === 'number' && !Number.isInteger(result)) {
                        result = parseFloat(result.toFixed(2));
                    }
                    // Unique accessor for this Cell
                    rowData[`${colKey}_${v.fieldId}`] = result;
                });
            } else {
                // Fill blanks ? or leave undefined
            }
        });
        data.push(rowData);
    });

    // Column Definitions
    const columns = [...rowFields.map(f => {
        const field = state.fields.find(field => field.id == f);
        return {
            accessorKey: f,
            header: field ? field.name : f
        };
    })];

    // Dynamic Columns
    sortedColKeys.forEach(colKey => {
        valueFields.forEach(v => {
            const field = state.fields.find(f => f.id == v.fieldId);
            const aggLabel = valueFields.length > 1 ? ` (${v.agg})` : '';
            columns.push({
                accessorKey: `${colKey}_${v.fieldId}`,
                header: `${colKey}${aggLabel}`
            });
        });
    });

    return { columns, data };
}

function getValue(row, accessor) {
    if (Array.isArray(row)) {
        return row[accessor];
    } else {
        return row[accessor];
    }
}

// --- TanStack Table Rendering ---

function renderTable(cols, data) {
    const gridDiv = document.getElementById('pivotGrid');
    gridDiv.innerHTML = '';

    if (data.length === 0) {
        gridDiv.innerHTML = '<div class="empty-state">No data generated</div>';
        return;
    }

    const { createTable, getCoreRowModel } = window.TableCore;

    // State for TanStack Table
    let sorting = [];
    let columnVisibility = {};
    let columnOrder = cols.map(c => c.accessorKey);
    let columnPinning = { left: [], right: [] };

    const table = createTable({
        data,
        columns: cols,
        state: {
            get sorting() { return sorting; },
            get columnVisibility() { return columnVisibility; },
            get columnOrder() { return columnOrder; },
            get columnPinning() { return columnPinning; }
        },
        onSortingChange: (updater) => {
            sorting = typeof updater === 'function' ? updater(sorting) : updater;
            // Re-render handled by logic, but here we might need to re-render table if interactive
            renderTableBody(table, tbody);
        },
        onColumnVisibilityChange: (updater) => {
            columnVisibility = typeof updater === 'function' ? updater(columnVisibility) : updater;
            renderTableBody(table, tbody);
        },
        onColumnOrderChange: (updater) => {
            columnOrder = typeof updater === 'function' ? updater(columnOrder) : updater;
            renderTableBody(table, tbody);
        },
        getCoreRowModel: getCoreRowModel()
    });

    const tableEl = document.createElement('table');
    const thead = document.createElement('thead');
    const tbody = document.createElement('tbody');

    // Headers
    table.getHeaderGroups().forEach(headerGroup => {
        const tr = document.createElement('tr');
        headerGroup.headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header.isPlaceholder ? '' : header.column.columnDef.header;
            tr.appendChild(th);
        });
        thead.appendChild(tr);
    });

    renderTableBody(table, tbody);

    tableEl.appendChild(thead);
    tableEl.appendChild(tbody);
    gridDiv.appendChild(tableEl);
}

function renderTableBody(table, tbody) {
    tbody.innerHTML = '';
    // Rows
    table.getRowModel().rows.forEach(row => {
        const tr = document.createElement('tr');
        row.getVisibleCells().forEach(cell => {
            const td = document.createElement('td');
            // Identify if this is a Row Header (dimension)
            const isRowHeader = state.config.rows.some(r => r == cell.column.id);
            if (isRowHeader) td.className = 'row-header';

            const val = cell.getValue();
            td.textContent = val === null || val === undefined ? '' : val;
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
}

// Global chart data reference
let lastChartData = { columns: [], data: [] };

function renderChart() {
    const canvas = document.getElementById('pivotChart');
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const chartType = document.getElementById('chartType')?.value || 'bar';
    const { columns, data } = lastChartData;

    // Clear canvas
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    if (data.length === 0 || columns.length < 2) {
        ctx.fillStyle = 'var(--vscode-foreground)';
        ctx.font = '14px sans-serif';
        ctx.textAlign = 'center';
        ctx.fillText('Add row and value fields to see chart', canvas.width / 2, canvas.height / 2);
        return;
    }

    // Assume first column is label, subsequent are values
    const labelCol = columns[0];
    const valueCols = columns.slice(1);

    // Extract labels and values
    const labels = data.map(row => String(row[labelCol.accessorKey] || ''));

    // Get max value for scaling
    let maxValue = 0;
    valueCols.forEach(col => {
        data.forEach(row => {
            const val = parseFloat(row[col.accessorKey]) || 0;
            if (val > maxValue) maxValue = val;
        });
    });

    if (maxValue === 0) maxValue = 1;

    const padding = 60;
    const chartWidth = canvas.width - padding * 2;
    const chartHeight = canvas.height - padding * 2;

    // Colors for multiple series
    const colors = ['#4fc3f7', '#81c784', '#ffb74d', '#e57373', '#ba68c8', '#64b5f6'];

    // Draw axes
    ctx.strokeStyle = 'var(--vscode-foreground)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(padding, padding);
    ctx.lineTo(padding, canvas.height - padding);
    ctx.lineTo(canvas.width - padding, canvas.height - padding);
    ctx.stroke();

    // Draw labels
    ctx.fillStyle = 'var(--vscode-foreground)';
    ctx.font = '10px sans-serif';
    ctx.textAlign = 'center';

    const barGroupWidth = chartWidth / labels.length;

    if (chartType === 'bar') {
        const barWidth = (barGroupWidth * 0.8) / valueCols.length;

        valueCols.forEach((col, colIdx) => {
            ctx.fillStyle = colors[colIdx % colors.length];

            data.forEach((row, rowIdx) => {
                const val = parseFloat(row[col.accessorKey]) || 0;
                const barHeight = (val / maxValue) * chartHeight;
                const x = padding + rowIdx * barGroupWidth + (barGroupWidth * 0.1) + colIdx * barWidth;
                const y = canvas.height - padding - barHeight;

                ctx.fillRect(x, y, barWidth - 2, barHeight);
            });
        });

        // Draw x-axis labels
        ctx.fillStyle = 'var(--vscode-foreground)';
        labels.forEach((label, idx) => {
            const x = padding + idx * barGroupWidth + barGroupWidth / 2;
            const displayLabel = label.length > 10 ? label.substring(0, 10) + '...' : label;
            ctx.fillText(displayLabel, x, canvas.height - padding + 15);
        });

    } else if (chartType === 'line') {
        valueCols.forEach((col, colIdx) => {
            ctx.strokeStyle = colors[colIdx % colors.length];
            ctx.lineWidth = 2;
            ctx.beginPath();

            data.forEach((row, rowIdx) => {
                const val = parseFloat(row[col.accessorKey]) || 0;
                const x = padding + rowIdx * barGroupWidth + barGroupWidth / 2;
                const y = canvas.height - padding - (val / maxValue) * chartHeight;

                if (rowIdx === 0) {
                    ctx.moveTo(x, y);
                } else {
                    ctx.lineTo(x, y);
                }
            });

            ctx.stroke();

            // Draw points
            ctx.fillStyle = colors[colIdx % colors.length];
            data.forEach((row, rowIdx) => {
                const val = parseFloat(row[col.accessorKey]) || 0;
                const x = padding + rowIdx * barGroupWidth + barGroupWidth / 2;
                const y = canvas.height - padding - (val / maxValue) * chartHeight;

                ctx.beginPath();
                ctx.arc(x, y, 4, 0, Math.PI * 2);
                ctx.fill();
            });
        });

        // Draw x-axis labels
        ctx.fillStyle = 'var(--vscode-foreground)';
        labels.forEach((label, idx) => {
            const x = padding + idx * barGroupWidth + barGroupWidth / 2;
            const displayLabel = label.length > 10 ? label.substring(0, 10) + '...' : label;
            ctx.fillText(displayLabel, x, canvas.height - padding + 15);
        });
    }

    // Draw Y-axis scale
    ctx.fillStyle = 'var(--vscode-foreground)';
    ctx.textAlign = 'right';
    for (let i = 0; i <= 5; i++) {
        const val = (maxValue / 5 * i).toFixed(0);
        const y = canvas.height - padding - (i / 5) * chartHeight;
        ctx.fillText(val, padding - 5, y + 4);
    }
}

