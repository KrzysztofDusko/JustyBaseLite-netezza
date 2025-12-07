// Enhanced resultPanel.js with Excel-like column filtering
// Global variables - grids is declared in the inline HTML script
// let grids = []; // Do not redeclare - already exists in HTML

// Entry point for resultPanelView.ts
function init() {
    try {
        renderSourceTabs();
        renderResultSetTabs();
        renderGrids();

        // Setup global keyboard shortcuts
        setupGlobalKeyboardShortcuts();
    } catch (e) {
        showError('Initialization error: ' + e.message);
        console.error(e);
    }
}

// Entry point for resultView.ts
function initializeResultView(data, columns) {
    try {
        // Adapt to resultSets format
        window.resultSets = [{
            data: data,
            columns: columns.map((c, i) => ({ name: c.header, index: i }))
        }];
        window.sources = ['Result'];
        window.activeSource = 'Result';

        // Hide source tabs for single result view
        const sourceTabs = document.getElementById('sourceTabs');
        if (sourceTabs) sourceTabs.style.display = 'none';

        renderGrids();

        // Setup global keyboard shortcuts
        setupGlobalKeyboardShortcuts();
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
    
    if (!window.resultSets || window.resultSets.length <= 1) {
        container.style.display = 'none';
        return;
    }

    container.style.display = 'flex';
    
    window.resultSets.forEach((rs, index) => {
        const tab = document.createElement('div');
        tab.className = 'result-set-tab' + (index === activeGridIndex ? ' active' : '');
        tab.textContent = `Result ${index + 1}`;
        tab.onclick = () => switchToResultSet(index);
        container.appendChild(tab);
    });
}

function switchToResultSet(index) {
    if (index < 0 || index >= grids.length) return;
    
    activeGridIndex = index;
    
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
    
    // Update row count for active grid
    if (grids[index] && grids[index].updateRowCount) {
        grids[index].updateRowCount();
    }
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
            createResultSetGrid(rs, index, container, createTable, getCoreRowModel, getSortedRowModel, getFilteredRowModel, getGroupedRowModel, getExpandedRowModel);
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

// Global drag state management
let globalDragState = {
    isDragging: false,
    dragType: null,
    draggedItem: null
};

function createResultSetGrid(rs, rsIndex, container, createTable, getCoreRowModel, getSortedRowModel, getFilteredRowModel, getGroupedRowModel, getExpandedRowModel) {
    const wrapper = document.createElement('div');
    wrapper.className = 'grid-wrapper' + (rsIndex === 0 ? ' active' : '');
    wrapper.style.height = '100%';
    wrapper.style.overflow = 'auto';
    wrapper.style.position = 'relative';
    wrapper.style.display = rsIndex === 0 ? 'block' : 'none';
    container.appendChild(wrapper);

    if (!rs.data || !Array.isArray(rs.data) || rs.data.length === 0) {
        wrapper.innerHTML = '<div style="padding: 20px; text-align: center; opacity: 0.6;">Empty result set</div>';
        grids.push(null);
        return;
    }

    if (!rs.columns || !Array.isArray(rs.columns)) {
        wrapper.innerHTML = '<div style="padding: 20px; text-align: center; color: red;">Invalid columns definition</div>';
        grids.push(null);
        return;
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
        // Get unique values for this column
        const uniqueValues = [...new Set(rs.data.map(row => {
            const value = Array.isArray(row) ? row[index] : row[String(index)];
            return value === null || value === undefined ? 'NULL' : String(value);
        }))].sort();

        return {
            id: String(index),
            accessorFn: (row) => {
                if (!row) return null;
                return Array.isArray(row) ? row[index] : row[String(index)];
            },
            header: col.name || `Col ${index}`,
            uniqueValues: uniqueValues,
            filterFn: (row, columnId, filterValue) => {
                if (!filterValue || filterValue.length === 0) return true;
                const cellValue = row.getValue(columnId);
                const stringValue = cellValue === null || cellValue === undefined ? 'NULL' : String(cellValue);
                return filterValue.includes(stringValue);
            }
        };
    });

    // State
    let sorting = [];
    let globalFilter = '';
    let grouping = [];
    let expanded = {};
    let columnOrder = columns.map(c => c.id);
    let columnFilters = [];

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
            columnPinning: { left: [], right: [] }
        },
        onSortingChange: (updater) => {
            sorting = typeof updater === 'function' ? updater(sorting) : updater;
            scheduleRender();
        },
        onGlobalFilterChange: (updater) => {
            globalFilter = typeof updater === 'function' ? updater(globalFilter) : updater;
            scheduleRender();
        },
        onColumnFiltersChange: (updater) => {
            columnFilters = typeof updater === 'function' ? updater(columnFilters) : updater;
            scheduleRender();
        },
        onGroupingChange: (updater) => {
            grouping = typeof updater === 'function' ? updater(grouping) : updater;
            scheduleRender();
            renderGrouping();
        },
        onExpandedChange: (updater) => {
            expanded = typeof updater === 'function' ? updater(expanded) : updater;
            scheduleRender();
        },
        onColumnOrderChange: (updater) => {
            columnOrder = typeof updater === 'function' ? updater(columnOrder) : updater;
            scheduleRender();
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
        renderGrouping: () => renderGrouping(),
        updateRowCount: () => updateRowCount(),
        render: () => render()
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
                console.log('Starting drag of chip:', colId);
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

                console.log('Drag over chip:', colId, 'Global state:', globalDragState);

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
                console.log('Drop on chip:', colId, 'Dragged item:', globalDragState.draggedItem);
                chip.classList.remove('drag-over');

                // Handle the drop specifically for this chip
                if (globalDragState.dragType === 'groupChip') {
                    const draggedColId = globalDragState.draggedItem;
                    if (draggedColId && draggedColId !== colId && grids[activeGridIndex] && grids[activeGridIndex].tanTable) {
                        const currentGrouping = grids[activeGridIndex].tanTable.getState().grouping;
                        const newGrouping = [...currentGrouping];
                        const fromIndex = newGrouping.indexOf(draggedColId);
                        const toIndex = newGrouping.indexOf(colId);

                        console.log('Reordering from', fromIndex, 'to', toIndex, 'Current:', currentGrouping);

                        if (fromIndex !== -1 && toIndex !== -1 && fromIndex !== toIndex) {
                            // Remove from old position
                            newGrouping.splice(fromIndex, 1);
                            // Insert at target position
                            newGrouping.splice(toIndex, 0, draggedColId);
                            console.log('New grouping order:', newGrouping);
                            grids[activeGridIndex].tanTable.setGrouping(newGrouping);
                        }
                    }
                }
            };

            chip.ondragend = () => {
                chip.classList.remove('dragging');
                console.log('Drag ended for chip:', colId);

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
    }

    function render() {
        try {
            createVirtualizer();
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

            columns.forEach(col => {
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

        virtualItems.forEach(virtualRow => {
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

            if (value === null || value === undefined) {
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

    render();

    // Setup cell selection events for rectangular selection with mouse
    setupCellSelectionEvents(wrapper, tanTable, columns.length);
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

    // Check if column has active filter
    const currentFilter = table.getColumn(header.column.id).getFilterValue();
    if (currentFilter && currentFilter.length > 0) {
        filterBtn.style.color = 'var(--vscode-charts-blue)';
        filterBtn.style.opacity = '1';
        filterBtn.innerHTML = '🔽';
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
        e.dataTransfer.setData('text/plain', header.column.id);
        e.dataTransfer.setData('type', 'column');
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
            const draggedColId = e.dataTransfer.getData('text/plain');
            const targetColId = header.column.id;

            if (draggedColId !== targetColId) {
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
    return th;
}

// Function to show column filter dropdown
function showColumnFilterDropdown(column, table, anchorElement, rsIndex) {
    // Remove any existing dropdown
    const existingDropdown = document.querySelector('.column-filter-dropdown');
    if (existingDropdown) {
        existingDropdown.remove();
    }

    // Get unique values for this column
    const uniqueValues = column.columnDef.uniqueValues || [];
    const currentFilter = column.getFilterValue() || [];

    // Create dropdown container
    const dropdown = document.createElement('div');
    dropdown.className = 'column-filter-dropdown';
    dropdown.style.position = 'absolute';
    dropdown.style.zIndex = '1000';
    dropdown.style.backgroundColor = 'var(--vscode-dropdown-background)';
    dropdown.style.border = '1px solid var(--vscode-dropdown-border)';
    dropdown.style.borderRadius = '3px';
    dropdown.style.minWidth = '200px';
    dropdown.style.maxHeight = '300px';
    dropdown.style.overflow = 'auto';
    dropdown.style.boxShadow = '0 4px 8px rgba(0,0,0,0.3)';
    dropdown.style.fontFamily = 'var(--vscode-font-family)';
    dropdown.style.fontSize = '13px';

    // Position dropdown below the filter button
    const rect = anchorElement.getBoundingClientRect();
    dropdown.style.top = (rect.bottom + 5) + 'px';
    dropdown.style.left = rect.left + 'px';

    // Header with search box and buttons
    const header = document.createElement('div');
    header.style.padding = '8px';
    header.style.borderBottom = '1px solid var(--vscode-panel-border)';
    header.style.display = 'flex';
    header.style.flexDirection = 'column';
    header.style.gap = '8px';

    // Search box
    const searchBox = document.createElement('input');
    searchBox.type = 'text';
    searchBox.placeholder = 'Search...';
    searchBox.style.width = '100%';
    searchBox.style.padding = '4px';
    searchBox.style.border = '1px solid var(--vscode-input-border)';
    searchBox.style.backgroundColor = 'var(--vscode-input-background)';
    searchBox.style.color = 'var(--vscode-input-foreground)';

    // Button row
    const buttonRow = document.createElement('div');
    buttonRow.style.display = 'flex';
    buttonRow.style.gap = '4px';

    const selectAllBtn = document.createElement('button');
    selectAllBtn.textContent = 'Select All';
    selectAllBtn.className = 'filter-btn';
    selectAllBtn.style.flex = '1';

    const clearBtn = document.createElement('button');
    clearBtn.textContent = 'Clear';
    clearBtn.className = 'filter-btn clear';
    clearBtn.style.flex = '1';

    const applyBtn = document.createElement('button');
    applyBtn.textContent = 'Apply';
    applyBtn.className = 'filter-btn primary';
    applyBtn.style.flex = '1';

    buttonRow.appendChild(selectAllBtn);
    buttonRow.appendChild(clearBtn);
    buttonRow.appendChild(applyBtn);

    header.appendChild(searchBox);
    header.appendChild(buttonRow);
    dropdown.appendChild(header);

    // Values list container
    const valuesList = document.createElement('div');
    valuesList.style.maxHeight = '200px';
    valuesList.style.overflow = 'auto';

    // Create checkboxes for each unique value
    let filteredValues = [...uniqueValues];
    const checkboxes = new Map();

    function renderValuesList() {
        valuesList.innerHTML = '';
        filteredValues.forEach(value => {
            const item = document.createElement('div');
            item.style.padding = '4px 8px';
            item.style.display = 'flex';
            item.style.alignItems = 'center';
            item.style.cursor = 'pointer';

            item.onmouseover = () => {
                item.style.backgroundColor = 'var(--vscode-list-hoverBackground)';
            };
            item.onmouseout = () => {
                item.style.backgroundColor = '';
            };

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.style.marginRight = '8px';
            checkbox.checked = currentFilter.length === 0 || currentFilter.includes(value);

            const label = document.createElement('span');
            label.textContent = value;
            label.style.flex = '1';

            item.onclick = () => {
                checkbox.checked = !checkbox.checked;
            };

            item.appendChild(checkbox);
            item.appendChild(label);
            valuesList.appendChild(item);

            checkboxes.set(value, checkbox);
        });
    }

    // Search functionality
    searchBox.oninput = () => {
        const searchTerm = searchBox.value.toLowerCase();
        filteredValues = uniqueValues.filter(value =>
            String(value).toLowerCase().includes(searchTerm)
        );
        renderValuesList();
    };

    // Button handlers
    selectAllBtn.onclick = () => {
        checkboxes.forEach(checkbox => {
            checkbox.checked = true;
        });
    };

    clearBtn.onclick = () => {
        checkboxes.forEach(checkbox => {
            checkbox.checked = false;
        });
    };

    applyBtn.onclick = () => {
        const selectedValues = [];
        checkboxes.forEach((checkbox, value) => {
            if (checkbox.checked) {
                selectedValues.push(value);
            }
        });

        // Apply filter
        if (selectedValues.length === uniqueValues.length || selectedValues.length === 0) {
            // All selected or none selected = no filter
            column.setFilterValue(undefined);
        } else {
            column.setFilterValue(selectedValues);
        }

        dropdown.remove();
    };

    renderValuesList();
    dropdown.appendChild(valuesList);

    // Add to document
    document.body.appendChild(dropdown);

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
function onFilterChanged() {
    const filterInput = document.getElementById('globalFilter');
    if (grids[activeGridIndex]) {
        grids[activeGridIndex].tanTable.setGlobalFilter(filterInput.value);
    }
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

function openInExcel() {
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
        command: 'openInExcel',
        data: csv
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
    console.log('Drop event - type:', type, 'target:', event.target);

    if (type === 'column') {
        const colId = event.dataTransfer.getData('text/plain');
        if (grids[activeGridIndex] && grids[activeGridIndex].tanTable) {
            const currentGrouping = grids[activeGridIndex].tanTable.getState().grouping;
            if (!currentGrouping.includes(colId)) {
                grids[activeGridIndex].tanTable.setGrouping([...currentGrouping, colId]);
            }
        }
    } else if (type === 'groupChip') {
        // Handle reordering of group chips
        const draggedColId = event.dataTransfer.getData('text/plain');
        console.log('Reordering group chip:', draggedColId);

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
            console.log('Current grouping:', currentGrouping);
            console.log('Dragged:', draggedColId, 'Target:', targetColId);

            if (draggedColId !== targetColId) {
                const newGrouping = [...currentGrouping];
                const fromIndex = newGrouping.indexOf(draggedColId);
                const toIndex = newGrouping.indexOf(targetColId);

                console.log('Moving from index', fromIndex, 'to', toIndex);

                if (fromIndex !== -1 && toIndex !== -1) {
                    // Remove from old position
                    newGrouping.splice(fromIndex, 1);
                    // Insert at new position (adjust index if we removed from earlier position)
                    const insertIndex = fromIndex < toIndex ? toIndex : toIndex + 1;
                    newGrouping.splice(insertIndex, 0, draggedColId);

                    console.log('New grouping:', newGrouping);
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

    function clearSelection() {
        selectedCells.forEach(cellId => {
            const cell = document.querySelector(`[data-cell-id="${cellId}"]`);
            if (cell) {
                cell.classList.remove('selected-cell');
            }
        });
        selectedCells.clear();
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

        clearSelection();

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

    wrapper.addEventListener('mousedown', (e) => {
        if (e.target.closest('td')) {
            isSelecting = true;
            startCell = getCellId(e.target);
            endCell = startCell;
            e.preventDefault();

            if (!e.ctrlKey && !e.metaKey) {
                clearSelection();
            }

            if (startCell) {
                selectedCells.add(startCell);
                const cell = document.querySelector(`[data-cell-id="${startCell}"]`);
                if (cell) {
                    cell.classList.add('selected-cell');
                }
                renderRowView();
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

    // Copy selection functionality
    window.copySelection = function (withHeaders = false) {
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
                const cell = document.querySelector(`[data-cell-id="${row}-${col}"]`);
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
    };

    window.selectAll = function () {
        const rows = wrapper.querySelectorAll('tbody tr[data-index]');
        clearSelection();

        rows.forEach(tr => {
            const cells = tr.querySelectorAll('td[data-cell-id]');
            cells.forEach(td => {
                const cellId = td.dataset.cellId;
                if (cellId) {
                    selectedCells.add(cellId);
                    td.classList.add('selected-cell');
                }
            });
        });
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

    if (rowIndices.length > 2) {
        content.innerHTML = '<div class="row-view-placeholder">Select 1 or 2 rows to compare</div>';
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
        const val1 = rows[rowIndices[0]].getValue(col.id);
        const val2 = rowIndices.length > 1 ? rows[rowIndices[1]].getValue(col.id) : null;

        let isDiff = false;
        if (rowIndices.length === 2) {
            // Simple strict equality check for diff
            if (String(val1) !== String(val2)) {
                isDiff = true;
            }
        }

        html += `<tr class="${isDiff ? 'diff-cell' : ''}">`;
        html += `<td><b>${col.columnDef.header}</b><br><small style="opacity:0.6">${col.columnDef.dataType || ''}</small></td>`;

        html += `<td class="${isDiff ? 'diff-cell-highlight' : ''}">${val1 ?? '<span class="null-value">NULL</span>'}</td>`;

        if (rowIndices.length > 1) {
            html += `<td class="${isDiff ? 'diff-cell-highlight' : ''}">${val2 ?? '<span class="null-value">NULL</span>'}</td>`;
        }

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
window.exportToCsv = exportToCsv;
window.onFilterChanged = onFilterChanged;
window.clearFilter = function () {
    const filter = document.getElementById('globalFilter');
    if (filter) filter.value = '';
};
