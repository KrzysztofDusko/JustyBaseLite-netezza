// Session Monitor Dashboard - Frontend JavaScript
(function () {
    const vscode = acquireVsCodeApi();

    // State
    let currentData = {
        sessions: [],
        queries: [],
        storage: [],
        resources: {}
    };

    // DOM Elements
    const refreshBtn = document.getElementById('refreshBtn');
    const autoRefreshCheckbox = document.getElementById('autoRefresh');
    const loadingOverlay = document.getElementById('loadingOverlay');
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    // Filter Inputs
    const sessionUserFilter = document.getElementById('sessionUserFilter');
    const queryUserFilter = document.getElementById('queryUserFilter');

    // Event Listeners
    refreshBtn.addEventListener('click', () => {
        vscode.postMessage({ command: 'refresh' });
    });

    autoRefreshCheckbox.addEventListener('change', (e) => {
        vscode.postMessage({ command: 'toggleAutoRefresh', enabled: e.target.checked });
    });

    // Filter listeners
    sessionUserFilter.addEventListener('input', () => {
        renderSessions();
    });

    queryUserFilter.addEventListener('input', () => {
        renderQueries();
    });

    // Tab switching
    tabButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            const tabId = btn.dataset.tab;

            tabButtons.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            tabContents.forEach(content => {
                content.classList.toggle('hidden', content.id !== tabId);
            });
        });
    });

    // Message handling from extension
    window.addEventListener('message', event => {
        const message = event.data;

        switch (message.command) {
            case 'setLoading':
                loadingOverlay.classList.toggle('visible', message.loading);
                break;
            case 'updateData':
                renderData(message.data);
                break;
            case 'error':
                showError(message.text);
                break;
        }
    });

    function renderData(data) {
        currentData = { ...currentData, ...data };

        if (data.sessions) renderSessions();
        if (data.queries) renderQueries();
        if (data.storage) renderStorage(data.storage);
        if (data.resources) renderResources(data.resources);
    }

    function renderSessions() {
        const sessions = currentData.sessions || [];
        const filterValue = sessionUserFilter.value.toLowerCase();

        const filteredSessions = sessions.filter(s => {
            if (!filterValue) return true;
            return (s.USERNAME || '').toLowerCase().includes(filterValue);
        });

        const tbody = document.querySelector('#sessionsTable tbody');
        const countEl = document.getElementById('sessionCount');

        countEl.textContent = `${filteredSessions.length} sessions`;

        if (filteredSessions.length === 0) {
            tbody.innerHTML = `<tr><td colspan="10" class="empty-state">
                <div class="empty-state-icon">📋</div>
                No active sessions found
            </td></tr>`;
            return;
        }

        tbody.innerHTML = filteredSessions.map(s => `
            <tr>
                <td>${s.ID || ''}</td>
                <td>${s.PID || ''}</td>
                <td><strong>${s.USERNAME || ''}</strong></td>
                <td>${s.DBNAME || ''}</td>
                <td>${s.TYPE || ''}</td>
                <td>${formatDate(s.CONNTIME)}</td>
                <td><span class="status-badge status-${(s.STATUS || '').toLowerCase()}">${s.STATUS || ''}</span></td>
                <td>${s.IPADDR || ''}</td>
                <td><div class="sql-preview" title="${escapeHtml(s.COMMAND || '')}">${escapeHtml(s.COMMAND || '')}</div></td>
                <td>
                    ${s.STATUS !== 'idle' ? `<button class="btn btn-danger kill-btn" data-session="${s.ID}">✕ Kill</button>` : ''}
                </td>
            </tr>
        `).join('');

        // Attach kill button handlers
        tbody.querySelectorAll('.kill-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const sessionId = parseInt(btn.dataset.session);
                vscode.postMessage({ command: 'killSession', sessionId });
            });
        });
    }

    function renderQueries() {
        const queries = currentData.queries || [];
        const filterValue = queryUserFilter.value.toLowerCase();

        const filteredQueries = queries.filter(q => {
            if (!filterValue) return true;
            return (q.USERNAME || '').toLowerCase().includes(filterValue);
        });

        const tbody = document.querySelector('#queriesTable tbody');
        const countEl = document.getElementById('queryCount');

        countEl.textContent = `${filteredQueries.length} queries`;

        if (filteredQueries.length === 0) {
            tbody.innerHTML = `<tr><td colspan="10" class="empty-state">
                <div class="empty-state-icon">⏱️</div>
                No running queries
            </td></tr>`;
            return;
        }

        tbody.innerHTML = filteredQueries.map(q => `
            <tr>
                <td>${q.QS_SESSIONID || ''}</td>
                <td><strong>${q.USERNAME || ''}</strong></td>
                <td>${q.QS_PLANID || ''}</td>
                <td><span class="status-badge status-${(q.QS_STATE || '').toLowerCase()}">${q.QS_STATE || ''}</span></td>
                <td>${q.QS_PRITXT || q.QS_PRIORITY || ''}</td>
                <td>${formatDate(q.QS_TSUBMIT)}</td>
                <td>${formatDate(q.QS_TSTART)}</td>
                <td>${formatCostInThousands(q.QS_ESTCOST)}</td>
                <td>${formatNumber(q.QS_RESROWS)}</td>
                <td><div class="sql-preview" title="${escapeHtml(q.QS_SQL || '')}">${escapeHtml(q.QS_SQL || '')}</div></td>
                <td>
                    <button class="btn btn-danger query-kill-btn" data-session="${q.QS_SESSIONID}">✕ Kill</button>
                </td>
            </tr>
        `).join('');

        // Attach kill button handlers for queries
        tbody.querySelectorAll('.query-kill-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const sessionId = parseInt(btn.dataset.session);
                vscode.postMessage({ command: 'killSession', sessionId });
            });
        });
    }

    function renderStorage(storage) {
        const tbody = document.querySelector('#storageTable tbody');
        const countEl = document.getElementById('storageCount');

        countEl.textContent = `${storage.length} schemas`;

        if (storage.length === 0) {
            tbody.innerHTML = `<tr><td colspan="7" class="empty-state">
                <div class="empty-state-icon">💾</div>
                No storage data available
            </td></tr>`;
            return;
        }

        tbody.innerHTML = storage.map(s => {
            const usagePercent = s.ALLOC_MB > 0 ? ((s.USED_MB / s.ALLOC_MB) * 100).toFixed(1) : 0;
            const skewClass = getSkewClass(s.AVG_SKEW);

            return `
                <tr>
                    <td>${s.DATABASE || ''}</td>
                    <td><strong>${s.SCHEMA || ''}</strong></td>
                    <td>${formatNumber(s.ALLOC_MB)} MB</td>
                    <td>${formatNumber(s.USED_MB)} MB</td>
                    <td>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: ${usagePercent}%"></div>
                        </div>
                        <div class="progress-text">${usagePercent}%</div>
                    </td>
                    <td><span class="skew-indicator ${skewClass}"></span>${s.AVG_SKEW || 0}</td>
                    <td>${s.TABLE_COUNT || 0}</td>
                </tr>
            `;
        }).join('');
    }

    function renderResources(resources) {
        const graContainer = document.getElementById('graTable');
        const sysUtilSummaryContainer = document.getElementById('sysUtilSummary');
        const sysUtilContainer = document.getElementById('sysUtilTable');

        // Render GRA data
        if (resources.gra && resources.gra.length > 0) {
            graContainer.innerHTML = renderGenericTable(resources.gra);
        } else {
            graContainer.innerHTML = '<div class="empty-state">No GRA data available</div>';
        }

        // Render System Utilization summary (outside scrollable area)
        if (resources.sysUtilSummary) {
            const s = resources.sysUtilSummary;
            sysUtilSummaryContainer.innerHTML = `
                <div class="summary-box">
                    <div class="summary-item">
                        <span class="summary-label">Host CPU</span>
                        <span class="summary-value">${s.AVG_HOST_CPU_PCT || 0}%</span>
                    </div>
                    <div class="summary-item">
                        <span class="summary-label">SPU CPU</span>
                        <span class="summary-value">${s.AVG_SPU_CPU_PCT || 0}%</span>
                    </div>
                    <div class="summary-item">
                        <span class="summary-label">Memory</span>
                        <span class="summary-value">${s.AVG_MEMORY_PCT || 0}%</span>
                    </div>
                    <div class="summary-item">
                        <span class="summary-label">Disk</span>
                        <span class="summary-value">${s.AVG_DISK_PCT || 0}%</span>
                    </div>
                    <div class="summary-item">
                        <span class="summary-label">Fabric</span>
                        <span class="summary-value">${s.AVG_FABRIC_PCT || 0}%</span>
                    </div>
                    <div class="summary-item">
                        <span class="summary-label">Samples</span>
                        <span class="summary-value">${s.SAMPLE_COUNT || 0}</span>
                    </div>
                </div>
            `;
        } else {
            sysUtilSummaryContainer.innerHTML = '';
        }

        // Render System Utilization table (inside scrollable area)
        if (resources.systemUtil && resources.systemUtil.length > 0) {
            sysUtilContainer.innerHTML = renderGenericTable(resources.systemUtil);
        } else {
            sysUtilContainer.innerHTML = '<div class="empty-state">No system utilization data available</div>';
        }
    }

    function renderGenericTable(data) {
        if (!data || data.length === 0) return '';

        const columns = Object.keys(data[0]);

        return `
            <table>
                <thead>
                    <tr>${columns.map(c => `<th>${c}</th>`).join('')}</tr>
                </thead>
                <tbody>
                    ${data.map(row => `
                        <tr>${columns.map(c => `<td>${formatValue(row[c])}</td>`).join('')}</tr>
                    `).join('')}
                </tbody>
            </table>
        `;
    }

    // Helper functions
    function formatDate(dateStr) {
        if (!dateStr) return '';
        try {
            const date = new Date(dateStr);
            return date.toLocaleString();
        } catch {
            return dateStr;
        }
    }

    function formatNumber(num) {
        if (num === null || num === undefined) return '';
        if (typeof num === 'number') {
            return num.toLocaleString();
        }
        return num;
    }

    function formatCostInThousands(cost) {
        if (cost === null || cost === undefined) return '';
        if (typeof cost === 'number' || typeof cost === 'bigint') {
            // Format with space as thousand separator
            return cost.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
        }
        // Handle string numbers
        if (typeof cost === 'string' && /^\d+$/.test(cost)) {
            return cost.replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
        }
        return cost;
    }

    function formatValue(val) {
        if (val === null || val === undefined) return '';
        if (typeof val === 'number') return val.toLocaleString();
        return escapeHtml(String(val));
    }

    function escapeHtml(str) {
        if (!str) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function getSkewClass(skew) {
        if (skew === null || skew === undefined) return 'skew-good';
        if (skew < 10) return 'skew-good';
        if (skew < 30) return 'skew-warn';
        return 'skew-bad';
    }

    function showError(text) {
        // Could show a toast or alert
        console.error('Session Monitor Error:', text);
    }
})();
