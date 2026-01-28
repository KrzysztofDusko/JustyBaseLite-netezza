export function showError(msg) {
    const container = document.getElementById('gridContainer');
    if (container) {
        container.innerHTML = `<div class="error-wrapper"><div class="error-view"><div class="error-title">Error</div>${msg}</div></div>`;
    }
}

export function updateRowCountInfo(resultSetIndex, totalRows, limitReached) {
    const rowCountInfo = document.getElementById('rowCountInfo');
    if (rowCountInfo) {
        let text = `Total rows: ${totalRows.toLocaleString()}`;
        if (limitReached) {
            text += ` (Limit reached)`;
        }
        rowCountInfo.textContent = text;
    }
}
