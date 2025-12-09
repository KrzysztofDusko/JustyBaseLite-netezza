// Search Worker
// Handles searching through large datasets in a background thread

self.onmessage = function (e) {
    const { command, id, data, query, columns } = e.data;

    if (command === 'setData') {
        // Store data for a specific result set
        // data structure: { [rsIndex]: { data: [], columns: [] } }
        if (!self.searchData) self.searchData = {};
        self.searchData[id] = { data, columns };

        // Ack
        self.postMessage({ command: 'setDataDone', id });
    }
    else if (command === 'search') {
        const resultId = id; // rsIndex
        const searchCtx = self.searchData ? self.searchData[resultId] : null;

        if (!searchCtx || !searchCtx.data) {
            self.postMessage({ command: 'searchResult', id: resultId, matchedIndices: null });
            return;
        }

        const rows = searchCtx.data;
        const cols = searchCtx.columns;
        const q = query ? String(query).toLowerCase() : '';

        if (!q) {
            // Empty query = match all
            self.postMessage({ command: 'searchResult', id: resultId, matchedIndices: null });
            return;
        }

        const matchedIndices = [];
        const CHUNK_SIZE = 1000;
        let i = 0;

        function processChunk() {
            const end = Math.min(i + CHUNK_SIZE, rows.length);
            for (; i < end; i++) {
                const row = rows[i];
                let match = false;

                // Iterate columns
                for (let c = 0; c < cols.length; c++) {
                    const col = cols[c];
                    let val;

                    // Handle different row structures (array vs object)
                    if (Array.isArray(row)) {
                        val = row[c];
                    } else if (col.accessorKey) {
                        val = row[col.accessorKey];
                    } else {
                        // Fallback or explicit index key
                        val = row[String(c)];
                    }

                    if (val === null || val === undefined) val = 'NULL';
                    else val = String(val);

                    if (val.toLowerCase().includes(q)) {
                        match = true;
                        break;
                    }
                }

                if (match) {
                    matchedIndices.push(i);
                }
            }

            if (i < rows.length) {
                // Schedule next chunk to keep worker responsive if needed, 
                // though usually we just run to completion in worker.
                // For extremely large datasets, this allows catching specific 'abort' messages if we implemented them.
                setTimeout(processChunk, 0);
            } else {
                self.postMessage({ command: 'searchResult', id: resultId, matchedIndices });
            }
        }

        processChunk();
    }
    else if (command === 'clearData') {
        if (self.searchData) {
            delete self.searchData[id];
        }
    }
};
