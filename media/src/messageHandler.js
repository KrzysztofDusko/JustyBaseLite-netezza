import { decode } from '@msgpack/msgpack';
import { updateRowCountInfo } from './utils';

export function setupStreamingMessageHandler(handlers) {
    window.addEventListener('message', event => {
        const message = event.data;
        const handler = handlers[message.command];
        if (handler) {
            handler(message);
        }
    });
}

export function handleAppendRows(message, state, renderers) {
    let { resultSetIndex, rows, totalRows, isLastChunk, limitReached } = message;

    // Decode MessagePack if rows is a Uint8Array
    if (rows instanceof Uint8Array || (rows && rows.type === 'Buffer') || (rows && typeof rows === 'object' && rows.data instanceof Array)) {
        try {
            const buffer = rows instanceof Uint8Array ? rows : new Uint8Array(rows.data || rows);
            rows = decode(buffer);
        } catch (e) {
            console.error('Failed to decode MessagePack rows:', e);
        }
    }

    if (state.resultSets && state.resultSets[resultSetIndex]) {
        const rs = state.resultSets[resultSetIndex];
        rs.data.push(...rows);
        rs.limitReached = limitReached;

        updateRowCountInfo(resultSetIndex, totalRows, limitReached);

        if (renderers.appendRows) {
            renderers.appendRows(resultSetIndex, rows, isLastChunk);
        }
    }
}
