import * as vscode from 'vscode';
import { runQuery } from '../core/queryRunner';
import { MetadataCache } from '../metadataCache';
import { ConnectionManager } from '../core/connectionManager';
import { DatabaseMetadata, SchemaMetadata, TableMetadata, ColumnMetadata } from '../metadata/types';

export class SqlCompletionItemProvider implements vscode.CompletionItemProvider {
    constructor(
        private context: vscode.ExtensionContext,
        private metadataCache: MetadataCache,
        private connectionManager: ConnectionManager
    ) { }

    public async provideCompletionItems(
        document: vscode.TextDocument,
        position: vscode.Position,
        _token: vscode.CancellationToken,
        _context: vscode.CompletionContext
    ): Promise<vscode.CompletionItem[] | vscode.CompletionList> {
        const text = document.getText();
        const cleanText = this.stripComments(text);

        // Parse local definitions (CTEs, Temp Tables)
        const localDefs = this.parseLocalDefinitions(cleanText);

        const linePrefix = document.lineAt(position).text.substr(0, position.character);
        const upperPrefix = linePrefix.toUpperCase();

        // Get previous line for multi-line pattern support
        const prevLine = position.line > 0 ? document.lineAt(position.line - 1).text : '';

        // Determine active connection
        let connectionName: string | undefined = this.connectionManager.getConnectionForExecution(
            document.uri.toString()
        );
        if (!connectionName) {
            connectionName = this.connectionManager.getActiveConnectionName() || undefined;
        }

        // Trigger background prefetch for this connection if not already done
        if (connectionName && !this.metadataCache.hasConnectionPrefetchTriggered(connectionName)) {
            this.metadataCache.triggerConnectionPrefetch(connectionName, q =>
                runQuery(this.context, q, true, connectionName!, this.connectionManager)
            );
        }

        // 1. Database Suggestion
        // Trigger: "FROM " or "JOIN " (with optional whitespace/newlines)
        if (/(FROM|JOIN)\s+$/.test(upperPrefix)) {
            const dbs = await this.getDatabases(connectionName);
            // Also suggest local definitions (Temp Tables / CTEs) here as they can be used in FROM/JOIN
            const localItems = localDefs.map(def => {
                const item = new vscode.CompletionItem(def.name, vscode.CompletionItemKind.Class);
                item.detail = def.type;
                return item;
            });
            return [...localItems, ...dbs];
        }
        // Check if FROM/JOIN is on previous line and current line is at start or typing identifier (not a dot pattern)
        if (/(?:FROM|JOIN)\s*$/i.test(prevLine)) {
            // Current line should be: empty, whitespace only, or partial identifier without dots
            if (/^\s*[a-zA-Z0-9_]*$/.test(linePrefix)) {
                const dbs = await this.getDatabases(connectionName);
                const localItems = localDefs.map(def => {
                    const item = new vscode.CompletionItem(def.name, vscode.CompletionItemKind.Class);
                    item.detail = def.type;
                    return item;
                });
                return [...localItems, ...dbs];
            }
        }

        // 2. Schema Suggestion
        // Trigger: "FROM DB." or "FROM DB. " (with possible trailing space or newline)
        const dbMatch = linePrefix.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.\s*$/i);
        if (dbMatch) {
            const dbName = dbMatch[1];
            const items = await this.getSchemas(connectionName, dbName);
            return new vscode.CompletionList(items, false);
        }
        // Check for DB. on current line with FROM/JOIN on previous line
        const dbMatchCurrent = linePrefix.match(/^\s*([a-zA-Z0-9_]+)\.\s*$/i);
        if (dbMatchCurrent && /(?:FROM|JOIN)\s*$/i.test(prevLine)) {
            const dbName = dbMatchCurrent[1];
            const items = await this.getSchemas(connectionName, dbName);
            return new vscode.CompletionList(items, false);
        }

        // 3. Table Suggestion (with Schema)
        // Trigger: "FROM DB.SCHEMA."
        const schemaMatch = linePrefix.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.$/i);
        if (schemaMatch) {
            const dbName = schemaMatch[1];
            const schemaName = schemaMatch[2];
            return this.getTables(connectionName, dbName, schemaName);
        }
        // Check for DB.SCHEMA. on current line with FROM/JOIN on previous line
        const schemaMatchCurrent = linePrefix.match(/^\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.\s*$/i);
        if (schemaMatchCurrent && /(?:FROM|JOIN)\s*$/i.test(prevLine)) {
            const dbName = schemaMatchCurrent[1];
            const schemaName = schemaMatchCurrent[2];
            return this.getTables(connectionName, dbName, schemaName);
        }

        // 4. Table Suggestion (Double dot / No Schema)
        // Trigger: "FROM DB.."
        const doubleDotMatch = linePrefix.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.\.$/i);
        if (doubleDotMatch) {
            const dbName = doubleDotMatch[1];
            return this.getTables(connectionName, dbName, undefined);
        }
        // Check for DB.. on current line with FROM/JOIN on previous line
        const doubleDotMatchCurrent = linePrefix.match(/^\s*([a-zA-Z0-9_]+)\.\.\s*$/i);
        if (doubleDotMatchCurrent && /(?:FROM|JOIN)\s*$/i.test(prevLine)) {
            const dbName = doubleDotMatchCurrent[1];
            return this.getTables(connectionName, dbName, undefined);
        }

        // 5. Column Suggestion (via Alias or Table name)
        // Trigger: Ends with "."
        if (linePrefix.trim().endsWith('.')) {
            // Look at the word before the dot.
            // "ALIAS." -> word is "ALIAS"
            const match = linePrefix.match(/([a-zA-Z0-9_]+)\.$/);
            if (match) {
                const identifier = match[1];

                // Check if it is an alias
                const aliasInfo = this.findAlias(cleanText, identifier);
                if (aliasInfo) {
                    // Check if the alias points to a local definition
                    const localDef = localDefs.find(d => d.name.toUpperCase() === aliasInfo.table.toUpperCase());
                    if (localDef) {
                        return localDef.columns.map(col => {
                            const item = new vscode.CompletionItem(col, vscode.CompletionItemKind.Field);
                            item.detail = 'Local Column';
                            return item;
                        });
                    }

                    // Otherwise, regular DB lookup
                    return this.getColumns(connectionName, aliasInfo.db, aliasInfo.schema, aliasInfo.table);
                }

                // Check if the identifier itself is a local definition (e.g. "CTE.")
                const localDef = localDefs.find(d => d.name.toUpperCase() === identifier.toUpperCase());
                if (localDef) {
                    return localDef.columns.map(col => {
                        const item = new vscode.CompletionItem(col, vscode.CompletionItemKind.Field);
                        item.detail = 'Local Column';
                        return item;
                    });
                }

                // Fallback: Check if it is a table in the current context (omitted for now as alias is priority)
            }
        }

        // 6. Keywords (Default)
        return this.getKeywords();
    }

    private stripComments(text: string): string {
        // Remove single line comments --...
        let clean = text.replace(/--.*$/gm, '');
        // Remove block comments /* ... */
        clean = clean.replace(/\/\*[\s\S]*?\*\//g, '');
        return clean;
    }

    private parseLocalDefinitions(text: string): { name: string; type: string; columns: string[] }[] {
        const definitions: { name: string; type: string; columns: string[] }[] = [];

        // 1. Temp Tables: CREATE TABLE TEMP_1 AS ( SELECT ... )
        // Regex to capture: CREATE TABLE name AS ( query )
        // We need to stop at matching closing paren.

        const tempTableRegex = /CREATE\s+TABLE\s+([a-zA-Z0-9_]+)\s+AS\s*\(/gi;
        let match;
        while ((match = tempTableRegex.exec(text)) !== null) {
            const tableName = match[1];
            const startIndex = match.index + match[0].length;
            const query = this.extractBalancedParenthesisContent(text, startIndex);
            if (query) {
                const columns = this.extractColumnsFromQuery(query);
                definitions.push({ name: tableName, type: 'Temp Table', columns });
            }
        }

        // 2. CTEs: WITH ABC AS ( ... ), DEF AS ( ... )
        // Find "WITH"
        const withRegex = /\bWITH\s+/gi;
        while ((match = withRegex.exec(text)) !== null) {
            let currentIndex = match.index + match[0].length;

            // Loop to parse multiple CTEs separated by comma
            while (true) {
                // Expect: CTE_NAME AS (
                const cteHeaderRegex = /^\s*([a-zA-Z0-9_]+)\s+AS\s*\(/i;
                const remainingText = text.substring(currentIndex);
                const cteMatch = remainingText.match(cteHeaderRegex);

                if (!cteMatch) {
                    break; // No more CTEs in this WITH block
                }

                const cteName = cteMatch[1];
                // cteMatch[0] is like "ABC AS ("
                // We need to find the content inside the parenthesis.

                // Let's adjust: find the first '(' after AS
                const relativeOpenParen = remainingText.indexOf('(', cteMatch.index! + cteMatch[1].length); // simplistic
                const absoluteOpenParen = currentIndex + relativeOpenParen;

                const query = this.extractBalancedParenthesisContent(text, absoluteOpenParen + 1); // +1 to start after '('

                if (query) {
                    const columns = this.extractColumnsFromQuery(query);
                    definitions.push({ name: cteName, type: 'CTE', columns });

                    // Move index past this CTE
                    currentIndex = absoluteOpenParen + 1 + query.length + 1; // +1 for closing ')'

                    // Check for comma
                    const nextCharRegex = /^\s*,/;
                    const nextText = text.substring(currentIndex);
                    if (nextCharRegex.test(nextText)) {
                        // Found comma, continue to next CTE
                        const commaMatch = nextText.match(nextCharRegex);
                        currentIndex += commaMatch![0].length;
                    } else {
                        // No comma, end of WITH block
                        break;
                    }
                } else {
                    break; // Failed to parse
                }
            }
        }

        // 3. Subqueries in JOINs: JOIN (SELECT ...) Alias
        const joinRegex = /\bJOIN\s+\(/gi;
        while ((match = joinRegex.exec(text)) !== null) {
            const startIndex = match.index + match[0].length;
            // match[0] is "JOIN (", so startIndex is after '('.

            const query = this.extractBalancedParenthesisContent(text, startIndex);

            if (query && /^\s*SELECT\b/i.test(query)) {
                // Found a subquery. Now look for alias after the closing parenthesis.
                // The query string returned is the content *inside*.
                // So the closing paren is at startIndex + query.length.

                const afterParenIndex = startIndex + query.length + 1; // +1 for the closing ')'
                const afterParen = text.substring(afterParenIndex);

                // Expect: optional AS, then Alias
                // We need to be careful not to match "ON ..." immediately if there is no alias (though invalid SQL for subquery in JOIN usually requires alias)
                // But usually: JOIN (SELECT ...) Alias ON ...

                const aliasMatch = afterParen.match(/^\s+(?:AS\s+)?([a-zA-Z0-9_]+)/i);
                if (aliasMatch) {
                    const alias = aliasMatch[1];
                    const columns = this.extractColumnsFromQuery(query);
                    definitions.push({ name: alias, type: 'Subquery', columns });
                }
            }
        }

        return definitions;
    }

    private extractBalancedParenthesisContent(text: string, startIndex: number): string | null {
        let balance = 1;
        let i = startIndex;
        for (; i < text.length; i++) {
            if (text[i] === '(') balance++;
            else if (text[i] === ')') balance--;

            if (balance === 0) {
                return text.substring(startIndex, i);
            }
        }
        return null;
    }

    private extractColumnsFromQuery(query: string): string[] {
        // Naive parser for top-level SELECT list
        // SELECT col1, col2 AS alias, col3 ... FROM ...
        // We need to stop at FROM. But FROM can be in subqueries.
        // We assume the first FROM that is not inside parenthesis belongs to the main query.

        // 1. Isolate the SELECT list
        const selectMatch = query.match(/^\s*SELECT\s+/i);
        if (!selectMatch) return [];

        const start = selectMatch[0].length;
        let selectList = '';
        let balance = 0;
        let fromIndex = -1;

        for (let i = start; i < query.length; i++) {
            if (query[i] === '(') balance++;
            else if (query[i] === ')') balance--;

            if (balance === 0) {
                // Check for FROM
                if (query.substr(i).match(/^\s+FROM\b/i)) {
                    fromIndex = i;
                    break;
                }
            }
        }

        if (fromIndex !== -1) {
            selectList = query.substring(start, fromIndex);
        } else {
            // Maybe no FROM (e.g. SELECT 1)
            selectList = query.substring(start);
        }

        // 2. Split by comma, respecting parenthesis
        const columns: string[] = [];
        let current = '';
        balance = 0;

        for (let i = 0; i < selectList.length; i++) {
            const char = selectList[i];
            if (char === '(') balance++;
            else if (char === ')') balance--;

            if (char === ',' && balance === 0) {
                columns.push(current.trim());
                current = '';
            } else {
                current += char;
            }
        }
        if (current.trim()) columns.push(current.trim());

        // 3. Extract alias or name from each part
        return columns.map(col => {
            // "col AS alias" -> alias
            // "col alias" -> alias
            // "col" -> col
            // "table.col" -> col

            // Remove comments from the column definition if any
            const asMatch = col.match(/\s+AS\s+([a-zA-Z0-9_]+)$/i);
            if (asMatch) return asMatch[1];

            const spaceMatch = col.match(/\s+([a-zA-Z0-9_]+)$/i);
            if (spaceMatch) return spaceMatch[1];

            // Just the name, maybe with dot
            const parts = col.split('.');
            return parts[parts.length - 1];
        });
    }

    /**
     * Find alias definition for a given identifier
     * e.g. "FROM DB.SCHEMA.TABLE T" -> T aliased to TABLE
     */
    private findAlias(text: string, alias: string): { db?: string; schema?: string; table: string } | null {
        // Regex to find: TABLE [AS] ALIAS
        // or SCHEMA.TABLE [AS] ALIAS
        // or DB.SCHEMA.TABLE [AS] ALIAS

        // We want to match whole word alias
        const regex = new RegExp(`(?:FROM|JOIN)\\s+([a-zA-Z0-9_\\.]+)(?:\\s+(?:AS\\s+)?([a-zA-Z0-9_]+))?`, 'gi');
        let match;
        while ((match = regex.exec(text)) !== null) {
            const fullRef = match[1]; // DB.SCHEMA.TABLE
            const foundAlias = match[2]; // ALIAS (optional)

            if (foundAlias && foundAlias.toUpperCase() === alias.toUpperCase()) {
                const parts = fullRef.split('.');
                if (parts.length === 3) {
                    return { db: parts[0], schema: parts[1], table: parts[2] };
                } else if (parts.length === 2) {
                    return { schema: parts[0], table: parts[1] }; // db undefined (current?)
                } else {
                    return { table: parts[0] };
                }
            } else if (!foundAlias) {
                // If no alias, the table name itself is the alias reference if it matches
                // e.g. FROM TABLE -> TABLE.col
                const parts = fullRef.split('.');
                const tableName = parts[parts.length - 1];
                if (tableName.toUpperCase() === alias.toUpperCase()) {
                    if (parts.length === 3) {
                        return { db: parts[0], schema: parts[1], table: parts[2] };
                    } else if (parts.length === 2) {
                        return { schema: parts[0], table: parts[1] };
                    } else {
                        return { table: parts[0] };
                    }
                }
            }
        }
        return null;
    }

    private getKeywords(): vscode.CompletionItem[] {
        const keywords = [
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'INSERT', 'INTO', 'VALUES',
            'UPDATE', 'SET', 'DELETE', 'CREATE', 'DROP', 'TABLE', 'VIEW', 'DATABASE', 'JOIN',
            'INNER', 'LEFT', 'RIGHT', 'OUTER', 'ON', 'AND', 'OR', 'NOT', 'NULL', 'IS', 'IN',
            'BETWEEN', 'LIKE', 'AS', 'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'WITH',
            'UNION', 'ALL'
        ];
        return keywords.map(k => {
            const item = new vscode.CompletionItem(k, vscode.CompletionItemKind.Keyword);
            item.detail = 'SQL Keyword';
            return item;
        });
    }

    private async getDatabases(connectionName?: string): Promise<vscode.CompletionItem[]> {
        if (!connectionName) return [];
        const cached = this.metadataCache.getDatabases(connectionName);
        if (cached) {
            return cached.map((item) => {
                const ci = new vscode.CompletionItem(item.label || item.DATABASE, item.kind || vscode.CompletionItemKind.Module);
                ci.detail = item.detail;
                return ci;
            });
        }

        try {
            const query = 'SELECT DATABASE FROM system.._v_database ORDER BY DATABASE';
            const resultJson = await runQuery(this.context, query, true, connectionName, this.connectionManager);
            if (!resultJson) return [];

            const results = JSON.parse(resultJson) as { DATABASE: string }[];
            const items: DatabaseMetadata[] = results.map(row => ({
                DATABASE: row.DATABASE,
                label: row.DATABASE,
                kind: 9, // Module
                detail: 'Database'
            }));

            this.metadataCache.setDatabases(connectionName, items);

            return items.map(item => {
                const ci = new vscode.CompletionItem(item.label!, item.kind);
                ci.detail = item.detail;
                return ci;
            });
        } catch (e: unknown) {
            console.error(e);
            return [];
        }
    }

    private async getSchemas(connectionName: string | undefined, dbName: string): Promise<vscode.CompletionItem[]> {
        if (!connectionName) return [];
        const cached = this.metadataCache.getSchemas(connectionName, dbName);
        if (cached) {
            return cached.map((item) => {
                const ci = new vscode.CompletionItem(item.label || item.SCHEMA, item.kind || vscode.CompletionItemKind.Folder);
                ci.detail = item.detail;
                ci.insertText = item.insertText;
                ci.sortText = item.sortText;
                ci.filterText = item.filterText;
                return ci;
            });
        }

        const statusBarDisposable = vscode.window.setStatusBarMessage(`Fetching schemas for ${dbName}...`);
        try {
            const query = `SELECT SCHEMA FROM ${dbName}.._V_SCHEMA ORDER BY SCHEMA`;
            const resultJson = await runQuery(this.context, query, true, connectionName, this.connectionManager);
            if (!resultJson) {
                return [];
            }

            const results = JSON.parse(resultJson) as { SCHEMA: string | null }[];
            const items: SchemaMetadata[] = results
                .filter(row => row.SCHEMA != null && row.SCHEMA !== '')
                .map(row => {
                    const schemaName = row.SCHEMA!;
                    return {
                        SCHEMA: schemaName,
                        label: schemaName,
                        kind: 19, // Folder
                        detail: `Schema in ${dbName}`,
                        insertText: schemaName,
                        sortText: schemaName,
                        filterText: schemaName
                    };
                });

            this.metadataCache.setSchemas(connectionName, dbName, items);

            return items.map(item => {
                const ci = new vscode.CompletionItem(item.label!, item.kind);
                ci.detail = item.detail;
                ci.insertText = item.insertText;
                ci.sortText = item.sortText;
                ci.filterText = item.filterText;
                return ci;
            });
        } catch (e: unknown) {
            console.error('[SqlCompletion] Error in getSchemas:', e);
            return [];
        } finally {
            statusBarDisposable.dispose();
        }
    }

    private async getTables(
        connectionName: string | undefined,
        dbName: string,
        schemaName?: string
    ): Promise<vscode.CompletionItem[]> {
        if (!connectionName) return [];
        const cacheKey = schemaName ? `${dbName}.${schemaName}` : `${dbName}..`;

        const cached = schemaName
            ? this.metadataCache.getTables(connectionName, cacheKey)
            : this.metadataCache.getTables(connectionName, cacheKey) ||
            this.metadataCache.getTablesAllSchemas(connectionName, dbName);

        if (cached) {
            return cached.map((item) => {
                const label = typeof item.label === 'string' ? item.label : (item.label?.label || item.OBJNAME || item.TABLENAME || '?');
                const ci = new vscode.CompletionItem(label, item.kind || vscode.CompletionItemKind.Class);
                ci.detail = item.detail;
                ci.sortText = item.sortText;
                return ci;
            });
        }

        const statusBarMessage = schemaName
            ? `Fetching tables for ${dbName}.${schemaName}...`
            : `Fetching tables for ${dbName}...`;
        const statusBarDisposable = vscode.window.setStatusBarMessage(statusBarMessage);

        try {
            let query = '';
            if (schemaName) {
                query = `SELECT OBJNAME, OBJID FROM ${dbName}.._V_OBJECT_DATA WHERE UPPER(DBNAME) = UPPER('${dbName}') AND UPPER(SCHEMA) = UPPER('${schemaName}') AND OBJTYPE='TABLE' ORDER BY OBJNAME`;
            } else {
                query = `SELECT OBJNAME, OBJID, SCHEMA FROM ${dbName}.._V_OBJECT_DATA WHERE UPPER(DBNAME) = UPPER('${dbName}') AND OBJTYPE='TABLE' ORDER BY OBJNAME`;
            }

            const resultJson = await runQuery(this.context, query, true, connectionName, this.connectionManager);
            if (!resultJson) return [];

            const results = JSON.parse(resultJson) as { OBJNAME: string; OBJID: number; SCHEMA?: string }[];
            const idMapForKey = new Map<string, number>();

            const items: TableMetadata[] = results.map(row => {
                const label = row.OBJNAME;
                const schema = row.SCHEMA || schemaName;
                const fullKey = schema
                    ? `${dbName}.${schema}.${row.OBJNAME}`
                    : `${dbName}..${row.OBJNAME}`;

                // Populate map while iterating
                idMapForKey.set(fullKey, row.OBJID);

                return {
                    OBJNAME: row.OBJNAME,
                    TABLENAME: row.OBJNAME,
                    OBJID: row.OBJID,
                    SCHEMA: schema,
                    label: label,
                    kind: 6, // Class
                    detail: schemaName ? 'Table' : `Table (${schema})`,
                    sortText: row.OBJNAME
                };
            });

            this.metadataCache.setTables(connectionName, cacheKey, items, idMapForKey);

            return items.map(item => {
                const label = typeof item.label === 'string' ? item.label : (item.label?.label || '?');
                const ci = new vscode.CompletionItem(label, item.kind);
                ci.detail = item.detail;
                ci.sortText = item.sortText;
                return ci;
            });
        } catch (e: unknown) {
            console.error(e);
            return [];
        } finally {
            statusBarDisposable.dispose();
        }
    }

    private async getColumns(
        connectionName: string | undefined,
        dbName: string | undefined,
        schemaName: string | undefined,
        tableName: string
    ): Promise<vscode.CompletionItem[]> {
        if (!connectionName) return [];
        let objId: number | undefined;
        const dbPrefix = dbName ? `${dbName}..` : '';

        const lookupKey =
            schemaName && dbName
                ? `${dbName}.${schemaName}.${tableName}`
                : dbName
                    ? `${dbName}..${tableName}`
                    : undefined;

        if (lookupKey) {
            objId = this.metadataCache.findTableId(connectionName, lookupKey);
        }

        const cacheKey = `${dbName || 'CURRENT'}.${schemaName || ''}.${tableName}`;
        const cached = this.metadataCache.getColumns(connectionName, cacheKey);
        if (cached) {
            return cached.map((item) => {
                const ci = new vscode.CompletionItem(item.label || item.ATTNAME, item.kind || vscode.CompletionItemKind.Field);
                ci.detail = item.detail;
                return ci;
            });
        }

        const statusMsg = vscode.window.setStatusBarMessage(`Fetching columns for ${tableName}...`);

        try {
            let query = '';
            if (objId) {
                query = `SELECT ATTNAME, FORMAT_TYPE FROM ${dbPrefix}_V_RELATION_COLUMN WHERE OBJID = ${objId} ORDER BY ATTNUM`;
            } else {
                const schemaClause = schemaName ? `AND UPPER(O.SCHEMA) = UPPER('${schemaName}')` : '';
                const dbClause = dbName ? `AND UPPER(O.DBNAME) = UPPER('${dbName}')` : '';
                query = `
                    SELECT C.ATTNAME, C.FORMAT_TYPE 
                    FROM ${dbPrefix}_V_RELATION_COLUMN C
                    JOIN ${dbPrefix}_V_OBJECT_DATA O ON C.OBJID = O.OBJID
                    WHERE UPPER(O.OBJNAME) = UPPER('${tableName}') ${schemaClause} ${dbClause}
                    ORDER BY C.ATTNUM
                `;
            }

            const resultJson = await runQuery(this.context, query, true, connectionName, this.connectionManager);
            if (!resultJson) return [];

            const results = JSON.parse(resultJson) as { ATTNAME: string; FORMAT_TYPE: string }[];

            const items: ColumnMetadata[] = results.map(row => ({
                ATTNAME: row.ATTNAME,
                FORMAT_TYPE: row.FORMAT_TYPE,
                label: row.ATTNAME,
                kind: 9, // Field
                detail: row.FORMAT_TYPE
            }));

            this.metadataCache.setColumns(connectionName, cacheKey, items);

            // Trigger background prefetch of all other columns in same schema (non-blocking)
            if (dbName) {
                const context = this.context;
                const cache = this.metadataCache;
                setTimeout(async () => {
                    try {
                        await cache.prefetchColumnsForSchema(
                            connectionName,
                            dbName,
                            schemaName,
                            q => runQuery(context, q, true, connectionName!, this.connectionManager)
                        );
                    } catch (e: unknown) {
                        console.error('[SqlCompletion] Background column prefetch error:', e);
                    }
                }, 100);
            }

            return items.map(item => {
                const ci = new vscode.CompletionItem(item.label!, item.kind);
                ci.detail = item.detail;
                return ci;
            });
        } catch (e: unknown) {
            console.error(e);
            return [];
        } finally {
            statusMsg.dispose();
        }
    }
}
