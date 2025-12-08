import * as vscode from 'vscode';
import { runQuery } from './queryRunner';
import { MetadataCache } from './metadataCache';

export class SqlCompletionItemProvider implements vscode.CompletionItemProvider {

    constructor(private context: vscode.ExtensionContext, private metadataCache: MetadataCache) {
    }

    public async provideCompletionItems(
        document: vscode.TextDocument,
        position: vscode.Position,
        token: vscode.CancellationToken,
        context: vscode.CompletionContext
    ): Promise<vscode.CompletionItem[] | vscode.CompletionList> {

        const text = document.getText();
        const cleanText = this.stripComments(text);

        // Parse local definitions (CTEs, Temp Tables)
        const localDefs = this.parseLocalDefinitions(cleanText);

        const linePrefix = document.lineAt(position).text.substr(0, position.character);
        const upperPrefix = linePrefix.toUpperCase();

        // Get previous line for multi-line pattern support
        const prevLine = position.line > 0 ? document.lineAt(position.line - 1).text : '';
        const prevLineUpper = prevLine.toUpperCase();

        // 1. Database Suggestion
        // Trigger: "FROM " or "JOIN " (with optional whitespace/newlines)
        if (/(FROM|JOIN)\s+$/.test(upperPrefix)) {
            const dbs = await this.getDatabases();
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
                const dbs = await this.getDatabases();
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
            const items = await this.getSchemas(dbName);
            return new vscode.CompletionList(items, false);
        }
        // Check for DB. on current line with FROM/JOIN on previous line
        const dbMatchCurrent = linePrefix.match(/^\s*([a-zA-Z0-9_]+)\.\s*$/i);
        if (dbMatchCurrent && /(?:FROM|JOIN)\s*$/i.test(prevLine)) {
            const dbName = dbMatchCurrent[1];
            const items = await this.getSchemas(dbName);
            return new vscode.CompletionList(items, false);
        }

        // 3. Table Suggestion (with Schema)
        // Trigger: "FROM DB.SCHEMA."
        const schemaMatch = linePrefix.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.$/i);
        if (schemaMatch) {
            const dbName = schemaMatch[1];
            const schemaName = schemaMatch[2];
            return this.getTables(dbName, schemaName);
        }
        // Check for DB.SCHEMA. on current line with FROM/JOIN on previous line
        const schemaMatchCurrent = linePrefix.match(/^\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.\s*$/i);
        if (schemaMatchCurrent && /(?:FROM|JOIN)\s*$/i.test(prevLine)) {
            const dbName = schemaMatchCurrent[1];
            const schemaName = schemaMatchCurrent[2];
            return this.getTables(dbName, schemaName);
        }

        // 4. Table Suggestion (Double dot / No Schema)
        // Trigger: "FROM DB.."
        const doubleDotMatch = linePrefix.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.\.$/i);
        if (doubleDotMatch) {
            const dbName = doubleDotMatch[1];
            return this.getTables(dbName, undefined);
        }
        // Check for DB.. on current line with FROM/JOIN on previous line
        const doubleDotMatchCurrent = linePrefix.match(/^\s*([a-zA-Z0-9_]+)\.\.\s*$/i);
        if (doubleDotMatchCurrent && /(?:FROM|JOIN)\s*$/i.test(prevLine)) {
            const dbName = doubleDotMatchCurrent[1];
            return this.getTables(dbName, undefined);
        }

        // 5. Column Suggestion (via Alias or Table name)
        // Trigger: Ends with "."
        if (linePrefix.trim().endsWith('.')) {
            const parts = linePrefix.trim().split(/[\s.]+/); // Split by space or dot
            // This split is a bit naive, let's look at the word before the dot.
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
                    return this.getColumns(aliasInfo.db, aliasInfo.schema, aliasInfo.table);
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

    private parseLocalDefinitions(text: string): { name: string, type: string, columns: string[] }[] {
        const definitions: { name: string, type: string, columns: string[] }[] = [];

        // 1. Temp Tables: CREATE TABLE TEMP_1 AS ( SELECT ... )
        // Regex to capture: CREATE TABLE name AS ( query )
        // We need to be careful with nested parenthesis. For simplicity, we assume the query is inside the first balanced parens after AS.
        // Or we just grab everything until the matching closing paren.

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
                const openParenIndex = currentIndex + cteMatch[0].length; // This is actually wrong, match[0] includes the opening paren
                // We need to find the content inside the parenthesis.
                // cteMatch[0] is like "ABC AS ("

                const queryContent = this.extractBalancedParenthesisContent(text, currentIndex + cteMatch[0].length - 1); // -1 because extractBalanced expects index of '('? No, let's make helper robust.

                // My helper extractBalancedParenthesisContent assumes we start searching *after* the opening paren? 
                // Let's make it start searching from the opening paren.

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
            // We need to find the content inside the parenthesis.
            // extractBalancedParenthesisContent expects startIndex to be *after* the opening paren?
            // Let's check the implementation of extractBalancedParenthesisContent.
            // It starts loop at startIndex.
            // If we pass startIndex, it will start checking from there.
            // But wait, my helper logic:
            // let balance = 1; let i = startIndex;
            // So it assumes we are already inside one level of parenthesis.
            // Yes, that matches "JOIN (".

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

        let selectList = "";
        let balance = 0;
        let fromIndex = -1;

        const start = selectMatch[0].length;
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
        let current = "";
        balance = 0;

        for (let i = 0; i < selectList.length; i++) {
            const char = selectList[i];
            if (char === '(') balance++;
            else if (char === ')') balance--;

            if (char === ',' && balance === 0) {
                columns.push(current.trim());
                current = "";
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

            // Remove comments from the column definition if any (though we stripped globally, inline comments might remain if logic wasn't perfect, but global strip should handle it)

            const asMatch = col.match(/\s+AS\s+([a-zA-Z0-9_]+)$/i);
            if (asMatch) return asMatch[1];

            const spaceMatch = col.match(/\s+([a-zA-Z0-9_]+)$/i);
            if (spaceMatch) return spaceMatch[1];

            // Just the name, maybe with dot
            const parts = col.split('.');
            return parts[parts.length - 1];
        });
    }

    private getKeywords(): vscode.CompletionItem[] {
        const keywords = [
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT',
            'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
            'CREATE', 'DROP', 'TABLE', 'VIEW', 'DATABASE',
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'ON',
            'AND', 'OR', 'NOT', 'NULL', 'IS', 'IN', 'BETWEEN', 'LIKE',
            'AS', 'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'WITH', 'UNION', 'ALL'
        ];
        return keywords.map(k => {
            const item = new vscode.CompletionItem(k, vscode.CompletionItemKind.Keyword);
            item.detail = 'SQL Keyword';
            return item;
        });
    }

    private async getDatabases(): Promise<vscode.CompletionItem[]> {
        const cached = this.metadataCache.getDatabases();
        if (cached) {
            return cached.map((item: any) => {
                if (item instanceof vscode.CompletionItem) return item;
                const ci = new vscode.CompletionItem(item.label, item.kind);
                ci.detail = item.detail;
                return ci;
            });
        }

        try {
            const query = "SELECT DATABASE FROM system.._v_database ORDER BY DATABASE";
            const resultJson = await runQuery(this.context, query, true);
            if (!resultJson) return [];

            const results = JSON.parse(resultJson);
            const items = results.map((row: any) => {
                const item = new vscode.CompletionItem(row.DATABASE, vscode.CompletionItemKind.Module);
                item.detail = 'Database';
                return item;
            });

            this.metadataCache.setDatabases(items);

            return items;
        } catch (e) {
            console.error(e);
            return [];
        }
    }

    private async getSchemas(dbName: string): Promise<vscode.CompletionItem[]> {
        const cached = this.metadataCache.getSchemas(dbName);
        if (cached) {
            return cached.map((item: any) => {
                if (item instanceof vscode.CompletionItem) return item;
                const ci = new vscode.CompletionItem(item.label, item.kind);
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
            const resultJson = await runQuery(this.context, query, true);
            if (!resultJson) {
                return [];
            }

            const results = JSON.parse(resultJson);
            const items = results
                .filter((row: any) => row.SCHEMA != null && row.SCHEMA !== '')
                .map((row: any) => {
                    const schemaName = row.SCHEMA;
                    const item = new vscode.CompletionItem(schemaName, vscode.CompletionItemKind.Folder);
                    item.detail = `Schema in ${dbName}`;
                    item.insertText = schemaName;
                    item.sortText = schemaName;
                    item.filterText = schemaName;
                    return item;
                });

            this.metadataCache.setSchemas(dbName, items);

            return items;
        } catch (e) {
            console.error('[SqlCompletion] Error in getSchemas:', e);
            return [];
        } finally {
            statusBarDisposable.dispose();
        }
    }

    private async getTables(dbName: string, schemaName?: string): Promise<vscode.CompletionItem[]> {
        const cacheKey = schemaName ? `${dbName}.${schemaName}` : `${dbName}..`;
        const cached = this.metadataCache.getTables(cacheKey);
        if (cached) {
            return cached.map((item: any) => {
                if (item instanceof vscode.CompletionItem) return item;
                const ci = new vscode.CompletionItem(item.label, item.kind);
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
            let query = "";
            if (schemaName) {
                query = `SELECT OBJNAME, OBJID FROM ${dbName}.._V_OBJECT_DATA WHERE UPPER(DBNAME) = UPPER('${dbName}') AND UPPER(SCHEMA) = UPPER('${schemaName}') AND OBJTYPE='TABLE' ORDER BY OBJNAME`;
            } else {
                query = `SELECT OBJNAME, OBJID, SCHEMA FROM ${dbName}.._V_OBJECT_DATA WHERE UPPER(DBNAME) = UPPER('${dbName}') AND OBJTYPE='TABLE' ORDER BY OBJNAME`;
            }

            const resultJson = await runQuery(this.context, query, true);
            if (!resultJson) return [];

            const results = JSON.parse(resultJson);

            const idMapForKey = new Map<string, number>();

            const items = results.map((row: any) => {
                const item = new vscode.CompletionItem(row.OBJNAME, vscode.CompletionItemKind.Class);
                item.detail = schemaName ? 'Table' : `Table (${row.SCHEMA})`;
                item.sortText = row.OBJNAME;
                const fullKey = schemaName ? `${dbName}.${schemaName}.${row.OBJNAME}` : `${dbName}..${row.OBJNAME}`;
                idMapForKey.set(fullKey, row.OBJID);
                if (!schemaName && row.SCHEMA) {
                    idMapForKey.set(`${dbName}.${row.SCHEMA}.${row.OBJNAME}`, row.OBJID);
                }

                return item;
            });

            this.metadataCache.setTables(cacheKey, items, idMapForKey);

            return items;
        } catch (e) {
            console.error(e);
            return [];
        } finally {
            statusBarDisposable.dispose();
        }
    }

    private async getColumns(dbName: string | undefined, schemaName: string | undefined, tableName: string): Promise<vscode.CompletionItem[]> {
        let objId: number | undefined;
        const dbPrefix = dbName ? `${dbName}..` : '';

        const lookupKey = schemaName && dbName ? `${dbName}.${schemaName}.${tableName}` : (dbName ? `${dbName}..${tableName}` : undefined);

        if (lookupKey) {
            objId = this.metadataCache.findTableId(lookupKey);
        }

        const cacheKey = `${dbName || 'CURRENT'}.${schemaName || ''}.${tableName}`;
        const cached = this.metadataCache.getColumns(cacheKey);
        if (cached) {
            return cached.map((item: any) => {
                if (item instanceof vscode.CompletionItem) return item;
                const ci = new vscode.CompletionItem(item.label, item.kind);
                ci.detail = item.detail;
                return ci;
            });
        }

        const statusMsg = vscode.window.setStatusBarMessage(`Fetching columns for ${tableName}...`);

        try {
            let query = "";
            if (objId) {
                query = `SELECT ATTNAME, FORMAT_TYPE FROM ${dbPrefix}_V_RELATION_COLUMN WHERE OBJID = ${objId} ORDER BY ATTNUM`;
            } else {
                const schemaClause = schemaName ? `AND UPPER(SCHEMA) = UPPER('${schemaName}')` : '';
                const dbClause = dbName ? `AND UPPER(DBNAME) = UPPER('${dbName}')` : '';
                query = `
                    SELECT C.ATTNAME, C.FORMAT_TYPE 
                    FROM ${dbPrefix}_V_RELATION_COLUMN C
                    JOIN ${dbPrefix}_V_OBJECT_DATA O ON C.OBJID = O.OBJID
                    WHERE UPPER(O.OBJNAME) = UPPER('${tableName}') ${schemaClause} ${dbClause}
                    ORDER BY C.ATTNUM
                `;
            }

            const resultJson = await runQuery(this.context, query, true);
            if (!resultJson) return [];

            const results = JSON.parse(resultJson);
            const items = results.map((row: any) => {
                const item = new vscode.CompletionItem(row.ATTNAME, vscode.CompletionItemKind.Field);
                item.detail = row.FORMAT_TYPE;
                return item;
            });

            this.metadataCache.setColumns(cacheKey, items);

            return items;
        } catch (e) {
            console.error(e);
            return [];
        } finally {
            statusMsg.dispose();
        }
    }

    private findAlias(text: string, alias: string): { db?: string, schema?: string, table: string } | null {
        // "FROM db.schema.table alias"
        // "JOIN table alias"

        // Use global search to find all occurrences, then filter out keywords.
        // We iterate to find a valid table reference that is NOT a keyword.
        const aliasPattern = new RegExp(`([a-zA-Z0-9_\\.]+) (?:AS\\s+)?${alias}\\b`, 'gi');

        const keywords = new Set([
            'SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT',
            'ON', 'AND', 'OR', 'NOT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'FULL',
            'UNION', 'EXCEPT', 'INTERSECT',
            'FROM', 'UPDATE', 'DELETE', 'INSERT', 'INTO', 'VALUES', 'SET'
        ]);

        let match;
        while ((match = aliasPattern.exec(text)) !== null) {
            const tableRef = match[1];

            // Check if tableRef is a keyword (false positive)
            if (keywords.has(tableRef.toUpperCase())) {
                continue;
            }

            // Valid candidate found
            const parts = tableRef.split('.');
            if (parts.length === 3) {
                return { db: parts[0], schema: parts[1], table: parts[2] };
            } else if (parts.length === 2) {
                return { table: parts[1], schema: parts[0] };
            } else {
                return { table: parts[0] };
            }
        }
        return null;
    }
}
