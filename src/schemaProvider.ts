import * as vscode from 'vscode';
import { runQuery } from './queryRunner';
import { ConnectionManager } from './connectionManager';
import { MetadataCache } from './metadataCache';

export class SchemaProvider implements vscode.TreeDataProvider<SchemaItem> {
    private _onDidChangeTreeData: vscode.EventEmitter<SchemaItem | undefined | null | void> = new vscode.EventEmitter<SchemaItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<SchemaItem | undefined | null | void> = this._onDidChangeTreeData.event;

    constructor(private context: vscode.ExtensionContext, private connectionManager: ConnectionManager, private metadataCache: MetadataCache) {
        // Listen for connection changes to refresh tree
        this.connectionManager.onDidChangeConnections(() => this.refresh());
    }

    refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    /**
     * Trigger background prefetch for a connection when database is expanded.
     * This warms up the cache shared with autocomplete and revealInSchema.
     */
    private triggerDatabasePrefetch(connectionName: string, dbName: string): void {
        // Don't block UI - run prefetch in background
        if (!this.metadataCache.hasConnectionPrefetchTriggered(connectionName)) {
            console.log(`[SchemaProvider] Triggering connection prefetch for: ${connectionName}`);
            this.metadataCache.triggerConnectionPrefetch(
                connectionName,
                async (query) => {
                    try {
                        return await runQuery(this.context, query, true, connectionName, this.connectionManager);
                    } catch (e) {
                        console.error('[SchemaProvider] Prefetch query error:', e);
                        return undefined;
                    }
                }
            );
        }
    }

    getTreeItem(element: SchemaItem): vscode.TreeItem {
        return element;
    }

    getParent(element: SchemaItem): SchemaItem | undefined {
        // Return parent based on context value
        if (element.contextValue === 'serverInstance') {
            return undefined; // Root
        } else if (element.contextValue === 'database') {
            return new SchemaItem(
                element.connectionName!,
                vscode.TreeItemCollapsibleState.Collapsed,
                'serverInstance',
                undefined, undefined, undefined, undefined, undefined,
                element.connectionName,
                undefined // customIconPath - we can't easily resolve it here without context, potentially issue?
            );
        } else if (element.contextValue.startsWith('typeGroup')) {
            // Parent is database
            return new SchemaItem(
                element.dbName!,
                vscode.TreeItemCollapsibleState.Collapsed,
                'database',
                element.dbName,
                undefined, undefined, undefined, undefined,
                element.connectionName
            );
        } else if (element.contextValue.startsWith('netezza:')) {
            // Parent is typeGroup
            return new SchemaItem(
                element.objType!,
                vscode.TreeItemCollapsibleState.Collapsed,
                `typeGroup:${element.objType}`,
                element.dbName,
                element.objType,
                undefined, undefined, undefined,
                element.connectionName
            );
        }
        return undefined;
    }

    async getChildren(element?: SchemaItem): Promise<SchemaItem[]> {
        if (!element) {
            // Root: Server Instances
            const connections = await this.connectionManager.getConnections();
            const iconPath = vscode.Uri.file(this.context.asAbsolutePath('netezza_icon64.png'));

            return connections.map(conn => new SchemaItem(
                conn.name,
                vscode.TreeItemCollapsibleState.Collapsed,
                'serverInstance',
                undefined, undefined, undefined, undefined, undefined,
                conn.name,
                undefined, // parentName
                iconPath // Pass custom icon
            ));
        } else if (element.contextValue === 'serverInstance') {
            // Children: Databases for this connection
            // Check cache first
            if (!element.connectionName) return [];

            const cachedDbs = this.metadataCache.getDatabases(element.connectionName);
            if (cachedDbs) {
                return cachedDbs.map((db: any) => new SchemaItem(
                    db.label || db.DATABASE, // simplified, dependent on what's stored
                    vscode.TreeItemCollapsibleState.Collapsed,
                    'database',
                    db.label || db.DATABASE,
                    undefined, undefined, undefined, undefined,
                    element.connectionName
                ));
            }

            try {
                const results = await runQuery(this.context, "SELECT DATABASE FROM system.._v_database ORDER BY DATABASE", true, element.connectionName, this.connectionManager);
                if (!results) {
                    return [];
                }
                const databases = JSON.parse(results);

                // Update Cache
                const cacheItems = databases.map((row: any) => ({
                    label: row.DATABASE,
                    kind: 9, // Module
                    detail: 'Database'
                }));
                this.metadataCache.setDatabases(element.connectionName, cacheItems);

                return databases.map((db: any) => new SchemaItem(
                    db.DATABASE,
                    vscode.TreeItemCollapsibleState.Collapsed,
                    'database',
                    db.DATABASE,
                    undefined, undefined, undefined, undefined,
                    element.connectionName
                ));
            } catch (e) {
                vscode.window.showErrorMessage(`Failed to load databases for ${element.connectionName}: ${e}`);
                return [];
            }
        } else if (element.contextValue === 'database') {
            // Children: Object Types (Groups)
            // Trigger background prefetch for this connection to warm up cache
            if (element.connectionName && element.dbName) {
                this.triggerDatabasePrefetch(element.connectionName, element.dbName);
            }
            // Check typeGroup cache first
            if (element.connectionName && element.dbName) {
                const cachedTypes = this.metadataCache.getTypeGroups(element.connectionName, element.dbName);
                if (cachedTypes && cachedTypes.length > 0) {
                    return cachedTypes.map((t: string) => new SchemaItem(
                        t,
                        vscode.TreeItemCollapsibleState.Collapsed,
                        `typeGroup:${t}`,
                        element.dbName,
                        t,
                        undefined, undefined, undefined,
                        element.connectionName
                    ));
                }
            }

            try {
                const query = `SELECT DISTINCT OBJTYPE FROM ${element.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${element.dbName}' ORDER BY OBJTYPE`;
                const results = await runQuery(this.context, query, true, element.connectionName, this.connectionManager);
                const types = JSON.parse(results || '[]');

                // Cache the type groups
                if (element.connectionName && element.dbName) {
                    const typeList = types.map((t: any) => t.OBJTYPE);
                    this.metadataCache.setTypeGroups(element.connectionName, element.dbName, typeList);
                }

                return types.map((t: any) => new SchemaItem(
                    t.OBJTYPE,
                    vscode.TreeItemCollapsibleState.Collapsed,
                    `typeGroup:${t.OBJTYPE}`,
                    element.dbName,
                    t.OBJTYPE,
                    undefined, undefined, undefined,
                    element.connectionName
                ));
            } catch (e) {
                vscode.window.showErrorMessage("Failed to load object types: " + e);
                return [];
            }
        } else if (element.contextValue.startsWith('typeGroup')) {
            // Children: Objects of specific type

            // OPTIMIZATION: Try cache first (Offline support)
            // Only for TABLE, VIEW, EXTERNAL TABLE as they are primarily cached
            if (element.connectionName && element.dbName && (element.objType === 'TABLE' || element.objType === 'VIEW' || element.objType === 'EXTERNAL TABLE')) {
                const cachedObjects = this.metadataCache.getObjectsWithSchema(element.connectionName, element.dbName);

                if (cachedObjects && cachedObjects.length > 0) {
                    const targetType = element.objType;

                    // Filter items matching the type
                    const filtered = cachedObjects.filter(obj => {
                        const item = obj.item;
                        // Check objType if available (preferred)
                        if (item.objType) {
                            return item.objType === targetType;
                        }
                        // Fallback to strict kind check if objType missing (legacy cache?)
                        if (targetType === 'VIEW') return item.kind === 18;
                        if (targetType === 'TABLE') return item.kind !== 18 && item.detail !== 'EXTERNAL TABLE';
                        if (targetType === 'EXTERNAL TABLE') return item.detail === 'EXTERNAL TABLE' || item.detail?.startsWith('EXTERNAL TABLE');
                        return false;
                    });

                    if (filtered.length > 0) {
                        return filtered.map(obj => new SchemaItem(
                            typeof obj.item.label === 'string' ? obj.item.label : (obj.item.label?.label || 'unknown'),
                            vscode.TreeItemCollapsibleState.Collapsed,
                            `netezza:${element.objType}`,
                            element.dbName,
                            element.objType,
                            obj.schema,
                            obj.objId, // Now includes objId from cache
                            undefined,
                            element.connectionName
                        ));
                    }
                }
            }

            try {
                let query: string;
                if (element.objType === 'PROCEDURE') {
                    // Start of Modification for Procedures
                    query = `SELECT PROCEDURESIGNATURE AS OBJNAME, SCHEMA, OBJID::INT AS OBJID, COALESCE(DESCRIPTION, '') AS DESCRIPTION FROM ${element.dbName}.._V_PROCEDURE WHERE DATABASE = '${element.dbName}' ORDER BY PROCEDURESIGNATURE`;
                } else {
                    query = `SELECT OBJNAME, SCHEMA, OBJID, COALESCE(DESCRIPTION, '') AS DESCRIPTION FROM ${element.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${element.dbName}' AND OBJTYPE = '${element.objType}' ORDER BY OBJNAME`;
                }
                const results = await runQuery(this.context, query, true, element.connectionName, this.connectionManager);
                const objects = JSON.parse(results || '[]');

                // Write-back to cache to warm it up
                // Group by Schema
                if (element.connectionName && element.dbName && (element.objType === 'TABLE' || element.objType === 'VIEW')) {
                    const objectsBySchema = new Map<string, { tables: any[], idMap: Map<string, number> }>();

                    for (const obj of objects) {
                        const schemaKey = obj.SCHEMA ? `${element.dbName}.${obj.SCHEMA}` : `${element.dbName}..`;
                        if (!objectsBySchema.has(schemaKey)) {
                            objectsBySchema.set(schemaKey, { tables: [], idMap: new Map() });
                        }
                        const entry = objectsBySchema.get(schemaKey)!;

                        entry.tables.push({
                            label: obj.OBJNAME,
                            kind: element.objType === 'VIEW' ? 18 : 7,
                            detail: obj.SCHEMA ? element.objType : `${element.objType} (${obj.SCHEMA})`,
                            sortText: obj.OBJNAME
                        });

                        const fullKey = obj.SCHEMA
                            ? `${element.dbName}.${obj.SCHEMA}.${obj.OBJNAME}`
                            : `${element.dbName}..${obj.OBJNAME}`;
                        entry.idMap.set(fullKey, obj.OBJID);
                    }

                    // Update Cache for each schema found
                    for (const [key, entry] of objectsBySchema) {
                        // We merge or overwrite? setTables overwrites. 
                        // Since we are fetching *all* objects of a type, we might only have partial data for a schema 
                        // (e.g. only TABLES, missing VIEWS if we are in the TABLE node).
                        // MetadataCache.setTables expects the full list for that key? 
                        // Actually setTables overwrites. If we only fetched TABLEs, and we overwrite, we lose VIEWs from cache if they were there.
                        // But typically completionProvider fetches both. 
                        // A safer bet is to only update if we fetched everything or check cache state.
                        // For now, let's skip overwriting if we think we might be partial.
                        // Or better: Use "Merge" strategy? MetadataCache doesn't support merge yet.
                        // Let's NOT write back here to avoid corrupting the cache with partial lists (e.g. only tables, no views).
                        // Providing partial data to autocomplete is worse than no data (user thinks views don't exist).
                    }
                    // Re-evaluating: The plan said "Write-back". 
                    // If I'm viewing "TABLES", I only get tables. If I overwrite the cache for "DB.SCHEMA", I delete "VIEWS" from that cache entry.
                    // Implementation Plan correction: I will skip write-back for now to avoid data loss, 
                    // unless I implement a merge method on MetadataCache.
                    // Let's implement a merge helper here or just skip. 
                    // Use case: User expands "Tables". We get all tables. 
                    // If we don't save, autocomplete is cold.
                    // If we save, autocomplete loses Views.
                    // Let's skip write-back for now to be safe.
                }

                return objects.map((obj: any) => {
                    const expandableTypes = ['TABLE', 'VIEW', 'EXTERNAL TABLE', 'SYSTEM VIEW', 'SYSTEM TABLE'];
                    const isExpandable = expandableTypes.includes(element.objType || '');
                    return new SchemaItem(
                        obj.OBJNAME,
                        isExpandable ? vscode.TreeItemCollapsibleState.Collapsed : vscode.TreeItemCollapsibleState.None,
                        `netezza:${element.objType}`,
                        element.dbName,
                        element.objType,
                        obj.SCHEMA,
                        obj.OBJID,
                        obj.DESCRIPTION,
                        element.connectionName
                    );
                });
            } catch (e) {
                vscode.window.showErrorMessage("Failed to load objects: " + e);
                return [];
            }
        } else if (element.contextValue.startsWith('netezza:')) {
            // Children: Columns
            const tableName = element.label; // SchemaItem label is the object name
            const schemaName = element.schema;
            const dbName = element.dbName;

            // Try cache first (works even without objId)
            if (element.connectionName && dbName) {
                const columnKey = `${dbName}.${schemaName || ''}.${tableName}`;
                const cachedCols = this.metadataCache.getColumns(element.connectionName, columnKey);

                // Check if cache has isPk property (new format) - if not, refetch
                if (cachedCols && cachedCols.length > 0 && cachedCols[0].isPk !== undefined) {
                    return cachedCols.map((col: any) => new SchemaItem(
                        col.detail ? `${col.label} (${col.detail})` : col.label, // Reconstruct label
                        vscode.TreeItemCollapsibleState.None,
                        'column',
                        element.dbName,
                        undefined,
                        undefined,
                        undefined,
                        col.documentation || '', // Assuming description stored in documentation or similar
                        element.connectionName,
                        tableName, // Parent (Table) Name
                        undefined,
                        col.isPk, // Retrieve isPk from cache
                        col.isFk  // Retrieve isFk from cache
                    ));
                }
                // If cache is stale (no isPk), fall through to refetch
            }

            // If no cached columns, try to query (need connection)
            if (!element.connectionName || !dbName) {
                return [];
            }

            try {
                // Use table name (RELATION) instead of OBJID for JOIN with _V_RELATION_KEYDATA
                // Also need to match SCHEMA for proper scoping
                const schemaClause = schemaName ? `AND UPPER(K.SCHEMA) = UPPER('${schemaName}')` : '';
                const query = `SELECT 
                        X.ATTNAME
                        , X.FORMAT_TYPE
                        , X.ATTNOTNULL::BOOL AS ATTNOTNULL
                        , X.COLDEFAULT
                        , COALESCE(X.DESCRIPTION, '') AS DESCRIPTION
                        , MAX(CASE WHEN K.CONTYPE = 'p' THEN 1 ELSE 0 END) AS IS_PK
                        , MAX(CASE WHEN K.CONTYPE = 'f' THEN 1 ELSE 0 END) AS IS_FK
                    FROM
                        ${dbName}.._V_RELATION_COLUMN X
                    INNER JOIN
                        ${dbName}.._V_OBJECT_DATA O ON X.OBJID = O.OBJID
                    LEFT JOIN
                        ${dbName}.._V_RELATION_KEYDATA K 
                        ON UPPER(K.RELATION) = UPPER(O.OBJNAME) 
                        AND UPPER(K.SCHEMA) = UPPER(O.SCHEMA)
                        AND UPPER(K.ATTNAME) = UPPER(X.ATTNAME)
                        AND K.CONTYPE IN ('p', 'f')
                    WHERE
                        UPPER(O.OBJNAME) = UPPER('${tableName}')
                        AND UPPER(O.DBNAME) = UPPER('${dbName}')
                        ${schemaName ? `AND UPPER(O.SCHEMA) = UPPER('${schemaName}')` : ''}
                    GROUP BY 
                        X.ATTNAME, X.FORMAT_TYPE, X.ATTNOTNULL, X.COLDEFAULT, X.DESCRIPTION, X.ATTNUM
                    ORDER BY 
                        X.ATTNUM`;

                const results = await runQuery(this.context, query, true, element.connectionName, this.connectionManager);
                const columns = JSON.parse(results || '[]');

                // Cache the results
                const columnKey = `${dbName}.${schemaName || ''}.${tableName}`;
                const cacheItems = columns.map((col: any) => ({
                    label: col.ATTNAME,
                    kind: 5, // Field
                    detail: col.FORMAT_TYPE,
                    documentation: col.DESCRIPTION,
                    isPk: Number(col.IS_PK) === 1,
                    isFk: Number(col.IS_FK) === 1
                }));
                this.metadataCache.setColumns(element.connectionName, columnKey, cacheItems);

                return columns.map((col: any) => new SchemaItem(
                    `${col.ATTNAME} (${col.FORMAT_TYPE})`,
                    vscode.TreeItemCollapsibleState.None,
                    'column',
                    element.dbName,
                    undefined,
                    undefined,
                    undefined,
                    col.DESCRIPTION,
                    element.connectionName,
                    tableName, // Parent (Table) Name
                    undefined,
                    Number(col.IS_PK) === 1,
                    Number(col.IS_FK) === 1
                ));
            } catch (e) {
                vscode.window.showErrorMessage("Failed to load columns: " + e);
                return [];
            }
        }

        return [];
    }
}

export class SchemaItem extends vscode.TreeItem {
    constructor(
        public readonly label: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly contextValue: string,
        public readonly dbName?: string,
        public readonly objType?: string,
        public readonly schema?: string,
        public readonly objId?: number,
        public readonly objectDescription?: string,
        public readonly connectionName?: string,
        public readonly parentName?: string, // Add parent (Table) name for stable ID
        customIconPath?: vscode.Uri,
        public readonly isPk?: boolean,
        public readonly isFk?: boolean
    ) {
        super(label, collapsibleState);

        // Build tooltip with Description if available
        let tooltipText = this.label;
        if (connectionName) {
            tooltipText += `\n[Server: ${connectionName}]`;
        }
        if (objectDescription && objectDescription.trim()) {
            tooltipText += `\n\n${objectDescription.trim()}`;
        }
        if (schema && contextValue.startsWith('netezza:')) {
            tooltipText += `\n\nSchema: ${schema}`;
        }

        if (this.isPk) tooltipText += `\n🔑 Primary Key`;
        if (this.isFk) tooltipText += `\n🔗 Foreign Key`;

        this.tooltip = tooltipText;

        this.description = schema ? `(${schema})` : '';

        // Generate a stable ID for the tree item to support reveal()
        // Format: connection|context|database|schema|label|parent
        const parts = [
            connectionName || 'global',
            contextValue,
            dbName || '',
            schema || '',
            objType || '',
            parentName || '',
            label,
            objId ? objId.toString() : ''
        ];
        this.id = parts.join('|');

        if (customIconPath) {
            this.iconPath = customIconPath;
        } else if (contextValue === 'serverInstance') {
            this.iconPath = new vscode.ThemeIcon('server');
        } else if (contextValue === 'database') {
            this.iconPath = new vscode.ThemeIcon('database');
        } else if (contextValue.startsWith('typeGroup')) {
            this.iconPath = new vscode.ThemeIcon('folder');
        } else if (contextValue.startsWith('netezza:')) {
            this.iconPath = this.getIconForType(objType);
        } else if (contextValue === 'column') {
            if (this.isPk) {
                this.iconPath = new vscode.ThemeIcon('key', new vscode.ThemeColor('charts.yellow'));
            } else if (this.isFk) {
                this.iconPath = new vscode.ThemeIcon('link', new vscode.ThemeColor('charts.blue'));
            } else {
                this.iconPath = new vscode.ThemeIcon('symbol-field');
            }
        }
    }

    private getIconForType(type?: string): vscode.ThemeIcon {
        switch (type) {
            case 'TABLE': return new vscode.ThemeIcon('table');
            case 'VIEW': return new vscode.ThemeIcon('eye');
            case 'PROCEDURE': return new vscode.ThemeIcon('gear');
            case 'FUNCTION': return new vscode.ThemeIcon('symbol-function');
            case 'AGGREGATE': return new vscode.ThemeIcon('symbol-operator');
            case 'EXTERNAL TABLE': return new vscode.ThemeIcon('server');
            default: return new vscode.ThemeIcon('file');
        }
    }
}
