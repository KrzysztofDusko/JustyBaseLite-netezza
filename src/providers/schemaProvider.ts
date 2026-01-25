import * as vscode from 'vscode';
import { runQueryRaw, queryResultToRows } from '../core/queryRunner';
import { ConnectionManager } from '../core/connectionManager';
import { MetadataCache } from '../metadataCache';
import { buildColumnMetadataQuery, parseColumnMetadata } from './tableMetadataProvider';
import { DatabaseMetadata, TableMetadata, ColumnMetadata } from '../metadata/types';
import { NZ_QUERIES, NZ_SYSTEM_VIEWS } from '../metadata';

export class SchemaProvider implements vscode.TreeDataProvider<SchemaItem> {
    private _onDidChangeTreeData: vscode.EventEmitter<SchemaItem | undefined | null | void> = new vscode.EventEmitter<
        SchemaItem | undefined | null | void
    >();
    readonly onDidChangeTreeData: vscode.Event<SchemaItem | undefined | null | void> = this._onDidChangeTreeData.event;

    constructor(
        private context: vscode.ExtensionContext,
        private connectionManager: ConnectionManager,
        private metadataCache: MetadataCache
    ) {
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
    private triggerDatabasePrefetch(connectionName: string, _dbName: string): void {
        // Don't block UI - run prefetch in background
        if (!this.metadataCache.hasConnectionPrefetchTriggered(connectionName)) {
            console.log(`[SchemaProvider] Triggering connection prefetch for: ${connectionName}`);
            this.metadataCache.triggerConnectionPrefetch(connectionName, async query => {
                try {
                    return await runQueryRaw(this.context, query, true, this.connectionManager, connectionName);
                } catch (e: unknown) {
                    console.error('[SchemaProvider] Prefetch query error:', e);
                    return undefined;
                }
            });
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
                undefined,
                undefined,
                undefined,
                undefined,
                undefined,
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
                undefined,
                undefined,
                undefined,
                undefined,
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
                undefined,
                undefined,
                undefined,
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

            return connections.map(
                conn =>
                    new SchemaItem(
                        conn.name,
                        vscode.TreeItemCollapsibleState.Collapsed,
                        'serverInstance',
                        undefined,
                        undefined,
                        undefined,
                        undefined,
                        undefined,
                        conn.name,
                        undefined, // parentName
                        iconPath // Pass custom icon
                    )
            );
        } else if (element.contextValue === 'serverInstance') {
            // Children: Databases for this connection
            // Check cache first
            if (!element.connectionName) return [];

            const cachedDbs = this.metadataCache.getDatabases(element.connectionName);
            if (cachedDbs) {
                return cachedDbs.map(
                    (db: DatabaseMetadata) =>
                        new SchemaItem(
                            db.label || db.DATABASE, // simplified, dependent on what's stored
                            vscode.TreeItemCollapsibleState.Collapsed,
                            'database',
                            db.label || db.DATABASE,
                            undefined,
                            undefined,
                            undefined,
                            undefined,
                            element.connectionName
                        )
                );
            }

            try {
                const result = await runQueryRaw(
                    this.context,
                    NZ_QUERIES.LIST_DATABASES,
                    true,
                    this.connectionManager,
                    element.connectionName
                );
                if (!result) {
                    return [];
                }
                const databases = queryResultToRows<{ DATABASE: string }>(result);

                // Update Cache
                const cacheItems: DatabaseMetadata[] = databases.map((row: { DATABASE: string }) => ({
                    DATABASE: row.DATABASE,
                    label: row.DATABASE,
                    kind: 9, // Module
                    detail: 'Database'
                }));
                this.metadataCache.setDatabases(element.connectionName, cacheItems);

                return databases.map(
                    (db: { DATABASE: string }) =>
                        new SchemaItem(
                            db.DATABASE,
                            vscode.TreeItemCollapsibleState.Collapsed,
                            'database',
                            db.DATABASE,
                            undefined,
                            undefined,
                            undefined,
                            undefined,
                            element.connectionName
                        )
                );
            } catch (e: unknown) {
                const errorMsg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage(`Failed to load databases for ${element.connectionName}: ${errorMsg}`);
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
                    return cachedTypes.map(
                        (t: string) =>
                            new SchemaItem(
                                t,
                                vscode.TreeItemCollapsibleState.Collapsed,
                                `typeGroup:${t}`,
                                element.dbName,
                                t,
                                undefined,
                                undefined,
                                undefined,
                                element.connectionName
                            )
                    );
                }
            }

            try {
                const query = `SELECT DISTINCT OBJTYPE FROM ${element.dbName}..${NZ_SYSTEM_VIEWS.OBJECT_DATA} WHERE DBNAME = '${element.dbName}' ORDER BY OBJTYPE`;
                const result = await runQueryRaw(
                    this.context,
                    query,
                    true,
                    this.connectionManager,
                    element.connectionName
                );
                const types = result ? queryResultToRows<{ OBJTYPE: string }>(result) : [];

                // Cache the type groups
                if (element.connectionName && element.dbName) {
                    const typeList = types.map((t: { OBJTYPE: string }) => t.OBJTYPE);
                    this.metadataCache.setTypeGroups(element.connectionName, element.dbName, typeList);
                }

                return types.map(
                    (t: { OBJTYPE: string }) =>
                        new SchemaItem(
                            t.OBJTYPE,
                            vscode.TreeItemCollapsibleState.Collapsed,
                            `typeGroup:${t.OBJTYPE}`,
                            element.dbName,
                            t.OBJTYPE,
                            undefined,
                            undefined,
                            undefined,
                            element.connectionName
                        )
                );
            } catch (e: unknown) {
                const errorMsg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage('Failed to load object types: ' + errorMsg);
                return [];
            }
        } else if (element.contextValue.startsWith('typeGroup')) {
            // Children: Objects of specific type

            // OPTIMIZATION: Try cache first (Offline support)
            // Only for TABLE, VIEW, EXTERNAL TABLE as they are primarily cached
            if (
                element.connectionName &&
                element.dbName &&
                (element.objType === 'TABLE' || element.objType === 'VIEW' || element.objType === 'EXTERNAL TABLE')
            ) {
                const cachedObjects = this.metadataCache.getObjectsWithSchema(element.connectionName, element.dbName);

                if (cachedObjects && cachedObjects.length > 0) {
                    const targetType = element.objType;

                    // Filter items matching the type
                    const filtered = cachedObjects.filter(obj => {
                        const item = obj.item as { objType?: string; kind?: number; detail?: string };
                        // Check objType if available (preferred)
                        if (item.objType) {
                            return item.objType === targetType;
                        }
                        // Fallback to strict kind check if objType missing (legacy cache?)
                        if (targetType === 'VIEW') return item.kind === 18;
                        if (targetType === 'TABLE') return item.kind !== 18 && item.detail !== 'EXTERNAL TABLE';
                        if (targetType === 'EXTERNAL TABLE')
                            return item.detail === 'EXTERNAL TABLE' || item.detail?.startsWith('EXTERNAL TABLE');
                        return false;
                    });

                    if (filtered.length > 0) {
                        return filtered.map(obj => {
                            const it = obj.item as { label?: string | { label: string } };
                            return new SchemaItem(
                                typeof it.label === 'string'
                                    ? it.label
                                    : it.label?.label || 'unknown',
                                vscode.TreeItemCollapsibleState.Collapsed,
                                `netezza:${element.objType}`,
                                element.dbName,
                                element.objType,
                                obj.schema,
                                obj.objId,
                                obj.description, // description from cache
                                element.connectionName,
                                undefined, // parentName
                                undefined, // customIconPath
                                undefined, // isPk
                                undefined, // isFk
                                obj.owner  // owner from cache
                            );
                        });
                    }
                }
            }

            try {
                let query: string;
                if (element.objType === 'PROCEDURE') {
                    // Start of Modification for Procedures
                    query = `SELECT PROCEDURESIGNATURE AS OBJNAME, SCHEMA, OBJID::INT AS OBJID, COALESCE(DESCRIPTION, '') AS DESCRIPTION, OWNER FROM ${element.dbName}.._V_PROCEDURE WHERE DATABASE = '${element.dbName}' ORDER BY PROCEDURESIGNATURE`;
                } else {
                    query = `SELECT OBJNAME, SCHEMA, OBJID, COALESCE(DESCRIPTION, '') AS DESCRIPTION, OWNER FROM ${element.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${element.dbName}' AND OBJTYPE = '${element.objType}' ORDER BY OBJNAME`;
                }
                const result = await runQueryRaw(
                    this.context,
                    query,
                    true,
                    this.connectionManager,
                    element.connectionName
                );
                const objects = result ? queryResultToRows<{ OBJNAME: string; SCHEMA?: string; OBJID?: number; DESCRIPTION?: string; OWNER?: string }>(result) : [];

                // Write-back to cache to warm it up
                // Group by Schema
                if (
                    element.connectionName &&
                    element.dbName &&
                    (element.objType === 'TABLE' || element.objType === 'VIEW')
                ) {
                    const objectsBySchema = new Map<string, { tables: TableMetadata[]; idMap: Map<string, number> }>();

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
                        if (obj.OBJID !== undefined) {
                            entry.idMap.set(fullKey, obj.OBJID);
                        }
                    }
                }

                return objects.map((obj: { OBJNAME: string; SCHEMA?: string; OBJID?: number; DESCRIPTION?: string; OWNER?: string }) => {
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
                        element.connectionName,
                        undefined, // parentName
                        undefined, // customIconPath
                        undefined, // isPk
                        undefined, // isFk
                        obj.OWNER  // owner
                    );
                });
            } catch (e: unknown) {
                const errorMsg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage('Failed to load objects: ' + errorMsg);
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
                    return cachedCols.map(
                        (col: ColumnMetadata) =>
                            new SchemaItem(
                                col.detail ? `${col.label || col.ATTNAME} (${col.detail})` : (col.label || col.ATTNAME), // Reconstruct label
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
                                col.isFk // Retrieve isFk from cache
                            )
                    );
                }
                // If cache is stale (no isPk), fall through to refetch
            }

            // If no cached columns, try to query (need connection)
            if (!element.connectionName || !dbName) {
                return [];
            }

            try {
                // Use centralized query builder from tableMetadataProvider
                const query = buildColumnMetadataQuery(dbName, schemaName || '', tableName as string);

                const results = await runQueryRaw(
                    this.context,
                    query,
                    true,
                    this.connectionManager,
                    element.connectionName
                );
                const parsedColumns = parseColumnMetadata(results);

                // Cache the results
                const columnKey = `${dbName}.${schemaName || ''}.${tableName}`;
                const cacheItems = parsedColumns.map(col => ({
                    ATTNAME: col.attname,
                    FORMAT_TYPE: col.formatType,
                    label: col.attname,
                    kind: 5, // Field
                    detail: col.formatType,
                    documentation: col.description,
                    isPk: col.isPk,
                    isFk: col.isFk
                }));
                this.metadataCache.setColumns(element.connectionName, columnKey, cacheItems);

                return parsedColumns.map(
                    col =>
                        new SchemaItem(
                            `${col.attname} (${col.formatType})`,
                            vscode.TreeItemCollapsibleState.None,
                            'column',
                            element.dbName,
                            undefined,
                            undefined,
                            undefined,
                            col.description,
                            element.connectionName,
                            tableName as string, // Parent (Table) Name
                            undefined,
                            col.isPk,
                            col.isFk
                        )
                );
            } catch (e: unknown) {
                const errorMsg = e instanceof Error ? e.message : String(e);
                vscode.window.showErrorMessage('Failed to load columns: ' + errorMsg);
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
        public readonly isFk?: boolean,
        public readonly owner?: string
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
        if (owner && contextValue.startsWith('netezza:')) {
            tooltipText += `\nOwner: ${owner}`;
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
            case 'TABLE':
                return new vscode.ThemeIcon('table');
            case 'VIEW':
                return new vscode.ThemeIcon('eye');
            case 'PROCEDURE':
                return new vscode.ThemeIcon('gear');
            case 'FUNCTION':
                return new vscode.ThemeIcon('symbol-function');
            case 'AGGREGATE':
                return new vscode.ThemeIcon('symbol-operator');
            case 'EXTERNAL TABLE':
                return new vscode.ThemeIcon('server');
            default:
                return new vscode.ThemeIcon('file');
        }
    }
}
