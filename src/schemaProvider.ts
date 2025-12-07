import * as vscode from 'vscode';
import { runQuery } from './queryRunner';
import { ConnectionManager } from './connectionManager';

export class SchemaProvider implements vscode.TreeDataProvider<SchemaItem> {
    private _onDidChangeTreeData: vscode.EventEmitter<SchemaItem | undefined | null | void> = new vscode.EventEmitter<SchemaItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<SchemaItem | undefined | null | void> = this._onDidChangeTreeData.event;

    constructor(private context: vscode.ExtensionContext, private connectionManager: ConnectionManager) { }

    refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: SchemaItem): vscode.TreeItem {
        return element;
    }

    getParent(element: SchemaItem): SchemaItem | undefined {
        // Return parent based on context value
        if (element.contextValue === 'database') {
            return undefined; // Database is root
        } else if (element.contextValue === 'typeGroup') {
            // Parent is database
            return new SchemaItem(
                element.dbName!,
                vscode.TreeItemCollapsibleState.Collapsed,
                'database',
                element.dbName
            );
        } else if (element.contextValue.startsWith('netezza:')) {
            // Parent is typeGroup
            return new SchemaItem(
                element.objType!,
                vscode.TreeItemCollapsibleState.Collapsed,
                'typeGroup',
                element.dbName,
                element.objType
            );
        } else if (element.contextValue === 'column') {
            // Columns don't have a defined parent in this structure
            return undefined;
        }
        return undefined;
    }

    async getChildren(element?: SchemaItem): Promise<SchemaItem[]> {
        const connectionString = await this.connectionManager.getConnectionString();
        if (!connectionString) {
            return [];
        }

        if (!element) {
            // Root: Databases
            try {
                const results = await runQuery(this.context, "SELECT DATABASE FROM system.._v_database ORDER BY DATABASE", true);
                if (!results) {
                    console.log("No results from database query");
                    return [];
                }
                const databases = JSON.parse(results);
                return databases.map((db: any) => new SchemaItem(db.DATABASE, vscode.TreeItemCollapsibleState.Collapsed, 'database', db.DATABASE));
            } catch (e) {
                vscode.window.showErrorMessage("Failed to load databases: " + e);
                console.error(e);
                return [];
            }
        } else if (element.contextValue === 'database') {
            // Children: Object Types (Groups)
            try {
                const query = `SELECT DISTINCT OBJTYPE FROM ${element.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${element.dbName}' ORDER BY OBJTYPE`;
                const results = await runQuery(this.context, query, true);
                const types = JSON.parse(results || '[]');
                return types.map((t: any) => new SchemaItem(t.OBJTYPE, vscode.TreeItemCollapsibleState.Collapsed, 'typeGroup', element.dbName, t.OBJTYPE));
            } catch (e) {
                vscode.window.showErrorMessage("Failed to load object types: " + e);
                return [];
            }
        } else if (element.contextValue === 'typeGroup') {
            // Children: Objects of specific type
            try {
                const query = `SELECT OBJNAME, SCHEMA, OBJID, COALESCE(DESCRIPTION, '') AS DESCRIPTION FROM ${element.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${element.dbName}' AND OBJTYPE = '${element.objType}' ORDER BY OBJNAME`;
                const results = await runQuery(this.context, query, true);
                const objects = JSON.parse(results || '[]');
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
                        obj.DESCRIPTION
                    );
                });
            } catch (e) {
                vscode.window.showErrorMessage("Failed to load objects: " + e);
                return [];
            }
        } else if (element.contextValue.startsWith('netezza:') && element.objId) {
            // Children: Columns
            try {
                const query = `SELECT 
                        X.ATTNAME
                        , X.FORMAT_TYPE
                        , X.ATTNOTNULL::BOOL AS ATTNOTNULL
                        , X.COLDEFAULT
                        , COALESCE(X.DESCRIPTION, '') AS DESCRIPTION
                    FROM
                        ${element.dbName}.._V_RELATION_COLUMN X
                    WHERE
                        X.OBJID = ${element.objId}
                    ORDER BY 
                        X.ATTNUM`;
                const results = await runQuery(this.context, query, true);
                const columns = JSON.parse(results || '[]');
                return columns.map((col: any) => new SchemaItem(
                    `${col.ATTNAME} (${col.FORMAT_TYPE})`,
                    vscode.TreeItemCollapsibleState.None,
                    'column',
                    element.dbName,
                    undefined,
                    undefined,
                    undefined,
                    col.DESCRIPTION
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
        public readonly objectDescription?: string
    ) {
        super(label, collapsibleState);
        
        // Build tooltip with description if available
        let tooltipText = this.label;
        if (objectDescription && objectDescription.trim()) {
            tooltipText += `\n\n${objectDescription.trim()}`;
        }
        if (schema && contextValue.startsWith('netezza:')) {
            tooltipText += `\n\nSchema: ${schema}`;
        }
        this.tooltip = tooltipText;
        
        this.description = schema ? `(${schema})` : '';

        if (contextValue === 'database') {
            this.iconPath = new vscode.ThemeIcon('database');
        } else if (contextValue === 'typeGroup') {
            this.iconPath = new vscode.ThemeIcon('folder');
        } else if (contextValue.startsWith('netezza:')) {
            this.iconPath = this.getIconForType(objType);
        } else if (contextValue === 'column') {
            this.iconPath = new vscode.ThemeIcon('symbol-field');
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
