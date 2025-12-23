/**
 * Unit tests for metadata/cacheStorage.ts
 */

import { CacheStorage } from '../metadata/cacheStorage';

describe('metadata/cacheStorage', () => {
    let storage: CacheStorage;

    beforeEach(() => {
        storage = new CacheStorage();
    });

    describe('Database Cache', () => {
        it('should return undefined when no databases cached', () => {
            expect(storage.getDatabases('conn1')).toBeUndefined();
        });

        it('should store and retrieve databases', () => {
            const dbs = [{ label: 'TESTDB' }, { label: 'PRODDB' }];
            storage.setDatabases('conn1', dbs);
            expect(storage.getDatabases('conn1')).toEqual(dbs);
        });

        it('should isolate databases by connection', () => {
            storage.setDatabases('conn1', [{ label: 'DB1' }]);
            storage.setDatabases('conn2', [{ label: 'DB2' }]);

            expect(storage.getDatabases('conn1')).toEqual([{ label: 'DB1' }]);
            expect(storage.getDatabases('conn2')).toEqual([{ label: 'DB2' }]);
        });
    });

    describe('Schema Cache', () => {
        it('should return undefined when no schemas cached', () => {
            expect(storage.getSchemas('conn1', 'MYDB')).toBeUndefined();
        });

        it('should store and retrieve schemas', () => {
            const schemas = [{ label: 'ADMIN' }, { label: 'PUBLIC' }];
            storage.setSchemas('conn1', 'MYDB', schemas);
            expect(storage.getSchemas('conn1', 'MYDB')).toEqual(schemas);
        });

        it('should isolate schemas by connection and database', () => {
            storage.setSchemas('conn1', 'DB1', [{ label: 'SCHEMA1' }]);
            storage.setSchemas('conn1', 'DB2', [{ label: 'SCHEMA2' }]);
            storage.setSchemas('conn2', 'DB1', [{ label: 'SCHEMA3' }]);

            expect(storage.getSchemas('conn1', 'DB1')).toEqual([{ label: 'SCHEMA1' }]);
            expect(storage.getSchemas('conn1', 'DB2')).toEqual([{ label: 'SCHEMA2' }]);
            expect(storage.getSchemas('conn2', 'DB1')).toEqual([{ label: 'SCHEMA3' }]);
        });
    });

    describe('Table Cache', () => {
        it('should return undefined when no tables cached', () => {
            expect(storage.getTables('conn1', 'MYDB.MYSCHEMA')).toBeUndefined();
        });

        it('should store and retrieve tables', () => {
            const tables = [{ label: 'TABLE1' }, { label: 'TABLE2' }];
            const idMap = new Map<string, number>();
            idMap.set('MYDB.MYSCHEMA.TABLE1', 1001);
            idMap.set('MYDB.MYSCHEMA.TABLE2', 1002);

            storage.setTables('conn1', 'MYDB.MYSCHEMA', tables, idMap);
            expect(storage.getTables('conn1', 'MYDB.MYSCHEMA')).toEqual(tables);
        });

        it('should handle double-dot pattern for tables', () => {
            const tables = [{ label: 'TABLE1' }];
            const idMap = new Map<string, number>();
            storage.setTables('conn1', 'MYDB..', tables, idMap);
            expect(storage.getTables('conn1', 'MYDB..')).toEqual(tables);
        });
    });

    describe('getTablesAllSchemas', () => {
        it('should return undefined when no tables in any schema', () => {
            expect(storage.getTablesAllSchemas('conn1', 'MYDB')).toBeUndefined();
        });

        it('should aggregate tables from all schemas', () => {
            const tables1 = [{ label: 'TABLE1' }];
            const tables2 = [{ label: 'TABLE2' }];
            const emptyIdMap = new Map<string, number>();

            storage.setTables('conn1', 'MYDB.SCHEMA1', tables1, emptyIdMap);
            storage.setTables('conn1', 'MYDB.SCHEMA2', tables2, emptyIdMap);

            const result = storage.getTablesAllSchemas('conn1', 'MYDB');
            expect(result).toHaveLength(2);
            expect(result!.map((t: any) => t.label)).toContain('TABLE1');
            expect(result!.map((t: any) => t.label)).toContain('TABLE2');
        });

        it('should deduplicate tables by name (case-insensitive)', () => {
            const tables1 = [{ label: 'SHARED_TABLE' }];
            const tables2 = [{ label: 'SHARED_TABLE' }]; // Same name in different schema
            const emptyIdMap = new Map<string, number>();

            storage.setTables('conn1', 'MYDB.SCHEMA1', tables1, emptyIdMap);
            storage.setTables('conn1', 'MYDB.SCHEMA2', tables2, emptyIdMap);

            const result = storage.getTablesAllSchemas('conn1', 'MYDB');
            expect(result).toHaveLength(1);
        });
    });

    describe('Column Cache', () => {
        it('should return undefined when no columns cached', () => {
            expect(storage.getColumns('conn1', 'MYDB.MYSCHEMA.MYTABLE')).toBeUndefined();
        });

        it('should store and retrieve columns', () => {
            const columns = [
                { label: 'ID', detail: 'INTEGER' },
                { label: 'NAME', detail: 'VARCHAR(100)' }
            ];
            storage.setColumns('conn1', 'MYDB.MYSCHEMA.MYTABLE', columns);
            expect(storage.getColumns('conn1', 'MYDB.MYSCHEMA.MYTABLE')).toEqual(columns);
        });
    });

    describe('findTableId', () => {
        it('should return undefined when table not found', () => {
            expect(storage.findTableId('conn1', 'MYDB.MYSCHEMA.UNKNOWN')).toBeUndefined();
        });

        it('should find table id from cache', () => {
            const tables = [{ label: 'MYTABLE' }];
            const idMap = new Map<string, number>();
            idMap.set('MYDB.MYSCHEMA.MYTABLE', 12345);

            storage.setTables('conn1', 'MYDB.MYSCHEMA', tables, idMap);
            expect(storage.findTableId('conn1', 'MYDB.MYSCHEMA.MYTABLE')).toBe(12345);
        });

        it('should search across multiple schemas', () => {
            const emptyIdMap1 = new Map<string, number>();
            const idMap2 = new Map<string, number>();
            idMap2.set('MYDB.SCHEMA2.TARGET', 99999);

            storage.setTables('conn1', 'MYDB.SCHEMA1', [], emptyIdMap1);
            storage.setTables('conn1', 'MYDB.SCHEMA2', [{ label: 'TARGET' }], idMap2);

            expect(storage.findTableId('conn1', 'MYDB.SCHEMA2.TARGET')).toBe(99999);
        });
    });

    describe('TypeGroup Cache', () => {
        it('should return undefined when no type groups cached', () => {
            expect(storage.getTypeGroups('conn1', 'MYDB')).toBeUndefined();
        });

        it('should store and retrieve type groups', () => {
            const types = ['TABLE', 'VIEW', 'EXTERNAL TABLE'];
            storage.setTypeGroups('conn1', 'MYDB', types);
            expect(storage.getTypeGroups('conn1', 'MYDB')).toEqual(types);
        });
    });

    describe('findObjectWithType', () => {
        beforeEach(() => {
            const tables = [
                { label: 'CUSTOMERS', objType: 'TABLE', kind: 6 },
                { label: 'ORDERS_VIEW', objType: 'VIEW', kind: 18 }
            ];
            const idMap = new Map<string, number>();
            idMap.set('MYDB.ADMIN.CUSTOMERS', 1001);
            idMap.set('MYDB.ADMIN.ORDERS_VIEW', 1002);

            storage.setTables('conn1', 'MYDB.ADMIN', tables, idMap);
        });

        it('should find object with type info', () => {
            const result = storage.findObjectWithType('conn1', 'MYDB', 'ADMIN', 'CUSTOMERS');
            expect(result).toBeDefined();
            expect(result!.objId).toBe(1001);
            expect(result!.objType).toBe('TABLE');
            expect(result!.schema).toBe('ADMIN');
            expect(result!.name).toBe('CUSTOMERS');
        });

        it('should find view with VIEW type', () => {
            const result = storage.findObjectWithType('conn1', 'MYDB', 'ADMIN', 'ORDERS_VIEW');
            expect(result).toBeDefined();
            expect(result!.objType).toBe('VIEW');
        });

        it('should be case-insensitive for object name', () => {
            const result = storage.findObjectWithType('conn1', 'MYDB', 'ADMIN', 'customers');
            expect(result).toBeDefined();
            expect(result!.name).toBe('CUSTOMERS');
        });

        it('should return undefined for non-existent object', () => {
            const result = storage.findObjectWithType('conn1', 'MYDB', 'ADMIN', 'UNKNOWN');
            expect(result).toBeUndefined();
        });

        it('should search all schemas when schema is undefined', () => {
            const result = storage.findObjectWithType('conn1', 'MYDB', undefined, 'CUSTOMERS');
            expect(result).toBeDefined();
            expect(result!.schema).toBe('ADMIN');
        });
    });

    describe('invalidateSchema', () => {
        it('should remove table cache for specific schema', () => {
            const tables = [{ label: 'TABLE1' }];
            const idMap = new Map<string, number>();
            storage.setTables('conn1', 'MYDB.ADMIN', tables, idMap);

            expect(storage.getTables('conn1', 'MYDB.ADMIN')).toBeDefined();

            storage.invalidateSchema('conn1', 'MYDB', 'ADMIN');

            expect(storage.getTables('conn1', 'MYDB.ADMIN')).toBeUndefined();
        });

        it('should not affect other schemas', () => {
            const tables1 = [{ label: 'TABLE1' }];
            const tables2 = [{ label: 'TABLE2' }];
            const idMap = new Map<string, number>();

            storage.setTables('conn1', 'MYDB.ADMIN', tables1, idMap);
            storage.setTables('conn1', 'MYDB.PUBLIC', tables2, idMap);

            storage.invalidateSchema('conn1', 'MYDB', 'ADMIN');

            expect(storage.getTables('conn1', 'MYDB.ADMIN')).toBeUndefined();
            expect(storage.getTables('conn1', 'MYDB.PUBLIC')).toBeDefined();
        });
    });

    describe('clearAll', () => {
        it('should clear all caches', () => {
            storage.setDatabases('conn1', [{ label: 'DB1' }]);
            storage.setSchemas('conn1', 'DB1', [{ label: 'SCHEMA1' }]);
            storage.setTables('conn1', 'DB1.SCHEMA1', [{ label: 'TABLE1' }], new Map());
            storage.setColumns('conn1', 'DB1.SCHEMA1.TABLE1', [{ label: 'COL1' }]);
            storage.setTypeGroups('conn1', 'DB1', ['TABLE']);

            storage.clearAll();

            expect(storage.getDatabases('conn1')).toBeUndefined();
            expect(storage.getSchemas('conn1', 'DB1')).toBeUndefined();
            expect(storage.getTables('conn1', 'DB1.SCHEMA1')).toBeUndefined();
            expect(storage.getColumns('conn1', 'DB1.SCHEMA1.TABLE1')).toBeUndefined();
            expect(storage.getTypeGroups('conn1', 'DB1')).toBeUndefined();
        });
    });

    describe('onDataChange callback', () => {
        it('should call callback when databases change', () => {
            const callback = jest.fn();
            storage.setOnDataChange(callback);

            storage.setDatabases('conn1', []);
            expect(callback).toHaveBeenCalledWith('db');
        });

        it('should call callback when schemas change', () => {
            const callback = jest.fn();
            storage.setOnDataChange(callback);

            storage.setSchemas('conn1', 'DB1', []);
            expect(callback).toHaveBeenCalledWith('schema');
        });

        it('should call callback when tables change', () => {
            const callback = jest.fn();
            storage.setOnDataChange(callback);

            storage.setTables('conn1', 'DB1.SCHEMA1', [], new Map());
            expect(callback).toHaveBeenCalledWith('table');
        });

        it('should call callback when columns change', () => {
            const callback = jest.fn();
            storage.setOnDataChange(callback);

            storage.setColumns('conn1', 'DB1.SCHEMA1.TABLE1', []);
            expect(callback).toHaveBeenCalledWith('column');
        });
    });
});
