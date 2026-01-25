/**
 * Unit tests for metadata/systemQueries.ts
 * Tests for NZ_QUERIES and NZ_SYSTEM_VIEWS constants
 */

import { NZ_QUERIES, NZ_SYSTEM_VIEWS, NZ_OBJECT_TYPES, NZ_CONSTRAINT_TYPES, qualifySystemView } from '../metadata/systemQueries';

describe('metadata/systemQueries', () => {
    describe('NZ_SYSTEM_VIEWS constants', () => {
        it('should define all required system views', () => {
            expect(NZ_SYSTEM_VIEWS.DATABASE).toBe('_V_DATABASE');
            expect(NZ_SYSTEM_VIEWS.SCHEMA).toBe('_V_SCHEMA');
            expect(NZ_SYSTEM_VIEWS.TABLE).toBe('_V_TABLE');
            expect(NZ_SYSTEM_VIEWS.VIEW).toBe('_V_VIEW');
            expect(NZ_SYSTEM_VIEWS.PROCEDURE).toBe('_V_PROCEDURE');
            expect(NZ_SYSTEM_VIEWS.OBJECT_DATA).toBe('_V_OBJECT_DATA');
            expect(NZ_SYSTEM_VIEWS.RELATION_COLUMN).toBe('_V_RELATION_COLUMN');
            expect(NZ_SYSTEM_VIEWS.EXTERNAL).toBe('_V_EXTERNAL');
            expect(NZ_SYSTEM_VIEWS.EXTOBJECT).toBe('_V_EXTOBJECT');
        });

        it('should define key constraint and distribution views', () => {
            expect(NZ_SYSTEM_VIEWS.RELATION_KEYDATA).toBe('_V_RELATION_KEYDATA');
            expect(NZ_SYSTEM_VIEWS.TABLE_DIST_MAP).toBe('_V_TABLE_DIST_MAP');
            expect(NZ_SYSTEM_VIEWS.TABLE_ORGANIZE_COLUMN).toBe('_V_TABLE_ORGANIZE_COLUMN');
        });
    });

    describe('NZ_OBJECT_TYPES constants', () => {
        it('should define common object types', () => {
            expect(NZ_OBJECT_TYPES.TABLE).toBe('TABLE');
            expect(NZ_OBJECT_TYPES.VIEW).toBe('VIEW');
            expect(NZ_OBJECT_TYPES.PROCEDURE).toBe('PROCEDURE');
            expect(NZ_OBJECT_TYPES.EXTERNAL_TABLE).toBe('EXTERNAL TABLE');
            expect(NZ_OBJECT_TYPES.SEQUENCE).toBe('SEQUENCE');
        });
    });

    describe('NZ_CONSTRAINT_TYPES constants', () => {
        it('should define constraint type codes', () => {
            expect(NZ_CONSTRAINT_TYPES.PRIMARY_KEY).toBe('p');
            expect(NZ_CONSTRAINT_TYPES.FOREIGN_KEY).toBe('f');
            expect(NZ_CONSTRAINT_TYPES.UNIQUE).toBe('u');
        });
    });

    describe('qualifySystemView helper', () => {
        it('should qualify view with database name using two-dot syntax', () => {
            const result = qualifySystemView('MYDB', NZ_SYSTEM_VIEWS.TABLE);
            expect(result).toBe('MYDB.._V_TABLE');
        });

        it('should uppercase database name', () => {
            const result = qualifySystemView('mydb', NZ_SYSTEM_VIEWS.VIEW);
            expect(result).toBe('MYDB.._V_VIEW');
        });

        it('should work with all system views', () => {
            expect(qualifySystemView('DB1', NZ_SYSTEM_VIEWS.OBJECT_DATA)).toBe('DB1.._V_OBJECT_DATA');
            expect(qualifySystemView('DB2', NZ_SYSTEM_VIEWS.RELATION_COLUMN)).toBe('DB2.._V_RELATION_COLUMN');
            expect(qualifySystemView('DB3', NZ_SYSTEM_VIEWS.PROCEDURE)).toBe('DB3.._V_PROCEDURE');
        });
    });

    describe('NZ_QUERIES.LIST_DATABASES', () => {
        it('should be a valid SQL query', () => {
            expect(NZ_QUERIES.LIST_DATABASES).toContain('SELECT');
            expect(NZ_QUERIES.LIST_DATABASES).toContain('DATABASE');
            expect(NZ_QUERIES.LIST_DATABASES).toContain(NZ_SYSTEM_VIEWS.DATABASE);
        });
    });

    describe('NZ_QUERIES.listSchemas', () => {
        it('should generate query with database prefix', () => {
            const query = NZ_QUERIES.listSchemas('TESTDB');
            expect(query).toContain('TESTDB.._V_SCHEMA');
            expect(query).toContain('SELECT');
            expect(query).toContain('SCHEMA');
        });

        it('should uppercase database name', () => {
            const query = NZ_QUERIES.listSchemas('testdb');
            expect(query).toContain('TESTDB.._V_SCHEMA');
        });
    });

    describe('NZ_QUERIES.getTableColumns', () => {
        it('should generate query with proper filters', () => {
            const query = NZ_QUERIES.getTableColumns('MYDB', 'ADMIN', 'CUSTOMERS');
            expect(query).toContain('MYDB.._V_RELATION_COLUMN');
            expect(query).toContain("D.SCHEMA = 'ADMIN'");
            expect(query).toContain("D.OBJNAME = 'CUSTOMERS'");
        });

        it('should uppercase all identifiers', () => {
            const query = NZ_QUERIES.getTableColumns('mydb', 'admin', 'customers');
            expect(query).toContain('MYDB.._V_RELATION_COLUMN');
            expect(query).toContain("D.SCHEMA = 'ADMIN'");
            expect(query).toContain("D.OBJNAME = 'CUSTOMERS'");
        });
    });

    describe('NZ_QUERIES.getDistributionKeys', () => {
        it('should generate query for distribution columns', () => {
            const query = NZ_QUERIES.getDistributionKeys('MYDB', 'ADMIN', 'ORDERS');
            expect(query).toContain('MYDB.._V_TABLE_DIST_MAP');
            expect(query).toContain("SCHEMA = 'ADMIN'");
            expect(query).toContain("TABLENAME = 'ORDERS'");
            expect(query).toContain('DISTSEQNO');
        });
    });

    describe('NZ_QUERIES.getOrganizeColumns', () => {
        it('should generate query for organize columns', () => {
            const query = NZ_QUERIES.getOrganizeColumns('MYDB', 'ADMIN', 'ORDERS');
            expect(query).toContain('MYDB.._V_TABLE_ORGANIZE_COLUMN');
            expect(query).toContain("SCHEMA = 'ADMIN'");
            expect(query).toContain("TABLENAME = 'ORDERS'");
            expect(query).toContain('ORGSEQNO');
        });
    });

    describe('NZ_QUERIES.getTableKeys', () => {
        it('should generate query for key constraints', () => {
            const query = NZ_QUERIES.getTableKeys('MYDB', 'ADMIN', 'ORDERS');
            expect(query).toContain('MYDB.._V_RELATION_KEYDATA');
            expect(query).toContain("SCHEMA = 'ADMIN'");
            expect(query).toContain("RELATION = 'ORDERS'");
            expect(query).toContain('CONSTRAINTNAME');
            expect(query).toContain('CONTYPE');
        });
    });

    describe('NZ_QUERIES.getObjectComment', () => {
        it('should generate query with DBNAME filter', () => {
            const query = NZ_QUERIES.getObjectComment('MYDB', 'ADMIN', 'ORDERS');
            expect(query).toContain('MYDB.._V_OBJECT_DATA');
            expect(query).toContain("DBNAME = 'MYDB'");
            expect(query).toContain("SCHEMA = 'ADMIN'");
            expect(query).toContain("OBJNAME = 'ORDERS'");
            expect(query).toContain('DESCRIPTION');
        });

        it('should include object type filter when provided', () => {
            const query = NZ_QUERIES.getObjectComment('MYDB', 'ADMIN', 'ORDERS', 'TABLE');
            expect(query).toContain("OBJTYPE = 'TABLE'");
        });
    });

    describe('NZ_QUERIES.getViewDefinition', () => {
        it('should generate query for view definition', () => {
            const query = NZ_QUERIES.getViewDefinition('MYDB', 'MY_VIEW');
            expect(query).toContain('MYDB.._V_VIEW');
            expect(query).toContain("UPPER(VIEWNAME) = 'MY_VIEW'");
            expect(query).toContain('DEFINITION');
        });

        it('should include schema filter when provided', () => {
            const query = NZ_QUERIES.getViewDefinition('MYDB', 'MY_VIEW', 'ADMIN');
            expect(query).toContain("UPPER(SCHEMA) = 'ADMIN'");
        });
    });

    describe('NZ_QUERIES.getProcedureDefinition', () => {
        it('should generate query for procedure definition', () => {
            const query = NZ_QUERIES.getProcedureDefinition('MYDB', 'MY_PROC');
            expect(query).toContain('MYDB.._V_PROCEDURE');
            expect(query).toContain("UPPER(PROCEDURE) = 'MY_PROC'");
            expect(query).toContain('PROCEDURESOURCE');
        });
    });

    describe('NZ_QUERIES.findTableSchema', () => {
        it('should generate query to find schema for a table', () => {
            const query = NZ_QUERIES.findTableSchema('MYDB', 'ORDERS');
            expect(query).toContain('MYDB.._V_OBJECT_DATA');
            expect(query).toContain("DBNAME = 'MYDB'");
            expect(query).toContain("UPPER(OBJNAME) = 'ORDERS'");
            expect(query).toContain('LIMIT 1');
        });
    });

    describe('NZ_QUERIES.searchTables', () => {
        it('should generate search query with pattern', () => {
            const query = NZ_QUERIES.searchTables('%ORDER%', 'MYDB');
            expect(query).toContain('MYDB.._V_TABLE');
            expect(query).toContain("TABLENAME) LIKE '%ORDER%'");
            expect(query).toContain('LIMIT 1000');
        });

        it('should generate global search query when no database specified', () => {
            const query = NZ_QUERIES.searchTables('%ORDER%');
            expect(query).toContain('_V_OBJECT_DATA');
            expect(query).toContain("OBJNAME) LIKE '%ORDER%'");
            expect(query).toContain('DBNAME AS DATABASE');
        });
    });

    describe('NZ_QUERIES.searchColumns', () => {
        it('should generate column search query', () => {
            const query = NZ_QUERIES.searchColumns('MYDB', '%ID%');
            expect(query).toContain('MYDB.._V_TABLE');
            expect(query).toContain('MYDB.._V_RELATION_COLUMN');
            expect(query).toContain("ATTNAME) LIKE '%ID%'");
            expect(query).toContain('LIMIT 1000');
        });
    });

    describe('NZ_QUERIES.listColumnsWithKeys', () => {
        it('should generate query with PK/FK info', () => {
            const query = NZ_QUERIES.listColumnsWithKeys('MYDB');
            expect(query).toContain('MYDB.._V_RELATION_COLUMN');
            expect(query).toContain('MYDB.._V_OBJECT_DATA');
            expect(query).toContain('MYDB.._V_RELATION_KEYDATA');
            expect(query).toContain('IS_PK');
            expect(query).toContain('IS_FK');
        });

        it('should filter by schema when provided', () => {
            const query = NZ_QUERIES.listColumnsWithKeys('MYDB', { schema: 'ADMIN' });
            expect(query).toContain("SCHEMA) = UPPER('ADMIN')");
        });

        it('should filter by table name when provided', () => {
            const query = NZ_QUERIES.listColumnsWithKeys('MYDB', { tableName: 'ORDERS' });
            expect(query).toContain("OBJNAME) = UPPER('ORDERS')");
        });
    });

    describe('NZ_QUERIES.listObjectsOfType', () => {
        it('should generate query for tables', () => {
            const query = NZ_QUERIES.listObjectsOfType('MYDB', 'TABLE');
            expect(query).toContain('MYDB.._V_OBJECT_DATA');
            expect(query).toContain("DBNAME = 'MYDB'");
            expect(query).toContain("OBJTYPE = 'TABLE'");
        });

        it('should handle procedures specially', () => {
            const query = NZ_QUERIES.listObjectsOfType('MYDB', 'PROCEDURE');
            expect(query).toContain('MYDB.._V_PROCEDURE');
            expect(query).toContain('PROCEDURESIGNATURE AS OBJNAME');
        });

        it('should filter by schema when provided', () => {
            const query = NZ_QUERIES.listObjectsOfType('MYDB', 'VIEW', 'ADMIN');
            expect(query).toContain("SCHEMA = 'ADMIN'");
        });
    });

    describe('NZ_QUERIES.getExternalTables', () => {
        it('should join external tables with data objects', () => {
            const query = NZ_QUERIES.getExternalTables('MYDB');
            expect(query).toContain('MYDB.._V_EXTERNAL');
            expect(query).toContain('MYDB.._V_EXTOBJECT');
            expect(query).toContain('EXTOBJNAME AS DATAOBJECT');
        });

        it('should filter by schema when provided', () => {
            const query = NZ_QUERIES.getExternalTables('MYDB', 'ADMIN');
            expect(query).toContain("E1.SCHEMA = 'ADMIN'");
        });
    });

    describe('NZ_QUERIES.getForeignKeyRelationships', () => {
        it('should generate FK query for ERD', () => {
            const query = NZ_QUERIES.getForeignKeyRelationships('MYDB', 'ADMIN');
            expect(query).toContain('MYDB.._V_RELATION_KEYDATA');
            expect(query).toContain("CONTYPE = 'f'");
            expect(query).toContain("SCHEMA = 'ADMIN'");
            expect(query).toContain('PKRELATION');
            expect(query).toContain('PKATTNAME');
        });
    });

    describe('NZ_QUERIES.findDependentViews', () => {
        it('should search in view definitions', () => {
            const query = NZ_QUERIES.findDependentViews('MYDB', 'CUSTOMERS');
            expect(query).toContain('MYDB.._V_VIEW');
            expect(query).toContain("DEFINITION) LIKE '%CUSTOMERS%'");
            expect(query).toContain("VIEWNAME != 'CUSTOMERS'");
        });
    });

    describe('NZ_QUERIES.findDependentProcedures', () => {
        it('should search in procedure sources', () => {
            const query = NZ_QUERIES.findDependentProcedures('MYDB', 'CUSTOMERS');
            expect(query).toContain('MYDB.._V_PROCEDURE');
            expect(query).toContain("PROCEDURESOURCE) LIKE '%CUSTOMERS%'");
        });
    });
});
