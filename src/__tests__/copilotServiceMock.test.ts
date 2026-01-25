
import * as vscode from 'vscode';
import { CopilotService } from '../services/copilotService';
import { ConnectionManager } from '../core/connectionManager';
import { MetadataCache } from '../metadataCache';
import { MockNzConnection } from '../__mocks__/mockNzConnection';
import { MockDataFactory } from '../__mocks__/mockDataFactories';

// Mock types
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type MockType = any;

// Mock vscode
jest.mock('vscode', () => ({
    Uri: { parse: jest.fn() },
    window: {
        activeTextEditor: undefined,
        createStatusBarItem: jest.fn().mockReturnValue({
            show: jest.fn(),
            hide: jest.fn(),
            text: '',
            tooltip: '',
            command: ''
        }),
        showWarningMessage: jest.fn(),
        showInformationMessage: jest.fn(),
        showErrorMessage: jest.fn(),
        showQuickPick: jest.fn()
    },
    commands: {
        executeCommand: jest.fn()
    },
    workspace: {
        getConfiguration: jest.fn().mockReturnValue({
            get: jest.fn()
        })
    },
    Range: jest.fn(),
    StatusBarAlignment: { Right: 1 },
    lm: {
        selectChatModels: jest.fn().mockResolvedValue([])
    }
}), { virtual: true });

// Mock createNzConnection
jest.mock('../core/nzConnectionFactory', () => ({
    createNzConnection: jest.fn()
}));

// Mock ddl generators to avoid implementation details
jest.mock('../ddl', () => ({
    generateTableDDL: jest.fn().mockResolvedValue('CREATE TABLE MOCKED_TABLE (COL1 INT);')
}));

// Mock ddl helpers creating connections
jest.mock('../ddl/helpers', () => ({
    createConnectionFromDetails: jest.fn(),
    executeQueryHelper: jest.requireActual('../ddl/helpers').executeQueryHelper // Keep actual helper logic using mock connection
}));

import { createNzConnection } from '../core/nzConnectionFactory';
import { generateTableDDL } from '../ddl';
import { createConnectionFromDetails } from '../ddl/helpers';

describe('CopilotService with Mock DB', () => {
    let service: CopilotService;
    let mockContext: MockType;
    let mockCache: MockType;
    let mockConnManager: MockType;
    let mockDbConnection: MockNzConnection;

    beforeEach(() => {
        // Setup mocks
        mockContext = {
            extensionUri: {},
            globalState: {
                get: jest.fn(),
                update: jest.fn()
            },
            workspaceState: {
                get: jest.fn(),
                update: jest.fn()
            }
        };

        mockCache = {};

        mockConnManager = {
            getActiveConnectionName: jest.fn().mockReturnValue('test-connection'),
            getConnectionForExecution: jest.fn().mockReturnValue('test-connection'),
            getDocumentConnection: jest.fn().mockReturnValue('test-connection'),
            getConnection: jest.fn().mockResolvedValue({
                host: 'host',
                database: 'TEST_DB',
                user: 'user',
                password: 'password'
            }),
            getCurrentDatabase: jest.fn().mockResolvedValue('TEST_DB')
        };

        // Setup mock DB connection
        mockDbConnection = new MockNzConnection();
        (createNzConnection as jest.Mock).mockReturnValue(mockDbConnection);
        (createConnectionFromDetails as jest.Mock).mockResolvedValue(mockDbConnection);

        service = new CopilotService(
            mockConnManager as ConnectionManager,
            mockContext as vscode.ExtensionContext,
            mockCache as MetadataCache
        );
    });

    describe('extractTableReferences', () => {
        // We access private method via cast to any for testing logic specifically
        it('should extract simple table names', () => {
            const sql = 'SELECT * FROM CUSTOMERS JOIN ORDERS ON 1=1';
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const refs = (service as any).extractTableReferences(sql);

            expect(refs).toEqual(expect.arrayContaining([
                expect.objectContaining({ name: 'CUSTOMERS' }),
                expect.objectContaining({ name: 'ORDERS' })
            ]));
        });

        it('should extract fully qualified names via DB..TABLE syntax', () => {
            const sql = 'SELECT * FROM DB1..CUSTOMERS';
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const refs = (service as any).extractTableReferences(sql);

            expect(refs).toEqual(expect.arrayContaining([
                expect.objectContaining({ database: 'DB1', name: 'CUSTOMERS' })
            ]));
        });

        it('should ignore references inside comments', () => {
            const sql = `
                SELECT * FROM VALID_TABLE
                -- SELECT * FROM COMMENT_TABLE
                /* 
                   JOIN OTHERS ON ...
                */
            `;
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const refs = (service as any).extractTableReferences(sql);

            expect(refs).toContainEqual(expect.objectContaining({ name: 'VALID_TABLE' }));
            expect(refs).not.toContainEqual(expect.objectContaining({ name: 'COMMENT_TABLE' }));
            expect(refs).not.toContainEqual(expect.objectContaining({ name: 'OTHERS' }));
        });
    });

    describe('gatherContext', () => {
        it('should gather DDL context for selected SQL', async () => {
            // Mock active editor
            const mockEditor = {
                document: {
                    getText: jest.fn().mockReturnValue('SELECT * FROM MY_TABLE'),
                    uri: { toString: () => 'file:///test.sql' }
                },
                selection: { isEmpty: true }
            };
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (vscode.window as any).activeTextEditor = mockEditor;

            // Mock finding schema - return PUBLIC
            // executeQueryHelper uses createNzConnection internally
            // But CopilotService uses createConnectionFromDetails imported from ddl/helpers

            // NOTE: ddl/helpers is tricky to mock if not exported. 
            // CopilotService calls `findTableSchema` which calls `executeQueryHelper`.
            // We need to ensure `findTableSchema` works.

            // Mock findTableSchema response in mockDb.
            // findTableSchema calls NZ_QUERIES.findTableSchema then executeQueryHelper
            // We can just match part of the query
            mockDbConnection.setMockData('FROM TEST_DB.._V_OBJECT_DATA', [
                MockDataFactory.createObjectDataRow('MY_TABLE', 'PUBLIC', 'TEST_DB', 'TABLE')
            ]);

            const context = await service.gatherContext();

            expect(context.selectedSql).toBe('SELECT * FROM MY_TABLE');
            expect(generateTableDDL).toHaveBeenCalled();
            expect(context.ddlContext).toContain('CREATE TABLE MOCKED_TABLE');
        });
    });
});
