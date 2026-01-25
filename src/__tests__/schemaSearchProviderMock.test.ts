
import { SchemaSearchProvider } from '../providers/schemaSearchProvider';
import { MockNzConnection } from '../__mocks__/mockNzConnection';
import { MockDataFactory } from '../__mocks__/mockDataFactories';
import { NZ_QUERIES } from '../metadata/systemQueries';
import * as vscode from 'vscode';
import { ConnectionManager } from '../core/connectionManager';
import { MetadataCache } from '../metadataCache';

// Mock types helper
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type MockType = any;

// Mock vscode
jest.mock('vscode', () => ({
    Uri: { parse: jest.fn() },
    EventEmitter: jest.fn().mockImplementation(() => ({
        event: jest.fn(),
        fire: jest.fn()
    })),
    window: {
        activeTextEditor: undefined,
        createStatusBarItem: jest.fn().mockReturnValue({
            show: jest.fn(),
            hide: jest.fn()
        })
    },
    commands: {
        executeCommand: jest.fn()
    },
    workspace: {
        getConfiguration: jest.fn().mockReturnValue({
            get: jest.fn()
        })
    }
}), { virtual: true });

// Mock createNzConnection
jest.mock('../core/nzConnectionFactory', () => ({
    createNzConnection: jest.fn()
}));

import { createNzConnection } from '../core/nzConnectionFactory';

describe('SchemaSearchProvider with Mock DB', () => {
    let provider: SchemaSearchProvider;
    let mockContext: MockType;
    let mockCache: MockType;
    let mockConnManager: MockType;
    let mockDbConnection: MockNzConnection;
    let mockWebview: MockType;

    beforeEach(() => {
        // Setup mocks
        mockContext = {
            extensionUri: {},
            // Mock globalState for QueryHistoryManager
            globalState: {
                get: jest.fn().mockReturnValue(undefined),
                update: jest.fn().mockResolvedValue(undefined)
            },
            globalStorageUri: { fsPath: '/tmp/test-storage' }
        };
        mockCache = {
            search: jest.fn().mockReturnValue([]),
            hasAllObjectsPrefetchTriggered: jest.fn().mockReturnValue(true),
            prefetchAllObjects: jest.fn()
        };
        mockConnManager = {
            getActiveConnectionName: jest.fn().mockReturnValue('test-connection'),
            getConnectionForExecution: jest.fn(),
            getConnection: jest.fn().mockResolvedValue({
                host: 'host',
                database: 'TEST_DB',
                user: 'user',
                password: 'password'
            })
        };

        // Setup mock DB connection
        mockDbConnection = new MockNzConnection();
        (createNzConnection as jest.Mock).mockReturnValue(mockDbConnection);

        // Setup mock webview structure
        mockWebview = {
            webview: {
                options: {},
                html: '',
                onDidReceiveMessage: jest.fn(),
                postMessage: jest.fn()
            }
        };

        provider = new SchemaSearchProvider(
            {} as vscode.Uri,
            mockContext as vscode.ExtensionContext,
            mockCache as MetadataCache,
            mockConnManager as ConnectionManager
        );

        // Initialize view - this connects the onDidReceiveMessage handler
        provider.resolveWebviewView(mockWebview, {} as vscode.WebviewViewResolveContext, {} as vscode.CancellationToken);
    });

    it('should search across multiple databases using UNION ALL', async () => {
        // 1. Mock list of databases
        mockDbConnection.setMockData(NZ_QUERIES.LIST_DATABASES, [
            MockDataFactory.createDatabaseRow('DB1'),
            MockDataFactory.createDatabaseRow('DB2')
        ]);

        // 2. Mock search results (the big UNION ALL query)
        const mockSearchResults = [
            {
                PRIORITY: 1,
                NAME: 'CUSTOMER_TABLE',
                SCHEMA: 'ADMIN',
                DATABASE: 'DB1',
                TYPE: 'TABLE',
                PARENT: '',
                DESCRIPTION: '',
                MATCH_TYPE: 'NAME'
            },
            {
                PRIORITY: 1,
                NAME: 'CUSTOMER_VIEW',
                SCHEMA: 'PUBLIC',
                DATABASE: 'DB2',
                TYPE: 'VIEW',
                PARENT: '',
                DESCRIPTION: '',
                MATCH_TYPE: 'NAME'
            }
        ];

        mockDbConnection.setMockData('UNION ALL', mockSearchResults);

        // Verify handler was attached
        if (mockWebview.webview.onDidReceiveMessage.mock.calls.length === 0) {
            throw new Error('Webview message handler was not attached!');
        }

        // Access the message handler to trigger search
        const messageHandler = mockWebview.webview.onDidReceiveMessage.mock.calls[0][0];

        await messageHandler({ type: 'search', value: 'CUSTOMER' });

        // Verify results were posted to webview
        expect(mockWebview.webview.postMessage).toHaveBeenCalledWith(
            expect.objectContaining({
                type: 'results',
                data: expect.arrayContaining([
                    expect.objectContaining({ NAME: 'CUSTOMER_TABLE', DATABASE: 'DB1' }),
                    expect.objectContaining({ NAME: 'CUSTOMER_VIEW', DATABASE: 'DB2' })
                ])
            })
        );
    });

    it('should search source code in view definitions', async () => {
        // 1. Mock list of databases
        mockDbConnection.setMockData(NZ_QUERIES.LIST_DATABASES, [
            MockDataFactory.createDatabaseRow('DB1')
        ]);

        // 2. Mock view search result
        // 2. Mock view search result
        const viewRow = MockDataFactory.createViewRow('MY_VIEW', 'ADMIN', 'DB1', 'CREATE VIEW MY_VIEW AS SELECT * FROM CUSTOMERS');
        const mockViews = [{ ...viewRow, NAME: viewRow.VIEWNAME }];

        // Match the specific query structure for view definition search
        mockDbConnection.setMockData('FROM DB1.._V_VIEW', mockViews);

        // Verify handler was attached
        if (mockWebview.webview.onDidReceiveMessage.mock.calls.length === 0) {
            throw new Error('Webview message handler was not attached!');
        }

        const messageHandler = mockWebview.webview.onDidReceiveMessage.mock.calls[0][0];

        // Search for 'CUSTOMERS' inside view definition
        await messageHandler({
            type: 'searchSource',
            value: 'CUSTOMERS',
            mode: 'raw'
        });

        // Verify result
        expect(mockWebview.webview.postMessage).toHaveBeenCalledWith(
            expect.objectContaining({
                type: 'results',
                data: expect.arrayContaining([
                    expect.objectContaining({
                        NAME: 'MY_VIEW',
                        MATCH_TYPE: 'SOURCE_CODE',
                        DATABASE: 'DB1'
                    })
                ])
            })
        );
    });
});
