/**
 * Mock for VS Code module - used in unit tests
 * This provides minimal mock implementations needed for testing
 */

export const window = {
    showInformationMessage: jest.fn(),
    showWarningMessage: jest.fn(),
    showErrorMessage: jest.fn(),
    createOutputChannel: jest.fn(() => ({
        appendLine: jest.fn(),
        show: jest.fn()
    }))
};

export const workspace = {
    getConfiguration: jest.fn(() => ({
        get: jest.fn()
    }))
};

export const Uri = {
    file: (path: string) => ({ fsPath: path })
};

export const commands = {
    registerCommand: jest.fn(),
    executeCommand: jest.fn()
};

export const ExtensionContext = {};

export enum TreeItemCollapsibleState {
    None = 0,
    Collapsed = 1,
    Expanded = 2
}

export enum DiagnosticSeverity {
    Error = 0,
    Warning = 1,
    Information = 2,
    Hint = 3
}

export class ThemeColor {
    constructor(public id: string) { }
}
