import * as vscode from 'vscode';

/**
 * Webview panel for collecting SQL variable values from the user.
 * Displays a form with input fields for each variable and returns the values when submitted.
 */
export class VariableInputPanel {
    private static currentPanel: VariableInputPanel | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private _disposables: vscode.Disposable[] = [];
    private _resolvePromise?: (value: Record<string, string> | undefined) => void;

    private constructor(
        panel: vscode.WebviewPanel,
        private extensionUri: vscode.Uri,
        private variables: string[],
        private defaults?: Record<string, string>
    ) {
        this._panel = panel;
        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);
        this._panel.webview.html = this._getHtmlForWebview(this._panel.webview);

        // Handle messages from the webview
        this._panel.webview.onDidReceiveMessage(
            message => {
                switch (message.command) {
                    case 'submit':
                        if (this._resolvePromise) {
                            this._resolvePromise(message.values);
                            this._resolvePromise = undefined;
                        }
                        this.dispose();
                        return;
                    case 'cancel':
                        if (this._resolvePromise) {
                            this._resolvePromise(undefined);
                            this._resolvePromise = undefined;
                        }
                        this.dispose();
                        return;
                }
            },
            null,
            this._disposables
        );
    }

    /**
     * Show the variable input panel and wait for user input.
     * @param variables - Array of variable names to collect values for
     * @param defaults - Optional default values for variables
     * @param extensionUri - Extension URI for loading resources
     * @returns Promise that resolves to variable values or undefined if cancelled
     */
    public static async show(
        variables: string[],
        defaults?: Record<string, string>,
        extensionUri?: vscode.Uri
    ): Promise<Record<string, string> | undefined> {
        if (!extensionUri) {
            throw new Error('extensionUri is required for VariableInputPanel');
        }

        // If panel already exists, dispose it first
        if (VariableInputPanel.currentPanel) {
            VariableInputPanel.currentPanel.dispose();
        }

        const panel = vscode.window.createWebviewPanel(
            'netezzaVariableInput',
            'SQL Variables',
            vscode.ViewColumn.One,
            {
                enableScripts: true,
                retainContextWhenHidden: false
            }
        );

        VariableInputPanel.currentPanel = new VariableInputPanel(
            panel,
            extensionUri,
            variables,
            defaults
        );

        // Return a promise that will be resolved when user submits or cancels
        return new Promise<Record<string, string> | undefined>(resolve => {
            VariableInputPanel.currentPanel!._resolvePromise = resolve;
        });
    }

    public dispose() {
        VariableInputPanel.currentPanel = undefined;
        this._panel.dispose();
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) {
                x.dispose();
            }
        }
    }

    private _getHtmlForWebview(webview: vscode.Webview) {
        const iconUri = webview.asWebviewUri(
            vscode.Uri.joinPath(this.extensionUri, 'netezza_icon64.png')
        );

        // Generate form fields for each variable
        const formFields = this.variables
            .map(varName => {
                const defaultValue = this.defaults?.[varName] || '';
                return `
                <div class="form-group">
                    <label for="var_${varName}">${varName}</label>
                    <input 
                        type="text" 
                        id="var_${varName}" 
                        name="${varName}"
                        placeholder="Enter value for ${varName}"
                        value="${this._escapeHtml(defaultValue)}"
                        autocomplete="off"
                    >
                </div>`;
            })
            .join('\n');

        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>SQL Variables</title>
            <style>
                :root {
                    --container-padding: 20px;
                    --input-padding-vertical: 6px;
                    --input-padding-horizontal: 8px;
                }
                body {
                    font-family: var(--vscode-font-family);
                    padding: 0;
                    margin: 0;
                    color: var(--vscode-foreground);
                    background-color: var(--vscode-editor-background);
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    min-height: 100vh;
                }
                
                .container {
                    width: 100%;
                    max-width: 600px;
                    padding: var(--container-padding);
                }

                .form-container {
                    background-color: var(--vscode-editorWidget-background);
                    border: 1px solid var(--vscode-widget-border);
                    padding: 30px;
                    border-radius: 4px;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                }
                
                h2 {
                    margin-top: 0;
                    margin-bottom: 25px;
                    font-size: 1.4em;
                    font-weight: 500;
                    padding-bottom: 10px;
                    border-bottom: 1px solid var(--vscode-panel-border);
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }

                .logo-header {
                    width: 32px;
                    height: 32px;
                    margin-right: 10px;
                }

                .description {
                    margin-bottom: 20px;
                    color: var(--vscode-descriptionForeground);
                    font-size: 13px;
                }

                .form-group {
                    margin-bottom: 18px;
                }

                label {
                    display: block;
                    margin-bottom: 6px;
                    font-weight: 600;
                    font-size: 12px;
                    color: var(--vscode-input-placeholderForeground);
                }

                input {
                    width: 100%;
                    padding: 8px 10px;
                    box-sizing: border-box;
                    background: var(--vscode-input-background);
                    color: var(--vscode-input-foreground);
                    border: 1px solid var(--vscode-input-border);
                    border-radius: 2px;
                    font-family: inherit;
                    font-size: 13px;
                }
                
                input:focus {
                    border-color: var(--vscode-focusBorder);
                    outline: 1px solid var(--vscode-focusBorder);
                }
                
                .actions {
                    margin-top: 30px;
                    display: flex;
                    gap: 12px;
                    padding-top: 20px;
                    border-top: 1px solid var(--vscode-panel-border);
                }
                
                button {
                    background: var(--vscode-button-background);
                    color: var(--vscode-button-foreground);
                    padding: 8px 16px;
                    border: none;
                    cursor: pointer;
                    border-radius: 2px;
                    font-size: 13px;
                    font-weight: 500;
                    flex: 1;
                }
                
                button:hover {
                    background: var(--vscode-button-hoverBackground);
                }
                
                button.secondary {
                    background: var(--vscode-button-secondaryBackground);
                    color: var(--vscode-button-secondaryForeground);
                }
                
                button.secondary:hover {
                    background: var(--vscode-button-secondaryHoverBackground);
                }

                .error {
                    color: var(--vscode-errorForeground);
                    font-size: 12px;
                    margin-top: 5px;
                    display: none;
                }

                .error.visible {
                    display: block;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="form-container">
                    <h2>
                        <img src="${iconUri}" class="logo-header" />
                        SQL Variables
                    </h2>
                    
                    <div class="description">
                        Enter values for the following SQL variables. All fields are required.
                    </div>

                    <form id="variableForm">
                        ${formFields}
                        
                        <div id="errorMessage" class="error">
                            Please fill in all required fields.
                        </div>
                        
                        <div class="actions">
                            <button type="submit" id="btnExecute">Execute</button>
                            <button type="button" class="secondary" id="btnCancel">Cancel</button>
                        </div>
                    </form>
                </div>
            </div>

            <script>
                const vscode = acquireVsCodeApi();
                const form = document.getElementById('variableForm');
                const errorMessage = document.getElementById('errorMessage');
                const btnCancel = document.getElementById('btnCancel');

                form.addEventListener('submit', (e) => {
                    e.preventDefault();
                    
                    const formData = new FormData(form);
                    const values = {};
                    let hasEmptyFields = false;

                    for (const [key, value] of formData.entries()) {
                        const trimmedValue = value.toString().trim();
                        if (trimmedValue === '') {
                            hasEmptyFields = true;
                            break;
                        }
                        values[key] = trimmedValue;
                    }

                    if (hasEmptyFields) {
                        errorMessage.classList.add('visible');
                        return;
                    }

                    errorMessage.classList.remove('visible');
                    vscode.postMessage({
                        command: 'submit',
                        values: values
                    });
                });

                btnCancel.addEventListener('click', () => {
                    vscode.postMessage({
                        command: 'cancel'
                    });
                });

                // Focus first input
                const firstInput = document.querySelector('input');
                if (firstInput) {
                    firstInput.focus();
                }

                // Clear error on input
                form.addEventListener('input', () => {
                    errorMessage.classList.remove('visible');
                });
            </script>
        </body>
        </html>`;
    }

    private _escapeHtml(text: string): string {
        const map: Record<string, string> = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, m => map[m]);
    }
}
