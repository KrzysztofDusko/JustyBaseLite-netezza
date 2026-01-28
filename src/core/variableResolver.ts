import * as vscode from 'vscode';
import { extractVariables, parseSetVariables, replaceVariablesInSql } from './variableUtils';
import { VariableInputPanel } from '../views/variableInputPanel';

// Prompt user for values for each variable. If `silent` is true and variables exist,
// throw an error because we cannot prompt in silent mode.
export async function promptForVariableValues(
    variables: Set<string>,
    silent: boolean,
    defaults?: Record<string, string>,
    extensionUri?: vscode.Uri
): Promise<Record<string, string>> {
    const values: Record<string, string> = {};
    if (variables.size === 0) return values;

    if (silent) {
        // If silent but defaults present for all variables, use them. Otherwise error.
        const missing = Array.from(variables).filter(v => !(defaults && defaults[v] !== undefined));
        if (missing.length > 0) {
            throw new Error(
                'Query contains variables but silent mode is enabled; cannot prompt for values. Missing: ' +
                missing.join(', ')
            );
        }
        for (const v of variables) {
            values[v] = defaults![v];
        }
        return values;
    }

    // Use webview panel for better UX
    const result = await VariableInputPanel.show(
        Array.from(variables),
        defaults,
        extensionUri
    );

    if (!result) {
        throw new Error('Variable input cancelled by user');
    }

    return result;
}

export async function resolveQueryVariables(
    query: string,
    silent: boolean,
    extensionUri?: vscode.Uri
): Promise<string> {
    // 1. Detect variables in SQL
    const parsed = parseSetVariables(query);
    const variables = extractVariables(parsed.sql);

    // 2. Parse inline SET variables is already done by parseSetVariables
    const inlineVars = parsed.setValues;

    // 3. Subtract inline vars from required prompts
    const remainingVars = new Set<string>();
    for (const v of variables) {
        if (inlineVars[v] === undefined) {
            remainingVars.add(v);
        }
    }

    // 4. Prompt for remaining
    const promptValues = await promptForVariableValues(remainingVars, silent, {}, extensionUri);

    // 5. Combine and replace
    const allValues = { ...inlineVars, ...promptValues };
    return replaceVariablesInSql(parsed.sql, allValues);
}
