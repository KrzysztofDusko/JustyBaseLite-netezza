import * as vscode from 'vscode';
import { SqlParser } from '../sql/sqlParser';

export class NetezzaDocumentLinkProvider implements vscode.DocumentLinkProvider {
    public provideDocumentLinks(document: vscode.TextDocument, _token: vscode.CancellationToken): vscode.DocumentLink[] {
        const links: vscode.DocumentLink[] = [];
        const text = document.getText();

        // Regex to find potential object references
        // We look for patterns with at least one dot to avoid linking every single word.
        // Matches:
        // DB..TABLE
        // DB.SCHEMA.TABLE
        // SCHEMA.TABLE
        // We allow alphanumeric, underscore, and quotes.
        // We use a simplified regex that captures the whole sequence including dots.

        const regex = /[a-zA-Z0-9_"]+(\.[a-zA-Z0-9_"]*)+/g;

        let match;
        while ((match = regex.exec(text)) !== null) {
            const startPos = document.positionAt(match.index);
            const endPos = document.positionAt(match.index + match[0].length);
            const range = new vscode.Range(startPos, endPos);

            // Use SqlParser to parse the matched string correctly
            // We pass the middle of the match to getObjectAtPosition to ensure it picks up the whole object
            // Actually, getObjectAtPosition expands from the offset.
            // Since we already found the range with regex, we can just parse the text directly or use getObjectAtPosition.
            // Let's use getObjectAtPosition to be consistent with its parsing logic (handling quotes etc).

            const objectInfo = SqlParser.getObjectAtPosition(text, match.index + Math.floor(match[0].length / 2));

            if (objectInfo) {
                // Check if this is an alias reference (ALIAS.COLUMN)
                // We detect this by checking if it's a simple two-part identifier (no database)
                // and if it appears in a context that suggests it's a column reference (not in FROM/JOIN)
                const matchedText = match[0];
                const parts = matchedText.split('.');

                // Skip if this looks like an alias.column pattern:
                // - Only 2 parts (ALIAS.COLUMN)
                // - No database specified in objectInfo
                // - Check if the first part is likely an alias by looking at the context
                if (parts.length === 2 && !objectInfo.database && this.isLikelyAliasReference(text, match.index)) {
                    continue; // Don't create a link for alias.column references
                }

                const args = {
                    name: objectInfo.name,
                    schema: objectInfo.schema,
                    database: objectInfo.database
                };

                const uri = vscode.Uri.parse(
                    `command:netezza.revealInSchema?${encodeURIComponent(JSON.stringify(args))}`
                );

                const link = new vscode.DocumentLink(range, uri);
                link.tooltip = `Reveal ${objectInfo.name} in Schema`;
                links.push(link);
            }
        }

        return links;
    }

    /**
     * Determines if a dotted identifier is likely an alias reference (e.g., ALIAS.COLUMN)
     * rather than a table reference (e.g., SCHEMA.TABLE).
     *
     * Heuristics:
     * - If it appears after FROM or JOIN keywords, it's likely a table reference
     * - If it appears in SELECT, WHERE, or other clauses, it's likely an alias.column reference
     */
    private isLikelyAliasReference(text: string, offset: number): boolean {
        // Look backwards to find the context
        const beforeText = text.substring(Math.max(0, offset - 200), offset);

        // Remove comments and normalize whitespace for better matching
        const cleanBefore = beforeText
            .replace(/--[^\n]*/g, '')
            .replace(/\/\*[\s\S]*?\*\//g, '')
            .toUpperCase();

        // Check if this appears right after FROM or JOIN (which would make it a table reference)
        // Pattern: FROM <whitespace> identifier or JOIN <whitespace> identifier
        if (/(?:FROM|JOIN)\s+[a-zA-Z0-9_"]*$/i.test(cleanBefore)) {
            return false; // This is a table reference in FROM/JOIN clause
        }

        // Check if we're in a SELECT, WHERE, ON, HAVING, ORDER BY, or GROUP BY clause
        // These contexts typically use ALIAS.COLUMN references
        const lastKeyword = cleanBefore.match(
            /\b(SELECT|WHERE|ON|HAVING|ORDER\s+BY|GROUP\s+BY|AND|OR|SET|VALUES)\b(?!.*\b(?:FROM|JOIN)\b)/
        );
        if (lastKeyword) {
            return true; // Likely an alias.column reference
        }

        // Default: if we can't determine, assume it's a table reference to be safe
        // This way we only skip underlining when we're confident it's an alias
        return false;
    }
}
