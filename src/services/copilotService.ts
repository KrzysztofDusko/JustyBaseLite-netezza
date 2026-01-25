/**
 * Copilot Service
 * 
 * Handles interaction with GitHub Copilot Chat using Language Models API
 * and provides context from SQL files (DDL, variables, query history)
 * 
 * @version 2.0 - Enhanced with better error handling, caching, and UX improvements
 */

import * as vscode from 'vscode';
import { ConnectionManager } from '../core/connectionManager';
import { MetadataCache } from '../metadataCache';
import { extractVariables } from '../core/variableUtils';
import { generateTableDDL } from '../ddl';
import { createConnectionFromDetails, executeQueryHelper } from '../ddl/helpers';
import { NzConnection, ConnectionDetails } from '../types';
import { NZ_QUERIES, NZ_OBJECT_TYPES, NZ_SYSTEM_VIEWS } from '../metadata/systemQueries';

export interface CopilotContext {
    selectedSql: string;
    ddlContext: string;
    variables: string;
    recentQueries: string;
    connectionInfo: string;
}

export interface TableReference {
    database?: string;
    schema?: string;
    name: string;
}

interface DDLCacheEntry {
    ddl: string;
    timestamp: number;
}

export class CopilotService {
    private selectedModelId: string | undefined;
    private statusBarItem: vscode.StatusBarItem;

    // DDL Cache to avoid redundant database queries
    private ddlCache = new Map<string, DDLCacheEntry>();
    private readonly DDL_CACHE_TTL = 60 * 60 * 1000; // 60 minutes
    private readonly MAX_TABLES_FOR_DDL = 10;

    // Netezza-specific optimization rules for AI prompts
    private readonly NETEZZA_OPTIMIZATION_RULES = `
NETEZZA OPTIMIZATION RULES TO APPLY:

1. Eliminate SELECT *
   Replace SELECT * with explicit column lists. Include only columns actually used in output or subsequent operations. This reduces I/O dramatically in Netezza's columnar architecture.

2. Push Filters into Subqueries
   Move WHERE conditions from outer query into CTEs/subqueries. Filter data as early as possible before joins and aggregations to reduce intermediate result sizes.

3. Align JOINs with Distribution Keys
   Check if join columns match table distribution keys. If not, consider redistributing smaller table or add distribution key to join condition when possible to avoid broadcast operations.

4. Replace Correlated Subqueries
   Convert correlated subqueries to JOINs or window functions. Correlated subqueries execute per row - JOINs and analytics leverage parallel processing across SPUs.

5. Simplify DISTINCT and GROUP BY
   Remove unnecessary DISTINCT operations. Ensure GROUP BY uses distribution keys when possible. Consider if aggregation is truly needed or if EXISTS/window functions could replace it.

6. Optimize Window Functions
   Add PARTITION BY on distribution key in window functions. Use ORDER BY only when necessary. Limit window frame size (ROWS BETWEEN) to minimum required range.

7. Use UNION ALL Instead of UNION
   Replace UNION with UNION ALL when duplicates don't matter. UNION performs implicit DISTINCT which causes expensive data redistribution and sorting.

8. Avoid Functions on Join/WHERE Columns
   Remove functions from join and filter columns: change WHERE YEAR(date_col) = 2024 to WHERE date_col BETWEEN '2024-01-01' AND '2024-12-31'. Functions prevent zone map usage.

9. Split Complex Queries with TEMP Tables
   Break multi-join, multi-aggregation queries into steps using CREATE TEMP TABLE AS SELECT. Distribute temp tables on appropriate keys for subsequent joins to control execution plan.
`;

    // NZPLSQL Stored Procedure documentation for AI prompts
    // Reference: https://www.ibm.com/docs/en/netezza?topic=grammar-nzplsql-structure
    private readonly NZPLSQL_PROCEDURE_REFERENCE = `
NZPLSQL STORED PROCEDURE REFERENCE (IBM Netezza):

PROCEDURE STRUCTURE (PREFERRED FORMAT):
\`\`\`sql
CREATE [OR REPLACE] PROCEDURE database.schema.procedure_name(parameters)
RETURNS return_type
EXECUTE AS CALLER
LANGUAGE NZPLSQL
AS
BEGIN_PROC
  [DECLARE
    -- Variable declarations
    variable_name data_type [:= default_value];
    variable_name table_name%ROWTYPE;  -- Row type from table
    variable_name table_name.column%TYPE;  -- Column type
  ]
  BEGIN
    -- Statements
    [EXCEPTION WHEN OTHERS THEN
      -- Exception handling
      ROLLBACK;  -- Required for TRANSACTION_ABORTED
      RAISE NOTICE 'Error: %', SQLERRM;
    ]
  END;
END_PROC;
\`\`\`

KEY SYNTAX RULES:
- ALWAYS use EXECUTE AS CALLER (preferred) - runs with caller's permissions
- Use BEGIN_PROC / END_PROC to wrap the procedure body (NOT just BEGIN/END)
- DECLARE section comes BEFORE the inner BEGIN block
- Variables are initialized to NULL by default
- Use := for assignment (not =)
- Statements end with semicolon (;)
- Labels use <<label_name>> syntax

EXECUTE AS OPTIONS:
- EXECUTE AS CALLER (PREFERRED) - procedure runs with the permissions of the calling user
- EXECUTE AS OWNER - procedure runs with the permissions of the procedure owner

VARIABLE DECLARATIONS:
\`\`\`sql
DECLARE
  v_count INTEGER;
  v_name VARCHAR(100) := 'default';
  v_rate NUMERIC(10,2) NOT NULL := 0.0;
  v_const CONSTANT INTEGER := 100;
  v_row my_table%ROWTYPE;  -- Inherits structure from table
  v_id my_table.id%TYPE;   -- Inherits type from column
\`\`\`

CONTROL STRUCTURES:
- IF condition THEN ... [ELSIF condition THEN ...] [ELSE ...] END IF;
- CASE expression WHEN value THEN ... [ELSE ...] END CASE;
- LOOP ... EXIT [WHEN condition]; END LOOP;
- WHILE condition LOOP ... END LOOP;
- FOR i IN 1..10 LOOP ... END LOOP;
- FOR record IN SELECT ... LOOP ... END LOOP;

DYNAMIC SQL (EXECUTE IMMEDIATE):
\`\`\`sql
EXECUTE IMMEDIATE 'INSERT INTO ' || table_name || ' VALUES (1, 2)';
EXECUTE IMMEDIATE 'UPDATE ' || quote_ident(col) || ' SET x = ' || quote_literal(val);
\`\`\`
- Use quote_ident() for identifiers (table/column names)
- Use quote_literal() for string values

RETURNING RESULT SETS:
\`\`\`sql
CREATE PROCEDURE my_proc() RETURNS REFTABLE(reference_table)
LANGUAGE NZPLSQL AS
BEGIN_PROC
BEGIN
  EXECUTE IMMEDIATE 'INSERT INTO ' || REFTABLENAME || ' SELECT * FROM source';
  RETURN REFTABLE;
END;
END_PROC;
\`\`\`

EXCEPTION HANDLING:
\`\`\`sql
BEGIN
  -- statements that may fail
EXCEPTION
  WHEN TRANSACTION_ABORTED THEN
    ROLLBACK;
    RAISE ERROR 'Transaction failed: %', SQLERRM;
  WHEN OTHERS THEN
    RAISE NOTICE 'Error caught: %', SQLERRM;
END;
\`\`\`

BUILT-IN VARIABLES:
- FOUND: TRUE if last query returned rows
- ROW_COUNT: Number of rows affected by last statement
- SQLERRM: Error message text in exception handler
- REFTABLENAME: Name of temp table for REFTABLE procedures

IMPORTANT NOTES:
- Netezza does NOT support nested transactions (no SAVEPOINT)
- BEGIN/END in NZPLSQL is for grouping, NOT transaction control
- Use CALL procedure_name() or EXECUTE PROCEDURE procedure_name() to invoke
- Procedures run within the caller's transaction context
`;

    private context: vscode.ExtensionContext;

    constructor(
        private connectionManager: ConnectionManager,
        context: vscode.ExtensionContext,
        private metadataCache?: MetadataCache
    ) {
        this.context = context;
        this.statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
        this.statusBarItem.command = 'justyBaseLite.copilot.changeModel';

        // Load persisted model if present (workspace-specific)
        const saved = this.context.workspaceState.get<string>('copilot.selectedModelId');
        if (saved) {
            this.selectedModelId = saved;
        }
    }

    /**
     * Initializes the service
     */
    public async init(): Promise<boolean> {
        try {
            // Get available Copilot models
            const models = await vscode.lm.selectChatModels({ vendor: 'copilot' });

            if (models.length === 0) {
                this.statusBarItem.text = '$(copilot) No Models';
                this.statusBarItem.tooltip = 'No Copilot models available';
                this.statusBarItem.show();
                return false;
            }

            // Validate persisted model still exists
            if (this.selectedModelId) {
                const persistedModel = models.find(m => m.id === this.selectedModelId);
                if (!persistedModel) {
                    console.warn(`[CopilotService] Persisted model ${this.selectedModelId} no longer available, clearing...`);
                    this.selectedModelId = undefined;
                    await this.context.workspaceState.update('copilot.selectedModelId', undefined);
                }
            }

            // If no valid model selected, pick a default
            if (!this.selectedModelId) {
                // Prefer gpt-4 or similar high capability models
                const preferred = models.find(m =>
                    m.family.toLowerCase().includes('gpt-4o') ||
                    m.family.toLowerCase().includes('claude-3-5-sonnet')
                ) || models[0];

                this.selectedModelId = preferred.id;
                await this.context.workspaceState.update('copilot.selectedModelId', this.selectedModelId);
                console.log(`[CopilotService] Auto-selected model: ${preferred.name || preferred.family}`);
            }

            this.updateStatusBar();
            return true;
        } catch (error) {
            console.error('[CopilotService] Failed to initialize:', error);
            this.statusBarItem.text = '$(copilot) Error';
            this.statusBarItem.tooltip = 'Click to select AI Model';
            this.statusBarItem.show();
            return false;
        }
    }

    // Note: cost/pricing is not hardcoded. When available, providers may include cost info
    // in `detail` or `tooltip` fields returned by `selectChatModels`. We parse and display
    // that information where present. No fallback mapping is used.

    private async selectModel(): Promise<void> {
        try {
            // Only get models from copilot vendor to avoid unsupported model errors
            const models = await vscode.lm.selectChatModels({ vendor: 'copilot' });

            const modelItems = models.map(m => {
                type ModelMeta = { detail?: string; tooltip?: string };
                const meta = m as unknown as ModelMeta;
                // prefer explicit cost/token info if provider attached it in detail/tooltip
                const explicit = meta.detail ?? meta.tooltip;
                const cost = explicit?.match(/(0x|\d+(?:\.\d+)?x)/i)?.[1];

                return {
                    label: `$(sparkle) ${m.name || m.family}`,
                    description: cost ? `${cost} cost` : undefined,
                    detail: `${m.vendor} • ${m.family} • Max tokens: ${m.maxInputTokens}${explicit ? ' • ' + explicit : ''}`,
                    modelId: m.id,
                    model: m
                };
            });

            if (modelItems.length === 0) {
                vscode.window.showWarningMessage('No AI models detected. Ensure GitHub Copilot is installed and you are signed in.');
                return;
            }

            const selected = await vscode.window.showQuickPick(modelItems, {
                placeHolder: 'Select AI Model for SQL Generation',
                matchOnDescription: true,
                matchOnDetail: true
            });

            if (selected) {
                this.selectedModelId = selected.modelId;
                this.updateStatusBar();
                // Persist selection (workspace-specific)
                try {
                    await this.context.workspaceState.update('copilot.selectedModelId', this.selectedModelId);
                } catch (e) {
                    console.warn('Failed to persist selected model:', e);
                }
                vscode.window.showInformationMessage(`Model switched to: ${selected.model.name || selected.model.family}`);
            }
        } catch (error) {
            vscode.window.showErrorMessage(`Failed to select model: ${error}`);
        }
    }

    private updateStatusBar() {
        if (this.selectedModelId) {
            vscode.lm.selectChatModels().then(models => {
                const model = models.find(m => m.id === this.selectedModelId);
                const name = model ? (model.name || model.family) : 'Copilot';

                // Try to extract cost from model metadata
                let costLabel = '';
                if (model) {
                    type ModelMeta = { detail?: string; tooltip?: string };
                    const meta = model as unknown as ModelMeta;
                    const explicit = meta.detail ?? meta.tooltip;
                    const cost = explicit?.match(/(0x|\d+(?:\.\d+)?x)/i)?.[1];
                    if (cost) {
                        costLabel = ` [${cost}]`;
                    }
                }

                this.statusBarItem.text = `$(copilot) ${name}${costLabel}`;
                this.statusBarItem.tooltip = `Using model: ${name}${costLabel}. Click to change.`;
                this.statusBarItem.show();
            });
        }
    }

    // New method to allow user to change model explicitly command
    public async changeModel(): Promise<void> {
        await this.selectModel();
    }

    /**
     * Clears persisted model selection (workspace-specific)
     * User will be prompted to select a model on next use
     */
    public async clearPersistedModel(): Promise<void> {
        try {
            await this.context.workspaceState.update('copilot.selectedModelId', undefined);
            this.selectedModelId = undefined;
            this.statusBarItem.text = '$(copilot) Select Model';
            this.statusBarItem.tooltip = 'Click to select AI Model';
            vscode.window.showInformationMessage('Persisted model selection cleared. You will be prompted to select a model on next use.');
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to clear model selection: ${msg}`);
        }
    }

    private getPrompt(type: 'optimize' | 'fix' | 'explain'): string {
        const config = vscode.workspace.getConfiguration('justyBaseLite.prompts');
        const defaults = {
            optimize: "Optimize the following IBM Netezza Performance Server (NPS) SQL query for performance and readability. Keep Netezza-specific syntax and features.",
            fix: "Fix the syntax errors in the following IBM Netezza Performance Server (NPS) SQL query. Preserve Netezza-specific SQL dialect features.",
            explain: "Explain what this IBM Netezza Performance Server (NPS) SQL query does, including any Netezza-specific features used."
        };
        return config.get<string>(type) || defaults[type];
    }

    /**
     * Helper to select Copilot response mode (Auto apply vs Chat)
     */
    private async selectCopilotMode(action: string): Promise<'auto' | 'chat' | undefined> {
        const result = await vscode.window.showQuickPick(
            [
                { label: '$(zap) Auto', description: 'Apply changes directly via diff', value: 'auto' as const },
                { label: '$(comment-discussion) Chat', description: 'Discuss in Copilot Chat', value: 'chat' as const }
            ],
            { placeHolder: `${action} - Select mode` }
        );
        return result?.value;
    }

    /**
     * Detects if SQL contains a stored procedure definition
     * Matches: CREATE PROCEDURE, CREATE OR REPLACE PROCEDURE
     */
    private isProcedureCode(sql: string): boolean {
        const procedurePattern = /CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\s+/i;
        return procedurePattern.test(sql);
    }

    /**
     * Gets Netezza-specific reference documentation for LM tools.
     * Used by NetezzaReferenceTool to provide optimization hints and NZPLSQL documentation.
     * 
     * @param topic Optional topic filter: 'optimization', 'nzplsql', or 'all' (default)
     */
    public getNetezzaReference(topic: 'optimization' | 'nzplsql' | 'all' = 'all'): string {
        if (topic === 'optimization') {
            return this.NETEZZA_OPTIMIZATION_RULES;
        } else if (topic === 'nzplsql') {
            return this.NZPLSQL_PROCEDURE_REFERENCE;
        } else {
            return `${this.NETEZZA_OPTIMIZATION_RULES}\n\n${this.NZPLSQL_PROCEDURE_REFERENCE}`;
        }
    }

    /**
     * Gathers context from current document and database state
     */
    async gatherContext(): Promise<CopilotContext> {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            throw new Error('No active editor');
        }

        const document = editor.document;
        const selection = editor.selection;
        const selectedSql = selection.isEmpty
            ? document.getText()
            : document.getText(new vscode.Range(selection.start, selection.end));

        if (!selectedSql.trim()) {
            throw new Error('No SQL selected or document is empty');
        }

        // Extract variables from SQL
        const variables = extractVariables(selectedSql);
        const variablesStr = variables.size > 0
            ? `Variables: ${Array.from(variables).join(', ')}`
            : 'No variables detected';

        // Get connection info
        const connectionName = this.connectionManager.getDocumentConnection(document.uri.toString())
            || this.connectionManager.getActiveConnectionName()
            || undefined;

        const connectionInfo = connectionName
            ? `Connected to: ${connectionName}`
            : 'No connection selected';

        // Extract table references from SQL
        const tableRefs = this.extractTableReferences(selectedSql);

        // Gather DDL for referenced tables
        const ddlContext = await this.gatherTablesDDL(tableRefs, connectionName);

        // Get recent queries from history (if available)
        const recentQueries = this.getRecentQueriesSummary();

        return {
            selectedSql,
            ddlContext,
            variables: variablesStr,
            recentQueries,
            connectionInfo
        };
    }

    /**
     * Extracts table references from SQL using improved regex patterns
     * Handles: TABLE, SCHEMA.TABLE, DB.SCHEMA.TABLE, and DB..TABLE (Netezza two-dot syntax)
     * Now with better handling of comments and string literals
     */
    private extractTableReferences(sql: string): TableReference[] {
        try {
            // Clean SQL: remove comments and string literals to avoid false positives
            let cleanedSql = sql
                .replace(/--.*$/gm, '') // Remove single-line comments
                .replace(/\/\*[\s\S]*?\*\//g, '') // Remove multi-line comments
                .replace(/'([^'\\]|\\.)*'/g, ''); // Remove string literals in single quotes

            const tables = new Map<string, TableReference>();

            // STEP 1: First extract DB..TABLE (Netezza two-dot syntax) and remove from SQL
            // This prevents the standard patterns from incorrectly parsing these
            const twoDotPatterns = [
                /FROM\s+(\w+)\.\.(\w+)/gi,
                /JOIN\s+(\w+)\.\.(\w+)/gi,
                /INSERT\s+INTO\s+(\w+)\.\.(\w+)/gi,
                /UPDATE\s+(\w+)\.\.(\w+)/gi,
                /DELETE\s+FROM\s+(\w+)\.\.(\w+)/gi
            ];

            for (const pattern of twoDotPatterns) {
                cleanedSql = cleanedSql.replace(pattern, (match, database, tableName) => {
                    const key = `${database}||${tableName}`.toUpperCase();
                    tables.set(key, { database, schema: undefined, name: tableName });
                    // Replace with placeholder to prevent re-matching
                    return match.replace(/\w+\.\.\w+/, '__EXTRACTED__');
                });
            }

            // STEP 2: Now extract standard patterns: DB.SCHEMA.TABLE, SCHEMA.TABLE, TABLE
            const standardPatterns = [
                /FROM\s+(?:(\w+)\.)?(?:(\w+)\.)?(\w+)/gi,
                /JOIN\s+(?:(\w+)\.)?(?:(\w+)\.)?(\w+)/gi,
                /INSERT\s+INTO\s+(?:(\w+)\.)?(?:(\w+)\.)?(\w+)/gi,
                /UPDATE\s+(?:(\w+)\.)?(?:(\w+)\.)?(\w+)/gi,
                /DELETE\s+FROM\s+(?:(\w+)\.)?(?:(\w+)\.)?(\w+)/gi
            ];

            for (const pattern of standardPatterns) {
                let match;
                while ((match = pattern.exec(cleanedSql)) !== null) {
                    let database: string | undefined;
                    let schema: string | undefined;
                    let tableName: string | undefined;

                    if (match[3]) {
                        // All 3 parts: DB.SCHEMA.TABLE
                        database = match[1];
                        schema = match[2];
                        tableName = match[3];
                    } else if (match[2]) {
                        // 2 parts: SCHEMA.TABLE
                        schema = match[1];
                        tableName = match[2];
                    } else if (match[1]) {
                        // 1 part: TABLE
                        tableName = match[1];
                    }

                    // Skip placeholder
                    if (tableName && tableName !== '__EXTRACTED__') {
                        const key = `${database || ''}|${schema || ''}|${tableName}`.toUpperCase();
                        tables.set(key, { database, schema, name: tableName });
                    }
                }
            }

            return Array.from(tables.values());
        } catch (e) {
            console.error('[CopilotService] Error extracting table references:', e);
            return [];
        }
    }

    /**
     * Gets cached DDL or fetches it from database
     */
    private async getCachedDDL(
        key: string,
        fetcher: () => Promise<string>
    ): Promise<string> {
        const cached = this.ddlCache.get(key);
        const now = Date.now();

        // Return cached if still valid
        if (cached && (now - cached.timestamp) < this.DDL_CACHE_TTL) {
            console.log(`[CopilotService] Using cached DDL for ${key}`);
            return cached.ddl;
        }

        // Fetch fresh DDL
        const ddl = await fetcher();
        this.ddlCache.set(key, { ddl, timestamp: now });
        return ddl;
    }

    /**
     * Clears DDL cache (useful for manual refresh)
     */
    clearDDLCache(): void {
        this.ddlCache.clear();
        console.log('[CopilotService] DDL cache cleared');
    }

    /**
     * Finds the schema of a table by querying the system catalog
     * Similar to revealInSchema strategy - search without specifying schema
     */
    private async findTableSchema(
        connection: NzConnection,
        database: string,
        tableName: string
    ): Promise<string | undefined> {
        try {
            // Use centralized query builder for finding table schema
            const sql = NZ_QUERIES.findTableSchema(database, tableName.replace(/'/g, "''"));

            interface SchemaRow { SCHEMA: string }
            const result = await executeQueryHelper<SchemaRow>(connection, sql);
            if (result && result.length > 0) {
                return result[0].SCHEMA;
            }
            return undefined;
        } catch (e) {
            console.warn(`[CopilotService] Failed to find schema for ${database}..${tableName}:`, e);
            return undefined;
        }
    }

    /**
     * Gathers DDL for referenced tables using live database queries
     * Now with caching and better error handling
     */
    private async gatherTablesDDL(
        tableRefs: TableReference[],
        connectionName: string | undefined
    ): Promise<string> {
        if (tableRefs.length === 0) {
            return 'No table references detected in SQL';
        }

        if (!connectionName) {
            const tableNames = tableRefs
                .map(t => {
                    const parts = [t.database, t.schema, t.name].filter(Boolean);
                    return parts.join('.');
                })
                .join(', ');
            return `Could not gather DDL - no connection selected.\nFound table references: ${tableNames}`;
        }

        const connectionDetails = await this.connectionManager.getConnection(connectionName);
        if (!connectionDetails) {
            return `Connection "${connectionName}" not found`;
        }

        // Create NzConnection from ConnectionDetails
        let nzConnection;
        try {
            nzConnection = await createConnectionFromDetails(connectionDetails);
        } catch (e) {
            return `Failed to create connection: ${e instanceof Error ? e.message : String(e)}`;
        }

        const ddlLines: string[] = [];
        const tablesToProcess = tableRefs.slice(0, this.MAX_TABLES_FOR_DDL);

        // Add warning if table limit exceeded
        if (tableRefs.length > this.MAX_TABLES_FOR_DDL) {
            ddlLines.push(`-- NOTE: Showing DDL for ${this.MAX_TABLES_FOR_DDL} out of ${tableRefs.length} tables (limit reached)`);
            ddlLines.push('-- To see more tables, reduce the number of table references in your query');
            ddlLines.push('');
        }

        try {
            for (const tableRef of tablesToProcess) {
                try {
                    // Get database - use reference or fall back to current database
                    let database = tableRef.database;
                    if (!database) {
                        // Try to get current database from connection
                        database = await this.connectionManager.getCurrentDatabase(connectionName) || undefined;
                    }
                    if (!database) {
                        ddlLines.push(`-- Table: ${tableRef.name} (cannot determine database)`);
                        ddlLines.push('');
                        continue;
                    }

                    // If schema is not specified, look it up in system catalog (like revealInSchema does)
                    let schema = tableRef.schema;
                    if (!schema) {
                        schema = await this.findTableSchema(nzConnection, database, tableRef.name);
                    }

                    if (!schema) {
                        ddlLines.push(`-- Table: ${database}..${tableRef.name} (table not found in database)`);
                        ddlLines.push('');
                        continue;
                    }

                    const tableName = tableRef.name;
                    const displayName = `${database}.${schema}.${tableName}`;

                    // Use cache to avoid redundant queries
                    const cacheKey = `${connectionName}|${database}|${schema}|${tableName}`;

                    const ddl = await this.getCachedDDL(cacheKey, async () => {
                        return await generateTableDDL(nzConnection, database!, schema!, tableName);
                    });

                    if (ddl && !ddl.toLowerCase().includes('not found')) {
                        ddlLines.push(`-- Table: ${displayName}`);
                        ddlLines.push(ddl);
                        ddlLines.push('');
                    } else {
                        ddlLines.push(`-- Table: ${displayName} (DDL not found)`);
                        ddlLines.push('');
                    }
                } catch (e) {
                    const displayName = `${tableRef.database || ''}.${tableRef.schema || ''}.${tableRef.name}`.replace(/^\.+/, '');
                    console.warn(`[CopilotService] Could not get DDL for ${displayName}:`, e);
                    ddlLines.push(`-- Table: ${displayName} (error retrieving DDL: ${e instanceof Error ? e.message : String(e)})`);
                    ddlLines.push('');
                }
            }
        } catch (e) {
            console.error('[CopilotService] Error gathering DDL:', e);
            ddlLines.push(`-- Error gathering DDL: ${e instanceof Error ? e.message : String(e)}`);
        } finally {
            // Close connection safely
            if (nzConnection) {
                try {
                    await nzConnection.close();
                } catch (e) {
                    console.warn('[CopilotService] Error closing connection:', e);
                }
            }
        }

        return ddlLines.length > 0
            ? ddlLines.join('\n')
            : `Could not retrieve DDL for tables: ${tableRefs.map(t => t.name).join(', ')}`;
    }

    /**
     * Formats DDL context for prompt - wraps in code block only if it contains valid DDL
     */
    private formatDdlForPrompt(ddlContext: string): string {
        // Check if DDL contains actual table definitions (CREATE TABLE, columns, etc.)
        // vs just error messages or "not found" notices
        const hasValidDdl = ddlContext.includes('CREATE TABLE') ||
            (ddlContext.includes('-- Table:') && !ddlContext.includes('error retrieving') && !ddlContext.includes('not found'));

        if (hasValidDdl) {
            return '```sql\n' + ddlContext + '\n```';
        } else {
            // Just return as plain text if it's errors/not found
            return ddlContext;
        }
    }

    /**
     * Gets a summary of recent queries for context
     */
    private getRecentQueriesSummary(): string {
        // This would integrate with QueryHistoryManager if available
        // For now, return placeholder
        return 'Recent queries history not yet available in this context';
    }

    /**
     * Sends a prompt to Copilot Chat (legacy method - kept for compatibility)
     */
    async sendToCopilotAgent(context: CopilotContext, userPrompt: string): Promise<void> {
        try {
            // Build comprehensive prompt with all context
            const systemPrompt = this.buildSystemPrompt(context);
            const fullPrompt = `${systemPrompt}\n\nUser request: ${userPrompt}`;

            // Send to Copilot Chat
            await vscode.commands.executeCommand(
                'workbench.action.chat.open',
                { query: fullPrompt }
            );

            vscode.window.showInformationMessage('✅ Sent to Copilot Chat with context');
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to send to Copilot: ${msg}`);
        }
    }

    /**
     * Builds the system prompt with all context
     */
    private buildSystemPrompt(context: CopilotContext): string {
        // Get language preference
        const config = vscode.workspace.getConfiguration('justyBaseLite.copilot');
        const preferredLanguage = config.get<string>('preferredLanguage') || 'english';

        let languageInstruction = '';
        if (preferredLanguage === 'system') {
            // Use VS Code's display language as proxy for system language
            const displayLanguage = vscode.env.language;
            if (displayLanguage && !displayLanguage.startsWith('en')) {
                languageInstruction = `\n\nRESPONSE LANGUAGE: Please respond in ${displayLanguage} language.`;
            }
        }

        return `You are an expert in IBM Netezza Performance Server (NPS) / IBM PureData System for Analytics SQL.${languageInstruction}

IMPORTANT NETEZZA SQL NAMING CONVENTIONS:
- Three-part name: DATABASE.SCHEMA.OBJECT - fully qualified reference to a table/view/procedure
- Two-part name with double dots: DATABASE..OBJECT - references object in the specified database (searches across schemas or uses default schema depending on configuration)
- Two-part name with single dot: SCHEMA.OBJECT - uses current/default database with specified schema
- Single name: OBJECT - uses current database and current schema
- System views like _V_TABLE, _V_VIEW, _V_PROCEDURE are in each database; use DATABASE.._V_TABLE to query a specific database's system views
- DATABASE..TABLE syntax is valid and CORRECT in Netezza - do NOT "fix" it by adding a schema name!
- Netezza supports specific SQL extensions: DISTRIBUTE ON, ORGANIZE ON, GROOM TABLE, GENERATE STATISTICS, etc.

DATABASE CONNECTION:
${context.connectionInfo}

REFERENCED TABLE SCHEMAS:
${this.formatDdlForPrompt(context.ddlContext)}

QUERY VARIABLES:
${context.variables}

RECENT CONTEXT:
${context.recentQueries}

CURRENT SQL:
\`\`\`sql
${context.selectedSql}
\`\`\``;
    }

    /**
     * Sends message to Copilot using Language Models API with automatic editing
     * This directly calls the LM API and applies edits programmatically
     */
    private async sendToLanguageModel(
        context: CopilotContext,
        userPrompt: string,
        shouldEdit: boolean = true,
        token?: vscode.CancellationToken
    ): Promise<string> {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            throw new Error('No active editor');
        }

        // Build comprehensive prompt
        const systemPrompt = this.buildSystemPrompt(context);
        const fullPrompt = `${systemPrompt}\n\nUser request:\n${userPrompt}\n\nIMPORTANT: Respond ONLY with the modified SQL code without any explanations or markdown formatting.`;

        // 1. Get Selected Model
        if (!this.selectedModelId) {
            await this.selectModel();
            if (!this.selectedModelId) {
                throw new Error('No AI model selected.');
            }
        }

        // Find the model object - only from copilot vendor to avoid unsupported models
        const allModels = await vscode.lm.selectChatModels({ vendor: 'copilot' });

        if (allModels.length === 0) {
            throw new Error('No Copilot models available. Ensure GitHub Copilot extension is installed and you are signed in.');
        }

        let model = allModels.find(m => m.id === this.selectedModelId);

        // If selected model is not available, clear it and pick first available
        if (!model) {
            console.warn(`[CopilotService] Model ${this.selectedModelId} not found, selecting first available model...`);
            model = allModels[0];
            this.selectedModelId = model.id;
            await this.context.workspaceState.update('copilot.selectedModelId', this.selectedModelId);
            this.updateStatusBar();

            vscode.window.showWarningMessage(
                `Previously selected model is unavailable. Switched to: ${model.name || model.family}`
            );
        }

        console.log(`[CopilotService] Using model: ${model.id} (${model.name || model.family})`);

        if (!model) {
            throw new Error('No valid Copilot model available.');
        }

        const messages = [vscode.LanguageModelChatMessage.User(fullPrompt)];
        const cancellationToken = token ?? new vscode.CancellationTokenSource().token;

        let response = '';
        try {
            console.log(`[CopilotService] Sending request to ${model.id}`);
            const modelResponse = await model.sendRequest(messages, {}, cancellationToken);
            for await (const chunk of modelResponse.text) {
                response += chunk;
            }
        } catch (err) {
            throw new Error(`Model request failed: ${err instanceof Error ? err.message : String(err)}`);
        }

        if (!response.trim()) {
            throw new Error('Model returned empty response.');
        }

        // Cleanup response
        const cleanResponse = response.trim()
            .replace(/^```sql\n?/i, '')
            .replace(/^```\n?/i, '')
            .replace(/\n?```$/i, '')
            .trim();

        // Apply Diff View if editing is requested
        if (shouldEdit) {
            await this.showDiff(editor.document, cleanResponse);
        }

        return cleanResponse;
    }

    /**
     * Sends message to Copilot Chat with /edit command
     * This allows Copilot to apply edits directly to the file
     */
    async askCopilotWithEdit(context: CopilotContext, userPrompt: string): Promise<void> {
        try {
            const systemPrompt = this.buildSystemPrompt(context);
            const editPrompt = `${systemPrompt}\n\n${userPrompt}\n\nPlease use /edit to apply changes directly to the file.`;

            // Use the chat command to open chat with our prompt
            await vscode.commands.executeCommand(
                'workbench.action.chat.open',
                { query: editPrompt }
            );

            vscode.window.showInformationMessage(
                '✅ Sent to Copilot Chat. Type /edit in the chat to apply suggestions directly.'
            );
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to open Copilot Chat: ${msg}`);
        }
    }

    /**
     * Quick action: Fix SQL with context
     * User selects between Auto (applies via diff) or Chat (interactive discussion)
     */
    async fixSql(): Promise<void> {
        try {
            const mode = await this.selectCopilotMode('Fix SQL');
            if (!mode) return;

            const context = await this.gatherContext();
            let prompt = this.getPrompt('fix');

            // Add NZPLSQL reference if code contains stored procedure
            if (this.isProcedureCode(context.selectedSql)) {
                prompt += `\n\n${this.NZPLSQL_PROCEDURE_REFERENCE}`;
            }

            if (mode === 'auto') {
                await this.sendToLanguageModel(context, prompt, true);
            } else {
                await this.sendToChatInteractive(context, prompt, 'Fix SQL');
            }
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error fixing SQL: ${msg}`);
        }
    }

    /**
     * Quick action: Optimize SQL with context
     * User selects between Auto (applies via diff) or Chat (interactive discussion)
     */
    async optimizeSql(): Promise<void> {
        try {
            const mode = await this.selectCopilotMode('Optimize SQL');
            if (!mode) return;

            const context = await this.gatherContext();
            const basePrompt = this.getPrompt('optimize');
            let prompt = `${basePrompt}\n\n${this.NETEZZA_OPTIMIZATION_RULES}`;

            // Add NZPLSQL reference if code contains stored procedure
            if (this.isProcedureCode(context.selectedSql)) {
                prompt += `\n\n${this.NZPLSQL_PROCEDURE_REFERENCE}`;
            }

            if (mode === 'auto') {
                await this.sendToLanguageModel(context, prompt, true);
            } else {
                await this.sendToChatInteractive(context, prompt, 'Optimize SQL');
            }
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error optimizing SQL: ${msg}`);
        }
    }

    /**
     * Quick action: Explain SQL with context
     * User selects between Document (new markdown) or Chat (interactive discussion)
     */
    async explainSql(): Promise<void> {
        try {
            const mode = await vscode.window.showQuickPick(
                [
                    { label: '$(file-text) Document', description: 'Show explanation in new document', value: 'document' as const },
                    { label: '$(comment-discussion) Chat', description: 'Discuss in Copilot Chat', value: 'chat' as const }
                ],
                { placeHolder: 'How would you like the explanation?' }
            );
            if (!mode) return;

            const context = await this.gatherContext();
            const prompt = this.getPrompt('explain');

            if (mode.value === 'document') {
                // Show explanation in a new markdown document
                const response = await this.sendToLanguageModel(context, prompt, false);
                const doc = await vscode.workspace.openTextDocument({
                    content: `# SQL Query Explanation\n\n${response}`,
                    language: 'markdown'
                });
                await vscode.window.showTextDocument(doc, vscode.ViewColumn.Beside);
                vscode.window.showInformationMessage('✅ SQL explanation opened in new editor');
            } else {
                await this.sendToChatInteractive(context, prompt, 'Explain SQL');
            }
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error explaining SQL: ${msg}`);
        }
    }

    /**
     * Custom question with full context
     * User enters question and selects response mode
     */
    async askCustomQuestion(): Promise<void> {
        try {
            const userQuestion = await vscode.window.showInputBox({
                prompt: 'Ask Copilot about this SQL (with full database context)',
                placeHolder: 'e.g., "How can I improve this query?" or "Add an index hint"'
            });

            if (!userQuestion) {
                return;
            }

            const context = await this.gatherContext();

            // Ask how user wants to receive the response
            const action = await vscode.window.showQuickPick(
                [
                    { label: '$(edit) Apply Changes', description: 'Copilot modifies the SQL in editor', value: 'edit' as const },
                    { label: '$(file-text) Document', description: 'Get response in new document', value: 'document' as const },
                    { label: '$(comment-discussion) Chat', description: 'Discuss in Copilot Chat', value: 'chat' as const }
                ],
                { placeHolder: 'How would you like Copilot to respond?' }
            );

            if (!action) {
                return;
            }

            if (action.value === 'edit') {
                await this.sendToLanguageModel(context, userQuestion, true);
            } else if (action.value === 'document') {
                const response = await this.sendToLanguageModel(context, userQuestion, false);
                const doc = await vscode.workspace.openTextDocument({
                    content: `# Copilot Advice\n\n**Question:** ${userQuestion}\n\n${response}`,
                    language: 'markdown'
                });
                await vscode.window.showTextDocument(doc, vscode.ViewColumn.Beside);
            } else {
                await this.sendToChatInteractive(context, userQuestion, 'Custom Question');
            }
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error: ${msg}`);
        }
    }

    /**
     * Generate SQL from natural language description (Interactive mode)
     * Gathers schema overview (tables + columns) and sends to Copilot Chat
     * for interactive SQL generation
     */
    async generateSqlInteractive(): Promise<void> {
        try {
            // Get user's natural language description
            const userDescription = await vscode.window.showInputBox({
                prompt: 'Describe the SQL query you need in natural language',
                placeHolder: 'e.g., "Find all customers who made purchases over $1000 last month"',
                ignoreFocusOut: true
            });

            if (!userDescription) {
                return;
            }

            // Show progress while gathering schema
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: 'Gathering database schema...',
                cancellable: false
            }, async () => {
                const schemaOverview = await this.gatherSchemaOverview();

                if (!schemaOverview) {
                    vscode.window.showErrorMessage('Could not gather schema information. Please ensure you are connected to a database.');
                    return;
                }

                // Build the prompt for SQL generation
                const generateSqlPrompt = this.buildGenerateSqlPrompt(userDescription, schemaOverview);

                // Create a minimal context (no SQL selected, just connection info)
                const connectionName = await this.connectionManager.getActiveConnectionName();
                const context: CopilotContext = {
                    selectedSql: '',
                    ddlContext: schemaOverview,
                    variables: '',
                    recentQueries: this.getRecentQueriesSummary(),
                    connectionInfo: connectionName ? `Connected to: ${connectionName}` : 'No active connection'
                };

                await this.sendToChatInteractiveWithCustomPrompt(context, generateSqlPrompt, 'Generate SQL');
            });
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error generating SQL: ${msg}`);
        }
    }

    /**
     * Gathers a compact schema overview for the current database
     * Returns a formatted string with table names and their columns
     * Optimized for token efficiency - only names, not full DDL
     */
    private async gatherSchemaOverview(): Promise<string | null> {
        const connectionName = await this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return null;
        }

        const connectionDetails = await this.connectionManager.getConnection(connectionName);
        if (!connectionDetails) {
            return null;
        }

        let nzConnection: NzConnection | null = null;
        try {
            nzConnection = await createConnectionFromDetails(connectionDetails);
            const database = await this.connectionManager.getCurrentDatabase(connectionName);

            if (!database) {
                return null;
            }

            // Use centralized query builder for columns with PK/FK info
            // Note: Uses specialized schema overview query with TABLE_DESCRIPTION and COLUMN_DESCRIPTION aliases
            const sql = NZ_QUERIES.listColumnsWithKeys(database, {
                objTypes: [NZ_OBJECT_TYPES.TABLE, NZ_OBJECT_TYPES.VIEW]
            });

            interface SchemaRow {
                SCHEMA: string;
                TABLENAME: string;
                DESCRIPTION: string;
                ATTNAME: string;
                FORMAT_TYPE: string;
                ATTNUM: number;
                IS_PK: number;
                IS_FK: number;
            }

            const result = await executeQueryHelper<SchemaRow>(nzConnection, sql);

            if (!result || result.length === 0) {
                return 'No tables found in database';
            }

            // Group columns by schema.table
            const tableMap = new Map<string, {
                schema: string;
                tableName: string;
                tableDescription: string;
                columns: Array<{ name: string; type: string; description: string; isPk: boolean; isFk: boolean }>;
            }>();

            for (const row of result) {
                // Use column names from centralized query (TABLENAME instead of TABLE_NAME)
                const key = `${row.SCHEMA}.${row.TABLENAME}`;
                if (!tableMap.has(key)) {
                    tableMap.set(key, {
                        schema: row.SCHEMA,
                        tableName: row.TABLENAME,
                        tableDescription: row.DESCRIPTION || '',
                        columns: []
                    });
                }
                tableMap.get(key)!.columns.push({
                    name: row.ATTNAME,
                    type: row.FORMAT_TYPE,
                    description: row.DESCRIPTION || '',
                    isPk: Number(row.IS_PK) === 1,
                    isFk: Number(row.IS_FK) === 1
                });
            }

            // Format output - compact but informative
            const lines: string[] = [];
            lines.push(`DATABASE: ${database}`);
            lines.push(`TABLES: ${tableMap.size}`);
            lines.push('');
            lines.push('SCHEMA OVERVIEW:');
            lines.push('================');
            lines.push('');

            // Group by schema for better organization
            const schemaGroups = new Map<string, typeof tableMap>();
            for (const [key, table] of tableMap) {
                if (!schemaGroups.has(table.schema)) {
                    schemaGroups.set(table.schema, new Map());
                }
                schemaGroups.get(table.schema)!.set(key, table);
            }

            for (const [schema, tables] of schemaGroups) {
                lines.push(`[SCHEMA: ${schema}]`);

                for (const [, table] of tables) {
                    const tableDesc = table.tableDescription ? ` -- ${table.tableDescription}` : '';
                    lines.push(`  TABLE: ${table.tableName}${tableDesc}`);

                    // List columns with types and key indicators
                    const columnList = table.columns.map(c => {
                        const keyIndicators: string[] = [];
                        if (c.isPk) keyIndicators.push('PK');
                        if (c.isFk) keyIndicators.push('FK');
                        const keyStr = keyIndicators.length > 0 ? ` [${keyIndicators.join(', ')}]` : '';
                        const desc = c.description ? ` (${c.description})` : '';
                        return `    - ${c.name}: ${c.type}${keyStr}${desc}`;
                    });
                    lines.push(columnList.join('\n'));
                    lines.push('');
                }
            }

            return lines.join('\n');
        } catch (e) {
            console.error('[CopilotService] Error gathering schema overview:', e);
            return null;
        } finally {
            if (nzConnection) {
                try {
                    await nzConnection.close();
                } catch (e) {
                    console.warn('[CopilotService] Error closing connection:', e);
                }
            }
        }
    }

    /**
     * Builds the prompt for SQL generation from natural language
     */
    private buildGenerateSqlPrompt(userDescription: string, schemaOverview: string): string {
        return `You are a Netezza SQL expert. The user wants to generate a SQL query based on their description.

IMPORTANT NETEZZA SQL NAMING CONVENTIONS:
- Three-part name: DATABASE.SCHEMA.OBJECT - fully qualified reference to a table/view/procedure
- Two-part name with double dots: DATABASE..OBJECT - references object in the specified database (searches across schemas or uses default schema depending on configuration)
- Two-part name with single dot: SCHEMA.OBJECT - uses current/default database with specified schema
- Single name: OBJECT - uses current database and current schema
- System views like _V_TABLE, _V_VIEW, _V_PROCEDURE are in each database; use DATABASE.._V_TABLE to query a specific database's system views
- DATABASE..TABLE syntax is valid and CORRECT in Netezza - do NOT "fix" it by adding a schema name!
- Netezza supports: DISTRIBUTE ON, ORGANIZE ON, GROOM TABLE, GENERATE STATISTICS, zone maps, etc.

USER REQUEST:
${userDescription}

AVAILABLE DATABASE SCHEMA:
\`\`\`
${schemaOverview}
\`\`\`

INSTRUCTIONS:
1. Analyze the user's request and identify which tables and columns are relevant
2. Generate a complete, executable SQL query for Netezza
3. Use proper JOIN conditions based on likely relationships (matching column names like ID fields)
4. Include appropriate WHERE clauses, GROUP BY, ORDER BY as needed
5. Use table aliases for readability
6. Add comments explaining the query logic

If the request is ambiguous, ask clarifying questions before generating the SQL.
If certain tables or columns seem missing, suggest what additional information might be needed.

Please generate the SQL query:`;
    }

    /**
     * Sends message to Copilot Chat with custom prompt (for SQL generation)
     */
    private async sendToChatInteractiveWithCustomPrompt(
        _context: CopilotContext,
        customPrompt: string,
        title: string
    ): Promise<void> {
        try {
            // For SQL generation, we use the custom prompt directly
            // The schema is already embedded in the prompt
            const fullPrompt = customPrompt;

            // Open Copilot Chat with the prompt
            await vscode.commands.executeCommand(
                'workbench.action.chat.open',
                { query: fullPrompt }
            );

            vscode.window.showInformationMessage(`✅ ${title} sent to Copilot Chat. Describe your query requirements for interactive SQL generation.`);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to open Copilot Chat: ${msg}`);
        }
    }

    /**
     * Diagnostic method: Shows available Language Models
     */
    async showAvailableModels(): Promise<void> {
        try {
            const allModels = await vscode.lm.selectChatModels();

            if (allModels.length === 0) {
                vscode.window.showWarningMessage('No Language Models available');
                return;
            }

            const modelInfo = allModels.map(m =>
                `• ${m.id}\n  Vendor: ${m.vendor}\n  Family: ${m.family}\n`
            ).join('\n');

            const copilotModels = allModels.filter(m => m.vendor === 'copilot');

            const doc = await vscode.workspace.openTextDocument({
                content: `# Available Language Models\n\n` +
                    `Total models: ${allModels.length}\n` +
                    `Copilot models: ${copilotModels.length}\n\n` +
                    `## All Models\n\n${modelInfo}`,
                language: 'markdown'
            });

            await vscode.window.showTextDocument(doc);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error getting models: ${msg}`);
        }
    }

    /**
     * Sends message to Copilot Chat for interactive discussion
     * Similar to describeDataWithCopilot but for SQL queries
     */
    private async sendToChatInteractive(context: CopilotContext, userPrompt: string, title: string): Promise<void> {
        try {
            const systemPrompt = this.buildSystemPrompt(context);
            const fullPrompt = `${systemPrompt}\n\n${userPrompt}`;

            // Open Copilot Chat with the prompt
            await vscode.commands.executeCommand(
                'workbench.action.chat.open',
                { query: fullPrompt }
            );

            vscode.window.showInformationMessage(`✅ ${title} sent to Copilot Chat. Check the Chat panel for interactive discussion.`);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to open Copilot Chat: ${msg}`);
        }
    }

    private async showDiff(document: vscode.TextDocument, newContent: string): Promise<void> {
        const editor = vscode.window.activeTextEditor;

        let rangeToReplace: vscode.Range;

        // Determine if we are replacing selection or full document
        if (editor && !editor.selection.isEmpty) {
            rangeToReplace = editor.selection;
        } else {
            rangeToReplace = new vscode.Range(
                new vscode.Position(0, 0),
                new vscode.Position(document.lineCount, 0)
            );
        }

        try {
            // Create a temporary document with suggested changes
            const originalText = document.getText(rangeToReplace);

            // Check if there are any changes
            if (originalText.trim() === newContent.trim()) {
                vscode.window.showInformationMessage('No changes detected - content is identical.');
                return;
            }

            // Create an untitled document with the new content
            const newDoc = await vscode.workspace.openTextDocument({
                content: newContent,
                language: document.languageId
            });

            // Create a temporary copy of the original for diff comparison
            const originalDoc = await vscode.workspace.openTextDocument({
                content: originalText,
                language: document.languageId
            });

            // Show diff editor
            await vscode.commands.executeCommand(
                'vscode.diff',
                originalDoc.uri,
                newDoc.uri,
                `AI Suggestions: ${document.fileName.split(/[\\\\/]/).pop() || 'file'}`,
                { preview: true }
            );

            // Ask user what to do with the changes (modal dialog - stays visible)
            const choice = await vscode.window.showInformationMessage(
                'Review the AI-suggested changes in the diff editor.',
                { modal: true },
                'Apply Changes',
                'Apply & Close Diff',
                'Discard'
            );

            if (choice === 'Apply Changes' || choice === 'Apply & Close Diff') {
                const edit = new vscode.WorkspaceEdit();
                edit.replace(document.uri, rangeToReplace, newContent);
                const success = await vscode.workspace.applyEdit(edit);

                if (success) {
                    vscode.window.showInformationMessage('✅ Changes applied successfully.');

                    if (choice === 'Apply & Close Diff') {
                        // Close the diff editor tabs
                        await vscode.commands.executeCommand('workbench.action.closeActiveEditor');
                    }
                } else {
                    vscode.window.showErrorMessage('Failed to apply changes.');
                }
            } else if (choice === 'Discard') {
                // Close the diff editor
                await vscode.commands.executeCommand('workbench.action.closeActiveEditor');
            }
        } catch (e) {
            console.error('Diff error:', e);
            vscode.window.showErrorMessage(`Failed to show diff: ${e instanceof Error ? e.message : String(e)}`);
        }
    }

    /**
     * Sends data to Copilot Chat for description and analysis
     * @param data The data rows to describe (array of objects)
     * @param sql Optional SQL query that generated the data
     */
    async describeDataWithCopilot(data: Record<string, unknown>[], sql?: string): Promise<void> {
        try {
            if (!data || data.length === 0) {
                vscode.window.showWarningMessage('No data to describe');
                return;
            }

            // Privacy confirmation - user must accept before sending data
            const rowCount = data.length;
            const columnCount = Object.keys(data[0] || {}).length;
            const dataSize = rowCount > 50 ? '50 (limited for context)' : rowCount;

            const confirmed = await vscode.window.showWarningMessage(
                `⚠️ Privacy Notice: You are about to send ${dataSize} rows with ${columnCount} columns to GitHub Copilot AI.\n\n` +
                `This data will be transmitted to external servers for analysis. ` +
                `Please ensure the data does NOT contain sensitive, confidential, or personally identifiable information.\n\n` +
                `Do you want to proceed?`,
                { modal: true },
                'Yes, Send to Copilot',
                'Cancel'
            );

            if (confirmed !== 'Yes, Send to Copilot') {
                vscode.window.showInformationMessage('Data analysis cancelled - no data was sent.');
                return;
            }

            // Convert data to markdown table
            const markdown = this.convertDataToMarkdown(data);

            // Build prompt
            let prompt = `Describe and analyze the following data from IBM Netezza Performance Server:\n\n`;

            if (sql) {
                prompt += `**Source Query:**\n\`\`\`sql\n${sql}\n\`\`\`\n\n`;
            }

            prompt += `**Data (${data.length} rows):**\n\n${markdown}\n\n`;
            prompt += `Please provide:\n`;
            prompt += `1. A summary of the data patterns and key observations\n`;
            prompt += `2. Any notable trends, outliers, or anomalies\n`;
            prompt += `3. Suggestions for further analysis if applicable`;

            // Open Copilot Chat with the prompt (this does NOT paste into SQL editor)
            await vscode.commands.executeCommand(
                'workbench.action.chat.open',
                { query: prompt }
            );

            vscode.window.showInformationMessage('✅ Data sent to Copilot Chat for analysis. Check the Chat panel for results.');
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to send data to Copilot: ${msg}`);
        }
    }

    /**
     * Fixes SQL error by sending error message and SQL to Copilot Chat
     * Called from Results panel when an error occurs
     * @param errorMessage The error message from the database
     * @param sql The SQL that caused the error
     */
    async fixSqlError(errorMessage: string, sql: string): Promise<void> {
        try {
            if (!sql.trim()) {
                vscode.window.showWarningMessage('No SQL to fix');
                return;
            }

            // Show progress while gathering DDL
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: 'Gathering table DDL for Copilot...',
                cancellable: false
            }, async () => {
                // Get language preference for response
                const config = vscode.workspace.getConfiguration('justyBaseLite.copilot');
                const preferredLanguage = config.get<string>('preferredLanguage') || 'english';

                let languageInstruction = '';
                if (preferredLanguage === 'system') {
                    const displayLanguage = vscode.env.language;
                    if (displayLanguage && !displayLanguage.startsWith('en')) {
                        languageInstruction = `\n\nPlease respond in ${displayLanguage} language.`;
                    }
                }

                // Extract table references and gather DDL (same as other Copilot functions)
                const tableRefs = this.extractTableReferences(sql);

                // Get active connection for DDL gathering
                const connectionName = this.connectionManager.getActiveConnectionName() || undefined;
                const ddlContext = await this.gatherTablesDDL(tableRefs, connectionName);

                const fixPrompt = this.getPrompt('fix');

                // Build comprehensive prompt with DDL context
                let prompt = `${fixPrompt}${languageInstruction}

IMPORTANT NETEZZA SQL NAMING CONVENTIONS:
- Three-part name: DATABASE.SCHEMA.OBJECT - fully qualified reference to a table/view/procedure
- Two-part name with double dots: DATABASE..OBJECT - references object in the specified database (searches across schemas or uses default schema depending on configuration)
- Two-part name with single dot: SCHEMA.OBJECT - uses current/default database with specified schema
- Single name: OBJECT - uses current database and current schema
- System views like _V_TABLE, _V_VIEW, _V_PROCEDURE are in each database; use DATABASE.._V_TABLE to query a specific database's system views
- DATABASE..TABLE syntax is valid and CORRECT in Netezza - do NOT "fix" it by adding a schema name!
- Netezza supports specific SQL extensions: DISTRIBUTE ON, ORGANIZE ON, GROOM TABLE, GENERATE STATISTICS, etc.

**Error from IBM Netezza:**
\`\`\`
${errorMessage}
\`\`\`

**SQL Query that caused the error:**
\`\`\`sql
${sql}
\`\`\`
`;

                // Add DDL context if available
                if (ddlContext && !ddlContext.includes('No table references') && !ddlContext.includes('Could not gather DDL')) {
                    prompt += `
**Referenced Table Schemas (DDL):**
\`\`\`sql
${ddlContext}
\`\`\`
`;
                }

                // Add NZPLSQL reference if code contains stored procedure
                if (this.isProcedureCode(sql)) {
                    prompt += `\n${this.NZPLSQL_PROCEDURE_REFERENCE}\n`;
                }

                prompt += `
Please:
1. Explain what caused this error
2. Provide the corrected SQL query
3. Explain the fix you made`;

                // Open Copilot Chat with the prompt
                await vscode.commands.executeCommand(
                    'workbench.action.chat.open',
                    { query: prompt }
                );
            });

            vscode.window.showInformationMessage('✅ Error sent to Copilot Chat for fixing. Check the Chat panel.');
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to send error to Copilot: ${msg}`);
        }
    }

    /**
     * Converts data array to markdown table format
     */
    private convertDataToMarkdown(data: Record<string, unknown>[]): string {
        if (data.length === 0) {
            return '*No data*';
        }

        // Limit to first 50 rows for context length
        const displayData = data.slice(0, 50);
        const hasMore = data.length > 50;

        // Get column names from first row
        const columns = Object.keys(displayData[0]);

        // Build header
        let markdown = '| ' + columns.join(' | ') + ' |\n';
        markdown += '| ' + columns.map(() => '---').join(' | ') + ' |\n';

        // Build rows
        for (const row of displayData) {
            const values = columns.map(col => {
                const val = row[col];
                if (val === null || val === undefined) {
                    return 'NULL';
                }
                // Escape pipe characters and limit length
                const str = String(val).replace(/\|/g, '\\|');
                return str.length > 100 ? str.substring(0, 97) + '...' : str;
            });
            markdown += '| ' + values.join(' | ') + ' |\n';
        }

        if (hasMore) {
            markdown += `\n*... and ${data.length - 50} more rows (total: ${data.length} rows)*`;
        }

        return markdown;
    }

    /**
     * Registers the @sql-copilot chat participant with handlers for /schema, /optimize, /fix, /explain commands.
     * This allows users to use #schema-like functionality through slash commands in the Copilot Chat.
     * 
     * Usage in chat:
     * - @sql-copilot /schema - Shows DDL for tables in current SQL file
     * - @sql-copilot /optimize <question> - Optimizes SQL with Netezza best practices
     * - @sql-copilot /fix <question> - Fixes SQL syntax errors
     * - @sql-copilot /explain - Explains what the SQL does
     */
    public registerChatParticipant(extensionContext: vscode.ExtensionContext): vscode.Disposable | undefined {
        try {
            // Create chat participant handler
            const handler: vscode.ChatRequestHandler = async (
                request: vscode.ChatRequest,
                chatContext: vscode.ChatContext,
                stream: vscode.ChatResponseStream,
                token: vscode.CancellationToken
            ) => {
                try {
                    // Handle different commands
                    if (request.command === 'schema') {
                        return await this.handleSchemaCommand(request, chatContext, stream, token);
                    } else if (request.command === 'optimize') {
                        return await this.handleOptimizeCommand(request, chatContext, stream, token);
                    } else if (request.command === 'fix') {
                        return await this.handleFixCommand(request, chatContext, stream, token);
                    } else if (request.command === 'explain') {
                        return await this.handleExplainCommand(request, chatContext, stream, token);
                    } else {
                        // Default: handle as general SQL question with context
                        return await this.handleGeneralQuery(request, chatContext, stream, token);
                    }
                } catch (e) {
                    const msg = e instanceof Error ? e.message : String(e);
                    stream.markdown(`❌ Error: ${msg}`);
                    return { metadata: { error: msg } };
                }
            };

            // Create the chat participant
            const participant = vscode.chat.createChatParticipant('netezza.sqlcopilot', handler);
            participant.iconPath = vscode.Uri.joinPath(extensionContext.extensionUri, 'Icon.png');

            // Add follow-up suggestions
            participant.followupProvider = {
                provideFollowups: (result, _context, _token) => {
                    const metadata = result.metadata as { command?: string };
                    if (metadata?.command === 'schema') {
                        return [
                            { prompt: 'Optimize the query for these tables', label: 'Optimize query', command: 'optimize' },
                            { prompt: 'Explain how these tables relate', label: 'Explain schema' }
                        ];
                    }
                    return [];
                }
            };

            console.log('[CopilotService] Chat participant @sql-copilot registered successfully');
            return participant;
        } catch (e) {
            console.error('[CopilotService] Failed to register chat participant:', e);
            return undefined;
        }
    }

    /**
     * Handles /schema command - extracts tables from current SQL and returns their DDL
     */
    private async handleSchemaCommand(
        request: vscode.ChatRequest,
        _chatContext: vscode.ChatContext,
        stream: vscode.ChatResponseStream,
        _token: vscode.CancellationToken
    ): Promise<vscode.ChatResult> {
        stream.progress('Analyzing SQL for table references...');

        // Get current editor content
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            stream.markdown('⚠️ No SQL file is currently open. Please open a SQL file first.');
            return { metadata: { command: 'schema', success: false } };
        }

        const document = editor.document;
        const selection = editor.selection;
        const sql = selection.isEmpty ? document.getText() : document.getText(selection);

        if (!sql.trim()) {
            stream.markdown('⚠️ No SQL content found. Please enter some SQL or open a SQL file.');
            return { metadata: { command: 'schema', success: false } };
        }

        // Extract table references
        const tableRefs = this.extractTableReferences(sql);

        if (tableRefs.length === 0) {
            stream.markdown('ℹ️ No table references found in the current SQL.\n\nMake sure your SQL contains `FROM`, `JOIN`, `INSERT INTO`, `UPDATE`, or `DELETE FROM` clauses.');
            return { metadata: { command: 'schema', success: false } };
        }

        stream.progress(`Found ${tableRefs.length} table(s). Fetching DDL...`);

        // Get connection name
        const connectionName = this.connectionManager.getDocumentConnection(document.uri.toString())
            || this.connectionManager.getActiveConnectionName()
            || undefined;

        // Gather DDL
        const ddlContext = await this.gatherTablesDDL(tableRefs, connectionName);

        // Format response
        stream.markdown(`## 📊 Schema Context for Current SQL\n\n`);
        stream.markdown(`**Connection:** ${connectionName || 'Not connected'}\n\n`);
        stream.markdown(`**Tables found:** ${tableRefs.map(t => `\`${t.database ? t.database + '.' : ''}${t.schema ? t.schema + '.' : ''}${t.name}\``).join(', ')}\n\n`);

        if (ddlContext.includes('CREATE TABLE') || ddlContext.includes('-- Table:')) {
            stream.markdown(`### Table Definitions (DDL)\n\n\`\`\`sql\n${ddlContext}\n\`\`\`\n`);
        } else {
            stream.markdown(`### Schema Information\n\n${ddlContext}\n`);
        }

        // Add reference to the file
        if (document.uri.scheme === 'file') {
            stream.reference(document.uri);
        }

        // If user provided additional prompt, add that context
        if (request.prompt.trim()) {
            stream.markdown(`\n---\n\n**Your question:** ${request.prompt}\n\n`);
            stream.markdown(`*Use the schema information above to answer your question about the SQL.*`);
        }

        return { metadata: { command: 'schema', success: true, tableCount: tableRefs.length } };
    }

    /**
     * Handles /optimize command - optimizes SQL with Netezza best practices
     */
    private async handleOptimizeCommand(
        request: vscode.ChatRequest,
        _chatContext: vscode.ChatContext,
        stream: vscode.ChatResponseStream,
        token: vscode.CancellationToken
    ): Promise<vscode.ChatResult> {
        stream.progress('Gathering context and optimizing SQL...');

        const context = await this.gatherContext();
        const basePrompt = this.getPrompt('optimize');
        let prompt = `${basePrompt}\n\n${this.NETEZZA_OPTIMIZATION_RULES}`;

        if (this.isProcedureCode(context.selectedSql)) {
            prompt += `\n\n${this.NZPLSQL_PROCEDURE_REFERENCE}`;
        }

        if (request.prompt.trim()) {
            prompt += `\n\nAdditional user instructions: ${request.prompt}`;
        }

        // Build messages for the model
        const systemPrompt = this.buildSystemPrompt(context);
        const fullPrompt = `${systemPrompt}\n\n${prompt}`;

        const messages = [vscode.LanguageModelChatMessage.User(fullPrompt)];

        // Use the request's model to generate response
        const response = await request.model.sendRequest(messages, {}, token);

        for await (const chunk of response.text) {
            stream.markdown(chunk);
        }

        return { metadata: { command: 'optimize', success: true } };
    }

    /**
     * Handles /fix command - fixes SQL syntax errors
     */
    private async handleFixCommand(
        request: vscode.ChatRequest,
        _chatContext: vscode.ChatContext,
        stream: vscode.ChatResponseStream,
        token: vscode.CancellationToken
    ): Promise<vscode.ChatResult> {
        stream.progress('Analyzing SQL for errors...');

        const context = await this.gatherContext();
        let prompt = this.getPrompt('fix');

        if (this.isProcedureCode(context.selectedSql)) {
            prompt += `\n\n${this.NZPLSQL_PROCEDURE_REFERENCE}`;
        }

        if (request.prompt.trim()) {
            prompt += `\n\nAdditional context about the error: ${request.prompt}`;
        }

        const systemPrompt = this.buildSystemPrompt(context);
        const fullPrompt = `${systemPrompt}\n\n${prompt}`;

        const messages = [vscode.LanguageModelChatMessage.User(fullPrompt)];
        const response = await request.model.sendRequest(messages, {}, token);

        for await (const chunk of response.text) {
            stream.markdown(chunk);
        }

        return { metadata: { command: 'fix', success: true } };
    }

    /**
     * Handles /explain command - explains what the SQL does
     */
    private async handleExplainCommand(
        request: vscode.ChatRequest,
        _chatContext: vscode.ChatContext,
        stream: vscode.ChatResponseStream,
        token: vscode.CancellationToken
    ): Promise<vscode.ChatResult> {
        stream.progress('Analyzing SQL...');

        const context = await this.gatherContext();
        let prompt = this.getPrompt('explain');

        if (request.prompt.trim()) {
            prompt += `\n\nFocus on: ${request.prompt}`;
        }

        const systemPrompt = this.buildSystemPrompt(context);
        const fullPrompt = `${systemPrompt}\n\n${prompt}`;

        const messages = [vscode.LanguageModelChatMessage.User(fullPrompt)];
        const response = await request.model.sendRequest(messages, {}, token);

        for await (const chunk of response.text) {
            stream.markdown(chunk);
        }

        return { metadata: { command: 'explain', success: true } };
    }

    /**
     * Handles general queries without specific command
     */
    private async handleGeneralQuery(
        request: vscode.ChatRequest,
        _chatContext: vscode.ChatContext,
        stream: vscode.ChatResponseStream,
        token: vscode.CancellationToken
    ): Promise<vscode.ChatResult> {
        stream.progress('Processing your SQL question...');

        // Try to gather context if there's an open SQL file
        let context: CopilotContext;
        try {
            context = await this.gatherContext();
        } catch {
            // No active editor or no SQL - use minimal context
            const connectionName = this.connectionManager.getActiveConnectionName();
            context = {
                selectedSql: '',
                ddlContext: 'No SQL file open',
                variables: '',
                recentQueries: '',
                connectionInfo: connectionName ? `Connected to: ${connectionName}` : 'No connection'
            };
        }

        const systemPrompt = this.buildSystemPrompt(context);
        const fullPrompt = `${systemPrompt}\n\nUser question: ${request.prompt}`;

        const messages = [vscode.LanguageModelChatMessage.User(fullPrompt)];
        const response = await request.model.sendRequest(messages, {}, token);

        for await (const chunk of response.text) {
            stream.markdown(chunk);
        }

        return { metadata: { command: 'general', success: true } };
    }

    /**
     * Gets schema context for provided SQL string.
     * Used by SchemaTool when SQL is passed as parameter.
     */
    public async getSchemaForSql(sql: string): Promise<string> {
        if (!sql.trim()) {
            return 'No SQL content provided.';
        }

        const tableRefs = this.extractTableReferences(sql);

        if (tableRefs.length === 0) {
            return 'No table references found in the provided SQL.';
        }

        const connectionName = this.connectionManager.getActiveConnectionName() || undefined;
        const ddlContext = await this.gatherTablesDDL(tableRefs, connectionName);

        return `Tables: ${tableRefs.map(t => t.name).join(', ')}\n\n${ddlContext}`;
    }

    /**
     * Gets schema context as a string for use in chat variables.
     * This method can be called externally to get the schema context for the current SQL.
     */
    public async getSchemaContextForCurrentSql(): Promise<string> {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            return 'No SQL file is currently open.';
        }

        const document = editor.document;
        const selection = editor.selection;
        const sql = selection.isEmpty ? document.getText() : document.getText(selection);

        if (!sql.trim()) {
            return 'No SQL content found.';
        }

        const tableRefs = this.extractTableReferences(sql);

        if (tableRefs.length === 0) {
            return 'No table references found in the current SQL.';
        }

        const connectionName = this.connectionManager.getDocumentConnection(document.uri.toString())
            || this.connectionManager.getActiveConnectionName()
            || undefined;

        const ddlContext = await this.gatherTablesDDL(tableRefs, connectionName);

        return `Tables: ${tableRefs.map(t => t.name).join(', ')}\n\n${ddlContext}`;
    }

    /**
     * Gets column metadata for specified tables.
     * Used by ColumnsTool to fetch column definitions.
     */
    public async getColumnsForTables(tables: string[], database?: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        // Convert table strings to TableReference objects
        const tableRefs: TableReference[] = [];

        for (const tableSpec of tables) {
            // Parse table specification: can be TABLE, SCHEMA.TABLE, or DB.SCHEMA.TABLE
            const parts = tableSpec.split('.');
            let db: string | undefined;
            let schema: string | undefined;
            let tableName: string;

            if (parts.length === 3) {
                [db, schema, tableName] = parts;
            } else if (parts.length === 2) {
                [schema, tableName] = parts;
                db = database;
            } else {
                tableName = parts[0];
                db = database;
            }

            tableRefs.push({
                database: db,
                schema: schema || undefined,
                name: tableName
            });
        }

        if (tableRefs.length === 0) {
            return 'No valid table references provided.';
        }

        // Use existing gatherTablesDDL method which handles connection and caching
        const ddlContext = await this.gatherTablesDDL(tableRefs, connectionName);

        return ddlContext;
    }

    /**
     * Gets list of tables from a database or all databases.
     * Used by TablesTool to list available tables.
     * When database is not specified, returns tables from ALL accessible databases.
     */
    public async getTablesFromDatabase(database?: string, schema?: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            // Try cache first if database is specified
            if (this.metadataCache && database) {
                // If schema provided, use exact lookup
                if (schema) {
                    const cacheKey = `${database}.${schema}`;
                    const cachedTables = this.metadataCache.getTables(connectionName, cacheKey);
                    if (cachedTables && cachedTables.length > 0) {
                        const lines: string[] = [`## Tables in ${database}.${schema}\n`];
                        lines.push('| Schema | Table | Type | Owner |');
                        lines.push('|--------|-------|------|-------|');
                        for (const t of cachedTables) {
                            const type = t.objType === 'VIEW' ? 'VIEW' : 'TABLE';
                            lines.push(`| ${schema} | ${t.label} | ${type} | ${t.OWNER || ''} |`);
                        }
                        lines.push(`\n**Total:** ${cachedTables.length} table(s)`);
                        return lines.join('\n');
                    }
                } else {
                    // Start prefetch for all objects if not already done, to ensure cache is warm for next time
                    if (!this.metadataCache.hasConnectionPrefetchTriggered(connectionName)) {
                        // Don't wait for prefetch here to avoid blocking, but trigger it
                        // However, for this call, we may need to fall back to DB if cache is empty
                    }

                    // Note: querying all tables in a DB from cache requires iterating all schemas in cache
                    // getTablesAllSchemas does exactly this
                    const cachedTables = this.metadataCache.getTablesAllSchemas(connectionName, database);
                    if (cachedTables && cachedTables.length > 0) {
                        const lines: string[] = [`## Tables in ${database} (All Schemas)\n`];
                        lines.push('| Schema | Table | Type | Owner |');
                        lines.push('|--------|-------|------|-------|');
                        for (const t of cachedTables) {
                            const type = t.objType === 'VIEW' ? 'VIEW' : 'TABLE';
                            // We need schema name here, but TableMetadata doesn't always have it if it comes from a list
                            // But getTablesAllSchemas returns TableMetadata which should have OBJNAME.
                            // The cache structure is per schema.
                            // Let's check TableMetadata definition. It has SCHEMA properly.
                            lines.push(`| ${t.SCHEMA || ''} | ${t.label} | ${type} | ${t.OWNER || ''} |`);
                        }
                        lines.push(`\n**Total:** ${cachedTables.length} table(s)`);
                        return lines.join('\n');
                    }
                }
            }

            // Fallback to live query
            // Get connection details
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            // Execute query using connection
            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                // Build query to get tables
                // When database is not specified, search across ALL databases using global system view
                let query: string;
                let headerText: string;
                const searchAllDatabases = !database;

                if (searchAllDatabases) {
                    // Use global _V_OBJECT_DATA view to search across all databases
                    if (schema) {
                        query = `
                            SELECT 
                                DBNAME AS database_name,
                                SCHEMA AS schema_name,
                                OBJNAME AS table_name,
                                OBJTYPE AS object_type,
                                OWNER AS owner
                            FROM ${NZ_SYSTEM_VIEWS.OBJECT_DATA}
                            WHERE OBJTYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'EXTERNAL TABLE')
                            AND SCHEMA = '${schema.toUpperCase()}'
                            ORDER BY DBNAME, SCHEMA, OBJNAME
                            LIMIT 500
                        `;
                        headerText = `## Tables in all databases, schema ${schema}\n`;
                    } else {
                        query = `
                            SELECT 
                                DBNAME AS database_name,
                                SCHEMA AS schema_name,
                                OBJNAME AS table_name,
                                OBJTYPE AS object_type,
                                OWNER AS owner
                            FROM ${NZ_SYSTEM_VIEWS.OBJECT_DATA}
                            WHERE OBJTYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'EXTERNAL TABLE')
                            ORDER BY DBNAME, SCHEMA, OBJNAME
                            LIMIT 500
                        `;
                        headerText = '## Tables in all databases\n';
                    }
                } else {
                    // Search in specific database
                    if (schema) {
                        query = `
                            SELECT 
                                '${database}' AS database_name,
                                SCHEMA AS schema_name,
                                TABLENAME AS table_name,
                                CASE RELKIND 
                                    WHEN 'r' THEN 'TABLE'
                                    WHEN 'v' THEN 'VIEW'
                                    WHEN 'm' THEN 'MATERIALIZED VIEW'
                                    WHEN 'e' THEN 'EXTERNAL TABLE'
                                    ELSE RELKIND
                                END AS object_type,
                                OWNER AS owner
                            FROM ${database}..${NZ_SYSTEM_VIEWS.TABLE}
                            WHERE SCHEMA = '${schema.toUpperCase()}'
                            ORDER BY SCHEMA, TABLENAME
                        `;
                    } else {
                        query = `
                            SELECT 
                                '${database}' AS database_name,
                                SCHEMA AS schema_name,
                                TABLENAME AS table_name,
                                CASE RELKIND 
                                    WHEN 'r' THEN 'TABLE'
                                    WHEN 'v' THEN 'VIEW'
                                    WHEN 'm' THEN 'MATERIALIZED VIEW'
                                    WHEN 'e' THEN 'EXTERNAL TABLE'
                                    ELSE RELKIND
                                END AS object_type,
                                OWNER AS owner
                            FROM ${database}..${NZ_SYSTEM_VIEWS.TABLE}
                            ORDER BY SCHEMA, TABLENAME
                        `;
                    }
                    headerText = `## Tables in ${database}${schema ? '.' + schema : ''}\n`;
                }

                const result = await executeQueryHelper(connection, query);

                if (!result || result.length === 0) {
                    const schemaInfo = schema ? ` in schema ${schema}` : '';
                    const dbInfo = database ? ` in database ${database}` : ' across all databases';
                    return `No tables found${dbInfo}${schemaInfo}.`;
                }

                // Format results as markdown table
                const lines: string[] = [
                    headerText,
                    '| Database | Schema | Table Name | Type | Owner |',
                    '|----------|--------|------------|------|-------|'
                ];

                for (const row of result) {
                    const dbName = (row as Record<string, unknown>).DATABASE_NAME || (row as Record<string, unknown>).database_name || '';
                    const schemaName = (row as Record<string, unknown>).SCHEMA_NAME || (row as Record<string, unknown>).schema_name || '';
                    const tableName = (row as Record<string, unknown>).TABLE_NAME || (row as Record<string, unknown>).table_name || '';
                    const objType = (row as Record<string, unknown>).OBJECT_TYPE || (row as Record<string, unknown>).object_type || '';
                    const owner = (row as Record<string, unknown>).OWNER || (row as Record<string, unknown>).owner || '';
                    lines.push(`| ${dbName} | ${schemaName} | ${tableName} | ${objType} | ${owner} |`);
                }

                lines.push(`\n**Total:** ${result.length} object(s)`);
                if (searchAllDatabases && result.length === 500) {
                    lines.push('\n*Note: Results limited to 500. Specify a database name to see all objects.*');
                }

                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to retrieve tables: ${msg}`;
        }
    }

    /**
     * Gets list of all databases accessible via the connection.
     * Used by DatabasesTool.
     */
    public async getDatabases(): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        // Try cache first
        if (this.metadataCache) {
            const cached = this.metadataCache.getDatabases(connectionName);
            if (cached && cached.length > 0) {
                const lines: string[] = ['## Available Databases (from Cache)\n'];
                for (const db of cached) {
                    lines.push(`- ${db.DATABASE}`);
                }
                lines.push(`\n**Total:** ${cached.length} database(s)`);
                return lines.join('\n');
            }
        }

        try {
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                const query = `
                    SELECT DATABASE, OWNER, CREATEDATE
                    FROM ${NZ_SYSTEM_VIEWS.DATABASE}
                    ORDER BY DATABASE
                `;
                const result = await executeQueryHelper(connection, query);

                if (!result || result.length === 0) {
                    return 'No databases found.';
                }

                const lines: string[] = [
                    '## Databases\n',
                    '| Database | Owner | Created |',
                    '|----------|-------|---------|'
                ];

                for (const row of result) {
                    const r = row as Record<string, unknown>;
                    const createDate = r.CREATEDATE ? String(r.CREATEDATE).substring(0, 10) : '';
                    lines.push(`| ${r.DATABASE} | ${r.OWNER} | ${createDate} |`);
                }

                lines.push(`\n**Total:** ${result.length} database(s)`);
                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to retrieve databases: ${msg}`;
        }
    }

    /**
     * Gets list of schemas in a database.
     * Used by SchemasTool.
     */
    public async getSchemas(database?: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            // Determine database
            let targetDb = database;
            if (!targetDb) {
                targetDb = await this.connectionManager.getCurrentDatabase(connectionName) || undefined;
            }

            if (targetDb && this.metadataCache) {
                const cached = this.metadataCache.getSchemas(connectionName, targetDb);
                // Check if cache actually has schemas (might return empty array if no schemas, which is valid, but we need to know if it was fetched)
                // getSchemas returns undefined if not cached
                if (cached) {
                    const lines: string[] = [`## Schemas in ${targetDb} (from Cache)\n`];
                    for (const s of cached) {
                        lines.push(`- ${s.SCHEMA}`);
                    }
                    lines.push(`\n**Total:** ${cached.length} schema(s)`);
                    return lines.join('\n');
                }
            }

            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                let query: string;
                let headerText: string;

                if (database) {
                    query = `
                        SELECT SCHEMA, OWNER, CREATEDATE
                        FROM ${database}..${NZ_SYSTEM_VIEWS.SCHEMA}
                        ORDER BY SCHEMA
                    `;
                    headerText = `## Schemas in ${database}\n`;
                } else {
                    // Get schemas from all databases using global view
                    query = `
                        SELECT DISTINCT DBNAME AS DATABASE, SCHEMA, OWNER
                        FROM ${NZ_SYSTEM_VIEWS.OBJECT_DATA}
                        WHERE OBJTYPE IN ('TABLE', 'VIEW', 'PROCEDURE')
                        ORDER BY DBNAME, SCHEMA
                        LIMIT 200
                    `;
                    headerText = '## Schemas across all databases\n';
                }

                const result = await executeQueryHelper(connection, query);

                if (!result || result.length === 0) {
                    return database ? `No schemas found in database ${database}.` : 'No schemas found.';
                }

                const lines: string[] = [headerText];

                if (database) {
                    lines.push('| Schema | Owner | Created |');
                    lines.push('|--------|-------|---------|');
                    for (const row of result) {
                        const r = row as Record<string, unknown>;
                        const createDate = r.CREATEDATE ? String(r.CREATEDATE).substring(0, 10) : '';
                        lines.push(`| ${r.SCHEMA} | ${r.OWNER} | ${createDate} |`);
                    }
                } else {
                    lines.push('| Database | Schema | Owner |');
                    lines.push('|----------|--------|-------|');
                    for (const row of result) {
                        const r = row as Record<string, unknown>;
                        lines.push(`| ${r.DATABASE} | ${r.SCHEMA} | ${r.OWNER} |`);
                    }
                }

                lines.push(`\n**Total:** ${result.length} schema(s)`);
                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to retrieve schemas: ${msg}`;
        }
    }

    /**
     * Gets list of procedures.
     * Used by ProceduresTool.
     */
    public async getProcedures(database?: string, schema?: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                let query: string;
                let headerText: string;
                const searchAllDatabases = !database;

                if (searchAllDatabases) {
                    // Search across all databases
                    query = `
                        SELECT DBNAME AS DATABASE, SCHEMA, OBJNAME AS PROCEDURE_NAME, OWNER
                        FROM ${NZ_SYSTEM_VIEWS.OBJECT_DATA}
                        WHERE OBJTYPE = 'PROCEDURE'
                        ${schema ? `AND SCHEMA = '${schema.toUpperCase()}'` : ''}
                        ORDER BY DBNAME, SCHEMA, OBJNAME
                        LIMIT 200
                    `;
                    headerText = schema ? `## Procedures in schema ${schema} across all databases\n` : '## Procedures across all databases\n';
                } else {
                    query = `
                        SELECT '${database}' AS DATABASE, SCHEMA, PROCEDURE AS PROCEDURE_NAME, 
                               PROCEDURESIGNATURE, RETURNS, OWNER, DESCRIPTION
                        FROM ${database}..${NZ_SYSTEM_VIEWS.PROCEDURE}
                        ${schema ? `WHERE SCHEMA = '${schema.toUpperCase()}'` : ''}
                        ORDER BY SCHEMA, PROCEDURE
                        LIMIT 200
                    `;
                    headerText = schema ? `## Procedures in ${database}.${schema}\n` : `## Procedures in ${database}\n`;
                }

                const result = await executeQueryHelper(connection, query);

                if (!result || result.length === 0) {
                    return 'No procedures found.';
                }

                const lines: string[] = [headerText];

                if (searchAllDatabases) {
                    lines.push('| Database | Schema | Procedure | Owner |');
                    lines.push('|----------|--------|-----------|-------|');
                    for (const row of result) {
                        const r = row as Record<string, unknown>;
                        lines.push(`| ${r.DATABASE} | ${r.SCHEMA} | ${r.PROCEDURE_NAME} | ${r.OWNER} |`);
                    }
                } else {
                    lines.push('| Schema | Procedure | Signature | Returns | Owner |');
                    lines.push('|--------|-----------|-----------|---------|-------|');
                    for (const row of result) {
                        const r = row as Record<string, unknown>;
                        const sig = r.PROCEDURESIGNATURE ? String(r.PROCEDURESIGNATURE).substring(0, 50) : '';
                        lines.push(`| ${r.SCHEMA} | ${r.PROCEDURE_NAME} | ${sig} | ${r.RETURNS || ''} | ${r.OWNER} |`);
                    }
                }

                lines.push(`\n**Total:** ${result.length} procedure(s)`);
                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to retrieve procedures: ${msg}`;
        }
    }

    /**
     * Gets list of views.
     * Used by ViewsTool.
     */
    public async getViews(database?: string, schema?: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                let query: string;
                let headerText: string;
                const searchAllDatabases = !database;

                if (searchAllDatabases) {
                    // Search across all databases
                    query = `
                        SELECT DBNAME AS DATABASE, SCHEMA, OBJNAME AS VIEW_NAME, OWNER
                        FROM ${NZ_SYSTEM_VIEWS.OBJECT_DATA}
                        WHERE OBJTYPE = 'VIEW'
                        ${schema ? `AND SCHEMA = '${schema.toUpperCase()}'` : ''}
                        ORDER BY DBNAME, SCHEMA, OBJNAME
                        LIMIT 200
                    `;
                    headerText = schema ? `## Views in schema ${schema} across all databases\n` : '## Views across all databases\n';
                } else {
                    query = `
                        SELECT '${database}' AS DATABASE, SCHEMA, VIEWNAME AS VIEW_NAME, OWNER
                        FROM ${database}..${NZ_SYSTEM_VIEWS.VIEW}
                        ${schema ? `WHERE SCHEMA = '${schema.toUpperCase()}'` : ''}
                        ORDER BY SCHEMA, VIEWNAME
                        LIMIT 200
                    `;
                    headerText = schema ? `## Views in ${database}.${schema}\n` : `## Views in ${database}\n`;
                }

                const result = await executeQueryHelper(connection, query);

                if (!result || result.length === 0) {
                    return 'No views found.';
                }

                const lines: string[] = [headerText];
                lines.push('| Database | Schema | View | Owner |');
                lines.push('|----------|--------|------|-------|');
                for (const row of result) {
                    const r = row as Record<string, unknown>;
                    lines.push(`| ${r.DATABASE} | ${r.SCHEMA} | ${r.VIEW_NAME} | ${r.OWNER} |`);
                }

                lines.push(`\n**Total:** ${result.length} view(s)`);
                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to retrieve views: ${msg}`;
        }
    }

    /**
     * Gets list of external tables with data object information.
     * Used by ExternalTablesTool.
     */
    public async getExternalTables(database?: string, schema?: string, dataObjectPattern?: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                let query: string;
                let headerText: string;
                const searchAllDatabases = !database;
                const dataObjFilter = dataObjectPattern ? dataObjectPattern.toUpperCase().replace(/\*/g, '%') : null;

                if (searchAllDatabases) {
                    // Search across all databases using global views
                    query = `
                        SELECT E1.DATABASE, E1.SCHEMA, E1.TABLENAME AS TABLE_NAME, 
                               E2.EXTOBJNAME AS DATA_OBJECT, E2.OWNER
                        FROM ${NZ_SYSTEM_VIEWS.EXTERNAL} E1
                        LEFT JOIN ${NZ_SYSTEM_VIEWS.EXTOBJECT} E2 ON E1.DATABASE = E2.DATABASE 
                            AND E1.SCHEMA = E2.SCHEMA AND E1.TABLENAME = E2.TABLENAME
                        WHERE 1=1
                        ${schema ? `AND E1.SCHEMA = '${schema.toUpperCase()}'` : ''}
                        ${dataObjFilter ? `AND UPPER(E2.EXTOBJNAME) LIKE '${dataObjFilter}'` : ''}
                        ORDER BY E1.DATABASE, E1.SCHEMA, E1.TABLENAME
                        LIMIT 200
                    `;
                    headerText = '## External Tables across all databases\n';
                } else {
                    query = `
                        SELECT E1.DATABASE, E1.SCHEMA, E1.TABLENAME AS TABLE_NAME, 
                               E2.EXTOBJNAME AS DATA_OBJECT, E2.OWNER, E1.REMOTESOURCE AS LOCATION
                        FROM ${database}..${NZ_SYSTEM_VIEWS.EXTERNAL} E1
                        LEFT JOIN ${database}..${NZ_SYSTEM_VIEWS.EXTOBJECT} E2 ON E1.DATABASE = E2.DATABASE 
                            AND E1.SCHEMA = E2.SCHEMA AND E1.TABLENAME = E2.TABLENAME
                        WHERE 1=1
                        ${schema ? `AND E1.SCHEMA = '${schema.toUpperCase()}'` : ''}
                        ${dataObjFilter ? `AND UPPER(E2.EXTOBJNAME) LIKE '${dataObjFilter}'` : ''}
                        ORDER BY E1.SCHEMA, E1.TABLENAME
                        LIMIT 200
                    `;
                    headerText = schema ? `## External Tables in ${database}.${schema}\n` : `## External Tables in ${database}\n`;
                }

                const result = await executeQueryHelper(connection, query);

                if (!result || result.length === 0) {
                    return dataObjFilter
                        ? `No external tables found matching data object pattern "${dataObjectPattern}".`
                        : 'No external tables found.';
                }

                const lines: string[] = [headerText];

                if (searchAllDatabases) {
                    lines.push('| Database | Schema | Table | Data Object | Owner |');
                    lines.push('|----------|--------|-------|-------------|-------|');
                    for (const row of result) {
                        const r = row as Record<string, unknown>;
                        lines.push(`| ${r.DATABASE} | ${r.SCHEMA} | ${r.TABLE_NAME} | ${r.DATA_OBJECT || ''} | ${r.OWNER || ''} |`);
                    }
                } else {
                    lines.push('| Schema | Table | Data Object | Owner | Location |');
                    lines.push('|--------|-------|-------------|-------|----------|');
                    for (const row of result) {
                        const r = row as Record<string, unknown>;
                        const loc = r.LOCATION ? String(r.LOCATION).substring(0, 50) : '';
                        lines.push(`| ${r.SCHEMA} | ${r.TABLE_NAME} | ${r.DATA_OBJECT || ''} | ${r.OWNER || ''} | ${loc} |`);
                    }
                }

                lines.push(`\n**Total:** ${result.length} external table(s)`);
                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to retrieve external tables: ${msg}`;
        }
    }

    /**
     * Gets the source code/definition of a view or procedure.
     * Used by GetObjectDefinitionTool.
     * 
     * NOTE: For VIEWS - the DEFINITION column requires connection to the same database.
     * When searching across databases for views, we connect to each database in parallel.
     * 
     * For PROCEDURES - PROCEDURESOURCE is accessible cross-database, so we can use
     * a single connection to search all databases.
     */
    public async getObjectDefinition(objectName: string, objectType: 'view' | 'procedure', database?: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            // Parse object name first to determine target database
            const parts = objectName.split('.');
            let db: string | undefined;
            let schema: string | undefined;
            let objName: string;

            if (parts.length === 3) {
                [db, schema, objName] = parts;
            } else if (parts.length === 2) {
                // Could be DB..NAME or SCHEMA.NAME
                if (parts[0] === '') {
                    // ..NAME format - invalid
                    objName = parts[1];
                } else if (parts[1] === '') {
                    // DB.. format - use db, no schema
                    db = parts[0];
                    objName = '';
                } else {
                    [schema, objName] = parts;
                }
            } else {
                objName = parts[0];
            }

            // Use provided database if not in name
            if (!db && database) {
                db = database;
            }

            objName = objName.toUpperCase();

            // For PROCEDURES: PROCEDURESOURCE is accessible cross-database
            // We can use a single connection to search all databases
            if (objectType === 'procedure') {
                return await this.getProcedureDefinitionCrossDatabase(
                    connectionDetails, objName, db, schema
                );
            }

            // For VIEWS: DEFINITION requires connection to the same database
            // If specific database is known, connect directly to it
            if (db) {
                return await this.getViewDefinitionFromDatabase(
                    connectionDetails, db, objName, schema
                );
            }

            // No database specified for VIEW - search across ALL databases in parallel
            // First, get list of databases
            const defaultConnection = await createConnectionFromDetails(connectionDetails);
            if (!defaultConnection) {
                return 'Could not establish database connection.';
            }

            let databases: string[];
            try {
                const dbResult = await executeQueryHelper<{ DATABASE: string }>(
                    defaultConnection,
                    NZ_QUERIES.LIST_DATABASES
                );
                databases = dbResult
                    .map(r => r.DATABASE)
                    .filter(d => d && d !== 'SYSTEM'); // Skip SYSTEM database
            } finally {
                defaultConnection.close();
            }

            if (databases.length === 0) {
                return 'No databases found to search for view.';
            }

            // Search each database in parallel (required for VIEW DEFINITION)
            const searchPromises = databases.map(async (dbName) => {
                try {
                    return await this.getViewDefinitionFromDatabase(
                        connectionDetails, dbName, objName, schema, true
                    );
                } catch {
                    // Ignore errors from individual databases (permission issues, etc.)
                    return null;
                }
            });

            const results = await Promise.all(searchPromises);
            const validResults = results.filter(r => r !== null && r !== '');

            if (validResults.length === 0) {
                return `View "${objectName}" not found in any database.`;
            }

            return validResults.join('\n---\n\n');

        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to get ${objectType} definition: ${msg}`;
        }
    }

    /**
     * Helper: Get procedure definition - PROCEDURESOURCE is accessible cross-database.
     * Uses a single connection to query all databases.
     */
    private async getProcedureDefinitionCrossDatabase(
        connectionDetails: ConnectionDetails,
        objName: string,
        database?: string,
        schema?: string
    ): Promise<string> {
        const connection = await createConnectionFromDetails(connectionDetails);
        if (!connection) {
            throw new Error('Could not establish database connection');
        }

        try {
            let query: string;

            if (database) {
                query = `
                    SELECT DATABASE, SCHEMA, PROCEDURE, PROCEDURESIGNATURE, RETURNS, ARGUMENTS, 
                           EXECUTEDASOWNER, OWNER, DESCRIPTION, PROCEDURESOURCE
                    FROM ${database.toUpperCase()}..${NZ_SYSTEM_VIEWS.PROCEDURE}
                    WHERE UPPER(PROCEDURE) = '${objName}'
                    ${schema ? `AND SCHEMA = '${schema.toUpperCase()}'` : ''}
                    LIMIT 10
                `;
            } else {
                // Search across all databases - works for procedures!
                query = `
                    SELECT DATABASE, SCHEMA, PROCEDURE, PROCEDURESIGNATURE, RETURNS, 
                           OWNER, PROCEDURESOURCE
                    FROM ${NZ_SYSTEM_VIEWS.PROCEDURE}
                    WHERE UPPER(PROCEDURE) = '${objName}'
                    ${schema ? `AND SCHEMA = '${schema.toUpperCase()}'` : ''}
                    LIMIT 10
                `;
            }

            const result = await executeQueryHelper(connection, query);

            if (!result || result.length === 0) {
                return `Procedure "${objName}" not found${database ? ` in ${database}` : ''}.`;
            }

            const lines: string[] = [];

            for (const row of result) {
                const r = row as Record<string, unknown>;
                const dbName = r.DATABASE || database || '';
                const schemaName = r.SCHEMA || '';
                const name = r.PROCEDURE || '';

                lines.push(`## Procedure: ${dbName}.${schemaName}.${name}\n`);
                lines.push(`**Signature:** ${r.PROCEDURESIGNATURE || ''}`);
                lines.push(`**Returns:** ${r.RETURNS || 'void'}`);
                if (r.ARGUMENTS) {
                    lines.push(`**Arguments:** ${r.ARGUMENTS}`);
                }
                lines.push(`**Owner:** ${r.OWNER}`);
                if (r.EXECUTEDASOWNER !== undefined) {
                    lines.push(`**Execute as Owner:** ${r.EXECUTEDASOWNER ? 'Yes' : 'No'}`);
                }
                if (r.DESCRIPTION) {
                    lines.push(`**Description:** ${r.DESCRIPTION}`);
                }
                lines.push('\n### Source Code\n');
                lines.push('```sql');
                lines.push(String(r.PROCEDURESOURCE || ''));
                lines.push('```\n');
            }

            return lines.join('\n');
        } finally {
            connection.close();
        }
    }

    /**
     * Helper: Get view definition from a specific database.
     * CRITICAL: Must connect to the target database to read DEFINITION column.
     */
    private async getViewDefinitionFromDatabase(
        connectionDetails: ConnectionDetails,
        database: string,
        objName: string,
        schema?: string,
        returnEmptyOnNotFound: boolean = false
    ): Promise<string> {
        // CRITICAL: Connect to the target database to access DEFINITION
        const connection = await createConnectionFromDetails(connectionDetails, database);
        if (!connection) {
            throw new Error(`Could not connect to database ${database}`);
        }

        try {
            const query = `
                SELECT SCHEMA, VIEWNAME, OWNER, DEFINITION
                FROM ${database}.._V_VIEW
                WHERE UPPER(VIEWNAME) = '${objName}'
                ${schema ? `AND SCHEMA = '${schema.toUpperCase()}'` : ''}
                LIMIT 1
            `;

            const result = await executeQueryHelper(connection, query);

            if (!result || result.length === 0) {
                if (returnEmptyOnNotFound) {
                    return '';
                }
                return `View "${objName}" not found in ${database}.`;
            }

            const lines: string[] = [];

            for (const row of result) {
                const r = row as Record<string, unknown>;
                const schemaName = r.SCHEMA || '';
                const name = r.VIEWNAME || '';

                lines.push(`## View: ${database}.${schemaName}.${name}\n`);
                lines.push(`**Owner:** ${r.OWNER}\n`);
                lines.push('### Definition\n');
                lines.push('```sql');
                lines.push(String(r.DEFINITION || ''));
                lines.push('```\n');
            }

            return lines.join('\n');
        } finally {
            connection.close();
        }
    }

    // ========== NEW LANGUAGE MODEL TOOLS - HELPER METHODS ==========

    /**
     * Executes a SELECT query and returns formatted results.
     * Used by ExecuteQueryTool. Only allows SELECT queries for safety.
     * @param sql The SQL query to execute (must be SELECT)
     * @param maxRows Maximum number of rows to return (default 100)
     */
    public async executeSelectQuery(sql: string, maxRows: number = 100): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        // Security: Only allow SELECT statements
        const normalizedSql = sql.trim().toUpperCase();
        if (!normalizedSql.startsWith('SELECT') && !normalizedSql.startsWith('WITH')) {
            return 'Error: Only SELECT queries are allowed for safety. Use the extension commands for DDL/DML operations.';
        }

        // Check for dangerous keywords
        const dangerousKeywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'TRUNCATE', 'ALTER', 'CREATE', 'GRANT', 'REVOKE'];
        for (const keyword of dangerousKeywords) {
            // Check if keyword appears as a statement (not in a subquery or CTE)
            const regex = new RegExp(`^\\s*(${keyword})\\s+`, 'im');
            if (regex.test(sql)) {
                return `Error: ${keyword} statements are not allowed. Only SELECT queries are permitted.`;
            }
        }

        try {
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                // Add LIMIT clause if not present to prevent huge result sets
                let safeSql = sql.trim();
                if (!safeSql.toUpperCase().includes(' LIMIT ')) {
                    safeSql = `${safeSql.replace(/;?\s*$/, '')} LIMIT ${maxRows}`;
                }

                const result = await executeQueryHelper(connection, safeSql);

                if (!result || result.length === 0) {
                    return 'Query executed successfully. No rows returned.';
                }

                // Format as markdown table
                const columns = Object.keys(result[0]);
                const lines: string[] = [
                    `## Query Results (${result.length} row${result.length > 1 ? 's' : ''})\n`,
                    '| ' + columns.join(' | ') + ' |',
                    '|' + columns.map(() => '---').join('|') + '|'
                ];

                for (const row of result.slice(0, maxRows)) {
                    const values = columns.map(col => {
                        const val = (row as Record<string, unknown>)[col];
                        if (val === null) return 'NULL';
                        if (val === undefined) return '';
                        return String(val).replace(/\|/g, '\\|').substring(0, 100); // Truncate long values
                    });
                    lines.push('| ' + values.join(' | ') + ' |');
                }

                if (result.length > maxRows) {
                    lines.push(`\n*... and ${result.length - maxRows} more rows (limited to ${maxRows})*`);
                }

                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Query execution failed: ${msg}`;
        }
    }

    /**
     * Gets sample data from a table.
     * Used by SampleDataTool.
     * @param tableName The table name (can be SCHEMA.TABLE or DB.SCHEMA.TABLE)
     * @param database Optional database name
     * @param sampleSize Number of rows to return (default 10)
     */
    public async getSampleData(tableName: string, database?: string, sampleSize: number = 10): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            // Parse table name
            const parts = tableName.split('.');
            let db: string | undefined;
            let schema: string | undefined;
            let table: string;

            if (parts.length === 3) {
                [db, schema, table] = parts;
            } else if (parts.length === 2) {
                [schema, table] = parts;
                db = database;
            } else {
                table = parts[0];
                db = database;
            }

            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            if (!db) {
                db = await this.connectionManager.getCurrentDatabase(connectionName) || undefined;
            }
            if (!db) {
                return 'Could not determine database. Please specify database name.';
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                // Find schema if not specified
                if (!schema) {
                    schema = await this.findTableSchema(connection, db, table);
                }
                if (!schema) {
                    return `Table "${table}" not found in database "${db}".`;
                }

                const fullTableName = `${db}.${schema}.${table}`;
                const sql = `SELECT * FROM ${fullTableName} LIMIT ${sampleSize}`;
                const result = await executeQueryHelper(connection, sql);

                if (!result || result.length === 0) {
                    return `Table ${fullTableName} is empty or has no accessible rows.`;
                }

                // Format as markdown table
                const columns = Object.keys(result[0]);
                const lines: string[] = [
                    `## Sample Data from ${fullTableName}\n`,
                    `*Showing ${result.length} of ${sampleSize} requested rows*\n`,
                    '| ' + columns.join(' | ') + ' |',
                    '|' + columns.map(() => '---').join('|') + '|'
                ];

                for (const row of result) {
                    const values = columns.map(col => {
                        const val = (row as Record<string, unknown>)[col];
                        if (val === null) return 'NULL';
                        if (val === undefined) return '';
                        return String(val).replace(/\|/g, '\\|').substring(0, 50);
                    });
                    lines.push('| ' + values.join(' | ') + ' |');
                }

                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to get sample data: ${msg}`;
        }
    }

    /**
     * Gets EXPLAIN plan for a SQL query.
     * Used by ExplainPlanTool.
     * @param sql The SQL query to explain
     * @param verbose Whether to use EXPLAIN VERBOSE
     */
    public async getExplainPlan(sql: string, verbose: boolean = false): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                const explainCmd = verbose ? 'EXPLAIN VERBOSE' : 'EXPLAIN';
                const explainSql = `${explainCmd} ${sql}`;
                const result = await executeQueryHelper(connection, explainSql);

                if (!result || result.length === 0) {
                    return 'No explain plan returned.';
                }

                // Format explain plan output
                const lines: string[] = [
                    `## Execution Plan${verbose ? ' (Verbose)' : ''}\n`,
                    '```'
                ];

                for (const row of result) {
                    // Netezza EXPLAIN returns rows with PLANTEXT or similar column
                    const values = Object.values(row as Record<string, unknown>);
                    lines.push(String(values[0] || ''));
                }

                lines.push('```\n');
                lines.push('### Plan Analysis Tips:');
                lines.push('- Look for **Redistribute** or **Broadcast** operations (expensive)');
                lines.push('- Check if **Zone Maps** are being used for filtering');
                lines.push('- Watch for **Spool** operations that may indicate temp table usage');
                lines.push('- **SPU** operations show parallel processing distribution');

                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to get explain plan: ${msg}`;
        }
    }

    /**
     * Searches schema for tables/columns matching a pattern.
     * Used by SearchSchemaTool.
     * @param pattern Search pattern (supports % wildcards)
     * @param searchType What to search: 'tables', 'columns', or 'all'
     * @param database Database to search in
     */
    public async searchSchema(pattern: string, searchType: 'tables' | 'columns' | 'all' = 'all', database?: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            const searchPattern = pattern.toUpperCase().replace(/\*/g, '%');
            // When database is specified, search only in that database
            // When not specified, search across ALL databases using global system views
            const searchAllDatabases = !database;
            const dbScope = database ? ` in ${database}` : ' across all databases';
            const lines: string[] = [`## Schema Search Results for "${pattern}"${dbScope}\n`];

            try {
                // Search tables
                if (searchType === 'tables' || searchType === 'all') {
                    // Use centralized search query
                    const tableQuery = NZ_QUERIES.searchTables(searchPattern, database);
                    const tableResults = await executeQueryHelper(connection, tableQuery);

                    if (tableResults && tableResults.length > 0) {
                        lines.push('### Tables/Views Found\n');
                        lines.push('| Database | Schema | Name | Type |');
                        lines.push('|----------|--------|------|------|');
                        for (const row of tableResults) {
                            const r = row as Record<string, unknown>;
                            lines.push(`| ${r.DATABASE} | ${r.SCHEMA} | ${r.TABLENAME} | ${r.TYPE} |`);
                        }
                        lines.push(`\n*Found ${tableResults.length} table(s)/view(s)*\n`);
                    } else if (searchType === 'tables') {
                        lines.push('No tables or views found matching the pattern.\n');
                    }
                }

                // Search columns
                if (searchType === 'columns' || searchType === 'all') {
                    let columnResults: Record<string, unknown>[] = [];

                    if (searchAllDatabases) {
                        // For all databases, we need to query each database separately as _V_RELATION_COLUMN is local
                        try {
                            const dbQuery = NZ_QUERIES.LIST_DATABASES;
                            const dbs = await executeQueryHelper<{ DATABASE: string }>(connection, dbQuery);

                            if (dbs && dbs.length > 0) {
                                const dbNames = dbs.map(d => d.DATABASE);
                                const resultsLimit = 100;

                                // Query each database individually (more reliable than UNION ALL)
                                for (const db of dbNames) {
                                    if (columnResults.length >= resultsLimit) break;

                                    // Use centralized search query for columns
                                    const dbColQuery = NZ_QUERIES.searchColumns(db, searchPattern);

                                    try {
                                        const dbResults = await executeQueryHelper(connection, dbColQuery);
                                        if (dbResults && dbResults.length > 0) {
                                            columnResults.push(...dbResults.slice(0, resultsLimit - columnResults.length));
                                        }
                                    } catch (e) {
                                        // Silently skip databases that fail (may not be accessible)
                                        console.debug(`[searchSchema] Could not search columns in database ${db}:`, e);
                                    }
                                }
                            }
                        } catch (e) {
                            console.error('[searchSchema] Error fetching databases for column search:', e);
                        }
                    } else {
                        // Use centralized search query for columns
                        const columnQuery = NZ_QUERIES.searchColumns(database!, searchPattern);
                        columnResults = await executeQueryHelper(connection, columnQuery);
                    }

                    if (columnResults && columnResults.length > 0) {
                        lines.push('### Columns Found\n');
                        lines.push('| Database | Schema | Table | Column | Data Type |');
                        lines.push('|----------|--------|-------|--------|-----------|');
                        for (const row of columnResults) {
                            const r = row as Record<string, unknown>;
                            lines.push(`| ${r.DATABASE} | ${r.SCHEMA} | ${r.TABLENAME} | ${r.COLUMN_NAME} | ${r.DATA_TYPE} |`);
                        }
                        lines.push(`\n*Found ${columnResults.length} column(s)*\n`);
                    } else if (searchType === 'columns') {
                        lines.push('No columns found matching the pattern.\n');
                    }
                }

                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Schema search failed: ${msg}`;
        }
    }

    /**
     * Gets table statistics including row count, size, and data skew.
     * Used by TableStatsTool.
     * @param tableName The table name
     * @param database Optional database name
     */
    public async getTableStats(tableName: string, database?: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            // Parse table name
            const parts = tableName.split('.');
            let db: string | undefined;
            let schema: string | undefined;
            let table: string;

            if (parts.length === 3) {
                [db, schema, table] = parts;
            } else if (parts.length === 2) {
                [schema, table] = parts;
                db = database;
            } else {
                table = parts[0];
                db = database;
            }

            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            if (!db) {
                db = await this.connectionManager.getCurrentDatabase(connectionName) || undefined;
            }
            if (!db) {
                return 'Could not determine database. Please specify database name.';
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                // Find schema if not specified
                if (!schema) {
                    schema = await this.findTableSchema(connection, db, table);
                }
                if (!schema) {
                    return `Table "${table}" not found in database "${db}".`;
                }

                const fullTableName = `${db}.${schema}.${table}`;
                const lines: string[] = [`## Table Statistics: ${fullTableName}\n`];

                // Get row count
                try {
                    const countQuery = `SELECT COUNT(*) AS ROW_COUNT FROM ${fullTableName}`;
                    const countResult = await executeQueryHelper(connection, countQuery);
                    if (countResult && countResult.length > 0) {
                        const count = (countResult[0] as Record<string, unknown>).ROW_COUNT;
                        lines.push(`**Row Count:** ${Number(count).toLocaleString()}`);
                    }
                } catch {
                    lines.push('**Row Count:** Unable to retrieve');
                }

                // Get table info from system catalog using centralized query
                const infoQuery = NZ_QUERIES.getTableStats(db, schema, table);
                const infoResult = await executeQueryHelper(connection, infoQuery);
                if (infoResult && infoResult.length > 0) {
                    const info = infoResult[0] as Record<string, unknown>;
                    lines.push(`**Distribution Key:** ${info.DIST_KEY || 'RANDOM'}`);
                    lines.push(`**Owner:** ${info.OWNER || 'N/A'}`);
                }

                // Check data skew (distribution across SPUs)
                lines.push('\n### Data Distribution (Skew Check)\n');
                try {
                    const skewQuery = `
                        SELECT DATASLICEID, COUNT(*) AS ROW_COUNT
                        FROM ${fullTableName}
                        GROUP BY DATASLICEID
                        ORDER BY DATASLICEID
                    `;
                    const skewResult = await executeQueryHelper(connection, skewQuery);
                    if (skewResult && skewResult.length > 0) {
                        const counts = skewResult.map(r => Number((r as Record<string, unknown>).ROW_COUNT));
                        const min = Math.min(...counts);
                        const max = Math.max(...counts);
                        const avg = counts.reduce((a, b) => a + b, 0) / counts.length;
                        const skewRatio = max > 0 ? ((max - min) / max * 100).toFixed(1) : '0';

                        lines.push(`**SPU Count:** ${skewResult.length}`);
                        lines.push(`**Min Rows/SPU:** ${min.toLocaleString()}`);
                        lines.push(`**Max Rows/SPU:** ${max.toLocaleString()}`);
                        lines.push(`**Avg Rows/SPU:** ${Math.round(avg).toLocaleString()}`);
                        lines.push(`**Skew Ratio:** ${skewRatio}%`);

                        if (Number(skewRatio) > 20) {
                            lines.push('\n⚠️ **Warning:** High data skew detected. Consider reviewing distribution key.');
                        } else {
                            lines.push('\n✅ Data distribution looks balanced.');
                        }
                    }
                } catch {
                    lines.push('Unable to retrieve skew information (DATASLICEID may not be available).');
                }

                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to get table statistics: ${msg}`;
        }
    }

    /**
     * Gets object dependencies (what uses this table, what this view depends on).
     * Used by DependenciesTool.
     * 
     * NOTE: This function connects to the specified database to properly access
     * _V_VIEW.DEFINITION and _V_PROCEDURE.PROCEDURESOURCE columns which require
     * the connection to be to the same database where the objects exist.
     * 
     * @param objectName The object name (table, view, procedure)
     * @param database Optional database name
     */
    public async getObjectDependencies(objectName: string, database?: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            let db = database;
            if (!db) {
                db = await this.connectionManager.getCurrentDatabase(connectionName) || undefined;
            }
            if (!db) {
                return 'Could not determine database. Please specify database name.';
            }

            // CRITICAL: Connect to the target database to access DEFINITION and PROCEDURESOURCE
            // These columns are only populated when connected to the database containing the objects
            const connection = await createConnectionFromDetails(connectionDetails, db);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            // Parse object name
            const parts = objectName.split('.');
            const objName = parts[parts.length - 1].toUpperCase();

            const lines: string[] = [`## Dependencies for ${objectName}\n`];

            try {
                // Find views that depend on this object using centralized query
                const dependentViewsQuery = NZ_QUERIES.findDependentViews(db, objName);
                const dependentViews = await executeQueryHelper(connection, dependentViewsQuery);

                if (dependentViews && dependentViews.length > 0) {
                    lines.push('### Views that reference this object\n');
                    lines.push('| Schema | View Name | Owner |');
                    lines.push('|--------|-----------|-------|');
                    for (const row of dependentViews) {
                        const r = row as Record<string, unknown>;
                        lines.push(`| ${r.SCHEMA} | ${r.VIEWNAME} | ${r.OWNER} |`);
                    }
                    lines.push('');
                } else {
                    lines.push('### Views that reference this object\n');
                    lines.push('No views found that reference this object.\n');
                }

                // If it's a view, show what it depends on using centralized query
                const viewDefQuery = NZ_QUERIES.getViewDefinition(db, objName);
                const viewDef = await executeQueryHelper(connection, viewDefQuery);

                if (viewDef && viewDef.length > 0) {
                    lines.push('### View Definition (for dependency analysis)\n');
                    lines.push('```sql');
                    lines.push(String((viewDef[0] as Record<string, unknown>).DEFINITION || ''));
                    lines.push('```\n');
                }

                // Find procedures that reference this object using centralized query
                const procQuery = NZ_QUERIES.findDependentProcedures(db, objName);
                const procs = await executeQueryHelper(connection, procQuery);

                if (procs && procs.length > 0) {
                    lines.push('### Procedures that reference this object\n');
                    lines.push('| Schema | Procedure | Owner |');
                    lines.push('|--------|-----------|-------|');
                    for (const row of procs) {
                        const r = row as Record<string, unknown>;
                        lines.push(`| ${r.SCHEMA} | ${r.PROC_NAME} | ${r.OWNER} |`);
                    }
                    lines.push('');
                }

                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Failed to get dependencies: ${msg}`;
        }
    }

    /**
     * Validates SQL syntax without executing.
     * Used by ValidateSqlTool.
     * @param sql The SQL to validate
     */
    public async validateSql(sql: string): Promise<string> {
        const connectionName = this.connectionManager.getActiveConnectionName();
        if (!connectionName) {
            return 'No active database connection. Please connect to a Netezza database first.';
        }

        try {
            const connectionDetails = await this.connectionManager.getConnection(connectionName);
            if (!connectionDetails) {
                return `Connection "${connectionName}" not found.`;
            }

            const connection = await createConnectionFromDetails(connectionDetails);
            if (!connection) {
                return 'Could not establish database connection.';
            }

            try {
                // Use EXPLAIN to validate syntax without executing
                // EXPLAIN will parse and plan the query, catching syntax errors
                const explainSql = `EXPLAIN ${sql}`;
                await executeQueryHelper(connection, explainSql);

                // If no error, SQL is valid
                return `✅ **SQL is valid**\n\nThe SQL syntax is correct and all referenced objects exist.`;
            } catch (e) {
                const msg = e instanceof Error ? e.message : String(e);

                // Parse error message for helpful feedback
                const lines: string[] = [
                    '❌ **SQL Validation Failed**\n',
                    '### Error Details\n',
                    '```',
                    msg,
                    '```\n'
                ];

                // Try to extract helpful information from error
                if (msg.includes('does not exist')) {
                    lines.push('💡 **Suggestion:** Check if the table/column name is spelled correctly and exists in the database.');
                } else if (msg.includes('syntax error')) {
                    lines.push('💡 **Suggestion:** Review SQL syntax near the indicated position.');
                } else if (msg.includes('permission denied')) {
                    lines.push('💡 **Suggestion:** You may not have permission to access this object.');
                }

                return lines.join('\n');
            } finally {
                connection.close();
            }
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            return `Validation failed: ${msg}`;
        }
    }
}

/**
 * Interface for Schema Tool input parameters
 */
export interface ISchemaToolParameters {
    sql?: string;
}

/**
 * Language Model Tool for getting SQL schema (DDL) from referenced tables.
 * This tool can be automatically invoked by Copilot when it needs database schema information.
 * 
 * Usage:
 * - Copilot can call this tool automatically in agent mode
 * - Users can reference it with #schema in chat
 * 
 * The tool extracts table references from SQL and fetches their DDL from the connected database.
 */
export class SchemaTool implements vscode.LanguageModelTool<ISchemaToolParameters> {

    constructor(private copilotService: CopilotService) { }

    /**
     * Prepares the tool invocation with confirmation message
     */
    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<ISchemaToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        // Determine what SQL we'll analyze
        const sqlSource = options.input.sql
            ? 'provided SQL'
            : 'current editor';

        return {
            invocationMessage: `Fetching table schema from ${sqlSource}...`,
            confirmationMessages: {
                title: 'Get SQL Schema',
                message: new vscode.MarkdownString(
                    `Analyze SQL and fetch table schemas (DDL) from the connected Netezza database?\n\n` +
                    `**Source:** ${sqlSource}`
                )
            }
        };
    }

    /**
     * Invokes the tool to get schema information
     */
    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<ISchemaToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            let schemaInfo: string;

            if (options.input.sql) {
                // Use provided SQL
                schemaInfo = await this.copilotService.getSchemaForSql(options.input.sql);
            } else {
                // Use current editor
                schemaInfo = await this.copilotService.getSchemaContextForCurrentSql();
            }

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(schemaInfo)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get SQL schema: ${errorMsg}. Make sure you have an active database connection.`);
        }
    }
}

/**
 * Interface for GetColumns Tool input parameters
 */
export interface IColumnsToolParameters {
    tables: string[];
    database?: string;
}

/**
 * Language Model Tool for getting column metadata for specified tables.
 * Users can reference it with #getColumns in chat.
 */
export class ColumnsTool implements vscode.LanguageModelTool<IColumnsToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<IColumnsToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const tableCount = options.input.tables?.length || 0;
        const dbInfo = options.input.database ? ` from database ${options.input.database}` : '';

        return {
            invocationMessage: `Fetching column metadata for ${tableCount} table(s)${dbInfo}...`,
            confirmationMessages: {
                title: 'Get Table Columns',
                message: new vscode.MarkdownString(
                    `Fetch column definitions for the following tables${dbInfo}?\n\n` +
                    `**Tables:** ${options.input.tables?.join(', ') || 'none'}`
                )
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<IColumnsToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const { tables, database } = options.input;

            if (!tables || tables.length === 0) {
                throw new Error('No tables specified. Please provide at least one table name.');
            }

            const columnsInfo = await this.copilotService.getColumnsForTables(tables, database);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(columnsInfo)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get columns: ${errorMsg}. Make sure you have an active database connection.`);
        }
    }
}

/**
 * Interface for GetTables Tool input parameters
 */
export interface ITablesToolParameters {
    database?: string;
    schema?: string;
}

/**
 * Language Model Tool for getting list of tables from a database or all databases.
 * When database is not specified, searches across ALL accessible databases.
 * Users can reference it with #getTables in chat.
 */
export class TablesTool implements vscode.LanguageModelTool<ITablesToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<ITablesToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const schemaInfo = options.input.schema ? ` (schema: ${options.input.schema})` : '';
        const dbInfo = options.input.database ? `database ${options.input.database}` : 'all databases';

        return {
            invocationMessage: `Fetching tables from ${dbInfo}${schemaInfo}...`,
            confirmationMessages: {
                title: 'Get Tables List',
                message: new vscode.MarkdownString(
                    `Fetch list of tables from **${dbInfo}**${schemaInfo}?`
                )
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<ITablesToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const { database, schema } = options.input;

            // Database is now optional - when not specified, searches all databases
            const tablesInfo = await this.copilotService.getTablesFromDatabase(database, schema);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(tablesInfo)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get tables: ${errorMsg}. Make sure you have an active database connection.`);
        }
    }
}

// ========== NEW LANGUAGE MODEL TOOLS ==========

/**
 * Interface for ExecuteQuery Tool input parameters
 */
export interface IExecuteQueryToolParameters {
    sql: string;
    maxRows?: number;
}

/**
 * Language Model Tool for executing SELECT queries.
 * Only allows read-only SELECT queries for safety.
 * Users can reference it with #executeQuery in chat.
 */
export class ExecuteQueryTool implements vscode.LanguageModelTool<IExecuteQueryToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<IExecuteQueryToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const maxRows = options.input.maxRows || 100;
        const sqlPreview = options.input.sql?.substring(0, 100) + (options.input.sql?.length > 100 ? '...' : '');

        return {
            invocationMessage: `Executing SELECT query (max ${maxRows} rows)...`,
            confirmationMessages: {
                title: 'Execute SQL Query',
                message: new vscode.MarkdownString(
                    `Execute the following SQL query (read-only)?\n\n\`\`\`sql\n${sqlPreview}\n\`\`\`\n\n**Max Rows:** ${maxRows}`
                )
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<IExecuteQueryToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const { sql, maxRows } = options.input;

            if (!sql) {
                throw new Error('SQL query is required.');
            }

            const result = await this.copilotService.executeSelectQuery(sql, maxRows || 100);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Query execution failed: ${errorMsg}`);
        }
    }
}

/**
 * Interface for SampleData Tool input parameters
 */
export interface ISampleDataToolParameters {
    table: string;
    database?: string;
    sampleSize?: number;
}

/**
 * Language Model Tool for getting sample data from a table.
 * Users can reference it with #sampleData in chat.
 */
export class SampleDataTool implements vscode.LanguageModelTool<ISampleDataToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<ISampleDataToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const size = options.input.sampleSize || 10;
        const dbInfo = options.input.database ? ` from ${options.input.database}` : '';

        return {
            invocationMessage: `Fetching ${size} sample rows from ${options.input.table}${dbInfo}...`,
            confirmationMessages: {
                title: 'Get Sample Data',
                message: new vscode.MarkdownString(
                    `Fetch ${size} sample rows from table **${options.input.table}**${dbInfo}?`
                )
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<ISampleDataToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const { table, database, sampleSize } = options.input;

            if (!table) {
                throw new Error('Table name is required.');
            }

            const result = await this.copilotService.getSampleData(table, database, sampleSize || 10);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get sample data: ${errorMsg}`);
        }
    }
}

/**
 * Interface for ExplainPlan Tool input parameters
 */
export interface IExplainPlanToolParameters {
    sql: string;
    verbose?: boolean;
}

/**
 * Language Model Tool for getting query execution plan.
 * Users can reference it with #explainPlan in chat.
 */
export class ExplainPlanTool implements vscode.LanguageModelTool<IExplainPlanToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<IExplainPlanToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const mode = options.input.verbose ? 'verbose' : 'standard';
        const sqlPreview = options.input.sql?.substring(0, 80) + (options.input.sql?.length > 80 ? '...' : '');

        return {
            invocationMessage: `Getting ${mode} execution plan...`,
            confirmationMessages: {
                title: 'Get Execution Plan',
                message: new vscode.MarkdownString(
                    `Get ${mode} execution plan for:\n\n\`\`\`sql\n${sqlPreview}\n\`\`\``
                )
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<IExplainPlanToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const { sql, verbose } = options.input;

            if (!sql) {
                throw new Error('SQL query is required.');
            }

            const result = await this.copilotService.getExplainPlan(sql, verbose || false);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get execution plan: ${errorMsg}`);
        }
    }
}

/**
 * Interface for SearchSchema Tool input parameters
 */
export interface ISearchSchemaToolParameters {
    pattern: string;
    searchType?: 'tables' | 'columns' | 'all';
    database?: string;
}

/**
 * Language Model Tool for searching schema objects.
 * Users can reference it with #searchSchema in chat.
 */
export class SearchSchemaTool implements vscode.LanguageModelTool<ISearchSchemaToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<ISearchSchemaToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const searchType = options.input.searchType || 'all';
        const dbInfo = options.input.database ? ` in ${options.input.database}` : '';

        return {
            invocationMessage: `Searching ${searchType} for "${options.input.pattern}"${dbInfo}...`,
            confirmationMessages: {
                title: 'Search Schema',
                message: new vscode.MarkdownString(
                    `Search for ${searchType} matching pattern **"${options.input.pattern}"**${dbInfo}?`
                )
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<ISearchSchemaToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const { pattern, searchType, database } = options.input;

            if (!pattern) {
                throw new Error('Search pattern is required.');
            }

            const result = await this.copilotService.searchSchema(pattern, searchType || 'all', database);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Schema search failed: ${errorMsg}`);
        }
    }
}

/**
 * Interface for TableStats Tool input parameters
 */
export interface ITableStatsToolParameters {
    table: string;
    database?: string;
}

/**
 * Language Model Tool for getting table statistics.
 * Users can reference it with #tableStats in chat.
 */
export class TableStatsTool implements vscode.LanguageModelTool<ITableStatsToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<ITableStatsToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const dbInfo = options.input.database ? ` in ${options.input.database}` : '';

        return {
            invocationMessage: `Getting statistics for ${options.input.table}${dbInfo}...`,
            confirmationMessages: {
                title: 'Get Table Statistics',
                message: new vscode.MarkdownString(
                    `Fetch statistics (row count, skew, distribution) for **${options.input.table}**${dbInfo}?`
                )
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<ITableStatsToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const { table, database } = options.input;

            if (!table) {
                throw new Error('Table name is required.');
            }

            const result = await this.copilotService.getTableStats(table, database);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get table statistics: ${errorMsg}`);
        }
    }
}

/**
 * Interface for Dependencies Tool input parameters
 */
export interface IDependenciesToolParameters {
    object: string;
    database?: string;
}

/**
 * Language Model Tool for getting object dependencies.
 * Users can reference it with #dependencies in chat.
 */
export class DependenciesTool implements vscode.LanguageModelTool<IDependenciesToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<IDependenciesToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const dbInfo = options.input.database ? ` in ${options.input.database}` : '';

        return {
            invocationMessage: `Finding dependencies for ${options.input.object}${dbInfo}...`,
            confirmationMessages: {
                title: 'Get Object Dependencies',
                message: new vscode.MarkdownString(
                    `Find all objects that depend on **${options.input.object}**${dbInfo}?`
                )
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<IDependenciesToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const { object, database } = options.input;

            if (!object) {
                throw new Error('Object name is required.');
            }

            const result = await this.copilotService.getObjectDependencies(object, database);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get dependencies: ${errorMsg}`);
        }
    }
}

/**
 * Interface for ValidateSql Tool input parameters
 */
export interface IValidateSqlToolParameters {
    sql: string;
}

/**
 * Language Model Tool for validating SQL syntax.
 * Users can reference it with #validateSql in chat.
 */
export class ValidateSqlTool implements vscode.LanguageModelTool<IValidateSqlToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<IValidateSqlToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const sqlPreview = options.input.sql?.substring(0, 80) + (options.input.sql?.length > 80 ? '...' : '');

        return {
            invocationMessage: 'Validating SQL syntax...',
            confirmationMessages: {
                title: 'Validate SQL',
                message: new vscode.MarkdownString(
                    `Validate syntax of:\n\n\`\`\`sql\n${sqlPreview}\n\`\`\``
                )
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<IValidateSqlToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const { sql } = options.input;

            if (!sql) {
                throw new Error('SQL is required.');
            }

            const result = await this.copilotService.validateSql(sql);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`SQL validation failed: ${errorMsg}`);
        }
    }
}

// ========== NEW TOOLS - Databases, Schemas, Procedures, Views, External Tables ==========

/**
 * Interface for Databases Tool input parameters
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface IDatabasesToolParameters {
    // No parameters - lists all databases
}

/**
 * Language Model Tool for getting list of databases.
 * Users can reference it with #databases in chat.
 */
export class DatabasesTool implements vscode.LanguageModelTool<IDatabasesToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        _options: vscode.LanguageModelToolInvocationPrepareOptions<IDatabasesToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        return {
            invocationMessage: 'Fetching list of databases...',
            confirmationMessages: {
                title: 'Get Databases',
                message: new vscode.MarkdownString('Fetch list of all databases accessible via the current connection?')
            }
        };
    }

    async invoke(
        _options: vscode.LanguageModelToolInvocationOptions<IDatabasesToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const result = await this.copilotService.getDatabases();

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get databases: ${errorMsg}`);
        }
    }
}

/**
 * Interface for Schemas Tool input parameters
 */
export interface ISchemasToolParameters {
    database?: string;
}

/**
 * Language Model Tool for getting list of schemas.
 * Users can reference it with #schemas in chat.
 */
export class SchemasTool implements vscode.LanguageModelTool<ISchemasToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<ISchemasToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const dbInfo = options.input.database ? ` in ${options.input.database}` : ' across all databases';

        return {
            invocationMessage: `Fetching schemas${dbInfo}...`,
            confirmationMessages: {
                title: 'Get Schemas',
                message: new vscode.MarkdownString(`Fetch list of schemas${dbInfo}?`)
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<ISchemasToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const result = await this.copilotService.getSchemas(options.input.database);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get schemas: ${errorMsg}`);
        }
    }
}

/**
 * Interface for Procedures Tool input parameters
 */
export interface IProceduresToolParameters {
    database?: string;
    schema?: string;
}

/**
 * Language Model Tool for getting list of procedures.
 * Users can reference it with #procedures in chat.
 */
export class ProceduresTool implements vscode.LanguageModelTool<IProceduresToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<IProceduresToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const dbInfo = options.input.database ? ` in ${options.input.database}` : ' across all databases';
        const schemaInfo = options.input.schema ? `, schema ${options.input.schema}` : '';

        return {
            invocationMessage: `Fetching procedures${dbInfo}${schemaInfo}...`,
            confirmationMessages: {
                title: 'Get Procedures',
                message: new vscode.MarkdownString(`Fetch list of stored procedures${dbInfo}${schemaInfo}?`)
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<IProceduresToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const result = await this.copilotService.getProcedures(options.input.database, options.input.schema);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get procedures: ${errorMsg}`);
        }
    }
}

/**
 * Interface for Views Tool input parameters
 */
export interface IViewsToolParameters {
    database?: string;
    schema?: string;
}

/**
 * Language Model Tool for getting list of views.
 * Users can reference it with #views in chat.
 */
export class ViewsTool implements vscode.LanguageModelTool<IViewsToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<IViewsToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const dbInfo = options.input.database ? ` in ${options.input.database}` : ' across all databases';
        const schemaInfo = options.input.schema ? `, schema ${options.input.schema}` : '';

        return {
            invocationMessage: `Fetching views${dbInfo}${schemaInfo}...`,
            confirmationMessages: {
                title: 'Get Views',
                message: new vscode.MarkdownString(`Fetch list of views${dbInfo}${schemaInfo}?`)
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<IViewsToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const result = await this.copilotService.getViews(options.input.database, options.input.schema);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get views: ${errorMsg}`);
        }
    }
}

/**
 * Interface for ExternalTables Tool input parameters
 */
export interface IExternalTablesToolParameters {
    database?: string;
    schema?: string;
    dataObjectPattern?: string;
}

/**
 * Language Model Tool for getting list of external tables.
 * Users can reference it with #externalTables in chat.
 */
export class ExternalTablesTool implements vscode.LanguageModelTool<IExternalTablesToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<IExternalTablesToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const dbInfo = options.input.database ? ` in ${options.input.database}` : ' across all databases';
        const schemaInfo = options.input.schema ? `, schema ${options.input.schema}` : '';
        const dataObjInfo = options.input.dataObjectPattern ? `, data object matching "${options.input.dataObjectPattern}"` : '';

        return {
            invocationMessage: `Fetching external tables${dbInfo}${schemaInfo}${dataObjInfo}...`,
            confirmationMessages: {
                title: 'Get External Tables',
                message: new vscode.MarkdownString(`Fetch list of external tables${dbInfo}${schemaInfo}${dataObjInfo}?`)
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<IExternalTablesToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const result = await this.copilotService.getExternalTables(
                options.input.database,
                options.input.schema,
                options.input.dataObjectPattern
            );

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get external tables: ${errorMsg}`);
        }
    }
}

/**
 * Interface for GetObjectDefinition Tool input parameters
 */
export interface IGetObjectDefinitionToolParameters {
    objectName: string;
    objectType: 'view' | 'procedure';
    database?: string;
}

/**
 * Language Model Tool for getting view/procedure source code.
 * Users can reference it with #objectDefinition in chat.
 */
export class GetObjectDefinitionTool implements vscode.LanguageModelTool<IGetObjectDefinitionToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<IGetObjectDefinitionToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const dbInfo = options.input.database ? ` in ${options.input.database}` : '';
        const typeName = options.input.objectType === 'view' ? 'view' : 'procedure';

        return {
            invocationMessage: `Fetching ${typeName} definition for ${options.input.objectName}${dbInfo}...`,
            confirmationMessages: {
                title: `Get ${typeName.charAt(0).toUpperCase() + typeName.slice(1)} Definition`,
                message: new vscode.MarkdownString(`Fetch source code of ${typeName} **${options.input.objectName}**${dbInfo}?`)
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<IGetObjectDefinitionToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        try {
            const { objectName, objectType, database } = options.input;

            if (!objectName) {
                throw new Error('Object name is required.');
            }
            if (!objectType || (objectType !== 'view' && objectType !== 'procedure')) {
                throw new Error('Object type must be "view" or "procedure".');
            }

            const result = await this.copilotService.getObjectDefinition(objectName, objectType, database);

            return new vscode.LanguageModelToolResult([
                new vscode.LanguageModelTextPart(result)
            ]);
        } catch (e) {
            const errorMsg = e instanceof Error ? e.message : String(e);
            throw new Error(`Failed to get object definition: ${errorMsg}`);
        }
    }
}

// ========== NETEZZA REFERENCE TOOL ==========

/**
 * Interface for NetezzaReference Tool input parameters
 */
export interface INetezzaReferenceToolParameters {
    topic?: 'optimization' | 'nzplsql' | 'all';
}

/**
 * Language Model Tool for getting Netezza-specific reference documentation.
 * Provides optimization rules and NZPLSQL stored procedure syntax reference.
 * 
 * This tool allows Copilot agents to access Netezza best practices and syntax
 * documentation during regular chat interactions without requiring the user
 * to use specific commands.
 * 
 * Topics:
 * - 'optimization': SQL optimization rules for Netezza (zone maps, distribution keys, etc.)
 * - 'nzplsql': NZPLSQL stored procedure syntax reference
 * - 'all': Both optimization and NZPLSQL documentation (default)
 */
export class NetezzaReferenceTool implements vscode.LanguageModelTool<INetezzaReferenceToolParameters> {

    constructor(private copilotService: CopilotService) { }

    async prepareInvocation(
        options: vscode.LanguageModelToolInvocationPrepareOptions<INetezzaReferenceToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.PreparedToolInvocation> {
        const topic = options.input.topic || 'all';
        const topicDescriptions: Record<string, string> = {
            'optimization': 'SQL optimization best practices',
            'nzplsql': 'NZPLSQL stored procedure syntax',
            'all': 'all Netezza documentation'
        };

        return {
            invocationMessage: `Getting Netezza reference: ${topicDescriptions[topic]}...`,
            confirmationMessages: {
                title: 'Get Netezza Reference',
                message: new vscode.MarkdownString(`Retrieve **${topicDescriptions[topic]}** for IBM Netezza?`)
            }
        };
    }

    async invoke(
        options: vscode.LanguageModelToolInvocationOptions<INetezzaReferenceToolParameters>,
        _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
        const topic = options.input.topic || 'all';
        const result = this.copilotService.getNetezzaReference(topic);

        return new vscode.LanguageModelToolResult([
            new vscode.LanguageModelTextPart(result)
        ]);
    }
}