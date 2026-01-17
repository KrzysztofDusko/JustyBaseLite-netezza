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
import { extractVariables } from '../core/variableUtils';
import { generateTableDDL } from '../ddl';
import { createConnectionFromDetails, executeQueryHelper } from '../ddl/helpers';
import { NzConnection } from '../types';

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
        context: vscode.ExtensionContext
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
     * Detects if SQL contains a stored procedure definition
     * Matches: CREATE PROCEDURE, CREATE OR REPLACE PROCEDURE
     */
    private isProcedureCode(sql: string): boolean {
        const procedurePattern = /CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\s+/i;
        return procedurePattern.test(sql);
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
            const sql = `
                SELECT SCHEMA 
                FROM ${database.toUpperCase()}.._V_OBJECT_DATA 
                WHERE UPPER(OBJNAME) = UPPER('${tableName.replace(/'/g, "''")}') 
                AND DBNAME = '${database.toUpperCase()}'
                AND OBJTYPE IN ('TABLE', 'VIEW', 'EXTERNAL TABLE')
                LIMIT 1
            `;
            
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

IMPORTANT NETEZZA SQL CONVENTIONS:
- Table references: Use DATABASE..TABLE (two dots) or DATABASE.SCHEMA.TABLE (three parts)
- DATABASE..TABLE syntax is valid and CORRECT in Netezza (do NOT change it to DATABASE.SCHEMA.TABLE)
- Three-part names: DATABASE.SCHEMA.TABLE are also valid
- Netezza supports specific SQL extensions and performance features
- Preserve Netezza-specific syntax like DISTRIBUTE ON, ORGANIZE ON, etc.

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
     * Quick action: Fix SQL with context (Auto mode - applies changes via diff)
     */
    async fixSql(): Promise<void> {
        try {
            const context = await this.gatherContext();
            let prompt = this.getPrompt('fix');
            
            // Add NZPLSQL reference if code contains stored procedure
            if (this.isProcedureCode(context.selectedSql)) {
                prompt += `\n\n${this.NZPLSQL_PROCEDURE_REFERENCE}`;
            }
            
            await this.sendToLanguageModel(context, prompt, true);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error fixing SQL: ${msg}`);
        }
    }

    /**
     * Quick action: Fix SQL with context (Interactive mode - opens chat)
     */
    async fixSqlInteractive(): Promise<void> {
        try {
            const context = await this.gatherContext();
            let prompt = this.getPrompt('fix');
            
            // Add NZPLSQL reference if code contains stored procedure
            if (this.isProcedureCode(context.selectedSql)) {
                prompt += `\n\n${this.NZPLSQL_PROCEDURE_REFERENCE}`;
            }
            
            await this.sendToChatInteractive(context, prompt, 'Fix SQL with Context');
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error fixing SQL: ${msg}`);
        }
    }

    /**
     * Quick action: Optimize SQL with context (Auto mode - applies changes via diff)
     */
    async optimizeSql(): Promise<void> {
        try {
            const context = await this.gatherContext();
            const basePrompt = this.getPrompt('optimize');
            let prompt = `${basePrompt}\n\n${this.NETEZZA_OPTIMIZATION_RULES}`;
            
            // Add NZPLSQL reference if code contains stored procedure
            if (this.isProcedureCode(context.selectedSql)) {
                prompt += `\n\n${this.NZPLSQL_PROCEDURE_REFERENCE}`;
            }
            
            await this.sendToLanguageModel(context, prompt, true);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error optimizing SQL: ${msg}`);
        }
    }

    /**
     * Quick action: Optimize SQL with context (Interactive mode - opens chat)
     */
    async optimizeSqlInteractive(): Promise<void> {
        try {
            const context = await this.gatherContext();
            const basePrompt = this.getPrompt('optimize');
            let prompt = `${basePrompt}\n\n${this.NETEZZA_OPTIMIZATION_RULES}`;
            
            // Add NZPLSQL reference if code contains stored procedure
            if (this.isProcedureCode(context.selectedSql)) {
                prompt += `\n\n${this.NZPLSQL_PROCEDURE_REFERENCE}`;
            }
            
            await this.sendToChatInteractive(context, prompt, 'Optimize SQL');
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error optimizing SQL: ${msg}`);
        }
    }

    /**
     * Quick action: Explain SQL with context
     * IMPROVED: Shows explanation in a new document instead of just console
     */
    async explainSql(): Promise<void> {
        try {
            const context = await this.gatherContext();
            const prompt = this.getPrompt('explain');

            // For explain, we don't edit - just show response
            const response = await this.sendToLanguageModel(context, prompt, false);
            
            // Show explanation in a new markdown document
            const doc = await vscode.workspace.openTextDocument({
                content: `# SQL Query Explanation\n\n${response}`,
                language: 'markdown'
            });
            
            await vscode.window.showTextDocument(doc, vscode.ViewColumn.Beside);
            
            vscode.window.showInformationMessage('✅ SQL explanation opened in new editor');
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error explaining SQL: ${msg}`);
        }
    }

    /**
     * Quick action: Explain SQL with context (Interactive mode - opens chat)
     */
    async explainSqlInteractive(): Promise<void> {
        try {
            const context = await this.gatherContext();
            const prompt = this.getPrompt('explain');
            await this.sendToChatInteractive(context, prompt, 'Explain SQL');
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error explaining SQL: ${msg}`);
        }
    }

    /**
     * Custom question with full context (Auto mode)
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
            
            // Ask if user wants to apply changes or just get advice
            const action = await vscode.window.showQuickPick(
                [
                    { 
                        label: '✏️ Apply SQL Changes', 
                        description: 'Copilot will modify the SQL in editor', 
                        value: 'edit' 
                    },
                    { 
                        label: '💡 Just Ask for Advice', 
                        description: 'Get response in new document without modifying code', 
                        value: 'advice' 
                    }
                ],
                { placeHolder: 'How would you like Copilot to respond?' }
            );

            if (!action) {
                return;
            }

            const shouldEdit = action.value === 'edit';
            
            if (shouldEdit) {
                await this.sendToLanguageModel(context, userQuestion, true);
            } else {
                // Show advice in new document
                const response = await this.sendToLanguageModel(context, userQuestion, false);
                const doc = await vscode.workspace.openTextDocument({
                    content: `# Copilot Advice\n\n**Question:** ${userQuestion}\n\n${response}`,
                    language: 'markdown'
                });
                await vscode.window.showTextDocument(doc, vscode.ViewColumn.Beside);
            }
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Error: ${msg}`);
        }
    }

    /**
     * Custom question with full context (Interactive mode - opens chat)
     */
    async askCustomQuestionInteractive(): Promise<void> {
        try {
            const userQuestion = await vscode.window.showInputBox({
                prompt: 'Ask Copilot about this SQL (opens in chat for interactive discussion)',
                placeHolder: 'e.g., "How can I improve this query?" or "Add an index hint"'
            });

            if (!userQuestion) {
                return;
            }

            const context = await this.gatherContext();
            await this.sendToChatInteractive(context, userQuestion, 'Custom Question');
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

            // Query to get all tables with their columns in a compact format
            // Groups columns by table and includes PK/FK information
            const sql = `
                SELECT 
                    O.SCHEMA,
                    O.OBJNAME AS TABLE_NAME,
                    COALESCE(O.DESCRIPTION, '') AS TABLE_DESCRIPTION,
                    C.ATTNAME AS COLUMN_NAME,
                    C.FORMAT_TYPE,
                    COALESCE(C.DESCRIPTION, '') AS COLUMN_DESCRIPTION,
                    C.ATTNUM,
                    MAX(CASE WHEN K.CONTYPE = 'p' THEN 1 ELSE 0 END) AS IS_PK,
                    MAX(CASE WHEN K.CONTYPE = 'f' THEN 1 ELSE 0 END) AS IS_FK
                FROM ${database.toUpperCase()}.._V_RELATION_COLUMN C
                JOIN ${database.toUpperCase()}.._V_OBJECT_DATA O ON C.OBJID = O.OBJID
                LEFT JOIN ${database.toUpperCase()}.._V_RELATION_KEYDATA K 
                    ON UPPER(K.RELATION) = UPPER(O.OBJNAME) 
                    AND UPPER(K.SCHEMA) = UPPER(O.SCHEMA)
                    AND UPPER(K.ATTNAME) = UPPER(C.ATTNAME)
                    AND K.CONTYPE IN ('p', 'f')
                WHERE UPPER(O.DBNAME) = UPPER('${database}')
                AND O.OBJTYPE IN ('TABLE', 'VIEW')
                GROUP BY O.SCHEMA, O.OBJNAME, O.DESCRIPTION, C.ATTNAME, C.FORMAT_TYPE, C.DESCRIPTION, C.ATTNUM
                ORDER BY O.SCHEMA, O.OBJNAME, C.ATTNUM
            `;

            interface SchemaRow {
                SCHEMA: string;
                TABLE_NAME: string;
                TABLE_DESCRIPTION: string;
                COLUMN_NAME: string;
                FORMAT_TYPE: string;
                COLUMN_DESCRIPTION: string;
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
                const key = `${row.SCHEMA}.${row.TABLE_NAME}`;
                if (!tableMap.has(key)) {
                    tableMap.set(key, {
                        schema: row.SCHEMA,
                        tableName: row.TABLE_NAME,
                        tableDescription: row.TABLE_DESCRIPTION,
                        columns: []
                    });
                }
                tableMap.get(key)!.columns.push({
                    name: row.COLUMN_NAME,
                    type: row.FORMAT_TYPE,
                    description: row.COLUMN_DESCRIPTION,
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

IMPORTANT NETEZZA SQL CONVENTIONS:
- Table references: Use DATABASE..TABLE (two dots) or DATABASE.SCHEMA.TABLE (three parts)
- DATABASE..TABLE syntax is valid and CORRECT in Netezza (do NOT change it to DATABASE.SCHEMA.TABLE)
- Three-part names: DATABASE.SCHEMA.TABLE are also valid
- Netezza supports specific SQL extensions and performance features

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
}