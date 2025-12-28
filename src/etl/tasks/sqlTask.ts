/**
 * SQL Task Executor
 * Executes SQL queries against the Netezza database
 */

import { EtlNode, EtlNodeExecutionResult, SqlNodeConfig } from '../etlTypes';
import { ExecutionContext, TaskExecutor } from '../etlExecutionEngine';
import { NzConnection, NzDataReader } from '../../types';

export class SqlTaskExecutor implements TaskExecutor {
    async execute(
        node: EtlNode,
        context: ExecutionContext
    ): Promise<EtlNodeExecutionResult> {
        const config = node.config as SqlNodeConfig;
        const startTime = new Date();

        if (!config.query || config.query.trim() === '') {
            return {
                nodeId: node.id,
                status: 'error',
                startTime,
                endTime: new Date(),
                error: 'SQL query is empty'
            };
        }

        let connection: NzConnection | null = null;

        try {
            // Resolve variables in the query
            let query = config.query;
            for (const [key, value] of Object.entries(context.variables)) {
                query = query.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value);
            }

            context.onProgress?.(`Executing SQL: ${query.substring(0, 100)}${query.length > 100 ? '...' : ''}`);

            // Use connection details from context
            const connDetails = context.connectionDetails;
            if (!connDetails) {
                throw new Error('No connection details available in context');
            }

            // eslint-disable-next-line @typescript-eslint/no-require-imports
            const NzConnectionClass = require('../../../libs/driver/src/NzConnection');
            connection = new NzConnectionClass({
                host: connDetails.host,
                port: connDetails.port || 5480,
                database: connDetails.database,
                user: connDetails.user,
                password: connDetails.password
            }) as NzConnection;

            await connection.connect();

            // Execute the query
            const cmd = connection.createCommand(query);

            if (config.timeout) {
                cmd.commandTimeout = config.timeout;
            }

            const reader: NzDataReader = await cmd.executeReader();

            // Collect results
            const columns: string[] = [];
            for (let i = 0; i < reader.fieldCount; i++) {
                columns.push(reader.getName(i));
            }

            const rows: unknown[][] = [];
            let rowCount = 0;

            while (await reader.read()) {
                const row: unknown[] = [];
                for (let i = 0; i < reader.fieldCount; i++) {
                    row.push(reader.getValue(i));
                }
                rows.push(row);
                rowCount++;
            }

            // Close reader
            if (reader.close) {
                await reader.close();
            }

            return {
                nodeId: node.id,
                status: 'success',
                startTime,
                endTime: new Date(),
                rowsAffected: rowCount,
                output: { columns, rows }
            };

        } catch (error) {
            return {
                nodeId: node.id,
                status: 'error',
                startTime,
                endTime: new Date(),
                error: String(error)
            };
        } finally {
            if (connection) {
                try {
                    await connection.close();
                } catch {
                    // Ignore close errors
                }
            }
        }
    }
}
