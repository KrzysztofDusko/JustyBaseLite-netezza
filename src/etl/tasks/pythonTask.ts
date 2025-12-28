/**
 * Python Task Executor
 * Executes Python scripts as part of ETL workflows
 */

import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EtlNode, EtlNodeExecutionResult, PythonNodeConfig } from '../etlTypes';
import { ExecutionContext, TaskExecutor } from '../etlExecutionEngine';

export class PythonTaskExecutor implements TaskExecutor {
    async execute(
        node: EtlNode,
        context: ExecutionContext
    ): Promise<EtlNodeExecutionResult> {
        const config = node.config as PythonNodeConfig;
        const startTime = new Date();

        // Validate configuration
        if (!config.script && !config.scriptPath) {
            return {
                nodeId: node.id,
                status: 'error',
                startTime,
                endTime: new Date(),
                error: 'Python script or script path is required'
            };
        }

        try {
            const interpreter = config.interpreter || this.findPythonInterpreter();
            context.onProgress?.(`Using Python interpreter: ${interpreter}`);

            let scriptPath: string;
            let tempScript = false;

            if (config.scriptPath) {
                // Use script file directly
                scriptPath = config.scriptPath;

                // Resolve variables in path
                for (const [key, value] of Object.entries(context.variables)) {
                    scriptPath = scriptPath.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value);
                }

                if (!fs.existsSync(scriptPath)) {
                    throw new Error(`Script file not found: ${scriptPath}`);
                }
            } else {
                // Write script to temp file
                let script = config.script;

                // Resolve variables in script
                for (const [key, value] of Object.entries(context.variables)) {
                    script = script.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value);
                }

                scriptPath = path.join(os.tmpdir(), `etl_script_${Date.now()}.py`);
                await fs.promises.writeFile(scriptPath, script, 'utf-8');
                tempScript = true;
            }

            context.onProgress?.(`Executing Python script: ${path.basename(scriptPath)}`);

            // Build arguments
            const args = [scriptPath, ...(config.arguments || [])];

            // Build environment with variables
            const env = {
                ...process.env,
                ...Object.fromEntries(
                    Object.entries(context.variables).map(([k, v]) => [`ETL_VAR_${k.toUpperCase()}`, v])
                )
            };

            // Execute Python
            const result = await this.runPython(interpreter, args, env, context, config.timeout);

            // Cleanup temp script
            if (tempScript) {
                try {
                    await fs.promises.unlink(scriptPath);
                } catch {
                    // Ignore cleanup errors
                }
            }

            if (result.exitCode === 0) {
                return {
                    nodeId: node.id,
                    status: 'success',
                    startTime,
                    endTime: new Date(),
                    output: result.stdout
                };
            } else {
                return {
                    nodeId: node.id,
                    status: 'error',
                    startTime,
                    endTime: new Date(),
                    error: result.stderr || `Python exited with code ${result.exitCode}`,
                    output: result.stdout
                };
            }

        } catch (error) {
            return {
                nodeId: node.id,
                status: 'error',
                startTime,
                endTime: new Date(),
                error: String(error)
            };
        }
    }

    /**
     * Find Python interpreter
     */
    private findPythonInterpreter(): string {
        // On Windows, try 'py' launcher first (Python launcher for Windows)
        if (process.platform === 'win32') {
            return 'py';
        }

        // On other platforms, use python3
        return 'python3';
    }

    /**
     * Run Python process
     */
    private runPython(
        interpreter: string,
        args: string[],
        env: NodeJS.ProcessEnv,
        context: ExecutionContext,
        timeout?: number
    ): Promise<{ exitCode: number; stdout: string; stderr: string }> {
        return new Promise((resolve) => {
            const proc = spawn(interpreter, args, {
                env,
                shell: true
            });

            let stdout = '';
            let stderr = '';

            proc.stdout.on('data', (data) => {
                const text = data.toString();
                stdout += text;
                context.onProgress?.(`[Python] ${text.trim()}`);
            });

            proc.stderr.on('data', (data) => {
                const text = data.toString();
                stderr += text;
                context.onProgress?.(`[Python ERROR] ${text.trim()}`);
            });

            proc.on('close', (code) => {
                resolve({
                    exitCode: code ?? 1,
                    stdout: stdout.trim(),
                    stderr: stderr.trim()
                });
            });

            proc.on('error', (err) => {
                resolve({
                    exitCode: 1,
                    stdout: '',
                    stderr: `Failed to start Python: ${err.message}`
                });
            });

            // Handle cancellation
            if (context.cancellationToken) {
                const disposable = context.cancellationToken.onCancellationRequested(() => {
                    proc.kill();
                    disposable.dispose();
                });
            }

            // Handle timeout
            if (timeout && timeout > 0) {
                setTimeout(() => {
                    proc.kill();
                    resolve({
                        exitCode: 1,
                        stdout: stdout.trim(),
                        stderr: `Execution timed out after ${timeout} seconds`
                    });
                }, timeout * 1000);
            }
        });
    }
}
