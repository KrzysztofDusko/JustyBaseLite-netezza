/**
 * Type declarations for the NzConnection driver
 * This provides TypeScript types for the dynamically loaded Netezza driver
 */

import { EventEmitter } from 'events';

export interface NzConnectionConfig {
    host: string;
    port?: number;
    database: string;
    user: string;
    password?: string;
}

export interface NzDataReader {
    fieldCount: number;
    read(): Promise<boolean>;
    nextResult(): Promise<boolean>;
    close(): Promise<void>;
    getName(index: number): string;
    getTypeName(index: number): string;
    getValue(index: number): unknown;
}

export interface NzCommand {
    commandTimeout: number;
    executeReader(): Promise<NzDataReader>;
    cancel(): Promise<void>;
}

export interface NzConnection extends EventEmitter {
    connect(): Promise<void>;
    close(): Promise<void>;
    createCommand(sql: string): NzCommand;
    on(event: 'notice', listener: (msg: { message: string }) => void): this;
    removeListener(event: 'notice', listener: (msg: { message: string }) => void): this;
}

export interface NzConnectionConstructor {
    new(config: NzConnectionConfig): NzConnection;
}

/**
 * Helper function to create NzConnection instance
 * Provides proper typing for the dynamically loaded driver
 */
export function createNzConnection(config: NzConnectionConfig): NzConnection {
    const NzConnectionClass = require('../../libs/driver/src/NzConnection') as NzConnectionConstructor;
    return new NzConnectionClass(config);
}
