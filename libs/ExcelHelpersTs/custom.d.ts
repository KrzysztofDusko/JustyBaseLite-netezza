declare module 'archiver' {
    interface Archiver {
        pipe(stream: any): any;
        append(source: any, data?: any): any;
        finalize(): Promise<void>;
        on(event: string, listener: (...args: any[]) => void): any;
    }

    function archiver(format: string, options?: any): Archiver;

    namespace archiver {
        export { Archiver };
        export function create(format: string, options?: any): Archiver;
    }

    export = archiver;
}
