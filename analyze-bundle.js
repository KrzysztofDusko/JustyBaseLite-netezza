const esbuild = require('esbuild');

esbuild.build({
    entryPoints: ['./src/extension.ts'],
    bundle: true,
    format: 'cjs',
    minify: true,
    platform: 'node',
    outfile: 'dist/extension.js',
    external: ['vscode'],
    metafile: true
}).then(result => {
    const inputs = Object.entries(result.metafile.inputs)
        .map(([file, data]) => ({ file, bytes: data.bytes }))
        .sort((a, b) => b.bytes - a.bytes)
        .slice(0, 30);

    console.log('\n=== TOP 30 LARGEST FILES IN BUNDLE ===\n');
    inputs.forEach((f, i) => {
        console.log(`${String(i + 1).padStart(2)}. ${(f.bytes / 1024).toFixed(1).padStart(8)} KB  ${f.file}`);
    });

    // Group by node_modules package
    const nodeModules = Object.entries(result.metafile.inputs)
        .filter(([file]) => file.includes('node_modules'))
        .reduce((acc, [file, data]) => {
            const match = file.match(/node_modules\/([^/]+)/);
            if (match) {
                const pkg = match[1];
                acc[pkg] = (acc[pkg] || 0) + data.bytes;
            }
            return acc;
        }, {});

    const sortedPackages = Object.entries(nodeModules)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 15);

    console.log('\n=== TOP 15 NODE_MODULES PACKAGES BY SIZE ===\n');
    sortedPackages.forEach(([pkg, bytes], i) => {
        console.log(`${String(i + 1).padStart(2)}. ${(bytes / 1024).toFixed(1).padStart(8)} KB  ${pkg}`);
    });

    // Source code vs dependencies
    const sourceSize = Object.entries(result.metafile.inputs)
        .filter(([file]) => !file.includes('node_modules'))
        .reduce((sum, [, data]) => sum + data.bytes, 0);

    const depsSize = Object.entries(result.metafile.inputs)
        .filter(([file]) => file.includes('node_modules'))
        .reduce((sum, [, data]) => sum + data.bytes, 0);

    console.log('\n=== SUMMARY ===\n');
    console.log(`Source code: ${(sourceSize / 1024).toFixed(1)} KB`);
    console.log(`Dependencies: ${(depsSize / 1024).toFixed(1)} KB`);
    console.log(`Total input: ${((sourceSize + depsSize) / 1024).toFixed(1)} KB`);
    console.log(`Output size: ${(result.metafile.outputs['dist/extension.js'].bytes / 1024).toFixed(1)} KB`);
});
