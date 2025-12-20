const esbuild = require('esbuild');
const { execSync } = require('child_process');

const production = process.argv.includes('--production');

// Build driver TypeScript before bundling
console.log('Building driver...');
execSync('npm run build', { cwd: './driver', stdio: 'inherit' });
console.log('Driver built successfully.');

async function main() {
  const ctx = await esbuild.context({
    entryPoints: ['./src/extension.ts'],
    bundle: true,
    format: 'cjs',
    minify: production,
    sourcemap: !production, // ✅ Sourcemap only in dev, not in production
    sourcesContent: true, // Required for debugging to map back to TS
    platform: 'node',
    outfile: 'dist/extension.js',
    external: ['vscode'],
    logLevel: 'info',

  });

  const watch = process.argv.includes('--watch');

  if (watch) {
    await ctx.watch();
  } else {
    await ctx.rebuild();
    await ctx.dispose();
  }
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});