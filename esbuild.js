const esbuild = require('esbuild');

const production = process.argv.includes('--production');

async function main() {
  const ctx = await esbuild.context({
    entryPoints: ['./src/extension.ts'],
    bundle: true,
    format: 'cjs',
    minify: production,
    sourcemap: !production, // ✅ Sourcemap tylko w dev, nie w production
    sourcesContent: false,
    platform: 'node',
    outfile: 'dist/extension.js',
    external: ['vscode'],
    logLevel: 'info',
    plugins: [
      {
        name: 'make-all-packages-external',
        setup(build) {
          let filter = /^[^.\/]|^\.[^.\/]|^\.\.[^\/]/;
          build.onResolve({ filter }, args => ({
            path: args.path,
            external: true
          }));
        }
      }
    ]
  });
  
  if (production) {
    await ctx.rebuild();
    await ctx.dispose();
  } else {
    await ctx.watch();
  }
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});