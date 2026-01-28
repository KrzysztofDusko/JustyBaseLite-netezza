const esbuild = require('esbuild');
const fs = require('fs');

const production = process.argv.includes('--production');

async function main() {
  // Main extension bundle
  const extensionCtx = await esbuild.context({
    entryPoints: ['./src/extension.ts'],
    bundle: true,
    format: 'cjs',
    minify: production,
    sourcemap: !production,
    sourcesContent: true,
    platform: 'node',
    outfile: 'dist/extension.js',
    external: ['vscode'],
    logLevel: 'info',
  });

  // Webview scripts bundle
  const webviewEntryPoints = [
    './media/resultPanel.js',
    './media/editDataPanel.js',
    './media/analysisPanel.js',
    './media/queryHistory.js',
    './media/sessionMonitor.js'
  ].filter(f => fs.existsSync(f));

  const webviewCtx = await esbuild.context({
    entryPoints: webviewEntryPoints,
    bundle: true,
    format: 'iife',
    minify: production,
    sourcemap: !production,
    platform: 'browser',
    outdir: 'dist/media',
    logLevel: 'info',
  });

  const watch = process.argv.includes('--watch');

  if (watch) {
    await Promise.all([
      extensionCtx.watch(),
      webviewCtx.watch()
    ]);
  } else {
    await Promise.all([
      extensionCtx.rebuild(),
      webviewCtx.rebuild()
    ]);
    await Promise.all([
      extensionCtx.dispose(),
      webviewCtx.dispose()
    ]);
  }
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});