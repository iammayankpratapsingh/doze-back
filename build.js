const esbuild = require('esbuild');
const path = require('path');
const fs = require('fs');
const dotenv = require('dotenv');

// Load environment variables from .env.production if it exists
const envPath = path.resolve(__dirname, '.env.production');
let env = {};
if (fs.existsSync(envPath)) {
  const parsed = dotenv.config({ path: envPath }).parsed;
  if (parsed) env = parsed;
}

// Prepare env variables for esbuild.define
const define = {};
for (const key in env) {
  define[`process.env.${key}`] = JSON.stringify(env[key]);
}

// Configurable entry/output
const entryFile = process.env.ENTRY || 'server.js';
const outFile = process.env.OUTFILE || './dist/index.js';
const isProd = process.env.NODE_ENV === 'production';

// Check if entry file exists
if (!fs.existsSync(entryFile)) {
  console.error(`❌ Entry file "${entryFile}" does not exist.`);
  process.exit(1);
}

// Ensure output directory exists
const outDir = path.dirname(outFile);
if (!fs.existsSync(outDir)) {
  fs.mkdirSync(outDir, { recursive: true });
}

// Build with esbuild
esbuild.build({
  entryPoints: [entryFile],
  bundle: true,
  platform: 'node',
  target: ['node14'],
  outfile: outFile,
  format: 'cjs',
  sourcemap: !isProd,
  minify: isProd,
  define,
  external: [
    // Add external dependencies here if needed
    'request',
    'yamlparser'
  ],
  logLevel: 'info',
}).then(() => {
  console.log('✅ Build completed successfully.');
}).catch((err) => {
  console.error('❌ Build failed:\n', err.message || err);
  process.exit(1);
});