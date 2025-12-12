// utils/dlog.js
const fs = require('fs');
const path = require('path');

// One flat file. Change via env if you want.
const LOG_FILE = process.env.DEBUG_LOG_FILE || path.join(process.cwd(), 'debug.log');

function fmt(v) {
  try { return typeof v === 'string' ? v : JSON.stringify(v); }
  catch { return String(v); }
}

function log(msg, meta) {
  const line = `[${new Date().toISOString()}] ${msg}${meta !== undefined ? ' ' + fmt(meta) : ''}\n`;
  fs.appendFile(LOG_FILE, line, () => {});
  // also echo to console so you see it in dev
  console.log('[DBG]', msg, meta ?? '');
}

function clear() {
  try { fs.writeFileSync(LOG_FILE, ''); } catch {}
}

module.exports = Object.assign(log, { file: LOG_FILE, clear });
