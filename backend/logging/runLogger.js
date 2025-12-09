const fs = require('fs');
const path = require('path');
const EventEmitter = require('events');

const runs = new Map();

function makeLogDir() {
  const base = path.join(__dirname, '..', '..', 'datapipeline', 'logs');
  if (!fs.existsSync(base)) fs.mkdirSync(base, { recursive: true });
  return base;
}

function createRun(runId) {
  const emitter = new EventEmitter();
  const logDir = makeLogDir();
  const logPath = path.join(logDir, `run_${runId}.log`);
  // ensure file exists
  try { fs.writeFileSync(logPath, `Run ${runId} started\n`); } catch (e) {}
  runs.set(runId, { emitter, path: logPath });
  return { emitter, logPath };
}

function append(runId, text) {
  const r = runs.get(runId);
  const line = typeof text === 'string' ? text : JSON.stringify(text);
  if (r) {
    try { fs.appendFileSync(r.path, line + '\n'); } catch (e) {}
    try { r.emitter.emit('data', line); } catch (e) {}
  } else {
    // fallback append to file
    const logDir = makeLogDir();
    const logPath = path.join(logDir, `run_${runId}.log`);
    try { fs.appendFileSync(logPath, line + '\n'); } catch (e) {}
  }
  // also write to console
  console.log(`[run ${runId}] ${line}`);
}

function getContents(runId) {
  const r = runs.get(runId);
  const p = r ? r.path : path.join(makeLogDir(), `run_${runId}.log`);
  try { return fs.readFileSync(p, 'utf8'); } catch (e) { return ''; }
}

function subscribe(runId, res) {
  const r = runs.get(runId) || createRun(runId);
  // send existing contents
  try { res.write(`data: ${JSON.stringify({ initial: getContents(runId) })}\n\n`); } catch (e) {}
  const onData = (chunk) => {
    try { res.write(`data: ${JSON.stringify({ line: chunk })}\n\n`); } catch (e) {}
  };
  r.emitter.on('data', onData);
  // return unsubscribe
  return () => r.emitter.removeListener('data', onData);
}

module.exports = { createRun, append, getContents, subscribe };
