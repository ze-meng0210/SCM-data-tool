import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, '..');
const stateDir = path.join(projectRoot, '.dev-server');
const signalFile = path.join(stateDir, 'restart.signal');
const reasonIndex = process.argv.indexOf('--reason');
const reason = reasonIndex >= 0 ? process.argv[reasonIndex + 1] : 'manual';

mkdirSync(stateDir, { recursive: true });
writeFileSync(signalFile, `${new Date().toISOString()} ${reason}\n`);
console.log(`[restart-dev] 已写入重启信号: ${reason}`);
