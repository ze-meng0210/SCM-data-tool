import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, statSync, watchFile, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import net from 'node:net';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, '..');
const stateDir = path.join(projectRoot, '.dev-server');
const signalFile = path.join(stateDir, 'restart.signal');
const stateFile = path.join(stateDir, 'state.json');

mkdirSync(stateDir, { recursive: true });

function getArgValue(flag, fallback = '') {
  const index = process.argv.indexOf(flag);
  if (index >= 0 && process.argv[index + 1]) {
    return process.argv[index + 1];
  }
  return fallback;
}

function getLanIp() {
  const interfaces = os.networkInterfaces();
  for (const group of Object.values(interfaces)) {
    for (const item of group || []) {
      if (item.family === 'IPv4' && !item.internal) {
        return item.address;
      }
    }
  }
  return '';
}

function findFreePort(startPort) {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.unref();
    server.on('error', () => resolve(findFreePort(startPort + 1)));
    server.listen(startPort, '0.0.0.0', () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

function openBrowser(url) {
  const child = spawn('open', [url], {
    detached: true,
    stdio: 'ignore',
  });
  child.unref();
}

function writeState(payload) {
  writeFileSync(stateFile, JSON.stringify(payload, null, 2));
}

function logBanner({ localUrl, lanUrl, backendUrl, version, buildTime }) {
  console.log('======================================');
  console.log('库存需求表本地开发服务器已启动');
  console.log('======================================');
  console.log(`前端链接: ${localUrl}`);
  console.log(`局域网链接: ${lanUrl || '未检测到可用网卡'}`);
  console.log(`后端 API: ${backendUrl}`);
  console.log(`版本号: ${version}`);
  console.log(`构建时间: ${buildTime}`);
  console.log('故障排查:');
  console.log('1. 如果 3000 被占用，脚本会自动切换到下一个可用端口。');
  console.log('2. 如果页面打不开，请确认本机防火墙未拦截 Node/Python。');
  console.log('3. 如果移动端打不开局域网地址，请确认设备在同一 Wi-Fi。');
  console.log('======================================');
}

function spawnProcess(command, args, env) {
  return spawn(command, args, {
    cwd: projectRoot,
    env,
    stdio: 'inherit',
  });
}

function killChild(child) {
  if (child && !child.killed) {
    child.kill('SIGTERM');
  }
}

async function syncLinks(env) {
  const args = [
    path.join(projectRoot, 'scripts', 'sync-links.mjs'),
    '--mode',
    'dev',
    '--local-url',
    env.APP_LOCAL_URL,
    '--lan-url',
    env.APP_LAN_URL,
    '--backend-url',
    env.APP_BACKEND_URL,
    '--version',
    env.APP_VERSION,
    '--timestamp',
    env.APP_BUILD_TIME,
  ];

  const child = spawn(process.execPath, args, {
    cwd: projectRoot,
    env,
    stdio: 'inherit',
  });

  await new Promise((resolve, reject) => {
    child.on('exit', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`sync-links exit code ${code}`));
      }
    });
  });
}

let backendChild;
let frontendChild;
let currentSignalMtime = existsSync(signalFile) ? statSync(signalFile).mtimeMs : 0;

async function startAll(reason = 'initial') {
  if (reason !== 'initial') {
    console.log(`[dev-server] 检测到 ${reason}，正在重启服务...`);
  }

  killChild(frontendChild);
  killChild(backendChild);

  const frontendPort = Number(getArgValue('--frontend-port')) || (await findFreePort(3000));
  const backendPort = Number(getArgValue('--backend-port')) || (await findFreePort(8000));
  const lanIp = getLanIp();
  const buildTime = new Date().toISOString();
  const version = `dev-${buildTime.replace(/[-:.TZ]/g, '').slice(0, 14)}`;
  const localUrl = `http://localhost:${frontendPort}`;
  const lanUrl = lanIp ? `http://${lanIp}:${frontendPort}` : '';
  const backendUrl = `http://127.0.0.1:${backendPort}`;

  const sharedEnv = {
    ...process.env,
    APP_ENV: 'development',
    APP_VERSION: version,
    APP_BUILD_TIME: buildTime,
    APP_PUBLIC_BASE_URL: localUrl,
    APP_LOCAL_URL: localUrl,
    APP_LAN_URL: lanUrl,
    APP_BACKEND_URL: backendUrl,
    VITE_PORT: String(frontendPort),
    VITE_API_TARGET: backendUrl,
  };

  await syncLinks(sharedEnv);

  backendChild = spawnProcess('python3', ['-m', 'uvicorn', 'main:app', '--host', '0.0.0.0', '--port', String(backendPort), '--reload'], sharedEnv);
  frontendChild = spawnProcess('npx', ['vite', '--config', 'vite.config.js', '--host', '0.0.0.0', '--port', String(frontendPort)], sharedEnv);

  writeState({
    pid: process.pid,
    frontendPid: frontendChild.pid,
    backendPid: backendChild.pid,
    localUrl,
    lanUrl,
    backendUrl,
    version,
    buildTime,
  });

  logBanner({ localUrl, lanUrl, backendUrl, version, buildTime });
  openBrowser(localUrl);

  frontendChild.on('exit', (code) => {
    console.log(`[dev-server] 前端进程退出，状态码: ${code ?? 'null'}`);
  });

  backendChild.on('exit', (code) => {
    console.log(`[dev-server] 后端进程退出，状态码: ${code ?? 'null'}`);
  });
}

function shutdown() {
  killChild(frontendChild);
  killChild(backendChild);
  process.exit(0);
}

watchFile(signalFile, { interval: 1000 }, async () => {
  if (!existsSync(signalFile)) {
    return;
  }

  const nextMtime = statSync(signalFile).mtimeMs;
  if (nextMtime !== currentSignalMtime) {
    currentSignalMtime = nextMtime;
    await startAll('pre-commit 重启信号');
  }
});

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

startAll().catch((error) => {
  console.error('[dev-server] 启动失败:', error);
  process.exit(1);
});
