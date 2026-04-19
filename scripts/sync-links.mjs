import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, '..');
const readmePath = path.join(projectRoot, 'README.md');
const jsonPath = path.join(projectRoot, 'latest-access-links.json');
const stateDir = path.join(projectRoot, '.dev-server');

function getArgValue(flag, fallback = '') {
  const index = process.argv.indexOf(flag);
  if (index >= 0 && process.argv[index + 1]) {
    return process.argv[index + 1];
  }
  return fallback;
}

function buildVersionedUrl(baseUrl, version, timestamp) {
  const normalized = (baseUrl || '').trim().replace(/\/$/, '');
  if (!normalized) {
    return '';
  }
  const query = new URLSearchParams();
  if (version) {
    query.set('v', version);
  }
  if (timestamp) {
    query.set('ts', timestamp);
  }
  const suffix = query.toString();
  return suffix ? `${normalized}?${suffix}` : normalized;
}

const mode = getArgValue('--mode', process.env.LINKS_MODE || 'manual');
const localUrl = getArgValue('--local-url', process.env.APP_LOCAL_URL || '');
const lanUrl = getArgValue('--lan-url', process.env.APP_LAN_URL || '');
const backendUrl = getArgValue('--backend-url', process.env.APP_BACKEND_URL || '');
const timestamp = getArgValue('--timestamp', process.env.APP_BUILD_TIME || new Date().toISOString());
const version = getArgValue('--version', process.env.APP_VERSION || process.env.GITHUB_SHA?.slice(0, 7) || 'dev');
const productionUrl = getArgValue('--base-url', process.env.DEPLOY_BASE_URL || process.env.APP_PUBLIC_BASE_URL || '');
const versionedProductionUrl = buildVersionedUrl(productionUrl, version, timestamp);

const payload = {
  mode,
  updated_at: timestamp,
  version,
  links: {
    local_url: localUrl,
    lan_url: lanUrl,
    backend_url: backendUrl,
    production_url: productionUrl,
    versioned_production_url: versionedProductionUrl,
  },
};

const markerStart = '<!-- ACCESS_LINKS:START -->';
const markerEnd = '<!-- ACCESS_LINKS:END -->';
const linkSection = `${markerStart}
## 最新访问链接

- 链接更新时间：${timestamp}
- 当前版本：${version}
- 本机开发链接：${localUrl || '待本地开发服务器启动后生成'}
- 局域网开发链接：${lanUrl || '待检测到局域网网卡后生成'}
- 后端接口地址：${backendUrl || '由开发编排脚本自动注入'}
- 生产部署链接：${productionUrl || '请在 GitHub 仓库变量 DEPLOY_BASE_URL 中配置'}
- 生产唯一链接：${versionedProductionUrl || '推送到 GitHub 后由 CI 自动生成'}
${markerEnd}`;

let readme = readFileSync(readmePath, 'utf8');
if (readme.includes(markerStart) && readme.includes(markerEnd)) {
  readme = readme.replace(new RegExp(`${markerStart}[\\s\\S]*?${markerEnd}`), linkSection);
} else {
  readme = `${readme.trim()}\n\n${linkSection}\n`;
}

mkdirSync(stateDir, { recursive: true });
writeFileSync(readmePath, readme);
writeFileSync(jsonPath, JSON.stringify(payload, null, 2));
writeFileSync(path.join(stateDir, 'latest-links.json'), JSON.stringify(payload, null, 2));
console.log('[sync-links] README.md 与 latest-access-links.json 已更新');
