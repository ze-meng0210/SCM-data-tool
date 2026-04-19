import './styles.css';
import QRCode from 'qrcode';

const REQUIRED_UPLOADS = [
  ['soh', '全球 SOH 库存'],
  ['overseas_po', '海外 Open PO'],
  ['china_po', '中国 Open PO'],
  ['in_transit', '中国 In Transit'],
  ['vmi', '中国 VMI'],
  ['demand_all', '办公电脑需求_全场景'],
  ['demand_hire', '办公电脑需求_入职场景'],
  ['demand_replace', '办公电脑需求_到期更换场景'],
  ['demand_monitor', '显示器和手绘板需求_全场景'],
];

const stockItems = REQUIRED_UPLOADS.slice(0, 5);
const demandItems = REQUIRED_UPLOADS.slice(5);

const app = document.querySelector('#app');

function renderUploadFields(items, groupKey) {
  return items
    .map(
      ([name, label]) => `
        <div class="upload-item">
          <div class="upload-item-title">${label}</div>
          <label class="upload-dropzone" for="${name}">
            <input class="file-input" type="file" id="${name}" name="${name}" accept=".xlsx,.xls" required />
            <div class="upload-dropzone-inner">
              <div class="upload-icon">+</div>
              <div class="upload-main">点击选择 Excel 文件</div>
              <div class="upload-sub">支持 .xlsx / .xls，文件名可任意</div>
              <div class="upload-file-name" id="${groupKey}_${name}_filename">未选择文件</div>
            </div>
          </label>
        </div>
      `,
    )
    .join('');
}

app.innerHTML = `
  <div class="wrap">
    <div class="hero">
      <div class="hero-badge">Supply Chain Data Platform</div>
      <h1>IT- SCM库存需求数据处理工具（ 办公终端）</h1>
      <p>开发环境由 Vite 热重载前端与 FastAPI API 共同驱动，支持本机链接、局域网链接、二维码和生产唯一访问链接展示。</p>
    </div>

    <div id="pageMessage"></div>

    <div class="layout three-col">
      <section class="card runtime-card">
        <div class="section-tag">访问链接</div>
        <h2>当前有效链接</h2>
        <p class="desc">控制台与页面都会同步展示当前可访问地址，便于桌面端与移动端调试。</p>
        <div class="runtime-meta-grid">
          <div class="meta-box"><div class="meta-label">环境</div><div class="meta-value" id="envName">-</div></div>
          <div class="meta-box"><div class="meta-label">版本</div><div class="meta-value" id="appVersion">-</div></div>
          <div class="meta-box"><div class="meta-label">构建时间</div><div class="meta-value" id="buildTime">-</div></div>
          <div class="meta-box"><div class="meta-label">API</div><div class="meta-value" id="backendUrl">-</div></div>
        </div>
        <div class="links-list" id="linksList"></div>
        <div class="qr-grid" id="qrGrid"></div>
      </section>

      <section class="card">
        <div class="section-tag">数据入口</div>
        <h2>上传原始文件</h2>
        <p class="desc">原始文件命名没有硬性要求。系统按上传槽位读取，不依赖文件名。</p>
        <form id="runForm" enctype="multipart/form-data">
          <div class="upload-groups">
            <div class="group-card">
              <div class="group-title">库存类数据</div>
              <div class="upload-list">${renderUploadFields(stockItems, 'stock')}</div>
            </div>
            <div class="group-card">
              <div class="group-title">需求类数据</div>
              <div class="upload-list">${renderUploadFields(demandItems, 'demand')}</div>
            </div>
          </div>
          <button id="runBtn" class="primary-btn" type="submit">开始处理</button>
          <div id="loading" class="loading">
            <div class="hint">任务正在执行中，请勿关闭页面。</div>
            <div class="progress-meta">
              <span id="progressStage">准备开始</span>
              <span id="progressPercent">0%</span>
            </div>
            <div class="bar"><div id="progressBar"></div></div>
          </div>
        </form>

        <div id="resultCard" class="card result-card hidden">
          <div class="section-tag">任务结果</div>
          <h2>处理结果</h2>
          <div class="result-meta-grid">
            <div class="meta-box"><div class="meta-label">任务ID</div><div class="meta-value" id="resultTaskId">-</div></div>
            <div class="meta-box"><div class="meta-label">状态</div><div class="meta-value" id="resultStatus">-</div></div>
            <div class="meta-box"><div class="meta-label">结果文件</div><div class="meta-value"><a class="ghost-link" id="resultDownload" href="#" target="_blank" rel="noreferrer">下载输出 Excel</a></div></div>
          </div>
          <div id="resultStats" class="stats-grid"></div>
          <details>
            <summary>查看执行日志</summary>
            <pre id="resultLog"></pre>
          </details>
        </div>
      </section>

      <section class="side-column">
        <div class="card side-card">
          <div class="section-tag">规则中心</div>
          <h2>映射规则管理</h2>
          <p class="desc">用于下载/更新 <code>正式标准化SPU映射表.xlsx</code>，上传后会立即应用到后续任务。</p>
          <p class="mapping-link-row"><a class="ghost-link" href="/mapping/formal/download">下载当前正式标准化SPU映射表</a></p>
          <form id="mappingForm" enctype="multipart/form-data">
            <div class="upload-item compact">
              <div class="upload-item-title">上传更新后的正式映射表</div>
              <label class="upload-dropzone compact" for="formal_mapping_file">
                <input class="file-input" type="file" id="formal_mapping_file" name="formal_mapping_file" accept=".xlsx,.xls" required />
                <div class="upload-dropzone-inner">
                  <div class="upload-icon">+</div>
                  <div class="upload-main">选择新的正式标准化SPU映射表</div>
                  <div class="upload-sub">需包含 sheet: 正式映射表 和标准化SPU相关列</div>
                  <div class="upload-file-name" id="mapping_formal_mapping_file_filename">未选择文件</div>
                </div>
              </label>
            </div>
            <button class="primary-btn secondary-btn" type="submit">上传并替换</button>
          </form>
        </div>

        <div class="card side-card troubleshooting-card">
          <div class="section-tag">故障排查</div>
          <h2>链接访问指引</h2>
          <ul id="troubleshootingList" class="tips-list"></ul>
        </div>
      </section>
    </div>
  </div>
`;

const form = document.getElementById('runForm');
const mappingForm = document.getElementById('mappingForm');
const btn = document.getElementById('runBtn');
const loading = document.getElementById('loading');
const progressStage = document.getElementById('progressStage');
const progressPercent = document.getElementById('progressPercent');
const progressBar = document.getElementById('progressBar');
const resultCard = document.getElementById('resultCard');
const resultTaskId = document.getElementById('resultTaskId');
const resultStatus = document.getElementById('resultStatus');
const resultDownload = document.getElementById('resultDownload');
const resultLog = document.getElementById('resultLog');
const resultStats = document.getElementById('resultStats');
const linksList = document.getElementById('linksList');
const qrGrid = document.getElementById('qrGrid');
const envName = document.getElementById('envName');
const appVersion = document.getElementById('appVersion');
const buildTime = document.getElementById('buildTime');
const backendUrl = document.getElementById('backendUrl');
const pageMessage = document.getElementById('pageMessage');
const troubleshootingList = document.getElementById('troubleshootingList');

function setMessage(message, type = 'success') {
  if (!message) {
    pageMessage.innerHTML = '';
    return;
  }
  pageMessage.innerHTML = `<div class="msg ${type}">${message}</div>`;
}

function bindFileInputs() {
  document.querySelectorAll('.file-input').forEach((input) => {
    input.addEventListener('change', () => {
      let nameId = '';
      if (input.id === 'formal_mapping_file') {
        nameId = 'mapping_formal_mapping_file_filename';
      } else if (['soh', 'overseas_po', 'china_po', 'in_transit', 'vmi'].includes(input.id)) {
        nameId = `stock_${input.id}_filename`;
      } else {
        nameId = `demand_${input.id}_filename`;
      }

      const target = document.getElementById(nameId);
      if (!target) {
        return;
      }

      if (input.files && input.files.length > 0) {
        target.textContent = input.files[0].name;
        target.classList.add('selected');
      } else {
        target.textContent = '未选择文件';
        target.classList.remove('selected');
      }
    });
  });
}

function renderStats(stats) {
  resultStats.innerHTML = '';
  Object.entries(stats || {}).forEach(([key, value]) => {
    const el = document.createElement('div');
    el.className = 'stat';
    el.innerHTML = `<div class="k">${key}</div><div class="v">${value}</div>`;
    resultStats.appendChild(el);
  });
}

function normalizeLinks(apiLinks = {}) {
  const links = [];
  const addLink = (label, url) => {
    if (!url) {
      return;
    }
    if (links.some((item) => item.url === url)) {
      return;
    }
    links.push({ label, url });
  };

  addLink('当前页面', window.location.href);
  addLink('本机开发链接', apiLinks.local_url);
  addLink('局域网开发链接', apiLinks.lan_url);
  addLink('生产部署链接', apiLinks.production_url);
  addLink('生产唯一链接', apiLinks.versioned_production_url);
  return links;
}

async function renderQrCodes(links) {
  qrGrid.innerHTML = '';
  for (const item of links.slice(0, 3)) {
    const wrapper = document.createElement('div');
    wrapper.className = 'qr-box';
    wrapper.innerHTML = `
      <div class="meta-label">${item.label}</div>
      <canvas width="140" height="140"></canvas>
      <div class="qr-url">${item.url}</div>
    `;
    qrGrid.appendChild(wrapper);
    const canvas = wrapper.querySelector('canvas');
    await QRCode.toCanvas(canvas, item.url, {
      margin: 1,
      width: 140,
      color: {
        dark: '#e2e8f0',
        light: '#0f1117',
      },
    });
  }
}

function renderLinks(runtimeInfo) {
  const links = normalizeLinks(runtimeInfo.links || {});
  linksList.innerHTML = links
    .map(
      (item) => `
        <div class="link-row">
          <div>
            <div class="meta-label">${item.label}</div>
            <a class="runtime-link" href="${item.url}" target="_blank" rel="noreferrer">${item.url}</a>
          </div>
        </div>
      `,
    )
    .join('');

  envName.textContent = runtimeInfo.environment || 'development';
  appVersion.textContent = runtimeInfo.version || 'dev';
  buildTime.textContent = runtimeInfo.build_time || new Date().toISOString();
  backendUrl.textContent = runtimeInfo.links?.backend_url || '通过 Vite 代理访问';

  troubleshootingList.innerHTML = (runtimeInfo.troubleshooting || [])
    .map((item) => `<li>${item}</li>`)
    .join('');

  renderQrCodes(links).catch((error) => {
    console.error('二维码生成失败:', error);
  });

  console.clear();
  console.log('[dev-links] 当前有效访问链接');
  links.forEach((item) => {
    console.log(`${item.label}: ${item.url}`);
  });
}

async function fetchRuntimeInfo() {
  try {
    const response = await fetch('/api/runtime-info');
    const data = await response.json();
    renderLinks(data);
  } catch (error) {
    console.error(error);
    setMessage('未能获取运行时链接信息，请确认后端服务已启动。', 'error');
  }
}

async function pollTask(taskId) {
  while (true) {
    const response = await fetch(`/api/task/${taskId}`);
    const data = await response.json();
    progressStage.textContent = data.message || '处理中';
    progressPercent.textContent = `${data.progress || 0}%`;
    progressBar.style.width = `${data.progress || 0}%`;

    if (data.status === 'success') {
      btn.disabled = false;
      resultCard.classList.remove('hidden');
      resultTaskId.textContent = data.task_id;
      resultStatus.textContent = 'success';
      resultDownload.href = data.download_url;
      resultLog.textContent = data.log || '';
      renderStats(data.stats);
      setMessage('处理完成，结果链接已可下载。');
      return;
    }

    if (data.status === 'failed') {
      btn.disabled = false;
      resultCard.classList.remove('hidden');
      resultTaskId.textContent = data.task_id;
      resultStatus.textContent = 'failed';
      resultDownload.removeAttribute('href');
      resultLog.textContent = data.log || '任务失败';
      renderStats(data.stats);
      setMessage('处理失败，请先查看日志与故障排查提示。', 'error');
      return;
    }

    await new Promise((resolve) => window.setTimeout(resolve, 1000));
  }
}

if (form) {
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    btn.disabled = true;
    loading.style.display = 'block';
    resultCard.classList.add('hidden');
    progressStage.textContent = '正在上传文件';
    progressPercent.textContent = '0%';
    progressBar.style.width = '0%';
    setMessage('');

    try {
      const formData = new FormData(form);
      const response = await fetch('/api/run', { method: 'POST', body: formData });
      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || '提交失败');
      }

      await pollTask(data.task_id);
    } catch (error) {
      btn.disabled = false;
      loading.style.display = 'none';
      setMessage(error.message || '提交失败', 'error');
    }
  });
}

if (mappingForm) {
  mappingForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    setMessage('');

    try {
      const formData = new FormData(mappingForm);
      const response = await fetch('/api/mapping/formal/upload', {
        method: 'POST',
        body: formData,
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || '上传失败');
      }
      setMessage(data.message || '映射表上传成功。');
      mappingForm.reset();
      document.getElementById('mapping_formal_mapping_file_filename').textContent = '未选择文件';
      document.getElementById('mapping_formal_mapping_file_filename').classList.remove('selected');
    } catch (error) {
      setMessage(error.message || '上传失败', 'error');
    }
  });
}

bindFileInputs();
fetchRuntimeInfo();
