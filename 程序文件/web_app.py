import contextlib
import html
import io
import os
import threading
import traceback
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from pipeline_runner import run_pipeline


APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent
RUNS_DIR = Path(os.environ.get("APP_RUNS_DIR", "/tmp/inventory_tool_runs"))
MAPPING_PATH = PROJECT_ROOT / "mapping rules" / "mapping.xlsx"
OPTIONAL_PO_MAPPING_PATH = PROJECT_ROOT / "mapping rules" / "overseas_po_spu_mapping.xlsx"
FORMAL_MAPPING_PATH = PROJECT_ROOT / "mapping rules" / "正式标准化SPU映射表.xlsx"

RUNS_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="库存需求表处理工具", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TASKS = {}

REQUIRED_UPLOADS = [
    ("soh", "全球 SOH 库存"),
    ("overseas_po", "海外 Open PO"),
    ("china_po", "中国 Open PO"),
    ("in_transit", "中国 In Transit"),
    ("vmi", "中国 VMI"),
    ("demand_all", "办公电脑需求_全场景"),
    ("demand_hire", "办公电脑需求_入职场景"),
    ("demand_replace", "办公电脑需求_到期更换场景"),
    ("demand_monitor", "显示器和手绘板需求_全场景"),
]


def render_page(message="", error=""):
    message_html = f'<div class="msg success">{html.escape(message)}</div>' if message else ""
    error_html = f'<div class="msg error"><pre>{html.escape(error)}</pre></div>' if error else ""

    stock_items = REQUIRED_UPLOADS[:5]
    demand_items = REQUIRED_UPLOADS[5:]

    def build_upload_fields(items, group_key):
        blocks = []
        for name, label in items:
            blocks.append(
                f"""
                <div class="upload-item">
                  <div class="upload-item-title">{label}</div>
                  <label class="upload-dropzone" for="{name}">
                    <input class="file-input" type="file" id="{name}" name="{name}" accept=".xlsx,.xls" required />
                    <div class="upload-dropzone-inner">
                      <div class="upload-icon">+</div>
                      <div class="upload-main">点击选择 Excel 文件</div>
                      <div class="upload-sub">支持 .xlsx / .xls，文件名可任意</div>
                      <div class="upload-file-name" id="{group_key}_{name}_filename">未选择文件</div>
                    </div>
                  </label>
                </div>
                """
            )
        return "".join(blocks)

    stock_fields_html = build_upload_fields(stock_items, "stock")
    demand_fields_html = build_upload_fields(demand_items, "demand")

    mapping_box = """
    <div class="card side-card">
      <div class="section-tag">规则中心</div>
      <h2>映射规则管理</h2>
      <p class="desc">用于下载/更新 <code>正式标准化SPU映射表.xlsx</code>（上传后会在后续任务中生效）。</p>
      <p class="mapping-link-row"><a class="ghost-link" href="/mapping/formal/download">下载当前正式标准化SPU映射表</a></p>
      <form action="/mapping/formal/upload" method="post" enctype="multipart/form-data">
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
    """

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>库存需求表处理工具</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    :root {{
      --bg: #0f1117;
      --bg2: #1a1b2e;
      --panel: rgba(255,255,255,0.05);
      --border: rgba(255,255,255,0.10);
      --border-strong: rgba(96,165,250,0.34);
      --text-main: #f1f5f9;
      --text-sub: #94a3b8;
      --text-muted: #64748b;
      --primary: #3b82f6;
      --primary-2: #6366f1;
      --shadow: 0 12px 36px rgba(2,6,23,.45);
    }}

    * {{ box-sizing: border-box; }}

    body {{
      margin: 0;
      color: var(--text-main);
      font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(59,130,246,.16), transparent 32%),
        radial-gradient(circle at top right, rgba(99,102,241,.16), transparent 28%),
        linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);
      min-height: 100vh;
    }}

    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 36px 20px 72px;
    }}

    .hero {{
      text-align: center;
      margin-bottom: 24px;
    }}

    .hero-badge {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 8px 14px;
      border-radius: 999px;
      background: rgba(59,130,246,.12);
      border: 1px solid rgba(96,165,250,.22);
      color: #bfdbfe;
      font-size: 12px;
      letter-spacing: .08em;
      text-transform: uppercase;
      margin-bottom: 18px;
    }}

    .hero h1 {{
      margin: 0;
      font-size: 40px;
      font-weight: 800;
      line-height: 1.15;
      letter-spacing: -0.02em;
    }}

    .hero p {{
      margin: 14px auto 0;
      max-width: 760px;
      color: var(--text-sub);
      font-size: 16px;
      line-height: 1.75;
    }}

    .layout {{
      display: grid;
      grid-template-columns: minmax(0, 1.45fr) minmax(320px, .85fr);
      gap: 24px;
      align-items: start;
    }}

    @media (max-width: 1024px) {{
      .layout {{ grid-template-columns: 1fr; }}
    }}

    .card {{
      position: relative;
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 24px;
      backdrop-filter: blur(18px);
      box-shadow: var(--shadow);
      transition: all .3s ease;
      overflow: hidden;
    }}

    .card:hover {{
      transform: translateY(-2px);
      border-color: var(--border-strong);
      box-shadow: 0 18px 42px rgba(2,6,23,.5);
    }}

    .side-card {{ position: sticky; top: 20px; }}

    .section-tag {{
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      color: #bfdbfe;
      background: rgba(59,130,246,.14);
      border: 1px solid rgba(96,165,250,.22);
      margin-bottom: 14px;
    }}

    .card h2 {{ margin: 0 0 8px; font-size: 22px; font-weight: 700; }}
    .desc {{ margin: 0; color: var(--text-sub); line-height: 1.75; }}
    .hint {{ font-size: 13px; color: var(--text-muted); line-height: 1.65; }}

    .msg {{
      border-radius: 16px;
      padding: 14px 16px;
      margin-bottom: 18px;
      border: 1px solid var(--border);
      backdrop-filter: blur(12px);
    }}

    .success {{ background: rgba(34,197,94,.12); color: #bbf7d0; border-color: rgba(34,197,94,.22); }}
    .error {{ background: rgba(239,68,68,.12); color: #fecaca; border-color: rgba(239,68,68,.22); }}
    .error pre {{ margin: 0; background: transparent; padding: 0; border: 0; color: inherit; }}

    .upload-groups {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 18px;
      margin-top: 18px;
    }}

    @media (min-width: 860px) {{
      .upload-groups {{ grid-template-columns: 1fr 1fr; }}
    }}

    .group-card {{
      border-radius: 16px;
      padding: 18px;
      background: linear-gradient(180deg, rgba(255,255,255,.045), rgba(255,255,255,.025));
      border: 1px solid rgba(255,255,255,.08);
    }}

    .group-title {{
      display: flex;
      align-items: center;
      gap: 10px;
      margin: 0 0 14px;
      font-size: 18px;
      font-weight: 700;
    }}

    .group-title::before {{
      content: "";
      width: 4px;
      height: 18px;
      border-radius: 999px;
      background: linear-gradient(180deg, var(--primary-2), var(--primary));
      box-shadow: 0 0 14px rgba(59,130,246,.45);
    }}

    .upload-list {{ display: grid; gap: 14px; }}
    .upload-item-title {{ margin-bottom: 8px; font-size: 14px; font-weight: 600; color: #dbeafe; }}

    .upload-dropzone {{
      position: relative;
      display: block;
      border: 1px dashed rgba(148,163,184,.35);
      border-radius: 14px;
      background: rgba(15,23,42,.35);
      cursor: pointer;
      transition: all .25s ease;
      overflow: hidden;
    }}

    .upload-dropzone:hover {{
      border-color: rgba(96,165,250,.72);
      box-shadow: 0 0 0 1px rgba(96,165,250,.18), 0 0 24px rgba(59,130,246,.12);
      transform: translateY(-1px);
    }}

    .upload-dropzone-inner {{ padding: 16px 16px 14px; text-align: center; }}
    .upload-dropzone.compact .upload-dropzone-inner {{ padding: 14px; }}

    .upload-icon {{
      width: 34px;
      height: 34px;
      margin: 0 auto 10px;
      border-radius: 12px;
      display: grid;
      place-items: center;
      font-size: 20px;
      color: #dbeafe;
      background: linear-gradient(135deg, rgba(99,102,241,.25), rgba(59,130,246,.28));
      border: 1px solid rgba(96,165,250,.22);
    }}

    .upload-main {{ font-size: 14px; font-weight: 600; color: var(--text-main); }}
    .upload-sub {{ margin-top: 6px; font-size: 12px; color: var(--text-muted); }}
    .upload-file-name {{ margin-top: 10px; font-size: 12px; color: var(--text-sub); word-break: break-all; }}
    .upload-file-name.selected {{ color: #86efac; }}
    .upload-file-name.selected::before {{ content: "[OK] "; color: #22c55e; font-weight: 700; }}

    .file-input {{ position: absolute; opacity: 0; pointer-events: none; width: 1px; height: 1px; }}

    .primary-btn {{
      width: 100%;
      margin-top: 18px;
      border: 0;
      border-radius: 14px;
      padding: 14px 18px;
      font-size: 15px;
      font-weight: 700;
      color: white;
      background: linear-gradient(135deg, var(--primary-2), var(--primary));
      box-shadow: 0 12px 24px rgba(59,130,246,.24);
      cursor: pointer;
      transition: all .3s ease;
    }}

    .primary-btn:hover {{
      transform: translateY(-1px);
      box-shadow: 0 14px 30px rgba(59,130,246,.32), 0 0 24px rgba(59,130,246,.18);
    }}

    .primary-btn[disabled] {{
      background: linear-gradient(135deg, rgba(99,102,241,.45), rgba(59,130,246,.45));
      box-shadow: none;
      cursor: not-allowed;
      transform: none;
    }}

    .secondary-btn {{ width: auto; min-width: 180px; }}
    .mapping-link-row {{ margin: 16px 0 14px; }}

    .ghost-link {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: #bfdbfe;
      text-decoration: none;
      font-weight: 600;
    }}

    .ghost-link:hover {{ color: white; text-shadow: 0 0 18px rgba(96,165,250,.35); }}

    .loading {{
      display: none;
      margin-top: 16px;
      padding: 14px 16px;
      border-radius: 14px;
      background: rgba(15,23,42,.45);
      border: 1px solid rgba(96,165,250,.16);
    }}

    .progress-meta {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin: 8px 0 8px;
      color: #cbd5e1;
      font-size: 13px;
      gap: 12px;
    }}

    .bar {{ height: 10px; background: rgba(255,255,255,.06); border-radius: 999px; overflow: hidden; border: 1px solid rgba(255,255,255,.06); }}
    .bar > div {{ width: 0%; height: 100%; background: linear-gradient(90deg, var(--primary-2), var(--primary)); box-shadow: 0 0 18px rgba(59,130,246,.38); transition: width .35s ease; }}

    .result-card {{ margin-top: 22px; }}
    .result-meta-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-top: 16px; }}
    .meta-box {{ padding: 14px 16px; border-radius: 14px; background: rgba(15,23,42,.34); border: 1px solid rgba(255,255,255,.06); }}
    .meta-label {{ color: var(--text-muted); font-size: 12px; margin-bottom: 6px; }}
    .meta-value {{ color: var(--text-main); font-weight: 700; font-size: 15px; word-break: break-all; }}

    .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-top: 16px; }}
    .stat {{ background: rgba(15,23,42,.34); border: 1px solid rgba(255,255,255,.07); border-radius: 14px; padding: 14px; }}
    .stat .k {{ color: var(--text-muted); font-size: 12px; margin-bottom: 8px; }}
    .stat .v {{ font-size: 22px; font-weight: 700; color: var(--text-main); }}

    details {{ margin-top: 18px; border-top: 1px solid rgba(255,255,255,.06); padding-top: 16px; }}
    details summary {{ cursor: pointer; font-weight: 600; color: #dbeafe; }}

    pre {{
      margin-top: 12px;
      white-space: pre-wrap;
      word-break: break-word;
      background: rgba(2,6,23,.82);
      color: #e2e8f0;
      padding: 16px;
      border-radius: 14px;
      overflow: auto;
      border: 1px solid rgba(255,255,255,.06);
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="hero-badge">Supply Chain Data Platform</div>
      <h1>供应链数据处理工具</h1>
      <p>上传 5 个库存类 Excel 与 4 个需求类 Excel，系统将自动执行标准化映射、数据处理与汇总计算，并生成结果文件。</p>
    </div>
    {message_html}
    {error_html}
    <div class="layout">
      <div class="card">
        <div class="section-tag">数据入口</div>
        <h2>上传原始文件</h2>
        <p class="desc">原始文件命名没有硬性要求。系统按上传槽位读取，不依赖文件名。</p>
        <form id="runForm" enctype="multipart/form-data">
          <div class="upload-groups">
            <div class="group-card">
              <div class="group-title">库存类数据</div>
              <div class="upload-list">{stock_fields_html}</div>
            </div>
            <div class="group-card">
              <div class="group-title">需求类数据</div>
              <div class="upload-list">{demand_fields_html}</div>
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
            <div class="meta-box">
              <div class="meta-label">任务ID</div>
              <div class="meta-value" id="resultTaskId">-</div>
            </div>
            <div class="meta-box">
              <div class="meta-label">状态</div>
              <div class="meta-value" id="resultStatus">-</div>
            </div>
            <div class="meta-box">
              <div class="meta-label">结果文件</div>
              <div class="meta-value"><a class="ghost-link" id="resultDownload" href="#" target="_blank">下载输出 Excel</a></div>
            </div>
          </div>
          <div id="resultStats" class="stats-grid"></div>
          <details>
            <summary>查看执行日志</summary>
            <pre id="resultLog"></pre>
          </details>
        </div>
      </div>
      <div>{mapping_box}</div>
    </div>
  </div>
  <script>
    const form = document.getElementById('runForm');
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

    function bindFileInputs() {{
      document.querySelectorAll('.file-input').forEach((input) => {{
        input.addEventListener('change', () => {{
          let nameId = '';
          if (input.id === 'formal_mapping_file') {{
            nameId = 'mapping_formal_mapping_file_filename';
          }} else if (['soh','overseas_po','china_po','in_transit','vmi'].includes(input.id)) {{
            nameId = `stock_${{input.id}}_filename`;
          }} else {{
            nameId = `demand_${{input.id}}_filename`;
          }}
          const target = document.getElementById(nameId);
          if (!target) return;
          if (input.files && input.files.length > 0) {{
            target.textContent = input.files[0].name;
            target.classList.add('selected');
          }} else {{
            target.textContent = '未选择文件';
            target.classList.remove('selected');
          }}
        }});
      }});
    }}

    function renderStats(stats) {{
      if (!resultStats) return;
      resultStats.innerHTML = '';
      Object.entries(stats || {{}}).forEach(([k, v]) => {{
        const el = document.createElement('div');
        el.className = 'stat';
        el.innerHTML = `<div class="k">${{k}}</div><div class="v">${{v}}</div>`;
        resultStats.appendChild(el);
      }});
    }}

    async function pollTask(taskId) {{
      while (true) {{
        const resp = await fetch(`/api/task/${{taskId}}`);
        const data = await resp.json();
        progressStage.textContent = data.message || '处理中';
        progressPercent.textContent = `${{data.progress || 0}}%`;
        progressBar.style.width = `${{data.progress || 0}}%`;

        if (data.status === 'success') {{
          btn.disabled = false;
          resultCard.classList.remove('hidden');
          resultTaskId.textContent = data.task_id;
          resultStatus.textContent = 'success';
          resultDownload.href = data.download_url;
          resultLog.textContent = data.log || '';
          renderStats(data.stats);
          return;
        }}

        if (data.status === 'failed') {{
          btn.disabled = false;
          resultCard.classList.remove('hidden');
          resultTaskId.textContent = data.task_id;
          resultStatus.textContent = 'failed';
          resultDownload.removeAttribute('href');
          resultLog.textContent = data.log || '任务失败';
          renderStats(data.stats);
          return;
        }}

        await new Promise(resolve => setTimeout(resolve, 1000));
      }}
    }}

    bindFileInputs();

    if (form) {{
      form.addEventListener('submit', async (event) => {{
        event.preventDefault();
        btn.disabled = true;
        loading.style.display = 'block';
        resultCard.classList.add('hidden');
        progressStage.textContent = '正在上传文件';
        progressPercent.textContent = '0%';
        progressBar.style.width = '0%';

        const formData = new FormData(form);
        const resp = await fetch('/api/run', {{ method: 'POST', body: formData }});
        const data = await resp.json();

        if (!resp.ok) {{
          btn.disabled = false;
          loading.style.display = 'none';
          alert(data.error || '提交失败');
          return;
        }}

        await pollTask(data.task_id);
      }});
    }}
  </script>
</body>
</html>"""



def ensure_mapping_files():
    if not MAPPING_PATH.exists():
        raise HTTPException(status_code=500, detail=f"未找到基础映射文件: {MAPPING_PATH}")


def validate_formal_mapping_xlsx(path: Path):
    xls = pd.ExcelFile(path)
    sheet = "正式映射表" if "正式映射表" in xls.sheet_names else xls.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet, nrows=5)
    required = {"原始表名", "原始字段名", "原始值"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"正式映射表缺少必要列: {sorted(required)}")
    if ("标准化SPU" not in df.columns) and ("标准SPU" not in df.columns):
        raise ValueError("正式映射表缺少列：标准化SPU（或 标准SPU）")


def update_task(task_id: str, **kwargs):
    if task_id in TASKS:
        TASKS[task_id].update(kwargs)


async def save_upload(upload: UploadFile, target_path: Path):
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("wb") as buffer:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            buffer.write(chunk)
    await upload.close()


def execute_pipeline_task(task_id: str, saved_paths: dict, output_path: Path):
    log_buffer = io.StringIO()

    def progress_callback(progress, stage, message):
        update_task(task_id, progress=progress, stage=stage, message=message)

    try:
        with contextlib.redirect_stdout(log_buffer), contextlib.redirect_stderr(log_buffer):
            stats = run_pipeline(
                soh=saved_paths["soh"],
                overseas_po=saved_paths["overseas_po"],
                china_po=saved_paths["china_po"],
                in_transit=saved_paths["in_transit"],
                vmi=saved_paths["vmi"],
                demand_all=saved_paths["demand_all"],
                demand_hire=saved_paths["demand_hire"],
                demand_replace=saved_paths["demand_replace"],
                demand_monitor=saved_paths["demand_monitor"],
                mapping=str(MAPPING_PATH),
                output=str(output_path),
                po_mapping=str(OPTIONAL_PO_MAPPING_PATH) if OPTIONAL_PO_MAPPING_PATH.exists() else None,
                formal_mapping=str(FORMAL_MAPPING_PATH) if FORMAL_MAPPING_PATH.exists() else None,
                progress_callback=progress_callback,
            )

        update_task(
            task_id,
            status="success",
            progress=100,
            stage="done",
            message="处理完成",
            output_path=str(output_path),
            stats=stats,
            log=log_buffer.getvalue(),
        )
    except Exception:
        update_task(
            task_id,
            status="failed",
            stage="failed",
            message="处理失败",
            log=traceback.format_exc(),
        )


@app.get("/", response_class=HTMLResponse)
async def index():
    return render_page()


@app.get("/mapping/formal/download")
async def download_formal_mapping():
    if not FORMAL_MAPPING_PATH.exists():
        raise HTTPException(status_code=404, detail="未找到正式标准化SPU映射表.xlsx")
    return FileResponse(
        path=str(FORMAL_MAPPING_PATH),
        filename="正式标准化SPU映射表.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.post("/mapping/formal/upload", response_class=HTMLResponse)
async def upload_formal_mapping(formal_mapping_file: UploadFile = File(...)):
    try:
        suffix = Path(formal_mapping_file.filename or "").suffix or ".xlsx"
        if suffix.lower() not in [".xlsx", ".xls"]:
            return render_page(error="仅支持上传 .xlsx/.xls 文件")

        tmp_dir = RUNS_DIR / "_mapping_uploads"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / f"formal_mapping_upload{suffix}"
        await save_upload(formal_mapping_file, tmp_path)
        validate_formal_mapping_xlsx(tmp_path)

        FORMAL_MAPPING_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.replace(FORMAL_MAPPING_PATH)
        return render_page(message="上传成功：正式标准化SPU映射表已更新（后续任务将使用新规则）。")
    except Exception as e:
        return render_page(error=f"上传失败：{e}")


@app.post("/api/run")
async def run_web_pipeline(
    soh: UploadFile = File(...),
    overseas_po: UploadFile = File(...),
    china_po: UploadFile = File(...),
    in_transit: UploadFile = File(...),
    vmi: UploadFile = File(...),
    demand_all: UploadFile = File(...),
    demand_hire: UploadFile = File(...),
    demand_replace: UploadFile = File(...),
    demand_monitor: UploadFile = File(...),
):
    ensure_mapping_files()

    task_id = datetime.now().strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]
    task_dir = RUNS_DIR / task_id
    input_dir = task_dir / "input"
    output_dir = task_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"result_{task_id}.xlsx"

    uploads = {
        "soh": soh,
        "overseas_po": overseas_po,
        "china_po": china_po,
        "in_transit": in_transit,
        "vmi": vmi,
        "demand_all": demand_all,
        "demand_hire": demand_hire,
        "demand_replace": demand_replace,
        "demand_monitor": demand_monitor,
    }

    saved_paths = {}
    try:
        for field_name, upload in uploads.items():
            suffix = Path(upload.filename or "").suffix or ".xlsx"
            file_name = f"{field_name}{suffix}"
            saved_path = input_dir / file_name
            await save_upload(upload, saved_path)
            saved_paths[field_name] = str(saved_path)

        TASKS[task_id] = {
            "task_id": task_id,
            "status": "running",
            "progress": 0,
            "stage": "queued",
            "message": "文件上传完成，等待开始处理",
            "output_path": str(output_path),
            "stats": {},
            "log": "",
        }

        threading.Thread(
            target=execute_pipeline_task,
            args=(task_id, saved_paths, output_path),
            daemon=True,
        ).start()

        return JSONResponse({"task_id": task_id, "status": "running"})
    except Exception:
        return JSONResponse({"error": traceback.format_exc()}, status_code=500)


@app.get("/api/task/{task_id}")
async def task_status(task_id: str):
    task = TASKS.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="未找到该任务")

    return {
        "task_id": task_id,
        "status": task.get("status"),
        "progress": task.get("progress", 0),
        "stage": task.get("stage", ""),
        "message": task.get("message", ""),
        "stats": task.get("stats", {}),
        "log": task.get("log", ""),
        "download_url": f"/download/{task_id}" if task.get("status") == "success" else "",
    }


@app.get("/download/{task_id}")
async def download_result(task_id: str):
    task = TASKS.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="未找到该任务")

    output_path = Path(task["output_path"])
    if task["status"] != "success" or not output_path.exists():
        raise HTTPException(status_code=404, detail="结果文件不存在")

    return FileResponse(
        path=str(output_path),
        filename=output_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/health")
async def health():
    ensure_mapping_files()
    return {"status": "ok"}
