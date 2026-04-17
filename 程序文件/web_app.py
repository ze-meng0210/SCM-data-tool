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

    fields_html = "".join(
        f"""
        <div class="field">
          <label for="{name}">{label}</label>
          <input type="file" id="{name}" name="{name}" accept=".xlsx,.xls" required />
        </div>
        """
        for name, label in REQUIRED_UPLOADS
    )

    mapping_box = """
    <div class="card">
      <h2>映射规则管理</h2>
      <p class="desc">用于下载/更新 <code>正式标准化SPU映射表.xlsx</code>（上传后会在后续任务中生效）。</p>
      <p class="hint">
        注意：如果部署在 Railway 这类平台上，文件系统可能是临时的；更新映射表通常只在本次部署实例生命周期内有效。
        如需长期保存，建议把更新后的映射表提交回 GitHub 或使用持久化存储。
      </p>
      <p><a href="/mapping/formal/download">下载当前正式标准化SPU映射表</a></p>
      <form action="/mapping/formal/upload" method="post" enctype="multipart/form-data">
        <div class="field">
          <label for="formal_mapping_file">上传更新后的正式标准化SPU映射表（.xlsx）</label>
          <input type="file" id="formal_mapping_file" name="formal_mapping_file" accept=".xlsx,.xls" required />
          <div class="hint">要求包含 sheet：<code>正式映射表</code>，并包含列：<code>原始表名</code>、<code>原始字段名</code>、<code>原始值</code>、<code>标准化SPU</code>（或 <code>标准SPU</code>）。</div>
        </div>
        <button type="submit">上传并替换</button>
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
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:#f5f7fb; color:#1f2937; margin:0; }}
    .wrap {{ max-width: 960px; margin: 0 auto; padding: 32px 20px 60px; }}
    .card {{ background:#fff; border-radius:16px; padding:24px; box-shadow:0 8px 24px rgba(15,23,42,.08); margin-bottom:20px; }}
    h1 {{ margin:0 0 8px; font-size:28px; }}
    p.desc {{ margin:0; color:#4b5563; }}
    form {{ display:grid; gap:14px; margin-top:18px; }}
    .field label {{ display:block; margin-bottom:6px; font-weight:600; }}
    .field input[type=file] {{ width:100%; }}
    button {{ background:#2563eb; color:#fff; border:0; border-radius:10px; padding:12px 18px; font-size:15px; cursor:pointer; }}
    button:hover {{ background:#1d4ed8; }}
    button[disabled] {{ background:#94a3b8; cursor:not-allowed; }}
    .msg {{ border-radius:12px; padding:14px 16px; margin-bottom:16px; }}
    .success {{ background:#ecfdf5; color:#166534; }}
    .error {{ background:#fef2f2; color:#991b1b; }}
    pre {{ white-space:pre-wrap; word-break:break-word; background:#0f172a; color:#e2e8f0; padding:16px; border-radius:12px; overflow:auto; }}
    .hint {{ font-size:13px; color:#6b7280; margin-top:6px; }}
    .grid {{ display:grid; grid-template-columns: 1fr; gap:20px; }}
    @media (min-width: 980px) {{ .grid {{ grid-template-columns: 1.2fr .8fr; }} }}
    .loading {{ display:none; margin-top:10px; }}
    .progress-meta {{ display:flex; justify-content:space-between; align-items:center; margin:8px 0 6px; color:#475569; font-size:13px; }}
    .bar {{ height:10px; background:#e5e7eb; border-radius:999px; overflow:hidden; }}
    .bar > div {{ width:0%; height:100%; background:#2563eb; transition: width .35s ease; }}
    .stats-grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin-top:12px; }}
    .stat {{ background:#f8fafc; border:1px solid #e5e7eb; border-radius:12px; padding:12px; }}
    .stat .k {{ color:#64748b; font-size:12px; margin-bottom:6px; }}
    .stat .v {{ font-size:20px; font-weight:700; }}
    .hidden {{ display:none; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>库存需求表处理工具</h1>
      <p class="desc">上传 5 个库存表和 4 个需求表，系统会自动调用现有 Python 处理逻辑并生成结果 Excel。</p>
      <p class="hint">系统固定使用 <code>mapping rules/mapping.xlsx</code> 作为基础规则，并自动读取同目录下的正式标准化 SPU 映射表。</p>
    </div>
    {message_html}
    {error_html}
    <div class="grid">
      <div class="card">
        <h2>上传原始文件</h2>
        <p class="hint">原始文件命名没有硬性要求。系统按你上传到的“对应槽位”读取，不依赖文件名。</p>
        <form id="runForm" enctype="multipart/form-data">
          {fields_html}
          <button id="runBtn" type="submit">开始处理</button>
          <div id="loading" class="loading">
            <div class="hint">正在处理，请勿关闭页面。</div>
            <div class="progress-meta">
              <span id="progressStage">准备开始</span>
              <span id="progressPercent">0%</span>
            </div>
            <div class="bar"><div id="progressBar"></div></div>
          </div>
        </form>
      </div>
      <div>
        {mapping_box}
      </div>
    </div>
    <div id="resultCard" class="card hidden">
      <h2>处理结果</h2>
      <p><strong>任务ID：</strong><span id="resultTaskId">-</span></p>
      <p><strong>状态：</strong><span id="resultStatus">-</span></p>
      <p><strong>结果文件：</strong><a id="resultDownload" href="#" target="_blank">下载输出 Excel</a></p>
      <div id="resultStats" class="stats-grid"></div>
      <details>
        <summary>查看执行日志</summary>
        <pre id="resultLog"></pre>
      </details>
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

    function renderStats(stats) {{
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
