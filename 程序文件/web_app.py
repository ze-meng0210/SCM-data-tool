import contextlib
import html
import io
import os
import traceback
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse

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


def render_page(message="", error="", task=None):
    message_html = f'<div class="msg success">{html.escape(message)}</div>' if message else ""
    error_html = f'<div class="msg error"><pre>{html.escape(error)}</pre></div>' if error else ""

    task_html = ""
    if task:
        stats_items = "".join(
            f"<li><strong>{html.escape(str(k))}</strong>: {html.escape(str(v))}</li>"
            for k, v in task.get("stats", {}).items()
        )
        preview_html = task.get("summary_preview_html", "")
        task_html = f"""
        <div class="card">
          <h2>最近一次处理结果</h2>
          <p><strong>任务ID：</strong>{html.escape(task['task_id'])}</p>
          <p><strong>状态：</strong>{html.escape(task['status'])}</p>
          <p><strong>结果文件：</strong><a href="/download/{html.escape(task['task_id'])}">下载输出 Excel</a></p>
          <ul>{stats_items}</ul>
          <details open>
            <summary>Summary_Region 预览（前 20 行）</summary>
            {preview_html}
          </details>
          <details>
            <summary>查看执行日志</summary>
            <pre>{html.escape(task.get('log', ''))}</pre>
          </details>
        </div>
        """

    fields_html = "".join(
        f"""
        <div class="field">
          <label for="{name}">{label}</label>
          <input type="file" id="{name}" name="{name}" accept=".xlsx,.xls" required />
        </div>
        """
        for name, label in REQUIRED_UPLOADS
    )

    mapping_box = f"""
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
    .wrap {{ max-width: 920px; margin: 0 auto; padding: 32px 20px 60px; }}
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
    details summary {{ cursor:pointer; font-weight:600; margin-bottom:12px; }}
    .hint {{ font-size:13px; color:#6b7280; margin-top:6px; }}
    .grid {{ display:grid; grid-template-columns: 1fr; gap:20px; }}
    @media (min-width: 980px) {{ .grid {{ grid-template-columns: 1.2fr .8fr; }} }}
    .loading {{ display:none; margin-top:10px; }}
    .bar {{ height:10px; background:#e5e7eb; border-radius:999px; overflow:hidden; }}
    .bar > div {{ width:35%; height:100%; background:#2563eb; animation: ind 1s linear infinite; }}
    @keyframes ind {{ 0% {{ transform: translateX(-100%); }} 100% {{ transform: translateX(300%); }} }}
    table {{ width:100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 8px; text-align:left; vertical-align: top; }}
    th {{ background: #f8fafc; position: sticky; top: 0; }}
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
        <form id="runForm" action="/run" method="post" enctype="multipart/form-data">
          {fields_html}
          <button id="runBtn" type="submit">开始处理</button>
          <div id="loading" class="loading">
            <div class="hint">正在处理，请勿关闭页面（处理完成后会返回结果页）。</div>
            <div class="bar"><div></div></div>
          </div>
        </form>
      </div>
      <div>
        {mapping_box}
      </div>
    </div>
    {task_html}
  </div>
  <script>
    const form = document.getElementById('runForm');
    const btn = document.getElementById('runBtn');
    const loading = document.getElementById('loading');
    if (form) {{
      form.addEventListener('submit', () => {{
        btn.disabled = true;
        loading.style.display = 'block';
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


async def save_upload(upload: UploadFile, target_path: Path):
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("wb") as buffer:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            buffer.write(chunk)
    await upload.close()


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


@app.post("/run", response_class=HTMLResponse)
async def run_web_pipeline(
    request: Request,
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

        log_buffer = io.StringIO()
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
            )

        # Build a lightweight HTML preview for Summary_Region (first 20 rows).
        summary_preview_html = ""
        try:
            summary_df = pd.read_excel(output_path, sheet_name="Summary_Region")
            preview_df = summary_df.head(20)
            summary_preview_html = preview_df.to_html(index=False, escape=True)
        except Exception:
            summary_preview_html = "<div class='hint'>预览生成失败（不影响结果文件下载）。</div>"

        TASKS[task_id] = {
            "task_id": task_id,
            "status": "success",
            "output_path": str(output_path),
            "stats": stats,
            "log": log_buffer.getvalue(),
            "summary_preview_html": summary_preview_html,
        }
        message = "处理完成，结果文件已生成。"
        return render_page(message=message, task=TASKS[task_id])
    except Exception:
        error_text = traceback.format_exc()
        TASKS[task_id] = {
            "task_id": task_id,
            "status": "failed",
            "output_path": "",
            "stats": {},
            "log": error_text,
        }
        return render_page(error=error_text)


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
