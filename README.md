# 库存需求表处理工具

这是一个基于 FastAPI 的 Excel 数据处理网页工具。

用户上传 5 个库存数据表和 4 个需求数据表后，系统会自动执行现有 Python 数据处理逻辑，并生成结果 Excel。

## 本地运行

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动服务

```bash
uvicorn main:app --host 127.0.0.1 --port 8000
```

### 3. 打开网页

```text
http://127.0.0.1:8000/
```

## Railway 部署

本项目已适配 Railway 部署：

- 根目录入口：`main.py`
- 启动命令：`uvicorn main:app --host 0.0.0.0 --port $PORT`
- 依赖文件：`requirements.txt`
- Railway 进程文件：`Procfile`

部署时需要确保仓库中保留以下规则文件：

- `mapping rules/mapping.xlsx`
- `mapping rules/正式标准化SPU映射表.xlsx`

## 项目结构

```text
.
├── main.py
├── Procfile
├── requirements.txt
├── README.md
├── 程序文件/
│   ├── web_app.py
│   ├── pipeline_runner.py
│   ├── main.py
│   └── ...
└── mapping rules/
    ├── mapping.xlsx
    └── 正式标准化SPU映射表.xlsx
```
