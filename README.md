# 供应链数据处理工具

这是一个基于 `FastAPI + Vite` 的 Excel 数据处理网页工具。

用户上传 5 个库存数据表和 4 个需求数据表后，系统会自动执行现有 Python 数据处理逻辑，并生成结果 Excel。当前项目已经补齐本地开发编排、链接维护、二维码展示与 Git 自动化所需的脚手架。

## 本地开发

### 1. 先安装依赖

Python：

```bash
pip install -r requirements.txt
```

Node.js 20+：

```bash
npm install
```

### 2. 启动开发环境

```bash
npm run dev
```

该命令会自动完成以下动作：

- 启动 Vite 热重载前端开发服务器，默认端口 `3000`，被占用时自动递增
- 启动 FastAPI 开发接口服务，默认端口 `8000`，被占用时自动递增
- 自动打开浏览器
- 在终端和页面内展示本机访问链接、局域网访问链接、版本号、构建时间和二维码
- 同步更新 `README.md` 与 `latest-access-links.json`

### 3. 手动构建生产前端

```bash
npm run build
```

构建完成后，FastAPI 会优先托管 `frontend/dist` 中的静态资源。

## 访问链接维护

<!-- ACCESS_LINKS:START -->
## 最新访问链接

- 链接更新时间：2026-04-19T00:00:00Z
- 当前版本：fallback-python-dev
- 本机开发链接：http://localhost:3000
- 局域网开发链接：http://192.168.1.6:3000
- 后端接口地址：http://localhost:3000
- 生产部署链接：请在 GitHub 仓库变量 DEPLOY_BASE_URL 中配置
- 生产唯一链接：推送到 GitHub 后由 CI 自动生成
<!-- ACCESS_LINKS:END -->

## Git 自动化

### Husky pre-commit

项目包含 `.husky/pre-commit`，在提交前会：

- 运行 `npm run sync:links` 同步 README 中的访问链接信息
- 运行 `npm run restart:dev -- --reason pre-commit`，向本地开发编排器发送重启信号

说明：

- 页面代码修改时，Vite HMR 会即时刷新前端
- Python 代码修改时，`uvicorn --reload` 会自动重启后端
- Git 提交时，pre-commit 会额外触发一次编排级重启，确保最新提交状态对应最新开发链接

## CI/CD

GitHub Actions 工作流位于 `.github/workflows/dev-links.yml`，在每次 `push` 后会：

- 安装 Python 与 Node 依赖
- 执行前端构建
- 可选触发外部部署 Webhook：`DEPLOY_WEBHOOK_URL`
- 使用仓库变量 `DEPLOY_BASE_URL` 生成生产部署链接与唯一版本化链接
- 自动回写 `README.md` 和 `latest-access-links.json`

## 生产部署

本项目仍兼容 Railway 或其他支持 Python Web 服务的平台：

- 根目录入口：`main.py`
- 启动命令：`uvicorn main:app --host 0.0.0.0 --port $PORT`
- 依赖文件：`requirements.txt`
- 构建前端：`npm install && npm run build`

部署时需要确保仓库中保留以下规则文件：

- `mapping rules/mapping.xlsx`
- `mapping rules/正式标准化SPU映射表.xlsx`

## 项目结构

```text
.
├── main.py
├── package.json
├── vite.config.js
├── Procfile
├── requirements.txt
├── latest-access-links.json
├── frontend/
│   ├── index.html
│   └── src/
├── scripts/
│   ├── dev-server.mjs
│   ├── restart-dev.mjs
│   └── sync-links.mjs
├── 程序文件/
│   ├── web_app.py
│   ├── pipeline_runner.py
│   └── ...
└── mapping rules/
    ├── mapping.xlsx
    └── 正式标准化SPU映射表.xlsx
```
