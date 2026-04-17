#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$SCRIPT_DIR/程序文件"
VENV_BIN="$SCRIPT_DIR/.venv/bin"
UVICORN_BIN="$VENV_BIN/uvicorn"
HOST="127.0.0.1"
PORT="8000"
URL="http://$HOST:$PORT/"
LOG_DIR="$SCRIPT_DIR/web_runs"
LOG_FILE="$LOG_DIR/web_app_server.log"

mkdir -p "$LOG_DIR"

echo "=============================="
echo "库存需求表网页工具启动器"
echo "=============================="
echo "项目目录: $SCRIPT_DIR"
echo "程序目录: $APP_DIR"
echo "访问地址: $URL"
echo ""

if [ ! -d "$APP_DIR" ]; then
  echo "错误: 未找到程序目录 $APP_DIR"
  echo ""
  read -r "?按回车键关闭窗口..."
  exit 1
fi

if [ ! -x "$UVICORN_BIN" ]; then
  echo "错误: 未找到 uvicorn，可执行文件路径为:"
  echo "$UVICORN_BIN"
  echo ""
  echo "请先确认 .venv 已正确创建。"
  echo ""
  read -r "?按回车键关闭窗口..."
  exit 1
fi

if lsof -iTCP:$PORT -sTCP:LISTEN >/dev/null 2>&1; then
  echo "检测到 $PORT 端口已有服务在运行，直接打开网页..."
  open "$URL"
  echo ""
  read -r "?按回车键关闭窗口..."
  exit 0
fi

echo "正在后台启动网页服务..."
cd "$APP_DIR"
nohup "$UVICORN_BIN" web_app:app --host "$HOST" --port "$PORT" > "$LOG_FILE" 2>&1 &

sleep 2

if lsof -iTCP:$PORT -sTCP:LISTEN >/dev/null 2>&1; then
  echo "启动成功，正在打开浏览器..."
  open "$URL"
  echo ""
  echo "服务日志文件:"
  echo "$LOG_FILE"
else
  echo "启动失败，请检查日志:"
  echo "$LOG_FILE"
fi

echo ""
read -r "?按回车键关闭窗口..."
