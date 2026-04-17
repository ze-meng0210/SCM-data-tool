#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$SCRIPT_DIR/程序文件"
cd "$APP_DIR"

OUTPUT_DIR="$SCRIPT_DIR/out put "
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUTPUT_FILE="$OUTPUT_DIR/output_double_click_${TIMESTAMP}.xlsx"

echo "=============================="
echo "IT资产供应链数据处理系统"
echo "=============================="
echo "项目目录: $SCRIPT_DIR"
echo "程序目录: $APP_DIR"
echo "输出文件: $OUTPUT_FILE"
echo ""

if ! command -v python3 >/dev/null 2>&1; then
  echo "错误: 未找到 python3，请先安装 Python 3。"
  echo ""
  read -r "?按回车键关闭窗口..."
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "开始执行数据处理..."
echo ""

python3 "$APP_DIR/main.py" \
  --soh "$SCRIPT_DIR/库存数据/全球SOH库存.xlsx" \
  --overseas-po "$SCRIPT_DIR/库存数据/海外open po库存.xlsx" \
  --china-po "$SCRIPT_DIR/库存数据/中国open po库存.xlsx" \
  --in-transit "$SCRIPT_DIR/库存数据/中国-in transit库存.xlsx" \
  --vmi "$SCRIPT_DIR/库存数据/中国VMI库存.xlsx" \
  --demand-all "$SCRIPT_DIR/全球需求数据/办公电脑需求_全场景.xlsx" \
  --demand-hire "$SCRIPT_DIR/全球需求数据/办公电脑需求_入职场景.xlsx" \
  --demand-replace "$SCRIPT_DIR/全球需求数据/办公电脑需求_到期更换场景.xlsx" \
  --demand-monitor "$SCRIPT_DIR/全球需求数据/显示器和手绘板需求_全场景.xlsx" \
  --mapping "$SCRIPT_DIR/mapping rules/mapping.xlsx" \
  --output "$OUTPUT_FILE"

echo ""
echo "处理完成。"
echo "结果文件已生成:"
echo "$OUTPUT_FILE"
echo ""
read -r "?按回车键关闭窗口..."
