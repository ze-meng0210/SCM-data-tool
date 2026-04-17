#!/bin/bash

BASE_DIR="/Users/bytedance/Desktop/库存需求表"

python3 "${BASE_DIR}/main.py" \
  --soh "${BASE_DIR}/全球_SOH库存.xlsx" \
  --overseas-po "${BASE_DIR}/海外地区_open po数据.xlsx" \
  --china-po "${BASE_DIR}/中国_open po库存.xlsx" \
  --in-transit "${BASE_DIR}/中国_in transit库存.xlsx" \
  --vmi "${BASE_DIR}/中国_VMI库存.xlsx" \
  --demand-all "${BASE_DIR}/全球需求数据/办公电脑需求_全场景.xlsx" \
  --demand-hire "${BASE_DIR}/全球需求数据/办公电脑需求_入职场景.xlsx" \
  --demand-replace "${BASE_DIR}/全球需求数据/办公电脑需求_到期更换场景.xlsx" \
  --demand-monitor "${BASE_DIR}/全球需求数据/显示器和手绘板需求_全场景.xlsx" \
  --mapping "${BASE_DIR}/mapping.xlsx" \
  --output "${BASE_DIR}/output_$(date +%Y%m%d_%H%M%S).xlsx"
