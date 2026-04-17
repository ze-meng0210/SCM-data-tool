import csv
import math
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from utils import read_excel_safely


ROOT = Path("/Users/bytedance/Desktop/库存需求表")

SOURCE_FILES: List[Tuple[str, str]] = [
    ("全球SOH库存", "库存数据/全球SOH库存.xlsx"),
    ("海外open_po库存", "库存数据/海外open po库存.xlsx"),
    ("中国open_po库存", "库存数据/中国open po库存.xlsx"),
    ("中国in_transit库存", "库存数据/中国-in transit库存.xlsx"),
    ("中国VMI库存", "库存数据/中国VMI库存.xlsx"),
    ("办公电脑需求_全场景", "全球需求数据/办公电脑需求_全场景.xlsx"),
    ("办公电脑需求_入职场景", "全球需求数据/办公电脑需求_入职场景.xlsx"),
    ("办公电脑需求_到期更换场景", "全球需求数据/办公电脑需求_到期更换场景.xlsx"),
    ("显示器和手绘板需求_全场景", "全球需求数据/显示器和手绘板需求_全场景.xlsx"),
]

TARGET_FIELDS: Dict[str, List[str]] = {
    "全球SOH库存": ["货格类型(Apollo)", "SPU", "SKU编号", "区域", "国家", "楼宇"],
    "海外open_po库存": ["订单状态", "商品名称", "到货地点所属国家区域", "到货地点所属国家中文名称"],
    "中国open_po库存": ["状态信息", "采购单号", "PO 单号", "PR 单号", "SKU 编号", "SKU 型号", "供应商名称"],
    "中国in_transit库存": [
        "状态信息",
        "需求单号",
        "需求楼宇",
        "所在区域",
        "所在城市",
        "关联采购单",
        "PO 单号",
        "SKU 型号编号",
        "SKU 型号规格",
        "供应商名称",
    ],
    "中国VMI库存": [
        "状态信息",
        "关联发货单",
        "PO 单号",
        "应到货楼宇",
        "实际到货楼宇",
        "所属区域",
        "SKU 型号信息",
        "供应商名称",
    ],
    "办公电脑需求_全场景": ["区域", "国家", "场景", "资产名称", "型号", "SKU名称", "AM SPU"],
    "办公电脑需求_入职场景": ["区域", "国家", "场景", "资产名称", "型号", "SKU名称", "AM SPU"],
    "办公电脑需求_到期更换场景": ["区域", "国家", "场景", "资产名称", "型号", "SKU名称", "AM SPU"],
    "显示器和手绘板需求_全场景": ["区域", "国家", "场景", "资产名称", "型号", "SKU名称", "AM SPU"],
}

KEY_CANDIDATES: Dict[str, List[Sequence[str]]] = {
    "中国open_po库存": [
        ("采购单号",),
        ("PO 单号",),
        ("PR 单号",),
        ("SKU 编号",),
        ("采购单号", "SKU 编号"),
        ("采购单号", "PO 单号"),
        ("采购单号", "PR 单号"),
        ("PO 单号", "SKU 编号"),
        ("PR 单号", "SKU 编号"),
    ],
    "中国in_transit库存": [
        ("需求单号",),
        ("PO 单号",),
        ("关联采购单",),
        ("SKU 型号编号",),
        ("需求单号", "SKU 型号编号"),
        ("需求单号", "PO 单号"),
        ("需求单号", "关联采购单"),
        ("关联采购单", "SKU 型号编号"),
        ("PO 单号", "SKU 型号编号"),
    ],
}


def normalize_cell(value: object) -> Optional[str]:
    if pd.isna(value):
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, (int,)):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
        return format(value, "g")
    text = str(value).strip()
    return text or None


def build_unique_rows(df: pd.DataFrame, source_name: str, target_fields: Iterable[str]) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    value_rows: List[Dict[str, object]] = []
    summary_rows: List[Dict[str, object]] = []

    for field in target_fields:
        if field not in df.columns:
            summary_rows.append(
                {
                    "source_file": source_name,
                    "field_name": field,
                    "status": "missing_column",
                    "row_count": len(df),
                    "non_empty_count": 0,
                    "unique_count": 0,
                }
            )
            continue

        normalized = df[field].map(normalize_cell)
        non_empty = normalized.dropna()
        counts = non_empty.value_counts(dropna=True)

        summary_rows.append(
            {
                "source_file": source_name,
                "field_name": field,
                "status": "ok",
                "row_count": len(df),
                "non_empty_count": int(non_empty.shape[0]),
                "unique_count": int(counts.shape[0]),
            }
        )

        for sort_order, (unique_value, occurrence_count) in enumerate(counts.sort_index().items(), start=1):
            value_rows.append(
                {
                    "source_file": source_name,
                    "field_name": field,
                    "unique_value": unique_value,
                    "occurrence_count": int(occurrence_count),
                    "sort_order": sort_order,
                }
            )

    return value_rows, summary_rows


def evaluate_key_candidate(df: pd.DataFrame, source_name: str, columns: Sequence[str]) -> Dict[str, object]:
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        return {
            "source_file": source_name,
            "candidate_key": " + ".join(columns),
            "column_count": len(columns),
            "missing_columns": ",".join(missing_columns),
            "total_rows": len(df),
            "complete_rows": 0,
            "distinct_complete_keys": 0,
            "duplicate_complete_rows": 0,
            "null_component_rows": len(df),
            "is_unique_on_complete_rows": False,
            "is_full_primary_key": False,
            "recommendation_rank": "",
        }

    subset = df.loc[:, list(columns)].copy()
    normalized_subset = subset.apply(lambda col: col.map(normalize_cell))
    complete_mask = normalized_subset.notna().all(axis=1)
    complete_rows = normalized_subset.loc[complete_mask].copy()

    if complete_rows.empty:
        distinct_complete_keys = 0
        duplicate_complete_rows = 0
    else:
        key_series = complete_rows.astype(str).agg(" | ".join, axis=1)
        distinct_complete_keys = int(key_series.nunique(dropna=True))
        duplicate_complete_rows = int(key_series.duplicated(keep=False).sum())

    total_rows = len(df)
    null_component_rows = int((~complete_mask).sum())
    is_unique_on_complete_rows = complete_rows.shape[0] == distinct_complete_keys and complete_rows.shape[0] > 0
    is_full_primary_key = is_unique_on_complete_rows and null_component_rows == 0

    return {
        "source_file": source_name,
        "candidate_key": " + ".join(columns),
        "column_count": len(columns),
        "missing_columns": "",
        "total_rows": total_rows,
        "complete_rows": int(complete_rows.shape[0]),
        "distinct_complete_keys": distinct_complete_keys,
        "duplicate_complete_rows": duplicate_complete_rows,
        "null_component_rows": null_component_rows,
        "is_unique_on_complete_rows": is_unique_on_complete_rows,
        "is_full_primary_key": is_full_primary_key,
        "recommendation_rank": "",
    }


def rank_key_candidates(rows: List[Dict[str, object]]) -> None:
    valid_rows = [row for row in rows if row["missing_columns"] == ""]
    full_keys = [row for row in valid_rows if row["is_full_primary_key"]]

    if full_keys:
        full_keys.sort(key=lambda row: (row["column_count"], row["duplicate_complete_rows"], row["candidate_key"]))
        for rank, row in enumerate(full_keys, start=1):
            row["recommendation_rank"] = rank
        return

    valid_rows.sort(
        key=lambda row: (
            row["duplicate_complete_rows"],
            row["null_component_rows"],
            row["column_count"],
            row["candidate_key"],
        )
    )
    for rank, row in enumerate(valid_rows[:3], start=1):
        row["recommendation_rank"] = rank


def write_csv(path: Path, rows: List[Dict[str, object]], columns: Sequence[str]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(columns))
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def main() -> None:
    unique_rows: List[Dict[str, object]] = []
    field_summary_rows: List[Dict[str, object]] = []
    key_analysis_rows: List[Dict[str, object]] = []
    overview_rows: List[Dict[str, object]] = []

    loaded_frames: Dict[str, pd.DataFrame] = {}

    for source_name, relative_path in SOURCE_FILES:
        file_path = ROOT / relative_path
        df = read_excel_safely(str(file_path))
        loaded_frames[source_name] = df

        overview_rows.append(
            {
                "source_file": source_name,
                "relative_path": relative_path,
                "row_count": len(df),
                "column_count": len(df.columns),
            }
        )

        target_fields = TARGET_FIELDS[source_name]
        current_unique_rows, current_summary_rows = build_unique_rows(df, source_name, target_fields)
        unique_rows.extend(current_unique_rows)
        field_summary_rows.extend(current_summary_rows)

    for source_name, candidates in KEY_CANDIDATES.items():
        current_rows = [evaluate_key_candidate(loaded_frames[source_name], source_name, columns) for columns in candidates]
        rank_key_candidates(current_rows)
        key_analysis_rows.extend(current_rows)

    write_csv(
        ROOT / "9表指定字段唯一值映射表.csv",
        unique_rows,
        ["source_file", "field_name", "unique_value", "occurrence_count", "sort_order"],
    )
    write_csv(
        ROOT / "9表指定字段汇总.csv",
        field_summary_rows,
        ["source_file", "field_name", "status", "row_count", "non_empty_count", "unique_count"],
    )
    write_csv(
        ROOT / "9表概览.csv",
        overview_rows,
        ["source_file", "relative_path", "row_count", "column_count"],
    )
    write_csv(
        ROOT / "中国_openpo_intransit_主键分析.csv",
        key_analysis_rows,
        [
            "source_file",
            "candidate_key",
            "column_count",
            "missing_columns",
            "total_rows",
            "complete_rows",
            "distinct_complete_keys",
            "duplicate_complete_rows",
            "null_component_rows",
            "is_unique_on_complete_rows",
            "is_full_primary_key",
            "recommendation_rank",
        ],
    )

    recommended_rows = [
        row
        for row in key_analysis_rows
        if row["recommendation_rank"] in (1, 2, 3)
    ]
    recommended_rows.sort(key=lambda row: (row["source_file"], str(row["recommendation_rank"]), row["candidate_key"]))
    write_csv(
        ROOT / "中国_openpo_intransit_主键推荐.csv",
        recommended_rows,
        [
            "source_file",
            "candidate_key",
            "column_count",
            "total_rows",
            "complete_rows",
            "distinct_complete_keys",
            "duplicate_complete_rows",
            "null_component_rows",
            "is_full_primary_key",
            "recommendation_rank",
        ],
    )

    print(f"已生成: {ROOT / '9表指定字段唯一值映射表.csv'}")
    print(f"已生成: {ROOT / '9表指定字段汇总.csv'}")
    print(f"已生成: {ROOT / '9表概览.csv'}")
    print(f"已生成: {ROOT / '中国_openpo_intransit_主键分析.csv'}")
    print(f"已生成: {ROOT / '中国_openpo_intransit_主键推荐.csv'}")


if __name__ == "__main__":
    main()
