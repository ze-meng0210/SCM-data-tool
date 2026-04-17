from pathlib import Path
import re

import pandas as pd

from mapping_loader import MappingLoader
from utils import read_excel_safely


BASE_DIR = Path("/Users/bytedance/Desktop/库存需求表")
OUTPUT_PATH = BASE_DIR / "正式标准化SPU映射表.xlsx"
MAPPING_PATH = BASE_DIR / "mapping rules/mapping.xlsx"
MANUAL_MAPPING_PATH = BASE_DIR / "手工新增映射关系.xlsx"

TARGET_SPUS = {
    "Mac 笔记本 标配",
    "Mac 笔记本 高配",
    "MBP 14''",
    "MBP 16''",
    "Win i5 笔记本 标配",
    "Win i5 笔记本 高配",
    "Win i7 笔记本 标配",
    "Win i7 笔记本 高配",
    "Win i7 台式机 标配",
    "Win i7 台式机 高配",
    "Mac 一体机",
    "手绘板",
    "1080P 显示器",
    "2K 显示器",
    "2K 显示器 高刷",
    "4K 显示器",
    "4K 显示器 高刷",
}

DIRECT_MAP = {
    "Mac 笔记本 标配": ("Mac 笔记本 标配", "原始值已是标准 SPU"),
    "Mac 笔记本 高配": ("Mac 笔记本 高配", "原始值已是标准 SPU"),
    'Mac 笔记本 14"高配': ("MBP 14''", '按 14" 高配 MacBook Pro 归类'),
    'Mac 笔记本 16"高配': ("MBP 16''", '按 16" 高配 MacBook Pro 归类'),
    "Mac 笔记本 1T": ("Mac 笔记本 标配", "现行业务口径归入 Mac 笔记本 标配，建议业务复核"),
    "Win i5 笔记本 32G标配": ("Win i5 笔记本 标配", "32G 视为同一标准档位"),
    "Win i5 笔记本 16G标配": ("Win i5 笔记本 标配", "16G 视为同一标准档位"),
    "Win i5 笔记本 32G高配": ("Win i5 笔记本 高配", "32G 高配映射"),
    "Win i5 笔记本 16G高配": ("Win i5 笔记本 高配", "16G 高配映射"),
    "Win i7 笔记本 32G标配": ("Win i7 笔记本 标配", "32G 视为同一标准档位"),
    "Win i7 笔记本 高配": ("Win i7 笔记本 高配", "原始值已是标准 SPU"),
    "Win i7 台式机 标配": ("Win i7 台式机 标配", "原始值已是标准 SPU"),
    "Win i7 台式机 高配": ("Win i7 台式机 高配", "原始值已是标准 SPU"),
    "Mac 一体机": ("Mac 一体机", "原始值已是标准 SPU"),
    "手绘板": ("手绘板", "原始值已是标准 SPU"),
    "1080P 显示器": ("1080P 显示器", "原始值已是标准 SPU"),
    "2K 显示器": ("2K 显示器", "原始值已是标准 SPU"),
    "2K 显示器 高刷": ("2K 显示器 高刷", "原始值已是标准 SPU"),
    "2K 120hz": ("2K 显示器 高刷", "2K + 120Hz 视为高刷"),
    "4K 显示器": ("4K 显示器", "原始值已是标准 SPU"),
    "4K 显示器 高刷": ("4K 显示器 高刷", "原始值已是标准 SPU"),
    "Corporate Phone without SIM Card": ("待确认", "手机类资产，不在目标 SPU 范围"),
    "iPhone SE（办公手机）": ("待确认", "手机类资产，不在目标 SPU 范围"),
    "办公手机 + eSIM": ("待确认", "手机/eSIM 资产，不在目标 SPU 范围"),
}

NON_TARGET_KEYWORDS = [
    "mouse",
    "keyboard",
    "trackpad",
    "sensor",
    "hub",
    "stand",
    "cable",
    "cleaner",
    "gloves",
    "tape",
    "label",
    "shipping boxes",
    "bubble wrap",
    "signage",
    "locker",
    "installation",
    "maintentance",
    "maintenance",
    "ddp",
    "vat expenses",
    "exchange rate",
    "simcards",
    "sim card",
    "iphone",
    "ipad",
    "switch",
    "usb-c",
    "bluetooth",
    "receiver",
    "duster",
    "microfibra",
    "nitrilo",
    "scotch",
    "touchpad",
    "hardware",
    "others",
    "cutter",
]

ENGLISH_TO_TARGET = {
    "Mac Standard": "Mac 笔记本 标配",
    "Mac High": "Mac 笔记本 高配",
    "MBP 14''": "MBP 14''",
    "MBP 16''": "MBP 16''",
    "Win i5 Standard": "Win i5 笔记本 标配",
    "Win i5 High": "Win i5 笔记本 高配",
    "Win i7 Standard": "Win i7 笔记本 标配",
    "Win i7 High": "Win i7 笔记本 高配",
    "Win i7 DT Standard": "Win i7 台式机 标配",
    "Win i7 DT High": "Win i7 台式机 高配",
    "iMac": "Mac 一体机",
    "Graphic Tablet": "手绘板",
    "1080P Display": "1080P 显示器",
    "2K Display": "2K 显示器",
    "4K Display": "4K 显示器",
    "Phone": "待确认",
    "Mac 1T": "Mac 笔记本 标配",
    "Mac 1T 14\"": "MBP 14''",
    "Mac 1T 16\"": "MBP 16''",
    "2K 120hz": "2K 显示器 高刷",
}


def normalize_historical_spu(value: str):
    raw = "" if pd.isna(value) else str(value).strip()
    if not raw:
        return "", "空规则值"
    if raw in TARGET_SPUS:
        return raw, "mapping rules 直接命中目标 SPU"
    if raw in DIRECT_MAP:
        mapped_spu, _ = DIRECT_MAP[raw]
        return mapped_spu, "mapping rules 历史别名映射"
    if raw in ENGLISH_TO_TARGET:
        mapped = ENGLISH_TO_TARGET[raw]
        if mapped == "待确认":
            return mapped, "mapping rules 指向非目标终端"
        return mapped, "mapping rules 英文标准值映射"
    return "待确认", f"mapping rules 命中值为 {raw}，但不在当前目标 SPU 列表"


def normalize_manual_spu(value: str):
    raw = "" if pd.isna(value) else str(value).strip()
    if not raw:
        return "", "手工映射为空"
    if raw in TARGET_SPUS:
        return raw, "手工新增映射关系命中"
    if raw in DIRECT_MAP:
        mapped_spu, _ = DIRECT_MAP[raw]
        return mapped_spu, "手工新增映射关系命中（历史别名转换）"
    if raw in ENGLISH_TO_TARGET:
        mapped = ENGLISH_TO_TARGET[raw]
        if mapped == "待确认":
            return mapped, "手工新增映射关系指向非目标终端"
        return mapped, "手工新增映射关系命中（英文标准值转换）"
    return "待确认", f"手工新增映射值为 {raw}，但不在当前目标 SPU 列表"


def classify_value(raw_value: str, historical_spu: str = ""):
    value = "" if pd.isna(raw_value) else str(raw_value).strip()
    if not value:
        return "", "空值"

    if historical_spu:
        mapped_spu, reason = normalize_historical_spu(historical_spu)
        if mapped_spu:
            return mapped_spu, reason

    if value in DIRECT_MAP:
        return DIRECT_MAP[value]

    lower_value = value.lower()

    if "wacom" in lower_value or "和冠" in value or "ptk" in lower_value:
        return "手绘板", "品牌/型号命中手绘板"

    if "imac" in lower_value:
        return "Mac 一体机", "名称包含 iMac"

    if (
        "macbook air" in lower_value
        or "macbookair" in lower_value
        or "macair" in lower_value
        or "mba13" in lower_value
        or "mba 13" in lower_value
        or "mac light" in lower_value
        or "mac标配" in value
        or "mac air" in lower_value
    ):
        return "Mac 笔记本 标配", "名称包含 MacBook Air / MBA"

    if "macbook pro" in lower_value or "mbp" in lower_value or "macpro" in lower_value or "mac pro" in lower_value:
        if re.search(r'16\s*["”″]', value) or " 16" in value:
            return "MBP 16''", '名称包含 MBP / MacBook Pro 且尺寸为 16"'
        if re.search(r'14\s*["”″]', value) or " 14" in value:
            return "MBP 14''", '名称包含 MBP / MacBook Pro 且尺寸为 14"'
        return "待确认", "MacBook Pro 类，但缺少清晰尺寸信息"

    if "u2724de" in lower_value:
        return "2K 显示器", "参照历史规则中的 U2724DE 机型"

    if any(token in lower_value for token in ["u2723qe", "s80ud27", "ls27d804", "s80ud", "u2725qe", "p2725qe"]):
        if "120hz" in lower_value:
            return "4K 显示器 高刷", "参照历史规则中的 4K 显示器机型且包含 120Hz"
        return "4K 显示器", "参照历史规则中的 4K 显示器机型"

    display_hint = any(token in lower_value for token in ["display", "monitor", "viewfinity", "ultrasharp"]) or "显示器" in value

    if display_hint and ("4k" in lower_value or "uhd" in lower_value):
        if "120hz" in lower_value or "高刷" in value:
            return "4K 显示器 高刷", "4K/UHD 且包含 120Hz/高刷"
        return "4K 显示器", "4K/UHD 显示器"

    if display_hint and ("2k" in lower_value or "qhd" in lower_value or "2560" in lower_value):
        if "120hz" in lower_value or "高刷" in value:
            return "2K 显示器 高刷", "2K/QHD 且包含 120Hz/高刷"
        return "2K 显示器", "2K/QHD 显示器"

    if display_hint and any(token in lower_value for token in ["1080", "fhd", "wuxga", "1920*1200", "1920x1200"]):
        return "1080P 显示器", "FHD/WUXGA 类显示器"

    if display_hint:
        return "待确认", "显示器类，但缺少足够分辨率/刷新率信息"

    is_desktop = any(token in lower_value for token in ["desktop", "台式", "tower", "mini pc"])
    is_aio = "all-in-one" in lower_value or " aio " in f" {lower_value} "

    if is_aio:
        return "待确认", "Windows 一体机不在当前目标 SPU 列表"

    if any(token in lower_value for token in ["u7", "i7", "ultra 7", "ultra7", "ryzen ai 7", "ryzen ai 9", "12800h", "ai 7"]):
        if is_desktop or any(token in lower_value for token in ["rtx", "5080"]):
            if any(token in lower_value for token in ["premium", "high", "高配", "rtx", "5080", "2.5k"]):
                return "Win i7 台式机 高配", "i7/U7 台式机且含高配特征"
            return "Win i7 台式机 标配", "i7/U7 台式机"
        if any(token in lower_value for token in ["premium", "high", "高配", "2.5k", "2.8k", "2k"]):
            return "Win i7 笔记本 高配", "i7/U7 笔记本且含高配特征"
        return "Win i7 笔记本 标配", "i7/U7 笔记本"

    if any(token in lower_value for token in ["u5", "i5", "ultra 5", "ultra5", "ai 5", "pro 340", "135u"]):
        if is_desktop:
            return "待确认", "Win i5 台式机不在当前目标 SPU 列表"
        if any(token in lower_value for token in ["premium", "high", "高配", "2.5k", "2k"]):
            return "Win i5 笔记本 高配", "i5/U5 笔记本且含高配特征"
        return "Win i5 笔记本 标配", "i5/U5 笔记本"

    if any(token in lower_value for token in ["latitude 7450", "pb14250", "pro 14 plus", "pro14 plus"]):
        return "Win i5 笔记本 标配", "参照历史规则中的 Latitude 7450 / Pro 14 Plus 口径"

    if any(keyword in lower_value for keyword in NON_TARGET_KEYWORDS):
        return "待确认", "配件/服务/费用/通信类物料，不纳入当前目标 SPU"

    if any(token in lower_value for token in ["elitebook", "latitude", "thinkpad", "zbook", "pro14", "笔记本", "notebook"]):
        return "待确认", "笔记本类，但缺少足够 CPU/档位信息"

    return "待确认", "缺少足够特征或不在当前目标 SPU 范围"


def count_values(series: pd.Series):
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    return cleaned.value_counts()


def normalize_key_part(value):
    return "" if pd.isna(value) else str(value).strip()


def load_manual_override_map():
    if not MANUAL_MAPPING_PATH.exists():
        return {}, pd.DataFrame()

    manual_df = pd.read_excel(MANUAL_MAPPING_PATH)
    required = {"原始表名", "原始字段名", "原始值"}
    if not required.issubset(manual_df.columns):
        return {}, manual_df

    target_col = "标准SPU" if "标准SPU" in manual_df.columns else "标准化SPU"
    if target_col not in manual_df.columns:
        return {}, manual_df

    override_map = {}
    for _, row in manual_df.iterrows():
        key = (
            normalize_key_part(row.get("原始表名")),
            normalize_key_part(row.get("原始字段名")),
            normalize_key_part(row.get("原始值")),
        )
        target_value = normalize_key_part(row.get(target_col))
        if key[0] and key[1] and key[2] and target_value:
            override_map[key] = target_value

    return override_map, manual_df


def build_rows(
    table_name,
    field_name,
    counts,
    helper_field_name="",
    helper_map=None,
    historical_rule_source="",
    historical_rule_map=None,
    manual_override_map=None,
):
    rows = []
    helper_map = helper_map or {}
    historical_rule_map = historical_rule_map or {}
    manual_override_map = manual_override_map or {}
    for raw_value, occurrence_count in counts.items():
        raw_key = normalize_key_part(raw_value)
        helper_value = helper_map.get(raw_key, "")
        manual_key = (table_name, field_name, raw_key)
        manual_spu = manual_override_map.get(manual_key, "")
        historical_spu = historical_rule_map.get(raw_key, "")
        if not historical_spu and helper_value:
            historical_spu = historical_rule_map.get(str(helper_value), "")

        classify_target = helper_value if helper_value else raw_value
        if manual_spu:
            mapped_spu, reason = normalize_manual_spu(manual_spu)
            rule_source = "手工新增映射关系"
            matched_historical = manual_spu
        else:
            mapped_spu, reason = classify_value(classify_target, historical_spu=historical_spu)
            rule_source = historical_rule_source if historical_spu else "脚本规则"
            matched_historical = historical_spu
        rows.append(
            {
                "原始表名": table_name,
                "原始字段名": field_name,
                "原始值": raw_value,
                "出现次数": int(occurrence_count),
                "标准化SPU": mapped_spu,
                "映射规则说明": reason,
                "规则来源": rule_source,
                "辅助字段名": helper_field_name,
                "辅助字段值": helper_value,
                "命中历史规则值": matched_historical,
            }
        )
    return rows


def main():
    rows = []
    mapping_loader = MappingLoader(str(MAPPING_PATH))
    manual_override_map, manual_mapping_df = load_manual_override_map()
    spu_standard_map = mapping_loader.get_spu_standard_mapping()
    po_model_map = {str(k).strip(): str(v).strip() for k, v in mapping_loader.mappings["po_sku_to_spu"]["model_to_spu"].items() if str(k).strip()}
    po_sku_map = {str(int(k)): str(v).strip() for k, v in mapping_loader.mappings["po_sku_to_spu"]["sku_to_spu"].items() if str(v).strip()}
    demand_spu_map = {}
    for sku_name, attrs in mapping_loader.mappings["demand_sku_to_attrs"].items():
        demand_spu = str(attrs.get("SPU标准化", "")).strip()
        if sku_name and demand_spu:
            demand_spu_map[str(sku_name).strip()] = demand_spu

    soh_df = pd.read_excel(BASE_DIR / "库存数据/全球SOH库存.xlsx")
    rows.extend(
        build_rows(
            "全球SOH库存表",
            "SPU",
            count_values(soh_df["SPU"]),
            historical_rule_source="mapping rules: SPU标准化(J:K)",
            historical_rule_map=spu_standard_map,
            manual_override_map=manual_override_map,
        )
    )

    overseas_po_df = pd.read_excel(BASE_DIR / "库存数据/海外open po库存.xlsx")
    overseas_historical_map = {}
    overseas_historical_map.update(po_model_map)
    overseas_historical_map.update(demand_spu_map)
    rows.extend(
        build_rows(
            "海外open PO表",
            "商品名称",
            count_values(overseas_po_df["商品名称"]),
            historical_rule_source="mapping rules: PO型号映射(R:T) / Demand SKU映射(AO:AW)",
            historical_rule_map=overseas_historical_map,
            manual_override_map=manual_override_map,
        )
    )

    china_po_df = pd.read_excel(BASE_DIR / "库存数据/中国open po库存.xlsx")
    po_helper = (
        china_po_df.dropna(subset=["SKU 编号"])
        .assign(key=china_po_df["SKU 编号"].astype(str))
        .drop_duplicates("key")
        .set_index("key")["SKU 型号"]
        .astype(str)
        .to_dict()
    )
    rows.extend(
        build_rows(
            "中国open PO表",
            "SKU 编号",
            count_values(china_po_df["SKU 编号"]),
            helper_field_name="SKU 型号",
            helper_map=po_helper,
            historical_rule_source="mapping rules: PO SKU/SPU映射(R:T)",
            historical_rule_map=po_sku_map | po_model_map,
            manual_override_map=manual_override_map,
        )
    )

    vmi_df = read_excel_safely(str(BASE_DIR / "库存数据/中国VMI库存.xlsx"))
    rows.extend(
        build_rows(
            "中国VMI库存表",
            "SKU 型号信息",
            count_values(vmi_df["SKU 型号信息"]),
            historical_rule_source="mapping rules: PO型号映射(R:T)",
            historical_rule_map=po_model_map,
            manual_override_map=manual_override_map,
        )
    )

    in_transit_df = read_excel_safely(str(BASE_DIR / "库存数据/中国-in transit库存.xlsx"))
    in_transit_helper = (
        in_transit_df.dropna(subset=["SKU 型号编号"])
        .assign(key=in_transit_df["SKU 型号编号"].astype(str))
        .drop_duplicates("key")
        .set_index("key")["SKU 型号规格"]
        .astype(str)
        .to_dict()
    )
    rows.extend(
        build_rows(
            "中国in transit表",
            "SKU 型号编号",
            count_values(in_transit_df["SKU 型号编号"]),
            helper_field_name="SKU 型号规格",
            helper_map=in_transit_helper,
            historical_rule_source="mapping rules: PO SKU/SPU映射(R:T)",
            historical_rule_map=po_sku_map | po_model_map,
            manual_override_map=manual_override_map,
        )
    )

    office_demand_files = [
        BASE_DIR / "全球需求数据/办公电脑需求_全场景.xlsx",
        BASE_DIR / "全球需求数据/办公电脑需求_入职场景.xlsx",
        BASE_DIR / "全球需求数据/办公电脑需求_到期更换场景.xlsx",
    ]
    office_demand_df = pd.concat([pd.read_excel(path) for path in office_demand_files], ignore_index=True)
    rows.extend(
        build_rows(
            "办公电脑需求表",
            "AM SPU",
            count_values(office_demand_df["AM SPU"]),
            historical_rule_source="mapping rules: SPU标准化(J:K)",
            historical_rule_map=spu_standard_map,
            manual_override_map=manual_override_map,
        )
    )

    monitor_demand_df = pd.read_excel(BASE_DIR / "全球需求数据/显示器和手绘板需求_全场景.xlsx")
    rows.extend(
        build_rows(
            "显示器和手绘板需求表",
            "AM SPU",
            count_values(monitor_demand_df["AM SPU"]),
            historical_rule_source="mapping rules: SPU标准化(J:K)",
            historical_rule_map=spu_standard_map,
            manual_override_map=manual_override_map,
        )
    )

    mapping_df = pd.DataFrame(rows)
    mapping_df["标准化SPU"] = mapping_df["标准化SPU"].fillna("")
    mapping_df = mapping_df.sort_values(
        ["原始表名", "原始字段名", "出现次数", "原始值"],
        ascending=[True, True, False, True],
    ).reset_index(drop=True)

    recommendation_df = pd.DataFrame(
        [
            {
                "原始表名": "中国open PO表",
                "推荐主映射字段": "SKU 编号",
                "辅助判定字段": "SKU 型号",
                "不建议作为映射键的字段": "采购单号 / PO 单号 / PR 单号",
                "原因": "SKU 编号是稳定物料标识，适合跨天复用；单号类字段属于业务流水号，会随每日数据变化。",
            },
            {
                "原始表名": "中国in transit表",
                "推荐主映射字段": "SKU 型号编号",
                "辅助判定字段": "SKU 型号规格",
                "不建议作为映射键的字段": "需求单号 / PO 单号 / 关联采购单",
                "原因": "SKU 型号编号是稳定物料标识，适合跨天复用；单号类字段属于业务流水号，会随每日数据变化。",
            },
        ]
    )

    pending_df = mapping_df[mapping_df["标准化SPU"] == "待确认"].copy()
    mapping_rules_reference_df = pd.DataFrame(
        [
            {"规则类型": "SPU标准化(J:K)", "原始值": k, "映射值": v}
            for k, v in spu_standard_map.items()
        ]
        + [
            {"规则类型": "PO型号映射(R:T)", "原始值": k, "映射值": v}
            for k, v in po_model_map.items()
        ]
        + [
            {"规则类型": "PO SKU映射(R:T)", "原始值": k, "映射值": v}
            for k, v in po_sku_map.items()
        ]
        + [
            {"规则类型": "Demand SKU映射(AO:AW)", "原始值": k, "映射值": v}
            for k, v in demand_spu_map.items()
        ]
    )

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        mapping_df.to_excel(writer, sheet_name="正式映射表", index=False)
        recommendation_df.to_excel(writer, sheet_name="字段选择建议", index=False)
        pending_df.to_excel(writer, sheet_name="待确认清单", index=False)
        mapping_rules_reference_df.to_excel(writer, sheet_name="mapping rules参考", index=False)
        manual_mapping_df.to_excel(writer, sheet_name="手工补充参考", index=False)

    print(f"saved={OUTPUT_PATH}")
    print(f"mapping_rows={len(mapping_df)}")
    print(f"pending_rows={len(pending_df)}")


if __name__ == "__main__":
    main()
