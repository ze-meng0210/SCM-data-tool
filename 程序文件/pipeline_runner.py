import os

import pandas as pd

from mapping_loader import MappingLoader
from soh_processor import SOHProcessor
from in_transit_processor import InTransitProcessor
from vmi_processor import VMIProcessor
from open_po_processor import OpenPOProcessor
from demand_processor import DemandProcessor
from summary_generator import SummaryGenerator
from excel_writer import ExcelWriter


def run_pipeline(
    soh,
    overseas_po,
    china_po,
    in_transit,
    vmi,
    demand_all,
    demand_hire,
    demand_replace,
    demand_monitor,
    mapping,
    output,
    po_mapping=None,
    formal_mapping=None,
    progress_callback=None,
):
    def report_progress(progress, stage, message):
        if progress_callback:
            progress_callback(progress=progress, stage=stage, message=message)

    output_dir = os.path.dirname(os.path.abspath(output))
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    report_progress(2, 'init', '开始初始化任务')
    print('=' * 60)
    print('IT 资产供应链数据自动化处理系统')
    print('=' * 60)

    print('\n[Step 1] 加载 Mapping 配置...')
    report_progress(8, 'mapping', '加载 Mapping 配置')
    mapping_loader = MappingLoader(mapping, formal_mapping_path=formal_mapping)
    print('✓ Mapping 配置加载完成')

    print('\n[Step 2] 处理 SOH 数据...')
    report_progress(18, 'soh', '处理 SOH 数据')
    soh_processor = SOHProcessor(soh, mapping_loader)
    soh_df = soh_processor.get_processed_df()
    print(f'✓ SOH 数据处理完成，共 {len(soh_df)} 行')

    print('\n[Step 3] 处理 In Transit 数据...')
    report_progress(30, 'in_transit', '处理 In Transit 数据')
    in_transit_processor = InTransitProcessor(in_transit, mapping_loader)
    in_transit_df = in_transit_processor.get_processed_df()
    print(f'✓ In Transit 数据处理完成，共 {len(in_transit_df)} 行')

    print('\n[Step 4] 处理 VMI 数据...')
    report_progress(42, 'vmi', '处理 VMI 数据')
    vmi_processor = VMIProcessor(vmi, mapping_loader)
    vmi_df = vmi_processor.get_processed_df()
    print(f'✓ VMI 数据处理完成，共 {len(vmi_df)} 行')

    print('\n[Step 5] 处理 Open PO 数据...')
    report_progress(56, 'open_po', '处理 Open PO 数据')
    open_po_processor = OpenPOProcessor(
        overseas_po,
        china_po,
        mapping_loader,
        po_mapping
    )
    open_po_overseas_df = open_po_processor.get_overseas_df()
    open_po_overseas_in_transit_df = open_po_processor.get_overseas_in_transit_df()
    open_po_china_df = open_po_processor.get_china_df()
    print(
        f'✓ Open PO 数据处理完成：海外纳入Summary {len(open_po_overseas_df)} 行，'
        f'海外单独展示 {len(open_po_overseas_in_transit_df)} 行，中国 {len(open_po_china_df)} 行'
    )

    print('\n[Step 6] 处理需求数据...')
    report_progress(70, 'demand', '处理需求数据')
    demand_files = [
        demand_all,
        demand_hire,
        demand_replace,
        demand_monitor
    ]
    demand_processor = DemandProcessor(demand_files, mapping_loader)
    demand_df = demand_processor.get_processed_df()
    print(f'✓ 需求数据处理完成，共 {len(demand_df)} 行')

    print('\n[Step 7] 生成汇总表...')
    report_progress(82, 'summary', '生成汇总表')
    summary_gen = SummaryGenerator(
        soh_df,
        in_transit_df,
        vmi_df,
        open_po_overseas_df,
        open_po_china_df,
        demand_df
    )
    summary_df = summary_gen.get_summary_df()
    print('✓ 汇总表生成完成')

    print('\n[Step 8] 收集未映射记录...')
    report_progress(90, 'unmapped', '收集未映射记录')
    all_unmapped = []
    all_unmapped.extend(soh_processor.get_unmapped())
    all_unmapped.extend(in_transit_processor.get_unmapped())
    all_unmapped.extend(vmi_processor.get_unmapped())
    all_unmapped.extend(open_po_processor.get_unmapped())
    all_unmapped.extend(demand_processor.get_unmapped())
    print(f'✓ 收集到 {len(all_unmapped)} 条未映射记录')

    print('\n[Step 9] 输出 Excel 文件...')
    report_progress(96, 'write_output', '写出结果 Excel')
    with ExcelWriter(output) as writer:
        writer.write_summary_region(summary_df)

        for region in ['AMS', 'APAC', 'EMEA']:
            empty_df = pd.DataFrame(columns=['备注'])
            empty_df.loc[0] = [f'{region} 区域明细表（后续版本实现）']
            writer.write_region_sheet(region, empty_df)

        writer.write_sheet('SOH', soh_df)
        writer.write_sheet('Ave Demand', demand_df)
        writer.write_open_po(open_po_overseas_df, open_po_china_df)
        if not open_po_overseas_in_transit_df.empty:
            writer.write_sheet('海外OpenPO_已发货部分验收', open_po_overseas_in_transit_df)
        writer.write_sheet('In transit', in_transit_df)
        writer.write_sheet('VMI 已验收', vmi_df)
        writer.write_sheet('Mapping', mapping_loader.get_raw_df())
        writer.write_unmapped_report(all_unmapped)

    print(f'✓ Excel 文件已输出至: {output}')
    print('\n' + '=' * 60)
    print('处理完成！')
    print('=' * 60)
    report_progress(100, 'done', '处理完成')

    return {
        'output_path': output,
        'soh_rows': len(soh_df),
        'in_transit_rows': len(in_transit_df),
        'vmi_rows': len(vmi_df),
        'open_po_overseas_rows': len(open_po_overseas_df),
        'open_po_overseas_transit_rows': len(open_po_overseas_in_transit_df),
        'open_po_china_rows': len(open_po_china_df),
        'demand_rows': len(demand_df),
        'summary_rows': len(summary_df),
        'unmapped_rows': len(all_unmapped),
    }
