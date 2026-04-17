import argparse
from pipeline_runner import run_pipeline


def parse_args():
    parser = argparse.ArgumentParser(description='IT 资产供应链数据自动化处理系统')
    
    parser.add_argument('--soh', required=True, help='全球_SOH库存.xlsx 文件路径')
    parser.add_argument('--overseas-po', required=True, help='海外地区_open_po数据.xlsx 文件路径')
    parser.add_argument('--china-po', required=True, help='中国_open_po库存.xlsx 文件路径')
    parser.add_argument('--in-transit', required=True, help='中国_in_transit库存.xlsx 文件路径')
    parser.add_argument('--vmi', required=True, help='中国_VMI库存.xlsx 文件路径')
    parser.add_argument('--demand-all', required=True, help='办公电脑需求_全场景.xlsx 文件路径')
    parser.add_argument('--demand-hire', required=True, help='办公电脑需求_入职场景.xlsx 文件路径')
    parser.add_argument('--demand-replace', required=True, help='办公电脑需求_到期更换场景.xlsx 文件路径')
    parser.add_argument('--demand-monitor', required=True, help='显示器和手绘板需求_全场景.xlsx 文件路径')
    parser.add_argument('--mapping', required=True, help='mapping.xlsx 文件路径')
    parser.add_argument('--po-mapping', help='overseas_po_spu_mapping.xlsx 文件路径（可选）')
    parser.add_argument('--output', required=True, help='输出 Excel 文件路径')
    return parser.parse_args()


def main():
    args = parse_args()
    run_pipeline(
        soh=args.soh,
        overseas_po=args.overseas_po,
        china_po=args.china_po,
        in_transit=args.in_transit,
        vmi=args.vmi,
        demand_all=args.demand_all,
        demand_hire=args.demand_hire,
        demand_replace=args.demand_replace,
        demand_monitor=args.demand_monitor,
        mapping=args.mapping,
        output=args.output,
        po_mapping=args.po_mapping,
    )


if __name__ == '__main__':
    main()
