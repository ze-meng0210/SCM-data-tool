import pandas as pd
from utils import read_excel_safely, filter_out_tenant


class DemandProcessor:
    MANUAL_SKU_NAME_SPU_MAP = {
        '苹果-MacBook Pro 14" M5 Pro MAC笔记本（18+20核/48G/1T）': "MBP 14''",
    }

    def __init__(self, file_paths, mapping_loader):
        self.file_paths = file_paths
        self.mapping_loader = mapping_loader
        self.df = None
        self.unmapped = []
        self._load_and_process()

    def _load_and_process(self):
        dfs = []
        for file_path in self.file_paths:
            df = read_excel_safely(file_path)
            dfs.append(df)
        
        self.df = pd.concat(dfs, ignore_index=True)
        self._filter_tenant()
        self._add_mapped_columns()

    def _filter_tenant(self):
        self.df = filter_out_tenant(self.df, label='Demand')

    def _add_mapped_columns(self):
        country_map = self.mapping_loader.get_country_mapping()
        
        country_col = []
        spu_mapped_col = []
        size_col = []
        cpu_col = []
        memory_col = []
        spu_detail_col = []
        life_cycle_col = []
        
        for _, row in self.df.iterrows():
            country_cn = str(row.get('国家', ''))
            country_en = country_map.get(country_cn, 'NA')
            country_col.append(country_en)
            
            am_spu = str(row.get('AM SPU', ''))
            sku_name = str(row.get('SKU名称', ''))
            manual_sku_name_spu = self.MANUAL_SKU_NAME_SPU_MAP.get(sku_name, '')
            sku_name_spu = self.mapping_loader.get_formal_standard_spu_from_tables(
                ['办公电脑需求表', '显示器和手绘板需求表'],
                'SKU名称',
                sku_name
            )
            if not sku_name_spu and manual_sku_name_spu:
                sku_name_spu = manual_sku_name_spu
            standard_spu = self.mapping_loader.get_formal_standard_spu_from_tables(
                ['办公电脑需求表', '显示器和手绘板需求表'],
                'AM SPU',
                am_spu
            )
            if not standard_spu and sku_name_spu:
                standard_spu = sku_name_spu
            if not standard_spu:
                standard_spu = self.mapping_loader.get_standard_spu(am_spu)
            
            sku_attrs = self.mapping_loader.get_demand_sku_attrs(sku_name)
            size_val = ''
            cpu_val = ''
            memory_val = ''
            spu_detail_val = ''
            life_cycle_val = ''
            
            if sku_attrs:
                size_val = sku_attrs.get('Size', '')
                cpu_val = sku_attrs.get('CPU', '')
                memory_val = sku_attrs.get('Memory', '')
                spu_detail_val = sku_attrs.get('SPU标准化', '')
                life_cycle_val = sku_attrs.get('LifeCycle', '')
            elif am_spu:
                fallback_spu = self.mapping_loader.get_formal_standard_spu_from_tables(
                    ['办公电脑需求表', '显示器和手绘板需求表'],
                    'AM SPU',
                    am_spu
                )
                if not fallback_spu:
                    fallback_spu = self.mapping_loader.get_standard_spu(am_spu)
                if fallback_spu and fallback_spu != am_spu:
                    standard_spu = fallback_spu
            elif sku_name_spu and (not standard_spu or standard_spu == am_spu):
                standard_spu = sku_name_spu
            
            if standard_spu == 'Mac High':
                size_from_attr = size_val
                if not size_from_attr and sku_attrs:
                    size_from_attr = sku_attrs.get('Size', '')
                if '16' in str(size_from_attr):
                    standard_spu = "MBP 16''"
                elif '14' in str(size_from_attr):
                    standard_spu = "MBP 14''"
            
            spu_mapped_col.append(standard_spu)
            size_col.append(size_val)
            cpu_col.append(cpu_val)
            memory_col.append(memory_val)
            spu_detail_col.append(spu_detail_val)
            life_cycle_col.append(life_cycle_val)
            
            if not sku_attrs and not sku_name_spu:
                self.unmapped.append({
                    'type': '需求未映射SKU名称',
                    'SKU名称': sku_name,
                    'AM SPU': am_spu
                })
        
        new_cols = pd.DataFrame({
            'Country': country_col,
            'SPU(映射后)': spu_mapped_col,
            'Size': size_col,
            'CPU': cpu_col,
            'Memory': memory_col,
            'SPU(详细)': spu_detail_col,
            'Life Cycle': life_cycle_col
        })
        
        self.df = pd.concat([new_cols, self.df], axis=1)

    def get_processed_df(self):
        return self.df

    def get_unmapped(self):
        return self.unmapped
