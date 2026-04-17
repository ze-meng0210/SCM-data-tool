import pandas as pd
from utils import read_excel_safely, filter_out_tenant


class SOHProcessor:
    def __init__(self, file_path, mapping_loader):
        self.file_path = file_path
        self.mapping_loader = mapping_loader
        self.df = None
        self.unmapped = []
        self._load_and_process()

    def _load_and_process(self):
        self.df = read_excel_safely(self.file_path)
        self.df = filter_out_tenant(self.df, label='SOH')
        self._filter_bin_types()
        self._add_mapped_columns()

    def _filter_bin_types(self):
        if '区域' in self.df.columns:
            print(f'SOH 原始区域分布: {self.df["区域"].value_counts().to_dict()}')

        if '货格类型(Apollo)' in self.df.columns:
            empty_count = int(
                self.df['货格类型(Apollo)'].isna().sum() +
                (self.df['货格类型(Apollo)'].astype(str).str.strip() == '').sum()
            )
            print('SOH: Skipped bin type filtering, all bin types are treated as valid')
            print(f'SOH: {empty_count} rows have empty bin type and were kept')

            if '区域' in self.df.columns:
                print(f'SOH 保留后区域分布: {self.df["区域"].value_counts().to_dict()}')

    def _add_mapped_columns(self):
        country_map = self.mapping_loader.get_country_mapping()
        
        central_warehouse_col = []
        country_col = []
        spu_mapped_col = []
        size_col = []
        cpu_col = []
        memory_col = []
        spu_detail_col = []
        life_cycle_col = []
        
        for _, row in self.df.iterrows():
            is_replacement = str(row.get('库存场景2级标签', '')) == '用于到期更换场景'
            building_name = str(row.get('楼宇', ''))
            is_central = self.mapping_loader.is_central_warehouse(building_name)
            
            if is_replacement and is_central:
                central_warehouse_col.append(building_name)
            else:
                central_warehouse_col.append('No')
            
            country_cn = str(row.get('国家', ''))
            country_en = country_map.get(country_cn, 'NA')
            country_col.append(country_en)
            
            original_spu = str(row.get('SPU', ''))
            # Prefer the latest formal mapping rules, then fall back to historical standardization.
            standard_spu = self.mapping_loader.get_formal_standard_spu('全球SOH库存表', 'SPU', original_spu)
            if not standard_spu:
                standard_spu = self.mapping_loader.get_standard_spu(original_spu)
            
            sku_id = row.get('SKU编号', 0)
            try:
                sku_id = int(float(sku_id))
            except:
                sku_id = 0
            
            sku_attrs = self.mapping_loader.get_soh_sku_attrs(sku_id)
            size_val = ''
            cpu_val = ''
            memory_val = ''
            spu_detail_val = ''
            lc_val = 'Old'
            
            if sku_attrs:
                size_val = sku_attrs.get('Size', '')
                cpu_val = sku_attrs.get('CPU', '')
                memory_val = sku_attrs.get('Memory', '')
                spu_detail_val = sku_attrs.get('SPU', '')
                lc_val = sku_attrs.get('LifeCycle', 'Old')
                if not lc_val:
                    lc_val = 'Old'
            
            if standard_spu == 'Mac High':
                if '16' in str(size_val):
                    standard_spu = "MBP 16''"
                elif '14' in str(size_val):
                    standard_spu = "MBP 14''"
            
            spu_mapped_col.append(standard_spu)
            size_col.append(size_val)
            cpu_col.append(cpu_val)
            memory_col.append(memory_val)
            spu_detail_col.append(spu_detail_val)
            life_cycle_col.append(lc_val)
            
            # Only report SOH as unmapped when SKU attributes are missing and SPU itself is not already recognized.
            if not sku_attrs and sku_id != 0 and not self.mapping_loader.is_known_standard_spu(standard_spu):
                self.unmapped.append({
                    'type': 'SOH未映射SKU',
                    'SKU编号': sku_id,
                    '资产编号': row.get('资产编号', ''),
                    '原始SPU': original_spu
                })
        
        new_cols = pd.DataFrame({
            '中央仓到期更换库存': central_warehouse_col,
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
