import os
import pandas as pd
from utils import read_excel_safely


class MappingLoader:
    def __init__(self, mapping_path):
        self.mapping_path = mapping_path
        self.formal_mapping_path = self._discover_formal_mapping_path()
        self.df_mapping = None
        self.mappings = {}
        self.formal_spu_mapping = {}
        self.standard_spu_keys = {
            'Mac Standard',
            'Mac High',
            "MBP 14''",
            "MBP 16''",
            'Win i5 Standard',
            'Win i5 High',
            'Win i7 Standard',
            'Win i7 High',
            'Win i7 DT Standard',
            'Win i7 DT High',
            'iMac',
            'Graphic Tablet',
            '1080P Display',
            '2K Display',
            '2K Display High Refresh',
            '4K Display',
            '4K Display High Refresh',
        }
        self._load_mapping()
        self._load_formal_spu_mapping()

    def _load_mapping(self):
        self.df_mapping = read_excel_safely(self.mapping_path)
        self._parse_all_mappings()

    def _discover_formal_mapping_path(self):
        mapping_dir = os.path.dirname(os.path.abspath(str(self.mapping_path)))
        candidate = os.path.join(mapping_dir, '正式标准化SPU映射表.xlsx')
        if os.path.exists(candidate):
            return candidate
        return ''

    def _normalize_lookup_value(self, value):
        if pd.isna(value):
            return ''

        s = str(value).strip()
        if not s or s.lower() == 'nan':
            return ''

        s_no_commas = s.replace(',', '')
        try:
            num = float(s_no_commas)
            if num.is_integer() and all(ch in '0123456789.-' for ch in s_no_commas):
                return str(int(num))
        except Exception:
            pass

        return s

    def _load_formal_spu_mapping(self):
        if not self.formal_mapping_path:
            return

        try:
            df = pd.read_excel(self.formal_mapping_path, sheet_name='正式映射表')
        except Exception:
            try:
                xl = pd.ExcelFile(self.formal_mapping_path)
                df = pd.read_excel(self.formal_mapping_path, sheet_name=xl.sheet_names[0])
            except Exception as e:
                print(f'Warning: Could not load formal mapping file: {e}')
                return

        required_cols = {'原始表名', '原始字段名', '原始值'}
        if not required_cols.issubset(df.columns):
            return

        target_col = '标准化SPU' if '标准化SPU' in df.columns else ('标准SPU' if '标准SPU' in df.columns else '')
        if not target_col:
            return

        mapping = {}
        for _, row in df.iterrows():
            table_name = self._normalize_lookup_value(row.get('原始表名'))
            field_name = self._normalize_lookup_value(row.get('原始字段名'))
            raw_value = self._normalize_lookup_value(row.get('原始值'))
            target_spu = self._normalize_lookup_value(row.get(target_col))
            if not table_name or not field_name or not raw_value or not target_spu or target_spu == '待确认':
                continue
            mapping[(table_name, field_name, raw_value)] = target_spu

        self.formal_spu_mapping = mapping

    def _parse_all_mappings(self):
        df = self.df_mapping

        self.mappings['region_country'] = self._parse_region_country(df)
        self.mappings['spu_standard'] = self._parse_spu_standard(df)
        self.mappings['central_warehouses'] = self._parse_central_warehouses(df)
        self.mappings['valid_bin_types'] = self._parse_valid_bin_types(df)
        self.mappings['soh_sku_to_attrs'] = self._parse_soh_sku_to_attrs(df)
        self.mappings['demand_sku_to_attrs'] = self._parse_demand_sku_to_attrs(df)
        self.mappings['po_sku_to_spu'] = self._parse_po_sku_to_spu(df)
        self.mappings['overseas_sku_info_to_id'] = self._parse_overseas_sku_info_to_id(df)

    def _parse_region_country(self, df):
        cols = ['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3']
        sub_df = df[cols].copy()
        sub_df.columns = ['区域', '国家中文', 'Country英文']
        sub_df = sub_df.dropna(subset=['国家中文']).iloc[1:].reset_index(drop=True)
        return sub_df.set_index('国家中文')['Country英文'].to_dict()

    def _parse_spu_standard(self, df):
        cols = ['Unnamed: 9', 'Unnamed: 10']
        sub_df = df[cols].copy()
        sub_df.columns = ['原始SPU名称', '标准SPU分类']
        sub_df = sub_df.dropna(subset=['原始SPU名称']).iloc[1:].reset_index(drop=True)
        return sub_df.set_index('原始SPU名称')['标准SPU分类'].to_dict()

    def _parse_central_warehouses(self, df):
        col = 'Unnamed: 12'
        values = df[col].dropna().iloc[1:].tolist()
        return values

    def _parse_valid_bin_types(self, df):
        col = 'Unnamed: 15'
        values = df[col].dropna().iloc[1:].tolist()
        return values

    def _parse_soh_sku_to_attrs(self, df):
        col_start = 26
        col_end = 38
        if col_end >= len(df.columns):
            col_end = len(df.columns) - 1
        sub_df = df.iloc[:, col_start:col_end+1].copy()
        if len(sub_df.columns) >= 13:
            sub_df.columns = ['SKU编号', 'AB', 'AC', '资产名称', '品牌', '型号', 
                             'AG', 'AH', 'Size', 'CPU', 'Memory', 'SPU', 'LifeCycle']
        sub_df = sub_df.dropna(subset=['SKU编号']).iloc[1:].reset_index(drop=True)
        sub_df['SKU编号'] = pd.to_numeric(sub_df['SKU编号'], errors='coerce').fillna(0).astype(int)
        sub_df = sub_df.drop_duplicates(subset=['SKU编号'], keep='first')
        return sub_df.set_index('SKU编号').to_dict('index')

    def _parse_demand_sku_to_attrs(self, df):
        col_start = 40
        col_end = 48
        if col_end >= len(df.columns):
            col_end = len(df.columns) - 1
        sub_df = df.iloc[:, col_start:col_end+1].copy()
        if len(sub_df.columns) >= 9:
            sub_df.columns = ['资产名称', '型号', 'SPU原始', 'SKU名称', 
                             'Size', 'CPU', 'Memory', 'SPU标准化', 'LifeCycle']
        sub_df = sub_df.dropna(subset=['SKU名称']).iloc[1:].reset_index(drop=True)
        sub_df = sub_df.drop_duplicates(subset=['SKU名称'], keep='first')
        return sub_df.set_index('SKU名称').to_dict('index')

    def _parse_po_sku_to_spu(self, df):
        cols = ['Unnamed: 17', 'Unnamed: 18', 'Unnamed: 19']
        sub_df = df[cols].copy()
        sub_df.columns = ['SKU编号', 'SKU型号信息', 'SPU']
        sub_df = sub_df.dropna(subset=['SKU编号']).iloc[1:].reset_index(drop=True)
        sub_df['SKU编号'] = pd.to_numeric(sub_df['SKU编号'], errors='coerce').fillna(0).astype(int)
        sub_df_sku = sub_df.drop_duplicates(subset=['SKU编号'], keep='first')
        sku_to_spu = sub_df_sku.set_index('SKU编号')['SPU'].to_dict()
        sub_df_model = sub_df.drop_duplicates(subset=['SKU型号信息'], keep='first')
        model_to_spu = sub_df_model.set_index('SKU型号信息')['SPU'].to_dict()
        return {'sku_to_spu': sku_to_spu, 'model_to_spu': model_to_spu}

    def _parse_overseas_sku_info_to_id(self, df):
        cols = ['Unnamed: 5', 'Unnamed: 6']
        sub_df = df[cols].copy()
        sub_df.columns = ['SKU型号信息', 'SKU编号']
        sub_df = sub_df.dropna(subset=['SKU型号信息']).iloc[1:].reset_index(drop=True)
        return sub_df.set_index('SKU型号信息')['SKU编号'].to_dict()

    def get_country_mapping(self):
        return self.mappings['region_country']

    def get_spu_standard_mapping(self):
        return self.mappings['spu_standard']

    def get_central_warehouses(self):
        return self.mappings['central_warehouses']

    def get_valid_bin_types(self):
        return self.mappings['valid_bin_types']

    def get_soh_sku_attrs(self, sku_id):
        return self.mappings['soh_sku_to_attrs'].get(sku_id, {})

    def get_demand_sku_attrs(self, sku_name):
        return self.mappings['demand_sku_to_attrs'].get(sku_name, {})

    def get_po_spu_by_sku(self, sku_id):
        return self.mappings['po_sku_to_spu']['sku_to_spu'].get(sku_id, '')

    def get_po_spu_by_model(self, model_info):
        return self.mappings['po_sku_to_spu']['model_to_spu'].get(model_info, '')

    def get_standard_spu(self, original_spu):
        fallback = {
            'Mac 笔记本 标配': 'Mac Standard',
            'Mac 笔记本 高配': 'Mac High',
            "MBP 14''": "MBP 14''",
            "MBP 16''": "MBP 16''",
            'Win i5 笔记本 标配': 'Win i5 Standard',
            'Win i5 笔记本 高配': 'Win i5 High',
            'Win i7 笔记本 标配': 'Win i7 Standard',
            'Win i7 笔记本 高配': 'Win i7 High',
            'Win i7 台式机 标配': 'Win i7 DT Standard',
            'Win i7 台式机 高配': 'Win i7 DT High',
            'Mac 一体机': 'iMac',
            '手绘板': 'Graphic Tablet',
            '1080P 显示器': '1080P Display',
            '2K 显示器': '2K Display',
            '2K 显示器 高刷': '2K Display High Refresh',
            '4K 显示器': '4K Display',
            '4K 显示器 高刷': '4K Display High Refresh',
            'Mac 1T': 'Mac Standard',
            'Mac 笔记本 1T': 'Mac Standard',
            '2K 120hz': '2K Display High Refresh',
            '2K 120hz 显示器': '2K Display High Refresh',
            'Mac 笔记本 14"高配': "MBP 14''",
            'Mac 笔记本 16"高配': "MBP 16''",
            'Mac Standard': 'Mac Standard',
            'Mac High': 'Mac High',
            'Win i5 Standard': 'Win i5 Standard',
            'Win i5 High': 'Win i5 High',
            'Win i7 Standard': 'Win i7 Standard',
            'Win i7 High': 'Win i7 High',
            'Win i7 DT Standard': 'Win i7 DT Standard',
            'Win i7 DT High': 'Win i7 DT High',
            'iMac': 'iMac',
            'Graphic Tablet': 'Graphic Tablet',
            '1080P Display': '1080P Display',
            '2K Display': '2K Display',
            '2K Display High Refresh': '2K Display High Refresh',
            '4K Display': '4K Display',
            '4K Display High Refresh': '4K Display High Refresh',
        }
        # Always trust the latest explicit standard labels first, otherwise stale rules in mapping.xlsx
        # may collapse new categories like high-refresh displays back into generic display buckets.
        if original_spu in fallback:
            return fallback[original_spu]

        result = self.mappings['spu_standard'].get(original_spu, None)
        if result:
            return result

        return original_spu

    def get_formal_standard_spu(self, table_name, field_name, raw_value):
        key = (
            self._normalize_lookup_value(table_name),
            self._normalize_lookup_value(field_name),
            self._normalize_lookup_value(raw_value),
        )
        if not all(key):
            return ''

        mapped_spu = self.formal_spu_mapping.get(key, '')
        if not mapped_spu:
            return ''

        standardized = self.get_standard_spu(mapped_spu)
        return '' if standardized == '待确认' else standardized

    def get_formal_standard_spu_from_tables(self, table_names, field_name, raw_value):
        for table_name in table_names:
            result = self.get_formal_standard_spu(table_name, field_name, raw_value)
            if result:
                return result
        return ''

    def is_known_standard_spu(self, spu_value):
        normalized = self.get_standard_spu(spu_value)
        return normalized in self.standard_spu_keys

    def is_central_warehouse(self, building_name):
        keywords = ['大钟寺', '新江湾', '深圳湾', '桂溪', '景湖']
        for kw in keywords:
            if kw in str(building_name):
                return True
        return False

    def get_raw_df(self):
        return self.df_mapping
