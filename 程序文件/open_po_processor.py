import pandas as pd
import os
from utils import read_excel_safely, filter_out_tenant


class OpenPOProcessor:
    def __init__(self, overseas_path, china_path, mapping_loader, po_mapping_path=None):
        self.overseas_path = overseas_path
        self.china_path = china_path
        self.mapping_loader = mapping_loader
        self.po_mapping_path = po_mapping_path
        
        self.overseas_df = None
        self.overseas_in_transit_df = None
        self.china_df = None
        self.unmapped = []
        self.po_historical_mapping = {}
        
        self._load_po_mapping()
        self._load_and_process_overseas()
        self._load_and_process_china()

    def _load_po_mapping(self):
        if self.po_mapping_path and os.path.exists(self.po_mapping_path):
            try:
                df = read_excel_safely(self.po_mapping_path)
                if len(df.columns) >= 2:
                    self.po_historical_mapping = dict(zip(df.iloc[:, 0].astype(str), df.iloc[:, 1]))
            except Exception as e:
                print(f'Warning: Could not load PO mapping file: {e}')

    def _map_product_name_to_spu(self, product_name):
        s = str(product_name)
        sl = s.lower()
        
        if s in self.po_historical_mapping:
            return self.po_historical_mapping[s]
        
        if 'macbook air' in sl or 'mba' in sl:
            return 'Mac 笔记本 标配'
        
        if 'macbook pro' in sl or 'mbp' in sl:
            if '16' in s:
                return "MBP 16''"
            if '14' in s:
                return "MBP 14''"
            return "MBP 14''"
        
        if 'imac' in sl:
            return 'Mac 一体机'
        
        if 'wacom' in sl or '手绘板' in sl:
            return '手绘板'
        
        if any(kw in sl for kw in ['display', 'monitor', '显示器']):
            if '4k' in sl or '4K' in s:
                return '4K 显示器'
            elif '2k' in sl or '2K' in s:
                return '2K 显示器'
            elif '1080' in sl:
                return '1080P 显示器'
            return '4K 显示器'
        
        if any(kw in sl for kw in ['desktop', '台式', ' dt ']):
            if any(kw in sl for kw in ['i7', 'u7', 'ultra7', 'ultra 7']):
                if any(kw in sl for kw in ['high', 'premium', '高配']):
                    return 'Win i7 台式机 高配'
                return 'Win i7 台式机 标配'
        
        if any(kw in sl for kw in ['i7', 'u7', 'ultra7', 'ultra 7']):
            if any(kw in sl for kw in ['high', 'premium', '高配', '4k', '2k+', '2k ']):
                return 'Win i7 笔记本 高配'
            return 'Win i7 笔记本 标配'
        
        if any(kw in sl for kw in ['i5', 'u5', 'ultra5', 'ultra 5']):
            if any(kw in sl for kw in ['high', 'premium', '高配', '4k', '2k+', '2k ']):
                return 'Win i5 笔记本 高配'
            return 'Win i5 笔记本 标配'
        
        if 'windows' in sl or 'win ' in sl:
            return 'Win i5 笔记本 标配'
        
        return ''

    def _load_and_process_overseas(self):
        self.overseas_df = read_excel_safely(self.overseas_path)
        self.overseas_df = filter_out_tenant(self.overseas_df, label='Overseas Open PO')
        
        if '订单状态' in self.overseas_df.columns:
            original_len = len(self.overseas_df)
            status_series = self.overseas_df['订单状态'].astype(str).str.strip()
            summary_statuses = ['已下单', '等待发货']
            transit_statuses = ['已部分验收', '已发货']

            self.overseas_in_transit_df = (
                self.overseas_df[status_series.isin(transit_statuses)]
                .copy()
                .reset_index(drop=True)
            )
            self.overseas_df = (
                self.overseas_df[status_series.isin(summary_statuses)]
                .copy()
                .reset_index(drop=True)
            )
            print(
                f'Overseas PO: Filtered from {original_len} to {len(self.overseas_df)} rows for Summary '
                f'and kept {len(self.overseas_in_transit_df)} rows in separate transit sheet'
            )
        
        country_map = self.mapping_loader.get_country_mapping()
        
        sep_col = [''] * len(self.overseas_df)
        spu_size_col = []
        country_col = []
        spu_mapped_col = []
        spu_col = []
        eta_col = [''] * len(self.overseas_df)
        
        for _, row in self.overseas_df.iterrows():
            product_name = str(row.get('商品名称', ''))
            spu_size_col.append(product_name)
            
            country_cn = str(row.get('到货地点所属国家中文名称', ''))
            country_en = country_map.get(country_cn, 'NA')
            country_col.append(country_en)
            
            standard_spu = self.mapping_loader.get_formal_standard_spu('海外open PO表', '商品名称', product_name)
            mapped_spu = self._map_product_name_to_spu(product_name)
            if standard_spu:
                mapped_spu = mapped_spu or product_name
            # Standardize via formal mapping first, then historical mapping + fallback.
            if not standard_spu:
                standard_spu = self.mapping_loader.get_standard_spu(mapped_spu)

            spu_col.append(mapped_spu)
            spu_mapped_col.append(standard_spu)
            
            if not mapped_spu:
                self.unmapped.append({
                    'type': '海外PO未映射商品名称',
                    '商品名称': product_name,
                    'po单编号': row.get('po单编号', '')
                })
        
        new_cols = pd.DataFrame({
            '': sep_col,
            'SPU-Size': spu_size_col,
            'Country': country_col,
            'SPU(映射)': spu_mapped_col,
            'SPU': spu_col,
            'ETA': eta_col
        })
        
        self.overseas_df = pd.concat([new_cols, self.overseas_df], axis=1)
        if self.overseas_in_transit_df is not None and not self.overseas_in_transit_df.empty:
            self.overseas_in_transit_df = self._add_overseas_mapping_columns(self.overseas_in_transit_df, country_map)

    def _add_overseas_mapping_columns(self, df, country_map):
        sep_col = [''] * len(df)
        spu_size_col = []
        country_col = []
        spu_mapped_col = []
        spu_col = []
        eta_col = [''] * len(df)

        for _, row in df.iterrows():
            product_name = str(row.get('商品名称', ''))
            spu_size_col.append(product_name)

            country_cn = str(row.get('到货地点所属国家中文名称', ''))
            country_en = country_map.get(country_cn, 'NA')
            country_col.append(country_en)

            standard_spu = self.mapping_loader.get_formal_standard_spu('海外open PO表', '商品名称', product_name)
            mapped_spu = self._map_product_name_to_spu(product_name)
            if standard_spu:
                mapped_spu = mapped_spu or product_name
            if not standard_spu:
                standard_spu = self.mapping_loader.get_standard_spu(mapped_spu)

            spu_col.append(mapped_spu)
            spu_mapped_col.append(standard_spu)

        new_cols = pd.DataFrame({
            '': sep_col,
            'SPU-Size': spu_size_col,
            'Country': country_col,
            'SPU(映射)': spu_mapped_col,
            'SPU': spu_col,
            'ETA': eta_col
        })

        return pd.concat([new_cols, df.reset_index(drop=True)], axis=1)

    def _load_and_process_china(self):
        self.china_df = read_excel_safely(self.china_path)
        self.china_df = filter_out_tenant(self.china_df, label='China Open PO')
        
        spu_col = []
        
        for _, row in self.china_df.iterrows():
            sku_id = row.get('SKU 编号', 0)
            try:
                sku_id = int(float(sku_id))
            except:
                sku_id = 0
            
            spu = self.mapping_loader.get_formal_standard_spu('中国open PO表', 'SKU 编号', sku_id)
            if not spu:
                spu = self.mapping_loader.get_po_spu_by_sku(sku_id)
                spu = self.mapping_loader.get_standard_spu(spu)
            if not spu:
                self.unmapped.append({
                    'type': '中国PO未映射SKU',
                    'SKU编号': sku_id,
                    '采购单号': row.get('采购单号', '')
                })
            spu_col.append(spu)
        
        self.china_df.insert(0, 'SPU', spu_col)

    def get_overseas_df(self):
        return self.overseas_df

    def get_china_df(self):
        return self.china_df

    def get_overseas_in_transit_df(self):
        if self.overseas_in_transit_df is None:
            return pd.DataFrame()
        return self.overseas_in_transit_df

    def get_unmapped(self):
        return self.unmapped
