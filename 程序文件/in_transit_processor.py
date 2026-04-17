from utils import read_excel_safely, filter_out_tenant


class InTransitProcessor:
    def __init__(self, file_path, mapping_loader):
        self.file_path = file_path
        self.mapping_loader = mapping_loader
        self.df = None
        self.unmapped = []
        self._load_and_process()

    def _load_and_process(self):
        self.df = read_excel_safely(self.file_path)
        self.df = filter_out_tenant(self.df, label='In Transit')
        self._add_spu_column()

    def _add_spu_column(self):
        spu_col = []
        
        for _, row in self.df.iterrows():
            sku_id = row.get('SKU 型号编号', 0)
            try:
                sku_id = int(float(sku_id))
            except:
                sku_id = 0
            
            spu = self.mapping_loader.get_formal_standard_spu('中国in transit表', 'SKU 型号编号', sku_id)
            if not spu:
                spu = self.mapping_loader.get_po_spu_by_sku(sku_id)
                spu = self.mapping_loader.get_standard_spu(spu)
            if not spu:
                self.unmapped.append({
                    'type': 'InTransit未映射SKU',
                    'SKU型号编号': sku_id,
                    '需求单号': row.get('需求单号', '')
                })
            spu_col.append(spu)
        
        self.df.insert(0, 'SPU', spu_col)

    def get_processed_df(self):
        return self.df

    def get_unmapped(self):
        return self.unmapped
