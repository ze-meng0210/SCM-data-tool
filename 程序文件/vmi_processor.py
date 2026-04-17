import re
from utils import read_excel_safely, filter_out_tenant


class VMIProcessor:
    def __init__(self, file_path, mapping_loader):
        self.file_path = file_path
        self.mapping_loader = mapping_loader
        self.df = None
        self.unmapped = []
        self._load_and_process()

    def _load_and_process(self):
        self.df = read_excel_safely(self.file_path)
        self.df = filter_out_tenant(self.df, label='VMI')
        self._add_spu_column()

    def _keyword_match_spu(self, model_info):
        s = str(model_info)
        sl = s.lower()

        size = None
        m = re.search(r'(14|16)\s*["″]', s)
        if m:
            try:
                size = int(m.group(1))
            except Exception:
                size = None

        if 'macbook pro' in sl and size == 16:
            return "MBP 16''"

        if 'macbook pro' in sl and size == 14:
            return "MBP 14''"

        if 'macbook air' in sl or ('苹果' in s and '笔记本' in s and ('air' in sl or 'm4' in sl or 'm5 10+8' in sl)):
            return 'Mac 笔记本 标配'

        if '/2k' in sl:
            return '2K 显示器'

        if '显示器' in s or 'display' in sl or 'monitor' in sl:
            if '4k' in sl:
                return '4K 显示器'
            if '2k' in sl:
                return '2K 显示器'
            if '1080' in sl:
                return '1080P 显示器'
            return '4K 显示器'

        if 'imac' in sl:
            return 'Mac 一体机'

        if 'wacom' in sl or '手绘板' in s:
            return '手绘板'

        if any(kw in sl for kw in ['u7', 'i7', 'ultra7', 'ultra 7']):
            if '台式' in s or 'desktop' in sl or ' dt ' in sl:
                if '高配' in s or '2.5k' in sl or '2k' in sl:
                    return 'Win i7 台式机 高配'
                return 'Win i7 台式机 标配'
            if '2.5k' in sl or '2k' in sl or '高配' in s:
                return 'Win i7 笔记本 高配'
            return 'Win i7 笔记本 标配'

        if any(kw in sl for kw in ['u5', 'i5', 'ultra5', 'ultra 5']):
            if '台式' in s or 'desktop' in sl or ' dt ' in sl:
                return 'Win i5 台式机 标配'
            if '2.5k' in sl or '2k' in sl or '高配' in s:
                return 'Win i5 笔记本 高配'
            return 'Win i5 笔记本 标配'

        if '笔记本' in s:
            if any(kw in sl for kw in ['dell', 'hp', 'lenovo']) or any(kw in s for kw in ['戴尔', '惠普', '联想']):
                return 'Win i5 笔记本 标配'

        return ''

    def _add_spu_column(self):
        spu_col = []
        
        for _, row in self.df.iterrows():
            model_info = str(row.get('SKU 型号信息', ''))
            spu = self.mapping_loader.get_formal_standard_spu('中国VMI库存表', 'SKU 型号信息', model_info)
            if not spu:
                spu = self.mapping_loader.get_po_spu_by_model(model_info)
                if not spu:
                    spu = self._keyword_match_spu(model_info)
                spu = self.mapping_loader.get_standard_spu(spu)
            if not spu:
                self.unmapped.append({
                    'type': 'VMI未映射SKU',
                    'SKU型号信息': model_info,
                    'IT资产编号': row.get('IT 资产编号', '')
                })
            spu_col.append(spu)
        
        self.df.insert(0, 'SPU', spu_col)

    def get_processed_df(self):
        return self.df

    def get_unmapped(self):
        return self.unmapped
