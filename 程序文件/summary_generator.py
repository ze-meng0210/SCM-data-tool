import pandas as pd


class SummaryGenerator:
    def __init__(self, soh_df, in_transit_df, vmi_df, open_po_overseas_df, open_po_china_df, demand_df):
        self.soh_df = soh_df
        self.in_transit_df = in_transit_df
        self.vmi_df = vmi_df
        self.open_po_overseas_df = open_po_overseas_df
        self.open_po_china_df = open_po_china_df
        self.demand_df = demand_df
        
        self.regions = ['中国大陆地区', 'AMS', 'APAC', 'EMEA']
        self.spu_keys = [
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
            '4K Display High Refresh'
        ]
        self.spu_display = [
            'Mac 笔记本 标配',
            'Mac 笔记本 高配',
            "MBP 14''",
            "MBP 16''",
            'Win i5 笔记本 标配',
            'Win i5 笔记本 高配',
            'Win i7 笔记本 标配',
            'Win i7 笔记本 高配',
            'Win i7 台式机 标配',
            'Win i7 台式机 高配',
            'Mac 一体机',
            '手绘板',
            '1080P 显示器',
            '2K 显示器',
            '2K 显示器 高刷',
            '4K 显示器',
            '4K 显示器 高刷'
        ]
        
        self.summary_df = None
        self._generate_summary()

    def _count_soh(self, region, spu_key, is_replacement=False, life_cycle=None):
        if 'SPU(映射后)' not in self.soh_df.columns or '区域' not in self.soh_df.columns:
            return 0
        
        mask = (self.soh_df['区域'] == region) & (self.soh_df['SPU(映射后)'] == spu_key)
        
        if is_replacement:
            mask = mask & (self.soh_df['库存场景2级标签'] == '用于到期更换场景')
        
        if life_cycle and 'Life Cycle' in self.soh_df.columns:
            mask = mask & (self.soh_df['Life Cycle'] == life_cycle)
        
        return len(self.soh_df[mask])

    def _sum_in_transit(self, region, spu_key):
        if region != '中国大陆地区' or 'SPU' not in self.in_transit_df.columns or '需求数量' not in self.in_transit_df.columns:
            return 0
        
        mask = self.in_transit_df['SPU'] == spu_key
        return self.in_transit_df[mask]['需求数量'].sum()

    def _count_vmi(self, region, spu_key):
        if region != '中国大陆地区' or 'SPU' not in self.vmi_df.columns:
            return 0
        
        mask = self.vmi_df['SPU'] == spu_key
        return len(self.vmi_df[mask])

    def _sum_open_po(self, region, spu_key, country=None):
        total = 0
        
        if region == '中国大陆地区' and 'SPU' in self.open_po_china_df.columns and '剩余总量 - 计算' in self.open_po_china_df.columns:
            mask = self.open_po_china_df['SPU'] == spu_key
            total += self.open_po_china_df[mask]['剩余总量 - 计算'].sum()
        else:
            if 'SPU(映射)' in self.open_po_overseas_df.columns and '下单数量' in self.open_po_overseas_df.columns and '到货地点所属国家区域' in self.open_po_overseas_df.columns:
                mask = (self.open_po_overseas_df['到货地点所属国家区域'] == region) & (self.open_po_overseas_df['SPU(映射)'] == spu_key)
                if country and 'Country' in self.open_po_overseas_df.columns:
                    mask = mask & (self.open_po_overseas_df['Country'] == country)
                total += self.open_po_overseas_df[mask]['下单数量'].sum()
        
        return total

    def _sum_demand(self, region, spu_key, scenario='全场景', country=None):
        if 'SPU(映射后)' not in self.demand_df.columns or '区域' not in self.demand_df.columns or '场景' not in self.demand_df.columns or '周均需求数量' not in self.demand_df.columns:
            return 0
        
        mask = (self.demand_df['区域'] == region) & (self.demand_df['SPU(映射后)'] == spu_key) & (self.demand_df['场景'] == scenario)
        
        if country and 'Country' in self.demand_df.columns:
            mask = mask & (self.demand_df['Country'] == country)
        
        return self.demand_df[mask]['周均需求数量'].sum()

    def _generate_summary(self):
        rows = []
        
        for region in self.regions:
            for spu_key, spu_display in zip(self.spu_keys, self.spu_display):
                rows.append({
                    'Region': region,
                    'SPU': spu_display,
                    'SPU_key': spu_key,
                    '': '',
                    '  ': ''
                })
        
        self.summary_df = pd.DataFrame(rows)
        
        soh_replacement = []
        soh_rest = []
        soh_total = []
        soh_active = []
        soh_old = []
        in_transit = []
        vmi_stock = []
        open_po = []
        inventory_total = []
        overall_demand = []
        new_hire_demand = []
        replacement_demand = []
        rest_demand = []
        wos_soh = []
        wos_replacement = []
        wos_total = []
        wos_active_only = []
        
        for _, row in self.summary_df.iterrows():
            region = row['Region']
            spu_key = row['SPU_key']
            
            s_r = self._count_soh(region, spu_key, is_replacement=True)
            s_t = self._count_soh(region, spu_key)
            s_rest = s_t - s_r
            s_a = self._count_soh(region, spu_key, life_cycle='Active')
            s_o = self._count_soh(region, spu_key, life_cycle='Old')
            
            it = self._sum_in_transit(region, spu_key)
            vm = self._count_vmi(region, spu_key)
            op = self._sum_open_po(region, spu_key)
            
            if region == '中国大陆地区':
                inv_t = s_t + it + vm + op
            else:
                inv_t = s_t + op
            
            o_d = self._sum_demand(region, spu_key, '全场景')
            n_d = self._sum_demand(region, spu_key, '入职场景')
            r_d = self._sum_demand(region, spu_key, '到期更换场景')
            rest_d = o_d - n_d - r_d
            
            soh_replacement.append(s_r)
            soh_rest.append(s_rest)
            soh_total.append(s_t)
            soh_active.append(s_a)
            soh_old.append(s_o)
            in_transit.append(it)
            vmi_stock.append(vm)
            open_po.append(op)
            inventory_total.append(inv_t)
            overall_demand.append(o_d)
            new_hire_demand.append(n_d)
            replacement_demand.append(r_d)
            rest_demand.append(rest_d)
            
            if o_d > 0:
                wos_soh.append(s_t / o_d if o_d != 0 else 'N/A')
                wos_replacement.append((s_r + vm) / r_d if r_d != 0 else 'N/A')
                wos_total.append(inv_t / o_d if o_d != 0 else 'N/A')
                wos_active_only.append((inv_t - s_o) / o_d if o_d != 0 else 'N/A')
            else:
                wos_soh.append('N/A')
                wos_replacement.append('N/A')
                wos_total.append('N/A')
                wos_active_only.append('N/A')
        
        self.summary_df['SOH-Replacement'] = soh_replacement
        self.summary_df['SOH-Rest'] = soh_rest
        self.summary_df['SOH-Total'] = soh_total
        self.summary_df['SOH-Active'] = soh_active
        self.summary_df['SOH-Old'] = soh_old
        self.summary_df['In Transit'] = in_transit
        self.summary_df['VMI Stock'] = vmi_stock
        self.summary_df['Open PO'] = open_po
        self.summary_df['Inventory-Total'] = inventory_total
        
        for i in range(9):
            self.summary_df[f'(预留{i+1})'] = ''
        
        self.summary_df['Overall Demand'] = overall_demand
        self.summary_df['New Hire Demand'] = new_hire_demand
        self.summary_df['Replacement Demand'] = replacement_demand
        self.summary_df['Rest Demand'] = rest_demand
        self.summary_df['WOS SOH'] = wos_soh
        self.summary_df['WOS Replacement'] = wos_replacement
        self.summary_df['WOS Total'] = wos_total
        self.summary_df['WOS Active Only'] = wos_active_only
        
        self._fill_mac_high_merge()

    def _fill_mac_high_merge(self):
        for region in self.regions:
            mac_high_idx = None
            mbp14_idx = None
            mbp16_idx = None
            
            for idx, row in self.summary_df.iterrows():
                if row['Region'] == region:
                    if row['SPU_key'] == 'Mac High':
                        mac_high_idx = idx
                    elif row['SPU_key'] == "MBP 14''":
                        mbp14_idx = idx
                    elif row['SPU_key'] == "MBP 16''":
                        mbp16_idx = idx
            
            if mac_high_idx is not None and mbp14_idx is not None and mbp16_idx is not None:
                numeric_cols = ['SOH-Replacement', 'SOH-Rest', 'SOH-Total', 'SOH-Active', 'SOH-Old',
                               'In Transit', 'VMI Stock', 'Open PO', 'Inventory-Total',
                               'Overall Demand', 'New Hire Demand', 'Replacement Demand', 'Rest Demand']
                for col in numeric_cols:
                    v14 = self.summary_df.at[mbp14_idx, col]
                    v16 = self.summary_df.at[mbp16_idx, col]
                    if isinstance(v14, str): v14 = 0
                    if isinstance(v16, str): v16 = 0
                    self.summary_df.at[mac_high_idx, col] = v14 + v16
                
                h = self.summary_df.at[mac_high_idx, 'SOH-Total']
                n = self.summary_df.at[mac_high_idx, 'Inventory-Total']
                j = self.summary_df.at[mac_high_idx, 'SOH-Old']
                f = self.summary_df.at[mac_high_idx, 'SOH-Replacement']
                l = self.summary_df.at[mac_high_idx, 'VMI Stock']
                x = self.summary_df.at[mac_high_idx, 'Overall Demand']
                z = self.summary_df.at[mac_high_idx, 'Replacement Demand']
                
                self.summary_df.at[mac_high_idx, 'WOS SOH'] = h / x if x > 0 else 'N/A'
                self.summary_df.at[mac_high_idx, 'WOS Replacement'] = (f + l) / z if z > 0 else 'N/A'
                self.summary_df.at[mac_high_idx, 'WOS Total'] = n / x if x > 0 else 'N/A'
                self.summary_df.at[mac_high_idx, 'WOS Active Only'] = (n - j) / x if x > 0 else 'N/A'

    def get_summary_df(self):
        return self.summary_df

    def get_regions(self):
        return self.regions

    def get_spu_list(self):
        return self.spu_keys
