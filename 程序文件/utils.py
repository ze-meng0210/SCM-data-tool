import pandas as pd
import zipfile
import os
import tempfile
import re
import shutil


def read_excel_safely(file_path):
    try:
        xl = pd.ExcelFile(file_path)
        return pd.read_excel(file_path, sheet_name=xl.sheet_names[0])
    except Exception:
        print(f'Warning: Could not read {file_path} normally, trying to fix...')
        return fix_and_read_excel(file_path)


def fix_and_read_excel(file_path):
    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        worksheets_dir = os.path.join(temp_dir, 'xl', 'worksheets')
        if os.path.exists(worksheets_dir):
            for ws_file in os.listdir(worksheets_dir):
                if ws_file.endswith('.xml'):
                    ws_path = os.path.join(worksheets_dir, ws_file)
                    with open(ws_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    content = re.sub(r'<dataValidations.*?</dataValidations>', '', content, flags=re.DOTALL)
                    with open(ws_path, 'w', encoding='utf-8') as f:
                        f.write(content)
        
        fixed_file = file_path.replace('.xlsx', '_temp_fixed.xlsx')
        with zipfile.ZipFile(fixed_file, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, temp_dir)
                    zip_ref.write(file_path, arcname)
        
        xl = pd.ExcelFile(fixed_file)
        df = pd.read_excel(fixed_file, sheet_name=xl.sheet_names[0])
        os.remove(fixed_file)
        return df
    finally:
        shutil.rmtree(temp_dir)


def filter_out_tenant(df, label='', excluded_tenants=None):
    excluded_tenants = excluded_tenants or {'懂车帝'}
    tenant_cols = [col for col in ['租户', '租户名称'] if col in df.columns]
    if not tenant_cols:
        return df

    original_len = len(df)
    mask = pd.Series(True, index=df.index)
    for col in tenant_cols:
        tenant_series = df[col].astype(str).str.strip()
        mask = mask & (~tenant_series.isin(excluded_tenants))

    filtered_df = df[mask].reset_index(drop=True)
    removed_count = original_len - len(filtered_df)
    if removed_count > 0:
        label_prefix = f'{label}: ' if label else ''
        excluded_text = ', '.join(sorted(excluded_tenants))
        print(f'{label_prefix}Filtered from {original_len} to {len(filtered_df)} rows after excluding tenants [{excluded_text}]')
    return filtered_df
