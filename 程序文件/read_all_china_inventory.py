import zipfile
import os
import shutil
import tempfile
import pandas as pd
import re

def fix_excel_file(input_path, output_path):
    """Fix Excel file by removing problematic data validation"""
    temp_dir = tempfile.mkdtemp()
    try:
        # Extract the Excel file
        with zipfile.ZipFile(input_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Find all worksheet XML files
        worksheets_dir = os.path.join(temp_dir, 'xl', 'worksheets')
        if os.path.exists(worksheets_dir):
            worksheet_files = os.listdir(worksheets_dir)
            
            # Process each worksheet file
            for ws_file in worksheet_files:
                if ws_file.endswith('.xml'):
                    ws_path = os.path.join(worksheets_dir, ws_file)
                    
                    # Read and modify the XML - remove data validation
                    with open(ws_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Remove dataValidations section
                    content = re.sub(r'<dataValidations.*?</dataValidations>', '', content, flags=re.DOTALL)
                    
                    # Write back
                    with open(ws_path, 'w', encoding='utf-8') as f:
                        f.write(content)
        
        # Create a new fixed Excel file
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, temp_dir)
                    zip_ref.write(file_path, arcname)
        
        return True
    except Exception as e:
        print(f'Error fixing {input_path}: {e}')
        return False
    finally:
        # Clean up temp directory
        shutil.rmtree(temp_dir)

def read_excel_file(file_path):
    """Read Excel file, fix if needed"""
    try:
        # Try normal read first
        return pd.read_excel(file_path)
    except Exception as e:
        print(f'Normal read failed for {file_path}, trying to fix...')
        # Fix and read
        fixed_path = file_path.replace('.xlsx', '_fixed.xlsx')
        if fix_excel_file(file_path, fixed_path):
            return pd.read_excel(fixed_path)
        else:
            raise e

# Read all three China inventory files
base_path = '/Users/bytedance/Desktop/库存需求表'

files = [
    '中国_VMI库存.xlsx',
    '中国_open po库存.xlsx'
]

dataframes = {}

# Read the already fixed file
print('Reading 中国_in transit库存_fixed.xlsx...')
dataframes['in_transit'] = pd.read_excel(os.path.join(base_path, '中国_in transit库存_fixed.xlsx'))
print(f'  - Shape: {dataframes["in_transit"].shape}')

# Read the other two
for file in files:
    print(f'\nReading {file}...')
    try:
        df = read_excel_file(os.path.join(base_path, file))
        key = file.replace('中国_', '').replace('.xlsx', '').replace(' ', '_')
        dataframes[key] = df
        print(f'  - Shape: {df.shape}')
        print(f'  - Columns: {df.columns.tolist()[:10]}...')
    except Exception as e:
        print(f'  - Failed: {e}')

print(f'\nSuccessfully loaded {len(dataframes)} DataFrames!')
