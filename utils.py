import pandas as pd
import numpy as np
import os
import re

def get_df_from_file(path):
    sheet_names = pd.ExcelFile(path).sheet_names
    res = {}
    for sheet_name in sheet_names:
        if sheet_name == 'Summary':
            continue
        for cols_seq in ['A,B,F,K,L', 'A,B,D,E,G:J', 'M,N,P:S', 'T,U,Y,Z']:
            df = pd.read_excel(path, sheet_name=sheet_name, usecols=cols_seq, header=5)
            df.columns = [re.sub(r'\.\d+', '', re.sub(r'Unnamed: \d+', '', ' '.join(x).strip())) for x in zip(df.iloc[0].fillna('').values, df.columns)]
            df = df.dropna()
            if len(df) > 0:
                if res.get(sheet_name):
                    res[sheet_name].append(df.copy())
                else:
                    res[sheet_name] = [df.copy()]
            else:
                continue
    return res

def product_code(code):
    if 'CGA' in code:
        return 'CGA' + code.replace('CGA', '')
    else:
        return code

def dfs_to_excel(path):
    dfs = get_df_from_file(path)
    path_splitted = path.split('/')
    end_path = '/'.join(path_splitted[:-1] + [''.join(path_splitted[-1].split('.')[:-1])])
    try:
        os.mkdir(end_path)
    except OSError as e:
        print(e)
    for key in dfs.keys():
        for i, d in enumerate(dfs[key]):
            d['ProdCode'] = d['ProdCode'].astype('str').apply(product_code)
            d.to_excel(f'{end_path}/{key}_{i}.xlsx', index=False)

if __name__ == '__main__':
    dfs_to_excel('Data/PSI 2021.xlsx')