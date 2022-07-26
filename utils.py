#import pandas as pd
#import numpy as np
#import os
import re
from xmlrpc.client import Boolean


def damerau_levenshtein_distance(s1, s2):
    d = {}
    lenstr1 = len(s1)
    lenstr2 = len(s2)
    for i in range(-1,lenstr1+1):
        d[(i,-1)] = i+1
    for j in range(-1,lenstr2+1):
        d[(-1,j)] = j+1
 
    for i in range(lenstr1):
        for j in range(lenstr2):
            if s1[i] == s2[j]:
                cost = 0
            else:
                cost = 1
            d[(i,j)] = min(
                           d[(i-1,j)] + 1, # deletion
                           d[(i,j-1)] + 1, # insertion
                           d[(i-1,j-1)] + cost, # substitution
                          )
            if i and j and s1[i]==s2[j-1] and s1[i-1] == s2[j]:
                d[(i,j)] = min (d[(i,j)], d[i-2,j-2] + 1) # transposition
 
    return d[lenstr1-1,lenstr2-1]

def find_max_look_like(string, string_list):
    res = []
    for s in string_list:
        alike = damerau_levenshtein_distance(string, s)
        res.append((s, alike))
    return sorted(res, key=lambda x: x[1])

def is_guid(s: str) -> Boolean:
    if isinstance(s, str) and re.match(r'^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$', s) and not re.match(r'^0{8}-0{4}-0{4}-0{4}-0{12}$', s):
        return True
    else:
        return False
        
# def get_df_from_file(path):
#     sheet_names = pd.ExcelFile(path).sheet_names
#     res = {}
#     for sheet_name in sheet_names:
#         if sheet_name == 'Summary':
#             continue
#         for cols_seq in ['A,B,F,K,L', 'A,B,D,E,G:J', 'M,N,P:S', 'T,U,Y,Z']:
#             df = pd.read_excel(path, sheet_name=sheet_name, usecols=cols_seq, header=5)
#             df.columns = [re.sub(r'\.\d+', '', re.sub(r'Unnamed: \d+', '', ' '.join(x).strip())) for x in zip(df.iloc[0].fillna('').values, df.columns)]
#             df = df.dropna()
#             if len(df) > 0:
#                 if res.get(sheet_name):
#                     res[sheet_name].append(df.copy())
#                 else:
#                     res[sheet_name] = [df.copy()]
#             else:
#                 continue
#     return res

# def product_code(code):
#     if 'CGA' in code:
#         return 'CGA' + code.replace('CGA', '')
#     else:
#         return code

# def dfs_to_excel(path):
#     dfs = get_df_from_file(path)
#     path_splitted = path.split('/')
#     end_path = '/'.join(path_splitted[:-1] + [''.join(path_splitted[-1].split('.')[:-1])])
#     try:
#         os.mkdir(end_path)
#     except OSError as e:
#         print(e)
#     for key in dfs.keys():
#         for i, d in enumerate(dfs[key]):
#             d['ProdCode'] = d['ProdCode'].astype('str').apply(product_code)
#             d.to_excel(f'{end_path}/{key}_{i}.xlsx', index=False)

if __name__ == '__main__':
    print(find_max_look_like('Серп', ['Настольная игра Серп', 'Серп. Дополнение']))
    pass