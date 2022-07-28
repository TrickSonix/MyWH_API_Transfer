#import pandas as pd
#import numpy as np
#import os
import re
import datetime as dt
from setup import COMPANY_TYPES, NDS_COMPARE


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

def is_guid(s: str) -> bool:
    if isinstance(s, str) and re.match(r'^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$', s) and not re.match(r'^0{8}-0{4}-0{4}-0{4}-0{12}$', s):
        return True
    else:
        return False


class JSONPermute:

    def __init__(self, item: dict, item_type: str, db) -> None:
        self.item = item
        self.item_type = item_type
        self.db = db

    def date_convert(self, date):
        return dt.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")

    def get_meta_by_guid(self, guid):
        item = next(self.db.find_by_guid(guid))
        if item.get('#mywh'):
            return item['#mywh'].get('meta')
        else:
            return {}

    def _permute_organization(self):
        result = {}
        result['name'] = self.item['#value'].get('Description') or ""
        result['companyType'] = 'entrepreneur'
        result['inn'] = self.item['#value'].get('ИНН') or ""
        result['okpo'] = self.item['#value'].get('КодПоОКПО') or ""
        result['ogrnip'] = self.item['#value'].get('ОГРН') or ""
        result['accounts'] = []
        accounts = self.db.get_items('Справочники', {"#type": "jcfg:CatalogObject.БанковскиеСчетаОрганизаций", "#value.Owner.#value": self.item['#value']['Ref']})
        for acc in accounts:
            temp = {}
            if acc.get('#mywh'):
                if acc['#mywh'].get('meta'):
                    temp['meta'] = acc['#mywh'].get('meta')
            temp['accountNumber'] = acc['#value']['НомерСчета']
            temp['bankLocation'] = acc['#value']['Ref']
            temp['bankName'] = (next(self.db.find_by_guid(acc['#value'].get('Банк'))).get('#value') or dict()).get('Description') or ""
            result['accounts'].append(temp)
            del temp
        cash_acc = self.db.get_items('Справочники', {"#type": "jcfg:CatalogObject.Кассы", "#value.Owner.#value": self.item['#value']['Ref']})
        for acc in cash_acc:
            temp = {}
            if acc.get('#mywh'):
                if acc['#mywh'].get('meta'):
                    temp['meta'] = acc['#mywh']['meta']
            temp['accountNumber'] = acc['#value']['Description']
            temp['bankLocation'] = acc['#value']['Ref']
            result['accounts'].append(temp)
            del temp
        return result
    
    def _permute_product(self):
        result = {}
        result['name'] = self.item['#value'].get('Description') or ""
        result['uom'] = {"meta": {
                "href": "https://online.moysklad.ru/api/remap/1.2/entity/uom/19f1edc0-fc42-4001-94cb-c9ec9c62ec10",
                "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/uom/metadata",
                "type": "uom",
                "mediaType": "application/json"
                }
            }
        result['article'] = self.item['#value'].get('Артикул') or ""
        result['shared'] = True
        return result
    
    def _permute_counterparty(self):
        result = {}
        result['name'] = self.item['#value'].get('Description') or ""
        result['companyType'] = COMPANY_TYPES.get(self.item['#value'].get('ЮрФизЛицо'))
        result['inn'] = self.item['#value'].get('ИНН') or ""
        result['okpo'] = self.item['#value'].get('КодПоОКПО') or ""
        result['kpp'] = self.item['#value'].get('КПП') or ""
        result['shared'] = True
        result['accounts'] = []
        accounts = self.db.get_items('Справочники', {"#type": "jcfg:CatalogObject.БанковскиеСчетаКонтрагентов", "#value.Owner.#value": self.item['#value']['Ref']})
        for acc in accounts:
            temp = {}
            if acc.get('#mywh'):
                if acc['#mywh'].get('meta'):
                    temp['meta'] = acc['#mywh'].get('meta')
            temp['accountNumber'] = acc['#value']['НомерСчета']
            temp['bankLocation'] = acc['#value']['Ref']
            temp['bankName'] = (next(self.db.find_by_guid(acc['#value'].get('Банк'))).get('#value') or dict()).get('Description') or ""
            result['accounts'].append(temp)
            del temp
        return result

    def _permute_expenseitem(self):
        result = {}
        result['name'] = self.item['#value'].get('Description') or ""
        result['description'] = self.item['#value'].get('Описание') or ""
        return result

    def _permute_store(self):
        result = {}
        result['name'] = self.item['#value'].get('Description') or ""
        return result

    def _permute_supply(self):
        result = {}
        result['agent'] = {'meta': self.get_meta_by_guid(self.item['#value']['Контрагент'])}
        result['agentAccount'] = {'meta': self.get_meta_by_guid(self.item['#value']['БанковскийСчетКонтрагента'])}
        result['applicable'] = self.item['#value'].get('Posted') or False
        result['moment'] = self.date_convert(self.item['#value'].get('Date') or "")
        result['organization'] = {'meta': self.get_meta_by_guid(self.item['#value']['Организация'])}
        result['organizationAccount'] = {'meta': self.get_meta_by_guid(self.item['#value']['БанковскийСчетОрганизации'])}
        result['shared'] = True
        result['store'] = {'meta': self.get_meta_by_guid(self.item['#value']['Склад'])}
        result['positions'] = []
        if self.item['#value'].get('Товары'):
            for pos in self.item['#value']['Товары']:
                temp = {}
                temp['assortment'] = {'meta': self.get_meta_by_guid(pos.get('Номенклатура'))}
                temp['price'] = pos['Цена']*100
                temp['quantity'] = pos['Количество']
                temp['vat'] = (NDS_COMPARE.get(pos['СтавкаНДС']) or [0, False])[0]
                temp['vatEnabled'] = (NDS_COMPARE.get(pos['СтавкаНДС']) or [0, False])[1]
                result['positions'].append(temp)
            del temp
        return result

    def _permute_enter(self):
        result = {}
        result['applicable'] = self.item['#value'].get('Posted') or False
        result['moment'] = self.date_convert(self.item['#value'].get('Date') or "")
        result['organization'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Организация'))}
        result['shared'] = True
        result['store'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Склад'))}
        result['description'] = self.item['#value'].get('Комментарий')
        result['positions'] = []
        if self.item['#value'].get('Товары'):
            for pos in self.item['#value']['Товары']:
                temp = {}
                temp['assortment'] = {'meta': self.get_meta_by_guid(pos.get('Номенклатура'))}
                temp['price'] = pos['Цена']*100
                temp['quantity'] = pos['Количество']
                result['positions'].append(temp)
                del temp
        return result

    def _permute_purchasereturn(self):
        result = {}
        result['agent'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Контрагент'))}
        result['agentAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетКонтрагента'))}
        result['applicable'] = self.item['#value'].get('Posted') or False
        result['moment'] = self.date_convert(self.item['#value'].get('Date') or "")
        result['organization'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Организация'))}
        result['organizationAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетОрганизации'))}
        result['shared'] = True
        result['store'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Склад'))}
        result['supply'] = {'meta': self.get_meta_by_guid(self.item['#value']['Товары'][0].get('ДокументПоступления'))} #вот эту хуйню проверь потом
        result['positions'] = []
        if self.item['#value'].get('Товары'):
            for pos in self.item['#value']['Товары']:
                temp = {}
                temp['assortment'] = {'meta': self.get_meta_by_guid(pos.get('Номенклатура'))}
                temp['price'] = pos['Цена']*100
                temp['quantity'] = pos['Количество']
                temp['vat'] = (NDS_COMPARE.get(pos['СтавкаНДС']) or [0, False])[0]
                temp['vatEnabled'] = (NDS_COMPARE.get(pos['СтавкаНДС']) or [0, False])[1]
                result['positions'].append(temp)
                del temp
        return result

    def _permute_customerorder(self):
        result = {}
        result['agent'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Контрагент'))}
        result['agentAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетКонтрагента'))}
        result['applicable'] = self.item['#value'].get('Posted') or False
        result['moment'] = self.date_convert(self.item['#value'].get('Date') or "")
        result['organization'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Организация'))}
        result['organizationAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетОрганизации'))}
        result['shared'] = True
        result['store'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Склад'))}
        result['positions'] = []
        if self.item['#value'].get('Товары'):
            for pos in self.item['#value']['Товары']:
                temp = {}
                temp['assortment'] = {'meta': self.get_meta_by_guid(pos.get('Номенклатура'))}
                temp['price'] = pos['Цена']*100
                temp['quantity'] = pos['Количество']
                temp['vat'] = (NDS_COMPARE.get(pos['СтавкаНДС']) or [0, False])[0]
                temp['vatEnabled'] = (NDS_COMPARE.get(pos['СтавкаНДС']) or [0, False])[1]
                temp['reserve'] = pos['Количество'] if pos['ВариантОбеспечения'] == 'Отгрузить' else 0
                result['positions'].append(temp)
                del temp
        return result

    def _permute_demand(self):
        result = {}
        result['agent'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Контрагент'))}
        result['agentAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетКонтрагента'))}
        result['applicable'] = self.item['#value'].get('Posted') or False
        result['moment'] = self.date_convert(self.item['#value'].get('Date') or "")
        result['organization'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Организация'))}
        result['organizationAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетОрганизации'))}
        result['shared'] = True
        result['store'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Склад'))}
        result['positions'] = []
        if self.item['#value'].get('Товары'):
            for pos in self.item['#value']['Товары']:
                temp = {}
                temp['assortment'] = {'meta': self.get_meta_by_guid(pos.get('Номенклатура'))}
                temp['price'] = pos['Цена']*100
                temp['quantity'] = pos['Количество']
                temp['vat'] = (NDS_COMPARE.get(pos['СтавкаНДС']) or [0, False])[0]
                temp['vatEnabled'] = (NDS_COMPARE.get(pos['СтавкаНДС']) or [0, False])[1]
                result['positions'].append(temp)
                del temp
            result['demand'] = {'meta': self.get_meta_by_guid(self.item['#value']['Товары'][0].get('ЗаказКлиента'))} #вот это тоже поменять бы потом
        return result

    def _permute_salesreturn(self):
        result = {}
        result['agent'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Контрагент'))}
        result['agentAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетКонтрагента'))}
        result['applicable'] = self.item['#value'].get('Posted') or False
        result['moment'] = self.date_convert(self.item['#value'].get('Date') or "")
        result['organization'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Организация'))}
        result['organizationAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетОрганизации'))}
        result['shared'] = True
        result['store'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Склад'))}
        result['positions'] = []
        if self.item['#value'].get('Товары'):
            for pos in self.item['#value']['Товары']:
                temp = {}
                temp['assortment'] = {'meta': self.get_meta_by_guid(pos.get('Номенклатура'))}
                temp['price'] = pos['Цена']*100
                temp['quantity'] = pos['Количество']
                temp['vat'] = (NDS_COMPARE.get(pos['СтавкаНДС']) or [0, False])[0]
                temp['vatEnabled'] = (NDS_COMPARE.get(pos['СтавкаНДС']) or [0, False])[1]
                result['positions'].append(temp)
                del temp
            result['demand'] = {'meta': self.get_meta_by_guid(self.item['#value']['Товары'][0].get('ДокументРеализации'))}
        return result

    def _permute_loss(self):
        result = {}
        result['agent'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Контрагент'))}
        result['agentAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетКонтрагента'))}
        result['applicable'] = self.item['#value'].get('Posted') or False
        result['moment'] = self.date_convert(self.item['#value'].get('Date') or "")
        result['organization'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Организация'))}
        result['organizationAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетОрганизации'))}
        result['shared'] = True
        result['store'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Склад'))}
        result['positions'] = []
        if self.item['#value'].get('Товары'):
            for pos in self.item['#value']['Товары']:
                temp = {}
                temp['assortment'] = {'meta': self.get_meta_by_guid(pos.get('Номенклатура'))}
                temp['price'] = 100
                temp['quantity'] = pos['Количество']
                result['positions'].append(temp)
                del temp
        return result

    def _permute_paymentin(self):
        result = {}
        result['agent'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Контрагент'))}
        result['agentAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетКонтрагента'))}
        result['applicable'] = self.item['#value'].get('Posted') or False
        result['moment'] = self.date_convert(self.item['#value'].get('Date') or "")
        result['organization'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Организация'))}
        result['organizationAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетОрганизации'))}
        result['shared'] = True
        result['sum'] = self.item['#value'].get('СуммаДокумента') or 0
        result['operations'] = []
        if self.item['#value'].get('РасшифровкаПлатежа'):
            for pos in self.item['#value']['РасшифровкаПлатежа'] or []:
                temp = {}
                temp['meta'] = self.get_meta_by_guid(pos.get('ОснованиеПлатежа'))
                result['operations'].append(temp)
                del temp
        return result

    def _permute_paymentout(self):
        result = {}
        result['agent'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Контрагент'))}
        result['agentAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетКонтрагента'))}
        result['applicable'] = self.item['#value'].get('Posted') or False
        result['moment'] = self.date_convert(self.item['#value'].get('Date') or "")
        result['organization'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('Организация'))}
        result['organizationAccount'] = {'meta': self.get_meta_by_guid(self.item['#value'].get('БанковскийСчетОрганизации'))}
        result['shared'] = True
        result['sum'] = self.item['#value'].get('СуммаДокумента') or 0
        result['operations'] = []
        if self.item['#value'].get('РасшифровкаПлатежа'):
            for pos in self.item['#value']['РасшифровкаПлатежа'] or []:
                temp = {}
                temp['meta'] = self.get_meta_by_guid(pos.get('ОснованиеПлатежа'))
                result['operations'].append(temp)
                del temp
        return result

    def permute(self) -> dict:
        result = {}
        if self.item.get('#mywh'):
            if self.item['#mywh'].get('meta'):
                result['meta'] = self.item['#mywh'].get('meta')
        
        if self.item_type == 'organization':
            result.update(self._permute_organization())
        if self.item_type == 'product':
            result.update(self._permute_product())
        if self.item_type == 'service':
            result.update(self._permute_product())
        if self.item_type == 'counterparty':
            result.update(self._permute_counterparty())
        # if self.item_type == 'counterpartyadjustment':
        #     result.update(self._permute_counterpartyadjustment())
        if self.item_type == 'expenseitem':
            result.update(self._permute_expenseitem())
        if self.item_type == 'store':
            result.update(self._permute_store())
        if self.item_type == 'supply':
            result.update(self._permute_supply())
        if self.item_type == 'enter':
            result.update(self._permute_enter())
        # if self.item_type == 'move':
        #     result.update(self._permute_move())
        if self.item_type == 'purchasereturn':
            result.update(self._permute_purchasereturn())
        if self.item_type == 'customerorder':
            result.update(self._permute_customerorder())
        if self.item_type == 'demand':
            result.update(self._permute_demand())
        if self.item_type == 'salesreturn':
            result.update(self._permute_salesreturn())
        if self.item_type == 'loss':
            result.update(self._permute_loss())
        if self.item_type == 'paymentin':
            result.update(self._permute_paymentin())
        if self.item_type == 'paymentout':
            result.update(self._permute_paymentout())
        # if self.item_type == 'cashin':
        #    result.update(self._permute_cashin())
        # if self.item_type == 'cashout':
        #     result.update(self._permute_cashout())
        
        return result



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