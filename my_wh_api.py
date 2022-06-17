from email import header
from sqlite3 import paramstyle
import requests
import os
from openpyxl import load_workbook
import datetime as dt
import copy
from setup import ACCESS_TOKEN, CUSTOMORDER, PSWD, USERNAME, HEADERS, CUSTOMORDER_COPIED_FIELDS, MAIN_URL, PRODUCT, PRODUCTS_DB
import json
import functools
import operator


class MyWHAPI():

    def __init__(self, username, pswd, access_token=None) -> None:
        self.access_token = access_token
        self.session = requests.Session()
        self.session.auth = (username, pswd)
        self.session.headers.update(HEADERS)

    @staticmethod
    def status_check(response):
        return response.status_code == requests.codes.ok

    # def start_session(self):
    #     self.session = requests.Session()
    #     self.session.auth = (self.username, self.pswd)
            
    @staticmethod
    def list_flatten(list_2):
        return functools.reduce(operator.iconcat, list_2, [])
        
    @staticmethod
    def to_query(params):    
        return ';'.join([f'{key}={item}' for key, item in zip(MyWHAPI.list_flatten([[key]*len(value) for key, value in params.items()]), MyWHAPI.list_flatten([x for x in params.values()]))])

    def request(self, method, path, **kwargs):
        with self.session:
            return self.session.request(method=method, url=MAIN_URL + path, **kwargs)

    def get_products(self, params={},  **kwargs):
        if os.path.exists(PRODUCTS_DB):
            with open(PRODUCTS_DB, 'r') as f:
                db = json.load(f)
        else:
            db = json.loads(self.request(method='GET', path=PRODUCT, params={'filter': self.to_query(params)}).text)['rows']
            with open(PRODUCTS_DB, 'w') as f:
                json.dump(db, f, indent=4)
        result = []
        missed = {}
        for key, value in params.items():
            for v in value:
                item = list(filter(lambda x: x.get(key)==v, db))
                if item:
                    result.extend(item)
                else:
                    if missed.get(key):
                        missed[key].append(v)
                    else:
                        missed[key] = [v]
        if missed:
            new = json.loads(self.request(method='GET', path=PRODUCT, params={'filter': self.to_query(missed)}).text)['rows']
            db.extend(new)
            result.extend(new)
            with open(PRODUCTS_DB, 'w') as f:
                json.dump(db, f, indent=4)
        return result

    def create_positions_fields(self, path, **kwargs):

        """Workbook structure must be:
            column # 1          2       3       4           5
                     article    name    units   unit_price  sum
            
        """
        fields = ['assortment', 'quantity', 'price', 'reserve'] #discount add?
        data = [x for x in load_workbook(path).worksheets[0].iter_rows(min_row=kwargs.get('wb_header') or 2, values_only=True) if str(x[2])!='0']

        result = []
        products = self.get_products(params={'article': [x[0] for x in data]})
        for row in data:
            body = {x:0 for x in fields}
            body['assortment'] = {'meta': list(filter(lambda x: x['article']==row[0], products))[0]['meta']}
            body['quantity'] = row[2]
            body['price'] = row[3]
            body['reserve'] = row[2]
            result.append(body)
            
        return result

    def create_customorder_body(self, copy_from=None, **kwargs):
        if copy_from:
            copied_order = self.request(
                method='GET', path=CUSTOMORDER, params={'search': copy_from}, headers=kwargs.get('headers'))
            if self.status_check(copied_order):
                copied_body = copied_order.json()
                result = {x:copied_body['rows'][0][x] for x in copied_body['rows'][0] if x in CUSTOMORDER_COPIED_FIELDS}
                result['name'] = kwargs.get('name')
                result['positions'] = self.create_positions_fields(kwargs.get('positions_data'))
                result['moment'] = kwargs.get('moment') or dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                result['description'] = kwargs.get('description')
                return json.dumps(result, indent=4)
            else:
                return copied_order.status_code


if __name__ =='__main__':

    mywh = MyWHAPI(USERNAME, PSWD)
    response = mywh.create_customorder_body(copy_from='HGT-00001', name='HGT-00015', moment='2022-01-30 00:00:00', positions_data='Data/PSI 2021/Apr_0.xlsx')
    #with open('Data/123.txt', 'w') as f:
        
    print('1')