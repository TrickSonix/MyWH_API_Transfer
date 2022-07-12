import requests
import os
from openpyxl import load_workbook, Workbook
import datetime as dt
from setup import REQUEST_LINKS, PSWD, USERNAME, HEADERS, CUSTOMORDER_COPIED_FIELDS, MAIN_URL, PRODUCTS_DB, MAX_DATA_SIZE
import json
import functools
import operator
from logger import setup_logger
from logging import DEBUG


"""Нужно будет протестировать изменения в коде и дальше уже пилить заполнение тела для разных запросов и распарса json'a из базы.
    Позже пойму где еще нужно добавить логгирование.
    Где-то еще можно добавить асинхронщины... Скорее всего в GET-запросах. Но если добавлять ее, то надо учитывать ограничения API моего склада (см документацию)
    
    Поскольку я таки пришел к тому что сделал БД для переноса, нужно будет туда напихать все справочники из моего склада, чтобы не делать запрос к апи каждый раз"""

basic_logger = setup_logger('basic_logger', 'logs/my_wh_api.log', DEBUG)


class MyWHAPI():

    def __init__(self, username, pswd, access_token=None) -> None:
        self.access_token = access_token
        self.session = requests.Session()
        self.session.auth = (username, pswd)
        self.session.headers.update(HEADERS)

    @staticmethod
    def status_check(response):
        return response.status_code == requests.codes.ok
            
    @staticmethod
    def list_flatten(list_2):
        return functools.reduce(operator.iconcat, list_2, [])
        
    @staticmethod
    def to_query(params):    
        """аргумент params передается в виде словаря, в котором все значения должны быть списками"""
        return ';'.join([f'{key}={item}' for key, item in zip(MyWHAPI.list_flatten([[key]*len(value) for key, value in params.items()]), MyWHAPI.list_flatten([x for x in params.values()]))])
    
    @staticmethod
    def json_to_excel(json_data, **kwargs):
        #пока что явно нужно указывать колонки, но наверное хорошо бы сделать какое-то значение по-умолчанию
        data = MyWHAPI.json_load(json_data)
        wb = Workbook()
        ws = wb.active
        if kwargs.get('columns'):
            for n_row, row in enumerate(data):
                for n_column, column in enumerate(kwargs['columns']):
                    if n_row == 0:
                        ws.cell(row=n_row+1, column=n_column+1, value=column)
                    else:
                        if not isinstance(row.get(column), str):
                            continue
                        else:
                            ws.cell(row=n_row+1, column=n_column+1, value=row.get(column))
            filename = kwargs.get('save_path') or f'{dt.datetime.now().strftime("%d-%m-%y %H-%M")}.xlsx' 
            wb.save(filename)
            basic_logger.info(f'File {filename} was written.')


    @staticmethod
    def json_load(json_data):
        json_logger = setup_logger('json_logger', f"{dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S')}_json.log", DEBUG)
        if isinstance(json_data, dict):
            return json_data
        if os.path.exists(json_data):
            with open(json_data, 'r') as f:
                try:
                    return json.loads(f)
                except json.decoder.JSONDecodeError as e:
                    json_logger.error(e)
                    json_logger.debug(f'JSON_data: *json*{json_data}*json*')
                    return {}
        else:
            try:
                return json.loads(json_data)
            except json.decoder.JSONDecodeError as e:
                json_logger.error(e)
                json_logger.debug(f'JSON_data: *json*{json_data}*json*')
                return {}

    def request(self, method, path, **kwargs):
        request_logger = setup_logger('request_logger', f"{dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S')}_request.log", DEBUG)
        request_logger.info(f'URL: {response.url}. Status code {response.status_code}')
        request_logger.debug(f'Response text: *json*{response.text}*json*')
        
        with self.session:
            response = self.session.request(method=method, url=MAIN_URL + path, **kwargs)
            response.raise_for_status()
            return response

    def get_products(self, params={},  **kwargs):
        """аргумент params передается в виде словаря, в котором все значения должны быть списками"""
        db = self.json_load(PRODUCTS_DB) 
        result = []
        missed = {}
        if not db:
            response = self.request(method='GET', path=REQUEST_LINKS['PRODUCT'], params={'filter': self.to_query(params)})
            db = self.json_load(response.json()).get('rows')
        for key, value in params.items():
    
            items = list(filter(lambda x: x.get(key) in value, db))
            result.extend(items)
            missed[key] = [x for x in value if x not in [x.get(key) for x in items]]
        
        if self.list_flatten(missed.values()):
            new_response = self.request(method='GET', path=REQUEST_LINKS['PRODUCT'], params={'filter': self.to_query(missed)})
            new = self.json_load(new_response.json()).get('rows')
            db.extend(new)
            result.extend(new)
        with open(PRODUCTS_DB, 'w') as f:
            json.dump(db, f, indent=4)
        return result

    def create_positions_fields_from_excel(self, path, **kwargs):
        #Вот эту функцию надо переделать в общий вид, потому что сейчас она делалась исключительно для 1 типа файлов.
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
        json_logger = setup_logger('json_logger', f"{dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S')}_json.log", DEBUG)
        if copy_from:
            copied_order = self.request(
                method='GET', path=REQUEST_LINKS['CUSTOMORDER'], params={'search': copy_from}, headers=kwargs.get('headers'))
            copied_body = copied_order.json()
            result = {x:copied_body['rows'][0][x] for x in copied_body['rows'][0] if x in CUSTOMORDER_COPIED_FIELDS}
            result['name'] = kwargs.get('name')
            result['positions'] = self.create_positions_fields(kwargs.get('positions_data'))
            result['moment'] = kwargs.get('moment') or dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            result['description'] = kwargs.get('description')
            json_logger.debug(f'Customorder body: *json*{result}*json*')
            return json.dumps(result, indent=4)
        else:
            return {}

    def create_json_data_body(self):
        #Это оказалось несколько сложнее чем я думал.... Нужно обмозговать как следует
        pass

if __name__ =='__main__':

    
    mywh = MyWHAPI(USERNAME, PSWD)
    #response = mywh.get_products(params={'pathName':['Товары интернет-магазинов/Настольные игры/CrowdGames']})
    #mywh.json_to_excel(json.dumps(response), columns=['id', 'name', 'article'])
    basic_logger.info('Finish')