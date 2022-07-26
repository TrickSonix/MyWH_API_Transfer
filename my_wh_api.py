import requests
import os
import sys
import time
from openpyxl import load_workbook, Workbook
import datetime as dt
from setup import REQUEST_LINKS, PSWD, USERNAME, HEADERS, CUSTOMERORDER_COPIED_FIELDS, MAIN_URL, PRODUCTS_DB, MAX_DATA_SIZE, REQUESTS_SECONDS_RESTRICTION, REQUESTS_LIMIT, TIMEOUT_TIME
import json
import functools
import operator
from logger import setup_logger
from logging import DEBUG
from ratelimit import limits
from ratelimit.exception import RateLimitException
from db_server import CrowdGamesDB


"""Нужно будет протестировать изменения в коде и дальше уже пилить заполнение тела для разных запросов и распарса json'a из базы.
    Позже пойму где еще нужно добавить логгирование.
    Где-то еще можно добавить асинхронщины... Скорее всего в GET-запросах. Но если добавлять ее, то надо учитывать ограничения API моего склада (см документацию)
    
    Поскольку я таки пришел к тому что сделал БД для переноса, нужно будет туда напихать все справочники из моего склада, чтобы не делать запрос к апи каждый раз
    
    demand.json надо пофиксить, а скорее всего вообще все переделать."""

basic_logger = setup_logger('basic_logger', 'logs/my_wh_api.log', DEBUG)
json_logger = setup_logger('json_logger', f"logs/{dt.datetime.now().strftime('%Y-%m-%d')}_json.log", DEBUG)
request_logger = setup_logger('request_logger', f"logs/{dt.datetime.now().strftime('%Y-%m-%d')}_request.log", DEBUG)
DB = CrowdGamesDB()

def sleep_and_retry(func):
    '''
    Return a wrapped function that rescues rate limit exceptions, sleeping the
    current thread until rate limit resets.
    :param function func: The function to decorate.
    :return: Decorated function.
    :rtype: function
    '''
    @functools.wraps(func)
    def wrapper(*args, **kargs):
        '''
        Call the rate limited function. If the function raises a rate limit
        exception sleep for the remaing time period and retry the function.
        :param args: non-keyword variable length argument list to the decorated function.
        :param kargs: keyworded variable length argument list to the decorated function.
        '''
        while True:
            try:
                return func(*args, **kargs)
            except RateLimitException as exception:
                basic_logger.info('Program stopped from requests restrictions')
                time.sleep(exception.period_remaining)
    return wrapper
    
class MyWHAPI:

    def __init__(self, username, pswd, access_token=None, data_size_restriction=10**6) -> None:
        self.access_token = access_token
        self.session = requests.Session()
        self.session.auth = (username, pswd)
        self.session.headers.update(HEADERS)
        self.data_size_restriction = data_size_restriction
            
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
        if isinstance(json_data, dict):
            return json_data
        if os.path.exists(json_data):
            with open(json_data, 'r') as f:
                try:
                    return json.load(f)
                except json.decoder.JSONDecodeError as e:
                    json_logger.error(e)
                    json_logger.debug(f'JSON_data: !{json_data}!')
                    return {}
        else:
            try:
                return json.loads(json_data)
            except json.decoder.JSONDecodeError as e:
                json_logger.error(e)
                json_logger.debug(f'JSON_data: !{json_data}!')
                return {}

    def _check_data_size(self, data):
        return sys.getsizeof(data) > self.data_size_restriction

    @sleep_and_retry
    @limits(calls=REQUESTS_SECONDS_RESTRICTION, period=REQUESTS_LIMIT)
    def request(self, method : str, path : str,  **kwargs) -> requests.Response:    
        try:
            with self.session as session:
                response = session.request(method=method, url=''.join([MAIN_URL, path or '']), timeout=TIMEOUT_TIME, **kwargs)
                request_logger.info(f'URL: {response.url}. Status code {response.status_code}')
                response.raise_for_status()
                return response
        except requests.exceptions.HTTPError as e:
            request_logger.debug(f'Not transfered request body: !{kwargs.get("data")}!')
            request_logger.exception(f'{e}', exc_info=True)
            request_logger.debug(f'Response text: !{response.text}!')
            return requests.Response()
        except requests.exceptions.Timeout as e:
            request_logger.debug(f'Timeout Error')
            request_logger.debug(f'Not transfered request body: !{kwargs.get("data")}!')
            request_logger.exception(f'{e}', exc_info=True)
            return requests.Response()

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

    def create_customerorder_body(self, copy_from=None, **kwargs):
        json_logger = setup_logger('json_logger', f"{dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S')}_json.log", DEBUG)
        if copy_from:
            copied_order = self.request(
                method='GET', path=REQUEST_LINKS['CUSTOMERORDER'], params={'search': copy_from}, headers=kwargs.get('headers'))
            copied_body = copied_order.json()
            result = {x:copied_body['rows'][0][x] for x in copied_body['rows'][0] if x in CUSTOMERORDER_COPIED_FIELDS}
            result['name'] = kwargs.get('name')
            result['positions'] = self.create_positions_fields_from_excel(kwargs.get('positions_data'))
            result['moment'] = kwargs.get('moment') or dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            result['description'] = kwargs.get('description')
            json_logger.debug(f'Customerorder body: !{result}!')
            return json.dumps(result, indent=4)
        else:
            return {}
    #Все что ниже херня и не работает
    # def create_body(self, item, path, body_schema : dict):
        
    #     temp = {}
    #     for key, path in body_schema.items():
    #         if '#' in key:
    #             continue
    #         if isinstance(path, dict):
    #             self.create_body(item,)
    #         field = self.get_field_from_item(item, path)
    #         if field:
    #             temp[key] = field
        # if isinstance(path, dict):
        #     for key, p in path.items(): 
        #         yield key, next(self.get_field_from_item(item, p))
        # elif isinstance(path, str):
        #     entity_list = path.split('->')
        #     if len(entity_list) == 1:
        #         entity_list = entity_list[0].split('/')


    # def create_json_data_body(self, schema, **kwargs):
    #     #Это оказалось несколько сложнее чем я думал.... Нужно обмозговать как следует
    #     body_schema = self.json_load(f'schemas/{schema.lower()}.json')
    #     data_body = []
    #     try:
    #         item_type = body_schema["#type"].split('.')[0].split(':')[-1]
    #         if kwargs.get('query'):
    #             query = kwargs.get('query').update({"#type": body_schema['#type']})
    #         else:
    #             query = {"#type": body_schema['#type']}
    #         items = DB.get_items(item_type, query)
    #     except Exception as e: #нужно будет позже понять какие ошибки тут отлавливать
    #         basic_logger.exception(f'{e}', exc_info=True)
    #         basic_logger.debug(f'Body schema: {body_schema}. Query: {query}')
    #         return {}
    #     res = []
    #     for item in items:
    #         if sys.getsizeof(res) > MAX_DATA_SIZE:
    #             data_body.append(res)
    #             res = []
    #         #res.append((temp, item['#value']['Ref']))
    #     if not data_body:
    #         data_body.append(res)
    #     return data_body



    
    def migrate(self, schema, path, **kwargs):
        data = self.create_json_data_body(schema=schema, **kwargs)
        for d, ref in data:
            response = self.request(method='POST', path=path, data=d)
            if response.status_code is None:
                basic_logger.warning(f'Something goes wrong with {schema} migration.')


    def db_migrate(self, **kwargs):
        for entity, path in REQUEST_LINKS.items():
            basic_logger.info(f'Start migrating {entity} entity.')
            self.migrate(schema=entity, path=path, **kwargs)
            basic_logger.info(f'{entity} entity succesefully migrated.')


if __name__ =='__main__':

    basic_logger.info('Start logging session.')
    mywh = MyWHAPI(USERNAME, PSWD)
    #response = mywh.get_products(params={'pathName':['Товары интернет-магазинов/Настольные игры/CrowdGames']})
    #mywh.json_to_excel(json.dumps(response), columns=['id', 'name', 'article'])
    basic_logger.info('Finish')