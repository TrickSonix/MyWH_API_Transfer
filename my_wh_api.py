from ftplib import error_perm
import requests
import os
import sys
import time
from openpyxl import load_workbook, Workbook
import datetime as dt
from setup import COLLECTIONS_COMPARE, REQUEST_LINKS, PSWD, USERNAME, HEADERS, CUSTOMERORDER_COPIED_FIELDS, MAIN_URL, MAX_DATA_SIZE, REQUESTS_SECONDS_RESTRICTION, REQUESTS_LIMIT, TIMEOUT_TIME, MAX_ITEM_COUNT
import json
import functools
import operator
from logger import setup_logger
from logging import DEBUG
from ratelimit import limits
from ratelimit.exception import RateLimitException
from db_server import CrowdGamesDB
from utils import JSONPermute


"""Нужно будет протестировать изменения в коде и дальше уже пилить заполнение тела для разных запросов и распарса json'a из базы.
    Позже пойму где еще нужно добавить логгирование.
    Где-то еще можно добавить асинхронщины... Скорее всего в GET-запросах. Но если добавлять ее, то надо учитывать ограничения API моего склада (см документацию)
    
    Поскольку я таки пришел к тому что сделал БД для переноса, нужно будет туда напихать все справочники из моего склада, чтобы не делать запрос к апи каждый раз
    
    demand.json надо пофиксить, а скорее всего вообще все переделать."""

basic_logger = setup_logger('basic_logger', 'logs/my_wh_api.log', DEBUG)
json_logger = setup_logger('json_logger', f"logs/{dt.datetime.now().strftime('%Y-%m-%d')}_json.log", DEBUG)
request_logger = setup_logger('request_logger', f"logs/{dt.datetime.now().strftime('%Y-%m-%d')}_request.log", DEBUG)


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

    def __init__(self, username, pswd, db: CrowdGamesDB, headers=HEADERS, max_data_size=MAX_DATA_SIZE, max_item_count=MAX_ITEM_COUNT, timeout_time=TIMEOUT_TIME) -> None:
        self.session = requests.Session()
        self.session.auth = (username, pswd)
        self.session.headers.update(headers)
        self.db = db
        self.max_data_size = max_data_size
        self.max_item_count = max_item_count
        self.timeout_time = timeout_time
            
    @staticmethod
    def list_flatten(list_2):
        return functools.reduce(operator.iconcat, list_2, [])
        
    @staticmethod
    def to_query(params):    
        """аргумент params передается в виде словаря, в котором все значения должны быть списками со значениями включающие оператор сравнения
        например: {'id=': 'lolkek'}"""
        return ';'.join([f'{key}{item}' for key, item in zip(MyWHAPI.list_flatten([[key]*len(value) for key, value in params.items()]), MyWHAPI.list_flatten([x for x in params.values()]))])
    
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
        if isinstance(json_data, dict) or isinstance(json_data, list):
            return json_data
        if os.path.exists(json_data):
            with open(json_data, 'r', encoding='utf-8') as f:
                try:
                    return json.load(f)
                except json.decoder.JSONDecodeError as e:
                    json_logger.error(e)
                    json_logger.debug(f'JSON_data: \n{json_data}')
                    return {}
        else:
            try:
                return json.loads(json_data)
            except json.decoder.JSONDecodeError as e:
                json_logger.error(e)
                json_logger.debug(f'JSON_data: \n{json_data}')
                return {}

    @staticmethod
    def dump_json(data, file_path=None, **kwargs) -> str:
        try:
            if file_path:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, **kwargs)
                return 'Success'
            else:
                return json.dumps(data, **kwargs)
        except json.JSONDecodeError as e:
            json_logger.exception(f'{e}')
            json_logger.info(f'Decoding error, while trying to jsonize {data}')
            return ''

    def _check_data_size(self, data, appended_data, restriction):
        return sys.getsizeof(json.dumps(data+[appended_data])) >= restriction

    @sleep_and_retry
    @limits(calls=REQUESTS_LIMIT, period=REQUESTS_SECONDS_RESTRICTION)
    def request(self, method : str, path : str,  **kwargs) -> requests.Response:    
        try:
            with self.session as session:
                response = session.request(method=method, url=''.join([MAIN_URL, path or '']), timeout=self.timeout_time, **kwargs)
                request_logger.info(f'URL: {response.url}. Method: {method}. Status code {response.status_code}')
                response.raise_for_status()
                return response
        except requests.exceptions.HTTPError as e:
            request_logger.debug(f'Not transfered or not fully transfered request body: \n{self.dump_json(self.json_load(kwargs.get("data")), indent=4)}')
            request_logger.exception(f'{e}', exc_info=True)
            request_logger.debug(f'Response text: !{response.text}!')
            return response
        except requests.exceptions.Timeout as e:
            request_logger.debug(f'Timeout Error')
            request_logger.exception(f'{e}', exc_info=True)
            request_logger.debug(f'Not transfered request body: \n{self.dump_json(self.json_load(kwargs.get("data")), indent=4)}')
            return response
        except requests.exceptions.ConnectionError as e:
            request_logger.debug(f'Connection Error. Try to not CRY.')
            request_logger.exception(f'{e}', exc_info=True)
            request_logger.debug(f'Not transfered request body: \n{self.dump_json(self.json_load(kwargs.get("data")), indent=4)}')
            return requests.Response()

    # def get_products(self, params={},  **kwargs):
    #     """аргумент params передается в виде словаря, в котором все значения должны быть списками"""
    #     db = self.json_load(PRODUCTS_DB) 
    #     result = []
    #     missed = {}
    #     if not db:
    #         response = self.request(method='GET', path=REQUEST_LINKS['PRODUCT'], params={'filter': self.to_query(params)})
    #         db = self.json_load(response.json()).get('rows')
    #     for key, value in params.items():
    
    #         items = list(filter(lambda x: x.get(key) in value, db))
    #         result.extend(items)
    #         missed[key] = [x for x in value if x not in [x.get(key) for x in items]]
        
    #     if self.list_flatten(missed.values()):
    #         new_response = self.request(method='GET', path=REQUEST_LINKS['PRODUCT'], params={'filter': self.to_query(missed)})
    #         new = self.json_load(new_response.json()).get('rows')
    #         db.extend(new)
    #         result.extend(new)
    #     with open(PRODUCTS_DB, 'w') as f:
    #         json.dump(db, f, indent=4)
    #     return result
  
    def create_json_data_body(self, body_schema, schema_name, **kwargs):
        try:
            item_type = body_schema["#type"].split('.')[0].split(':')[-1]
            if kwargs.get('query'):
                new_query = kwargs.get('query')
                new_query.update({"#type": body_schema['#type']})
            else:
                new_query = {"#type": body_schema['#type']}
            if item_type == 'DocumentObject' and kwargs.get('only_posted_documents'):
                new_query.update({"#value.Posted": True})
            items = self.db.get_items(item_type, new_query)
        except Exception as e: #нужно будет позже понять какие ошибки тут отлавливать
            new_query = kwargs.get('query')
            basic_logger.exception(f'{e}', exc_info=True)
            basic_logger.debug(f'Body schema: {body_schema}. Query: {new_query}')
            return []
        res = []
        for item in items:
            perm = JSONPermute(item, schema_name.lower(), self.db)
            permuted_data = perm.permute()
            item_ref = item['#value']['Ref']
            if self._check_data_size(res, (item_ref, permuted_data), self.max_data_size) or len(res)+1 > self.max_item_count:
                yield res
                res = []
            res.append((item_ref, permuted_data))
            ref_type = item['#type']
            json_logger.debug(f'{ref_type} item with ref {item_ref} was permuted into: \n{self.dump_json(permuted_data, indent=4)}')
        
        yield res
    
    def update_db_metas(self, zipped_data):
        error_count = 0
        for ref, resp in zipped_data:
            if resp.get('errors'):
                err = '\n'.join([x['error'] for x in resp['errors']])
                basic_logger.error(f'Error occured in response with {ref} item: \n{err}')
                error_count += 1
                continue
            meta = resp.get('meta')
            
            if meta:
                update = self.db.update_item(ref, meta)
                if update:
                    basic_logger.info(f'Success in update {ref} item')
                else:
                    basic_logger.info(f'Fail in update to DB {ref} item')
                    error_count +=1
            else:
                basic_logger.info(f'Fail in update to DB {ref} item')
                error_count +=1
            if resp.get('accounts'):
                for acc in resp['accounts'].get('rows'):
                    acc_meta = acc['meta']
                    acc_ref = acc['bankLocation']
                    update_acc = self.db.update_item(acc_ref, acc_meta)
                    if update_acc:
                        basic_logger.info(f'Success in update {acc_ref} item')
                    else:
                        basic_logger.info(f'Fail in update to DB {acc_ref} item')
                        error_count +=1
        return error_count
    
    def restore_meta(self, path, data: list[dict]):
        start_time = dt.datetime.now()
        href = path
        while True:
            all_entitys = self.request('GET', path=href)
            response_data = all_entitys.json()['rows']
            refs_list = [x.get('description', "missed").split(' ')[-1] for x in data]
            missed_meta = []
            for entity in response_data:
                cur_ref = entity.get('description', "00000000-0000-0000-0000-000000000000").split(' ')[-1]
                if cur_ref in refs_list:
                    missed_meta.append((cur_ref, entity['meta']))
                    basic_logger.info(f'Restored meta for {cur_ref}')
            if response_data.get('nextHref'):
                href = '/'.join(['', *response_data['meta'].get('nextHref').split('/')[6:]])
            else:
                return missed_meta
            if dt.datetime.now() - start_time > dt.timedelta(minutes=60):
                basic_logger.warning(f'Failed to restore meta.')
                return [({}, {})]

    
    def migrate(self, schema, path, **kwargs):
        if schema.lower() == 'product':
            query = {"#value.ТипНоменклатуры": "Товар"}
        elif schema.lower() == 'service':
            query = {"#value.ТипНоменклатуры": "Услуга"}
        else:
            query = {}
        if kwargs.get('skip_existing'):
            query.update({"#mywh": {"$exists": False}})
        body_schema = self.json_load(f'schemas/{schema.lower()}.json')
        data = self.create_json_data_body(body_schema=body_schema, query=query, schema_name=schema, **kwargs)
        error_count = 0
        for data_part in data:
            zipped_meta = None
            if schema.lower() == 'organization' or schema.lower() == 'counterparty':
                params = {'expand': 'accounts'}
            else:
                params = {}

            response = self.request(method='POST', path=path, data=self.dump_json([x[1] for x in data_part]), params=params)

            if response.status_code != 200 or response.status_code is None:
                basic_logger.warning(f'Something goes wrong with {schema} migration. Get {response.status_code} from server.')
                if response.status_code == 504 or response.status_code is None:
                    basic_logger.info(f'Trying to restore meta.')
                    zipped_meta = self.restore_meta(path, data_part)
                    error_count += 1
                    if not zipped_meta[0][0]:
                        error_count +=1
                        basic_logger.critical(f'Critical SHIT happened with {schema} migration. Trying to do ctrl+z....')
                        success = self.ctrl_z(path, params={"limit": '500'})
                        if success:
                            print('Suppose to check logs.')
                            input('Ctrl+z done, go check logs.')
                        else:
                            input('Critical error, need to check logs.')
                else:
                    response_json = response.json()
                error_count += 1
            else:
                response_json = response.json()
            if not zipped_meta:
                zipped_meta = zip([x[0] for x in data_part], response_json)
            error_count += self.update_db_metas(zipped_meta)
        return error_count
            
    # def make_documents_bonds(self, data, entity, **kwargs):
    #   судя по всему эта хуйня не нужна
    #     path = REQUEST_LINKS.get(entity)
    #     if not path:
    #         return False
    #     self.request('POST', path=path, data=data, **kwargs) #пока что хуйня, смотри выше как формируются данные
        

    def db_migrate(self, **kwargs):
        for entity, path in REQUEST_LINKS.items():
            basic_logger.info(f'Start migrating {entity} entity.')
            errors = self.migrate(schema=entity, path=path, **kwargs)
            if errors == 0:
                basic_logger.info(f'{entity} entity succesefully migrated.')
            else:
                basic_logger.info(f'{entity} was migrated with {errors} errors.')
            input('Press any key to continue')

    def ctrl_z(self, path, mode='delete', **kwargs):
        """Вот эту вот функцию надо будет переписать как-то покрасивее.
        query и params - взаимоисключающие переменные"""
        if kwargs.get('query') and kwargs.get('params'):
            basic_logger.warning(f'ctrl_z gets mutually exclusive arguments param and query.')
            return False
        if kwargs.get('query'):
            query = self.to_query(kwargs['query'])
        else:
            query = self.to_query({})
        params = {}
        if kwargs.get('params'):
            params = kwargs['params']
        else:
            params = {'filter': query}
        href = path
        while True:
            data_to_delete = self.request('GET', href, params=params)
            if data_to_delete.status_code == 200:
                try:
                    response_json = data_to_delete.json()
                except requests.JSONDecodeError as e:
                    basic_logger.exception(f'{e}', exc_info=True)
                    return False
                errors = 0
                metas = [{'meta': x['meta']} for x in response_json.get('rows', [])]
                if metas:
                    response = self.request('POST', '/'.join([path, mode]), data=self.dump_json(metas))
                    if response.status_code != 200:
                        basic_logger.info(f'In ctrl_z server give {response.status_code}')
                        errors += 1
                    else:
                        if response_json['meta'].get('nextHref'):
                            continue
                        else:
                            basic_logger.info("\n".join([x.get("info", 'No info') for x in response.json()]))
                            basic_logger.info(f'Удаление завершено с  {errors} ошибками.')
                            return True
                else:
                    basic_logger.warning(f'Ctrl+z dont find any data in MyWH to delete.')
                    basic_logger.debug(f'Request params: {params}')
                    return False


if __name__ =='__main__':

    basic_logger.info('Start logging session.')
    database = CrowdGamesDB()
    mywh = MyWHAPI(USERNAME, PSWD, database)
    #response = mywh.get_products(params={'pathName':['Товары интернет-магазинов/Настольные игры/CrowdGames']})
    #mywh.json_to_excel(json.dumps(response), columns=['id', 'name', 'article'])
    mywh.db_migrate(only_posted_documents=True)
    #mywh.ctrl_z('/entity/demand', params={"limit": '500'})
    basic_logger.info('Finish')