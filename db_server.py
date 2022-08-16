import collections
import pip
import pymongo as pm
import os
import json
from pymongo import MongoClient, InsertOne
from requests import JSONDecodeError
from logger import setup_logger
from logging import DEBUG, INFO
from utils import is_guid
from setup import COLLECTIONS_COMPARE



db_logger = setup_logger('db_logger', 'logs/MongoDB_log.log', level=INFO)




def get_readed_files(log_file='logs/MongoDB_log.log'):
    with open(log_file, 'r') as f:
        for line in f.readlines():
            if 'Current working directory:' in line:
                w_dir = line.split(' ')[-1].strip().split('\\')
            if '.json was readed' in line:
                yield os.path.join(*w_dir, line.split(' ')[-3])


class CrowdGamesDB:

    def __init__(self, db='CrowdGamesDB') -> None:
        self.client = MongoClient('localhost', 27017)
        self.db = db

    def import_db(self, db_path = 'D:\Никита\Работа\CrowdGames\JSON_DB', stop_by_error=False, skip_exists_ref=False, skip_by_logs=False, skip_directorys=False, read_directorys=False):
        #Когда будешь импортировать актуал - СОЗДАВАЙ СРАЗУ ИНДЕКС НА Ref, чтобы избежать дублирования документов.
        assert not (skip_directorys and read_directorys)

        db_logger.info(f'Starting import DB.')
        db_logger.info(f'Current path of copied DB: {db_path}')
        error_count = 0
        refs_list = []
        if skip_by_logs:
            readed_files = list(get_readed_files())
        else:
            readed_files = []
        for directory in [x for x in os.listdir(db_path) if (x in (read_directorys or os.listdir(db_path)) and x not in (skip_directorys or []))]:
            if '.' not in directory:
                
                db_logger.info(f'Current working directory: {directory}')
                with self.client.start_session() as session:
                    collection = self.client[self.db][directory]
                    #collection.create_index({"#value.Ref": "text"}, {'unique': True})
                    for subdirectory in os.listdir(f'{db_path}\{directory}'):
                        db_logger.info(f'Current working directory: {directory}\{subdirectory}')
                        data_to_insert = {subdirectory: []}
                        for file in [x for x in os.listdir(f'{os.path.join(db_path, directory, subdirectory)}') if os.path.join(directory, subdirectory, x) not in readed_files]:
                            try:
                                with open(f'{os.path.join(db_path, directory, subdirectory, file)}', 'r', encoding='ansi') as f:
                                    data = json.load(f)
                                    db_logger.info(f'{os.path.join(db_path, directory, subdirectory, file)} was readed')
                                    if skip_exists_ref:
                                        if (data["#value"].get('Ref') not in refs_list) and not next(self.find_by_guid(data["#value"].get('Ref'))):
                                            refs_list.append(data["#value"].get('Ref', "Undiefined"))
                                            data_to_insert[subdirectory].append(InsertOne(data))
                                    else:
                                        data_to_insert[subdirectory].append(InsertOne(data))
                            except (OSError, JSONDecodeError, KeyError) as e:
                                db_logger.exception(f'{e}', exc_info=True)
                                if stop_by_error:
                                    break
                                else:
                                    error_count += 1
                                    continue

                        if data_to_insert[subdirectory]:
                            try:
                                collection.bulk_write(data_to_insert[subdirectory], session=session)
                            except (pm.errors.BulkWriteError, pm.errors.ConnectionFailure, pm.errors.OperationFailure, pm.errors.DocumentTooLarge) as e:
                                db_logger.exception(f'{e}', exc_info=True)
                                if stop_by_error:
                                    break
                                else:
                                    error_count += 1
                                    continue
                            db_logger.info(f'{len(data_to_insert[subdirectory])} insertions was maded.')
                        else:
                            continue
        else:
            db_logger.info(f'Database import was successeful. With {error_count} errors.')

    def get_items(self, collection, query):
        with self.client.start_session() as session:
            if self.client[self.db][COLLECTIONS_COMPARE.get(collection, collection)].count_documents(filter=query, session=session) == 0:
                yield {}
            else:
                cursor = self.client[self.db][COLLECTIONS_COMPARE.get(collection, collection)].find(query, session=session, no_cursor_timeout=True)
                for result in cursor:
                    yield result

    def find_by_guid(self, guid):
        if is_guid(guid):
            count = 0
            with self.client.start_session() as session:
                for collection in COLLECTIONS_COMPARE.values():
                    count += self.client[self.db][collection].count_documents(filter={"#value.Ref": guid}, session=session)
                    if count > 0:
                        cursor = self.client[self.db][collection].find_one({"#value.Ref": guid}, session=session)
                        yield cursor
                if count == 0:
                    yield {}
        else:
            yield {}

    def update_item(self, guid, data):
        data_to_update = next(self.find_by_guid(guid))
        try:
            with self.client.start_session() as session:
                collection = COLLECTIONS_COMPARE.get(data_to_update["#type"].split('.')[0].split(':')[-1])
                updated = self.client[self.db][collection].update_one({'#type':data_to_update['#type'], "#value.Ref": data_to_update['#value']['Ref']}, {'$set':{'#mywh.meta': data}}, session=session)
                db_logger.info(f'In {collection} in document ref {guid} was maded {updated.modified_count} modifications.')
                db_logger.debug(f'Updating metadata: \n{json.dumps(data, indent=4, ensure_ascii=False)}')
                return True
        except Exception as e:
            db_logger.info(f'Something goes wrong.')
            db_logger.exception(f'{e}', exc_info=True)
            db_logger.debug(f'Passed data: \n{json.dumps(data, indent=4, ensure_ascii=False)}')
            return False

    def delete_one_item(self, collection, query):
        collection = COLLECTIONS_COMPARE.get(collection) or collection
        with self.client.start_session() as session:
            try:
                result = self.client[self.db][collection].delete_one(query, session=session)
                if result.deleted_count == 1:
                    db_logger.info(f'Item with {query} was deleted')
                else:
                    db_logger.info(f'Nothing deleted with {query}')
            except Exception as e:
                db_logger.info(f'Something goes wrong with {query} delete from {collection}.')
                db_logger.exception(f'{e}', exc_info=True)

    def delete_all_mywh(self, collection=None, query=None):
        db_logger.info(f'Trying to delete mywh data from {query}')
        with self.client.start_session() as session:
            if not collection:
                collection = COLLECTIONS_COMPARE.values()
            for cols in collection:
                try:
                    updated = self.client[self.db][cols].update_many(query or {}, {'$unset': {'#mywh': {"$exists": True}}}, session=session)
                    db_logger.info(f'In {cols} was maded {updated.modified_count} modifications.')
                except Exception as e:
                    db_logger.info(f'Something goes wrong.')
                    db_logger.exception(f'{e}', exc_info=True)

    def drop_duplicates_by_ref(self, collection, pipeline):
        collection = COLLECTIONS_COMPARE.get(collection, collection)
        with self.client.start_session() as session:
            db_coll = self.client[self.db][collection]
            refs_to_delete = list(db_coll.aggregate(pipeline=pipeline, session=session))
            for ref in refs_to_delete:
                try:
                    result = db_coll.delete_one({"#value.Ref": ref["#value"].get("Ref")})
                    if result.deleted_count == 1:
                        db_logger.info(f'Item with {ref["#value"].get("Ref")} was deleted')
                    else:
                        db_logger.info(f'Nothing deleted with {ref["#value"].get("Ref")}')
                except Exception as e:
                    db_logger.info(f'Something goes wrong with {ref["#value"].get("Ref")} delete from {collection}.')
                    db_logger.exception(f'{e}', exc_info=True)





if __name__ == '__main__':
    inst = CrowdGamesDB('CrowdGamesActual')
    #inst.import_db(skip_exists_ref=True, read_directorys=['Справочники'], db_path='D:\Никита\Работа\CrowdGames\Kek')
    #inst.delete_all_mywh(query={"#type": {"$in":["jcfg:DocumentObject.ПриходныйКассовыйОрдер", "jcfg:DocumentObject.СписаниеБезналичныхДенежныхСредств", "jcfg:DocumentObject.ПоступлениеБезналичныхДенежныхСредств", "jcfg:DocumentObject.РасходныйКассовыйОрдер"]}}) #{"#type": {"$in":["jcfg:DocumentObject.РеализацияТоваровУслуг", "jcfg:DocumentObject.ПоступлениеБезналичныхДенежныхСредств", "jcfg:DocumentObject.СписаниеНедостачТоваров", "jcfg:DocumentObject.СписаниеБезналичныхДенежныхСредств", "jcfg:DocumentObject.СписаниеБезналичныхДенежныхСредств"]}})
   # inst.drop_duplicates_by_ref('ПланыВидовХарактеристик', duplicates_pipeline)
    #inst.update_item("7a6c6eee-fb57-11e7-80d6-00505691ab3c", 'loshad')
    print('Success')

