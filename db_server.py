from unittest import result
import pymongo as pm
import os
import json

from pymongo import MongoClient, InsertOne
from requests import JSONDecodeError
from logger import setup_logger
from logging import DEBUG
from utils import is_guid
from setup import COLLECTIONS_COMPARE


db_logger = setup_logger('db_logger', 'logs/MongoDB_log.log', level=DEBUG)

client = MongoClient('localhost', 27017)


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

    def import_db(self, db_path = 'D:\Никита\Работа\CrowdGames\JSON_DB', stop_by_error=False):
        db_logger.info(f'Starting import DB.')
        db_logger.info(f'Current path of copied DB: {db_path}')
        error_count = 0
        readed_files = list(get_readed_files())
        for directory in os.listdir(db_path):
            if '.' not in directory:
                
                db_logger.info(f'Current working directory: {directory}')
                with self.client.start_session() as session:
                    collection = self.client[self.db][directory]
                    for subdirectory in os.listdir(f'{db_path}\{directory}'):
                        db_logger.info(f'Current working directory: {directory}\{subdirectory}')
                        data_to_insert = {subdirectory: []}
                        for file in [x for x in os.listdir(f'{os.path.join(db_path, directory, subdirectory)}') if os.path.join(directory, subdirectory, x) not in readed_files]:
                            try:
                                with open(f'{os.path.join(db_path, directory, subdirectory, file)}', 'r', encoding='ansi') as f:
                                    data = json.load(f)
                                    data_to_insert[subdirectory].append(InsertOne(data))
                                    db_logger.info(f'{os.path.join(db_path, directory, subdirectory, file)} was readed')
                            except (OSError, JSONDecodeError) as e:
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
            for result in self.client[self.db][COLLECTIONS_COMPARE.get(collection) or collection].find(query, session=session):
                yield result

    def find_by_guid(self, guid):
        if is_guid(guid):
            with self.client.start_session() as session:
                for collection in COLLECTIONS_COMPARE.values():
                    for result in self.client[self.db][collection].find({"#value.Ref": guid}, session=session):
                        yield result
        else:
            yield {}

    def update_item(self, guid, data):
        data_to_update = next(self.find_by_guid(guid))
        try:
            with self.client.start_session() as session:
                collection = COLLECTIONS_COMPARE.get(data_to_update["#type"].split('.')[0].split(':')[-1])
                updated = self.client[self.db][collection].update_one({'#type':data_to_update['#type'], "#value.Ref": data_to_update['#value']['Ref']}, {'$set':{'#mywh':{'meta': data}}}, session=session)
                db_logger.info(f'In {collection} in document ref {guid} was maded {updated.modified_count} modifications.')
                db_logger.debug(f'Updating metadata: {data}')
                return True
        except Exception as e:
            db_logger.info(f'Something goes wrong.')
            db_logger.exception(f'{e}', exc_info=True)
            return False

    def delete_all_mywh(self, query=None):

        with self.client.start_session() as session:
                for collection in COLLECTIONS_COMPARE.values():
                    try:
                        updated = self.client[self.db][collection].update_many(query or {}, {'$unset': {'#mywh': {"$exists": True}}}, session=session)
                        db_logger.info(f'In DB was maded {updated.modified_count} modifications. All mywh info deleted.')
                        return True
                    except Exception as e:
                        db_logger.info(f'Something goes wrong.')
                        db_logger.exception(f'{e}', exc_info=True)
                        return False

if __name__ == '__main__':
    inst = CrowdGamesDB()
    #inst.import_db()
    inst.delete_all_mywh({"$or": [{"#type": "jcfg:CatalogObject.Контрагенты"}, {"#type": "jcfg:CatalogObject.Организации"}, {"#type": "jcfg:CatalogObject.Кассы"}, {"#type": "jcfg:CatalogObject.БанковскиеСчетаОрганизаций"}, {"#type": "jcfg:CatalogObject.КлассификаторБанков"}, {"#type": "jcfg:CatalogObject.БанковскиеСчетаКонтрагентов"}]})

