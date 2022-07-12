import pymongo as pm
import os
import json

from pymongo import MongoClient, InsertOne
from requests import JSONDecodeError
from logger import setup_logger
from logging import DEBUG


db_logger = setup_logger('db_logger', 'logs/MongoDB_log.log', level=DEBUG)

client = MongoClient('localhost', 27017)
db = client['CrowdGamesDB']

def import_db(db_path = 'D:\Никита\Работа\CrowdGames\JSON_DB', stop_by_error=False):
    
    db_logger.info(f'Current db_path: {db_path}')
    error_count = 0
    for directory in os.lisdir(db_path):
        if '.' not in directory:
            
            db_logger.info(f'Current working directory: {directory}')
            collection = db[directory]
            for subdirectory in os.listdir(f'{db_path}\{directory}'):
                db_logger.info(f'Current working directory: {directory}\{subdirectory}')
                data_to_insert = {subdirectory: []}
                for file in os.listdir(f'{db_path}\{directory}\{subdirectory}'):
                    try:
                        with open(f'{os.path.join(db_path, directory, subdirectory, file)}', 'r', encoding='ansi') as f:
                            data = json.loads(f)
                            data_to_insert[subdirectory].append(InsertOne({subdirectory: data}))
                            db_logger.info(f'{file.name} was readed')
                    except (OSError, JSONDecodeError) as e:
                        db_logger.exception(f'{e}', exc_info=True)
                        if stop_by_error:
                            break
                        else:
                            error_count += 1
                            continue

                try:
                    collection.bulk_write(data_to_insert)
                except (pm.errors.BulkWriteError, pm.errors.ConnectionFailure) as e:
                    db_logger.exception(f'{e}', exc_info=True)
                    if stop_by_error:
                        break
                    else:
                        error_count += 1
                        continue
                db_logger.info(f'{len(data_to_insert)} insertions was maded.')
    else:
        db_logger.info(f'Database import was successeful. With {error_count} errors.')






if __name__ == '__main__':
    
    pass

