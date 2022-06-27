import logging
import pyjsonviewer
import os

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

def view_json_from_log(filename): #Рот ебал этого модуля для отображения json, нихуя не понял почему он не работает...
    pass

        
if __name__ == '__main__':
    view_json_from_log('logs/123.log')