import os
import json
import pyjsonviewer as pjv

if __name__ == '__main__':
    file = input('Enter filename: ')
    with open(f'../JSON_DB/Документы/Документы.ВводОстатков/{file}', 'r', encoding='ansi') as f:
        a = json.load(f)
    pjv.view_data(json_data=a)
    input()

    print(f'"{a.split(".")}"')