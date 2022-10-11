import contextlib
import json
import logging
import os
import re
import sqlite3
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from cache_inn import GetINNApi
import validate_inn
from deep_translator import GoogleTranslator
from fuzzywuzzy import fuzz
from multiprocessing import Pool

worker_count = 8

if not os.path.exists(f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging"):
    os.mkdir(f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging")

json_handler = logging.FileHandler(filename=f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging/{os.path.basename(__file__)}.log")
logger = logging.getLogger("file_handler")
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(json_handler)
logger.setLevel(logging.INFO)

console_out = logging.StreamHandler()
logger_stream = logging.getLogger("stream")
if logger_stream.hasHandlers():
    logger_stream.handlers.clear()
logger_stream.addHandler(console_out)
logger_stream.setLevel(logging.INFO)

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]


df = pd.read_csv(input_file_path)
df.columns = ['company_name']
df = df.drop_duplicates(subset='company_name', keep="first")
df = df.replace({np.nan: None})
df['company_name_rus'] = None
df['company_inn'] = None
df['company_name_unified'] = None
df['is_inn_found_auto'] = None
df['confidence_rate'] = None
parsed_data = df.to_dict('records')


def add_values_in_dict(provider, dict_data, inn=None, value=None, company_name_rus=None):
    translated = GoogleTranslator(source='en', target='ru').translate(company_name_rus)
    if value:
        api_inn, api_name_inn = provider.get_inn_from_value(translated)
        return api_inn, api_name_inn, translated
    inn, api_name_inn = provider.get_inn(inn)
    translated = re.sub(" +", " ", translated)
    api_name_inn = re.sub(" +", " ", api_name_inn)
    translated = translated.translate({ord(c): " " for c in ",'!@#$%^&*()[]{};<>?\|`~-=_+"})
    api_name_inn = api_name_inn.translate({ord(c): "" for c in ",'!@#$%^&*()[]{};<>?\|`~-=_+"})
    dict_data['company_name_rus'] = translated
    dict_data["company_inn"] = inn
    dict_data["company_name_unified"] = api_name_inn
    dict_data['is_inn_found_auto'] = True
    dict_data['confidence_rate'] = fuzz.partial_ratio(api_name_inn.upper(), translated.upper())


def get_inn_from_str(value, dict_data):
    inn = re.findall(r"\d+", value)
    cache_inn = GetINNApi("inn_and_uni_company_name", conn)
    list_inn = []
    for item_inn in inn:
        with contextlib.suppress(Exception):
            item_inn2 = validate_inn.validate(item_inn)
            list_inn.append(item_inn2)
    if list_inn:
        add_values_in_dict(cache_inn, dict_data, inn=list_inn[0], company_name_rus=value)
    else:
        cache_name_inn = GetINNApi("company_name_and_inn", conn)
        api_inn, api_name_inn, translated = add_values_in_dict(cache_name_inn, dict_data, value=value, company_name_rus=value)
        add_values_in_dict(cache_inn, dict_data, inn=api_inn, company_name_rus=translated)


def parse_data(i, dict_data):
    for key, value in dict_data.items():
        with contextlib.suppress(Exception):
            if key == 'company_name':
                get_inn_from_str(value, dict_data)

    logger.info(f'{i} data is {dict_data}')
    logger_stream.info(f'{i} data is {dict_data}')
    basename = os.path.basename(input_file_path)
    output_file_path = os.path.join(output_folder, f'{basename}_{i}.json')
    with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
        json.dump(dict_data, f, ensure_ascii=False, indent=4)


procs = []
path = f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/cache_inn/cache_inn.db"
fle = Path(path)
if not os.path.exists(os.path.dirname(fle)):
    os.makedirs(os.path.dirname(fle))
fle.touch(exist_ok=True)
conn = sqlite3.connect(path)
with Pool(processes=worker_count) as pool:
    for i, dict_data in enumerate(parsed_data, 2):
        proc = pool.apply_async(parse_data, (i, dict_data))
        procs.append(proc)

    results = [proc.get() for proc in procs]

conn.close()



