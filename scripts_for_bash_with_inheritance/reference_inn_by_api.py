import re
import sys
import sqlite3
import contextlib
import numpy as np
import validate_inn
import pandas as pd
from __init__ import *
from pathlib import Path
from fuzzywuzzy import fuzz
from pandas import DataFrame
from typing import List, Tuple
from sqlite3 import Connection
from cache_inn import GetINNApi
from multiprocessing import Pool
from pandas.io.parsers import TextFileReader
from deep_translator import GoogleTranslator


def replace_forms_organizations(company_name: str) -> str:
    """
    Deleting organization forms for the accuracy of determining confidence_rate.
    """
    for elem in replaced_words:
        company_name: str = company_name.replace(elem, "")
    return company_name.translate({ord(c): "" for c in '"'}).strip()


def compare_different_fuzz(company_name: str, translated: str, fuzz_company_name: int, data: dict) -> int:
    """
    Comparing the maximum value of the two confidence_rate.
    """
    company_name_en: str = GoogleTranslator(source='ru', target='en').translate(company_name)
    data["company_name_unified_en"] = company_name_en
    fuzz_company_name_two: int = fuzz.partial_ratio(company_name_en.upper(), translated.upper())
    return max(fuzz_company_name, fuzz_company_name_two)


def get_company_name_by_inn(provider: GetINNApi, data: dict, inn: list, sentence: str,
                            translated: str = None, cache_name_inn: GetINNApi = None) -> None:
    """
    We get a unified company name from the sentence itself for the found INN. And then we are looking for a company
    on the website https://www.rusprofile.ru/.
    """
    if not translated:
        translated: str = GoogleTranslator(source='en', target='ru').translate(sentence)
    data['is_inn_found_auto'] = True
    data['company_name_rus'] = translated
    if inn == 'empty':
        inn, sentence = get_company_name_by_sentence(cache_name_inn, sentence, is_english=True)
    inn, company_name = provider.get_inn(inn)
    company_name: str = re.sub(" +", " ", company_name)
    data["company_inn"] = inn
    data["company_name_unified"] = company_name
    company_name = replace_forms_organizations(company_name)
    fuzz_company_name = fuzz.partial_ratio(company_name.upper(), translated.upper())
    fuzz_company_name = compare_different_fuzz(company_name, translated, fuzz_company_name, data)
    data['confidence_rate'] = fuzz_company_name


def get_company_name_by_sentence(provider: GetINNApi, sentence: str, is_english: bool = False) -> Tuple[str, str]:
    """
    We send the sentence to the Yandex search engine (first we pre-process: translate it into Russian) by the link
    https://xmlriver.com/search_yandex/xml?user=6390&key=e3b3ac2908b2a9e729f1671218c85e12cfe643b0&query=<value> INN
    """
    sentence = sentence.translate({ord(c): " " for c in r",'!@#$%^&*()[]{};?\|~-=_+"})
    if is_english:
        inn, translated = provider.get_inn_from_value(sentence)
        return inn, translated
    for quote in replaced_quotes:
        sentence: str = sentence.replace(quote, '"')
    # sentence = re.sub(r'\s*("|\')\s*', r'\1', sentence)
    sentence = re.sub(r'(")\s*(")', r'\1\2', sentence)
    sentence = re.sub(r'(")\1+', r'\1', sentence)
    sentence = re.sub(" +", " ", sentence)
    translated: str = GoogleTranslator(source='en', target='ru').translate(sentence)
    inn, translated = provider.get_inn_from_value(translated)
    return inn, translated


def find_international_company(cache_inn: GetINNApi, sentence: str, data: dict) -> None:
    """
    Looking for international companies.
    """
    for country_and_city in countries_and_cities:
        if re.findall(country_and_city, sentence.upper()):
            data["is_company_name_international"] = True
            get_company_name_by_inn(cache_inn, data, inn=[], sentence=sentence)
    data["is_company_name_international"] = False


def get_inn_from_row(sentence: str, data: dict) -> None:
    """
    Full processing of the sentence, including 1). inn search by offer -> company search by inn,
    2). inn search in yandex by request -> company search by inn.
    """
    sentence = '16700833183; OOO "SADOVAYA TEKHNIKA IINSTRUMENTY"BOLSHAYA POCHTOVAYA STR 40,BUILDING 1, THE 3.FLOOR, ROOM 7A**'
    list_inn: list = []
    inn: list = re.findall(r"\d+", sentence)
    cache_inn: GetINNApi = GetINNApi("inn_and_uni_company_name", conn)
    for item_inn in inn:
        with contextlib.suppress(Exception):
            item_inn2 = validate_inn.validate(item_inn)
            list_inn.append(item_inn2)
    find_international_company(cache_inn, sentence, data)
    if list_inn:
        get_company_name_by_inn(cache_inn, data, inn=list_inn[0], sentence=sentence)
    else:
        cache_name_inn: GetINNApi = GetINNApi("company_name_and_inn", conn)
        inn, translated = get_company_name_by_sentence(cache_name_inn, sentence)
        get_company_name_by_inn(cache_inn, data, inn, sentence, translated=translated, cache_name_inn=cache_name_inn)


def write_to_json(index: int, data: dict) -> None:
    """
    Writing data to json.
    """
    logger.info(f'{index} data is {data}')
    logger_stream.info(f'{index} data is {data}')
    basename: str = os.path.basename(os.path.abspath(sys.argv[1]))
    output_file_path: str = os.path.join(sys.argv[2], f'{basename}_{index}.json')
    with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def parse_data(index: int, data: dict) -> None:
    """
    Processing each row.
    """
    for key, sentence in data.items():
        try:
            if key == 'company_name':
                get_inn_from_row(sentence, data)
        except Exception as ex:
            logger.info(f'Error in inn {ex}: {index} data is {data}')
            logger_stream.info(f'Error in inn {ex}: {index} data is {data}')
    write_to_json(index, data)


def create_file_for_cache() -> str:
    """
    Creating a file for recording INN caches and sentence.
    """
    path_cache: str = f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/cache_inn/cache_inn.db"
    fle: Path = Path(path_cache)
    if not os.path.exists(os.path.dirname(fle)):
        os.makedirs(os.path.dirname(fle))
    fle.touch(exist_ok=True)
    return path_cache


def convert_csv_to_dict(filename: str) -> List[dict]:
    """
    Csv data representation in json.
    """
    dataframe: TextFileReader | DataFrame = pd.read_csv(filename)
    dataframe.columns = ['company_name']
    dataframe = dataframe.drop_duplicates(subset='company_name', keep="first")
    dataframe = dataframe.replace({np.nan: None})
    dataframe['company_name_rus'] = None
    dataframe['company_name_unified_en'] = None
    dataframe['company_inn'] = None
    dataframe['company_name_unified'] = None
    dataframe['is_inn_found_auto'] = None
    dataframe["is_company_name_international"] = None
    dataframe['confidence_rate'] = None
    return dataframe.to_dict('records')


if __name__ == "__main__":
    procs: list = []
    path: str = create_file_for_cache()
    conn: Connection = sqlite3.connect(path)
    parsed_data: List[dict] = convert_csv_to_dict(os.path.abspath(sys.argv[1]))
    with Pool(processes=worker_count) as pool:
        for i, dict_data in enumerate(parsed_data, 2):
            proc = pool.apply_async(parse_data, (i, dict_data))
            procs.append(proc)
        [proc.get() for proc in procs]
    conn.close()
