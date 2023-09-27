import re
import sys
import time
import sqlite3
import contextlib
from csv import DictWriter

import numpy as np
import validate_inn
import pandas as pd
from __init__ import *
from pathlib import Path
from fuzzywuzzy import fuzz
from pandas import DataFrame
from sqlite3 import Connection
from typing import List, Tuple, Union
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from pandas.io.parsers import TextFileReader
from multiprocessing import Pool, Queue, Value
from clickhouse_connect.driver.query import QueryResult
from deep_translator import GoogleTranslator, exceptions
from inn_api import LegalEntitiesParser, SearchEngineParser


class ReferenceInn(object):
    def __init__(self, filename, directory):
        self.filename: str = filename
        self.directory = directory

    @staticmethod
    def connect_to_db() -> Tuple[Client, QueryResult]:
        """
        Connecting to clickhouse.
        :return: Client ClickHouse.
        """
        try:
            client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                        username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            logger.info("Successfully connect ot db")
            fts: QueryResult = client.query("SELECT DISTINCT recipients_tin, name_of_the_contract_holder FROM fts")
            # Чтобы проверить, есть ли данные. Так как переменная образуется, но внутри нее могут быть ошибки.
            print(fts.result_rows[0])
        except Exception as ex_connect:
            logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            print("error_connect_db", file=sys.stderr)
            sys.exit(1)
        return client, fts

    @staticmethod
    def replace_forms_organizations(company_name: str) -> str:
        """
        Deleting organization forms for the accuracy of determining confidence_rate.
        """
        for elem in REPLACED_WORDS:
            company_name: str = company_name.replace(elem, "")
        return company_name.translate({ord(c): "" for c in '"'}).strip()

    @staticmethod
    def replace_quotes(sentence: str, quotes: list = None, replaced_str: str = '"') -> str:
        """
        Deleting organization forms for the accuracy of determining confidence_rate.
        """
        if quotes is None:
            quotes = REPLACED_QUOTES
        for quote in quotes:
            sentence = sentence.replace(quote, replaced_str)
        return sentence

    @staticmethod
    def compare_different_fuzz(company_name: str, translated: str, fuzz_company_name: int, data: dict) -> int:
        """
        Comparing the maximum value of the two confidence_rate.
        """
        try:
            company_name_en: str = GoogleTranslator(source='ru', target='en').translate(company_name[:4500])
        except exceptions.NotValidPayload:
            company_name_en = company_name
        data["company_name_unified_en"] = company_name_en
        fuzz_company_name_two: int = fuzz.partial_ratio(company_name_en.upper(), translated.upper())
        return max(fuzz_company_name, fuzz_company_name_two)

    def get_company_name_by_inn(self, provider: LegalEntitiesParser, data: dict, inn: str, sentence: str, index: int,
                                translated: str = None) -> None:
        """
        We get a unified company name from the sentence itself for the found INN. And then we are looking for a company
        on the website https://www.rusprofile.ru/.
        """
        if not translated:
            translated: str = GoogleTranslator(source='en', target='ru').translate(sentence[:4500] + " ")
        data['is_inn_found_auto'] = True
        data['company_name_rus'] = translated
        inn, company_name, is_cache = provider.get_company_name_from_cache(inn, index)
        logger.info(f"Transleted is {translated}. Index is {index}", pid=os.getpid())
        data["company_inn"] = inn
        data["is_company_name_from_cache"] = is_cache
        company_name: str = re.sub(" +", " ", company_name)
        data["company_name_unified"] = company_name
        company_name = self.replace_forms_organizations(company_name)
        fuzz_company_name: int = fuzz.partial_ratio(company_name.upper(), translated.upper())
        fuzz_company_name = self.compare_different_fuzz(company_name, translated, fuzz_company_name, data)
        data['confidence_rate'] = fuzz_company_name
        logger.info(f"Data was written successfully to the dictionary. Data is {sentence}", pid=os.getpid())

    def get_company_name_by_sentence(self, provider: SearchEngineParser, sentence: str, index: int) \
            -> Tuple[dict, str]:
        """
        We send the sentence to the Yandex search engine (first we pre-process: translate it into Russian) by the link
        https://xmlriver.com/search_yandex/xml?user=6390&key=e3b3ac2908b2a9e729f1671218c85e12cfe643b0&query=<value> INN
        """
        sign = '/'
        sentence: str = sentence.translate({ord(c): " " for c in r".,!@#$%^&*()[]{};?\|~=_+"})
        sentence = self.replace_quotes(sentence, replaced_str=' ')
        sentence = re.sub(" +", " ", sentence).strip() + sign
        translated: str = GoogleTranslator(source='en', target='ru').translate(sentence[:4500])
        translated = self.replace_quotes(translated, quotes=['"', '«', '»', sign], replaced_str=' ')
        translated = re.sub(" +", " ", translated).strip()
        api_inn, translated = provider.get_company_name_from_cache(translated, index)
        return api_inn, translated

    def get_inn_from_row(self, sentence: str, data: dict, index: int, fts: QueryResult) -> None:
        """
        Full processing of the sentence, including 1). inn search by offer -> company search by inn,
        2). inn search in yandex by request -> company search by inn.
        """
        list_inn: list = []
        logger.info(f"Processing of a row with index {index} begins. Data is {sentence}", pid=os.getpid())
        all_list_inn: list = re.findall(r"\d+", sentence)
        cache_inn: LegalEntitiesParser = LegalEntitiesParser()
        for item_inn in all_list_inn:
            with contextlib.suppress(Exception):
                item_inn2 = validate_inn.validate(item_inn)
                list_inn.append(item_inn2)
        data['original_file_name'] = os.path.basename(self.filename)
        data['original_file_parsed_on'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if list_inn:
            self.get_company_name_by_inn(cache_inn, data, inn=list_inn[0], sentence=sentence, index=index)
        else:
            cache_name_inn: SearchEngineParser = SearchEngineParser("company_name_and_inn", conn)
            api_inn, translated = self.get_company_name_by_sentence(cache_name_inn, sentence, index)
            for inn, inn_count in api_inn.items():
                self.join_fts(fts, data, inn, inn_count + 1)
                self.get_company_name_by_inn(cache_inn, data, inn, sentence, translated=translated, index=index)
                self.write_to_csv(index, data)

    @staticmethod
    def join_fts(fts: QueryResult, data: dict, inn: str, inn_count: int):
        data['company_inn_count'] = inn_count
        data["is_fts_found"] = False
        index_recipients_tin: int = fts.column_names.index('recipients_tin')
        index_name_of_the_contract_holder: int = fts.column_names.index('name_of_the_contract_holder')
        for rows in fts.result_rows:
            if rows[index_recipients_tin] == inn:
                data["is_fts_found"] = True
                data["fts_company_name"] = rows[index_name_of_the_contract_holder]

    @staticmethod
    def to_csv(output_file_path: str, data: dict, operator: str):
        with open(output_file_path, operator) as csvfile:
            writer = DictWriter(csvfile, fieldnames=list(data.keys()))
            if operator == 'w':
                writer.writeheader()
            writer.writerow(data)

    def write_to_csv(self, index: int, data: dict) -> None:
        """
        Writing data to json.
        """
        basename: str = os.path.basename(self.filename)
        output_file_path: str = os.path.join(self.directory, f'{basename}')
        if os.path.exists(output_file_path):
            self.to_csv(output_file_path, data, 'a')
        else:
            self.to_csv(output_file_path, data, 'w')
        logger.info(f"Data was written successfully to the file. Index is {index}. Data is {data}", pid=os.getpid())

    def add_index_in_queue(self, is_queue: bool, sentence: str, index: int) -> None:
        """
        Adding an index to the queue or writing empty data.
        """
        if not is_queue:
            logger.error(f"An error occured in which the processor was added to the queue. Index is {index}. "
                         f"Data is {sentence}", pid=os.getpid())
            retry_queue.put(index)
        else:
            data_queue: dict = not_parsed_data[index - 2]
            data_queue['original_file_name'] = os.path.basename(self.filename)
            data_queue['original_file_parsed_on'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.write_to_csv(index, data_queue)

    @staticmethod
    def stop_parse_data(index: int, ex_interrupt: str) -> None:
        """

        """
        error_message: str = f'Too many requests to the translator. Exception - {ex_interrupt}'
        logger.error(error_message, pid=os.getpid())
        logger_stream.error(f'много_запросов_к_переводчику_на_строке_{index}')
        error_flag.value = 1
        pool.terminate()

    def parse_data(self, index: int, data: dict, fts: QueryResult, is_queue: bool = False) -> None:
        """
        Processing each row.
        """
        for key, sentence in data.items():
            try:
                if key == 'company_name':
                    self.get_inn_from_row(str(sentence), data, index, fts)
            except (IndexError, ValueError, TypeError, sqlite3.OperationalError) as ex:
                logger.error(f'Not found inn INN Yandex. Data is {index, sentence} (most likely a foreign company). '
                             f'Exception - {ex}', pid=os.getpid())
                logger_stream.error(f'Not found INN in Yandex. Data is {index, sentence} '
                                    f'(most likely a foreign company). Exception - {ex}')
            except (exceptions.TooManyRequests, AssertionError) as ex_interrupt:
                self.stop_parse_data(index, ex_interrupt)
            except Exception as ex_full:
                logger.error(f'Unknown errors. Exception is {ex_full}. Data is {index, sentence}', pid=os.getpid())
                self.add_index_in_queue(is_queue, sentence, index)

    @staticmethod
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

    def convert_csv_to_dict(self) -> List[dict]:
        """
        Csv data representation in json.
        """
        dataframe: Union[TextFileReader, DataFrame] = pd.read_csv(self.filename, dtype=str)
        dataframe.columns = ['company_name']
        # dataframe = dataframe.drop_duplicates(subset='company_name', keep="first")
        dataframe = dataframe.replace({np.nan: None})
        dataframe['company_name'] = dataframe['company_name'].replace({'_x000D_': ''}, regex=True)
        dataframe['company_name_rus'] = None
        dataframe['company_inn'] = None
        dataframe['company_inn_count'] = None
        dataframe['is_fts_found'] = None
        dataframe['fts_company_name'] = None
        dataframe['company_name_unified'] = None
        dataframe['company_name_unified_en'] = None
        dataframe['is_inn_found_auto'] = None
        dataframe['original_file_name'] = None
        dataframe['original_file_parsed_on'] = None
        dataframe['confidence_rate'] = None
        dataframe['is_company_name_from_cache'] = None
        return dataframe.to_dict('records')


if __name__ == "__main__":
    logger.info("The script has started its work")
    logger.info(f'File is {os.path.basename(sys.argv[1])}')
    reference_inn: ReferenceInn = ReferenceInn(os.path.abspath(sys.argv[1]), sys.argv[2])
    path: str = reference_inn.create_file_for_cache()
    conn: Connection = sqlite3.connect(path)
    not_parsed_data: List[dict] = reference_inn.convert_csv_to_dict()
    client_clickhouse, fts_results = reference_inn.connect_to_db()
    error_flag = Value('i', 0)
    retry_queue: Queue = Queue()

    with Pool(processes=WORKER_COUNT) as pool:
        for i, dict_data in enumerate(not_parsed_data, 2):
            pool.apply_async(reference_inn.parse_data, (i, dict_data, fts_results))
        logger.info("Processors will be attached and closed. Next, the queue will be processed", pid=os.getpid())
        pool.close()
        pool.join()
    logger.info(f"All rows have been processed. Is the queue empty? {retry_queue.empty()}", pid=os.getpid())

    if not retry_queue.empty():
        time.sleep(120)
        logger.info(f"Processing of processes that are in the queue. Size queue is {retry_queue.qsize()}",
                    pid=os.getpid())
        parsed_data = []
        with Pool(processes=WORKER_COUNT) as pool:
            while not retry_queue.empty():
                index_queue = retry_queue.get()
                pool.apply_async(reference_inn.parse_data,
                                 (index_queue, not_parsed_data[index_queue - 2], fts_results, True))
            pool.close()
            pool.join()
