import re
import sys
import json
import time
import sqlite3
import requests
import datetime
import contextlib
import numpy as np
import validate_inn
import pandas as pd
from __init__ import *
from queue import Queue
from pathlib import Path
from csv import DictWriter
from fuzzywuzzy import fuzz
from requests import Response
from sqlite3 import Connection
from notifiers.core import Provider
from pandas import DataFrame, Series
from clickhouse_connect import get_client
from threading import current_thread, Lock
from pandas.io.parsers import TextFileReader
from clickhouse_connect.driver import Client
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple, Union, Dict, Optional
from clickhouse_connect.driver.query import QueryResult
from deep_translator import GoogleTranslator, exceptions
from inn_api import LegalEntitiesParser, SearchEngineParser


class ReferenceInn(object):
    def __init__(self, filename, directory):
        self.conn: Optional[Connection] = None
        self.filename: str = filename
        self.directory = directory
        self.lock: Lock = Lock()
        self.telegram: Dict[str, Optional[int,str]] = {'company_name_unified': 0, 'is_fts_found': 0, 'all_company': 0, 'errors': []}

    @staticmethod
    def connect_to_db() -> Tuple[Client, dict]:
        # sourcery skip: use-dictionary-union
        """
        Connecting to clickhouse.
        :return: Client ClickHouse.
        """
        try:
            client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                        username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            logger.info("Successfully connect to db")
            fts: QueryResult = client.query(
                "SELECT recipients_tin, senders_tin, name_of_the_recipient, senders_name "
                "FROM fts "
                "GROUP BY recipients_tin, senders_tin, name_of_the_recipient, senders_name"
            )
            # Чтобы проверить, есть ли данные. Так как переменная образуется, но внутри нее могут быть ошибки.
            print(fts.result_rows[0])
            fts_recipients_inn: dict = {row[0]: row[2] for row in fts.result_rows}
            fts_senders_inn: dict = {row[1]: row[3] for row in fts.result_rows}
            return client, {**fts_recipients_inn, **fts_senders_inn}
        except Exception as ex_connect:
            logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            telegram(f'Отсутствует подключение к базе данных при получение данных из таблица fts')
            print("error_connect_db", file=sys.stderr)
            sys.exit(1)

    def push_data_to_db(self, start_time_script: str):
        """
        Push all data to clickhouse.
        """
        try:
            client: Client = get_client(host=get_my_env_var('HOST'), database="default",
                                        username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            basename: str = os.path.basename(self.filename)
            output_file_path: str = os.path.join(f"{os.path.dirname(self.directory)}/csv",
                                                 f'{start_time_script}_{basename}')
            df: DataFrame = pd.read_csv(output_file_path, dtype={"company_inn": str, "confidence_rate": "Int64"})
            df = df.replace({np.nan: None, "NaT": None})
            # client.insert_df("reference_inn_all", df, database="default")
        except Exception as ex:
            logger.error(f"Error is {ex}")

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

    def compare_different_fuzz(self, company_name: str, translated: Optional[str], data: dict) -> None:
        """
        Comparing the maximum value of the two confidence_rate.
        """
        if company_name and translated:
            company_name: str = re.sub(" +", " ", company_name)
            company_name = self.replace_forms_organizations(company_name)
            fuzz_company_name: int = fuzz.partial_ratio(company_name.upper(), translated.upper())
            try:
                company_name_en: str = GoogleTranslator(source='ru', target='en').translate(company_name[:4500])
            except exceptions.NotValidPayload:
                company_name_en = company_name
            fuzz_company_name_two: int = fuzz.partial_ratio(company_name_en.upper(), translated.upper())
            data['confidence_rate'] = max(fuzz_company_name, fuzz_company_name_two)

    def get_all_data(
            self,
            fts: dict,
            provider: LegalEntitiesParser,
            data: dict,
            inn: Union[str, None],
            sentence: str,
            index: int,
            num_inn_in_fts: dict,
            list_inn_in_fts: list,
            translated:
            Optional[str] = None,
            inn_count: int = 1,
            sum_count_inn: int = 1,
            enforce_get_company: bool = False
    ) -> None:
        """
        We get a unified company name from the sentence itself for the found INN. And then we are looking for a company
        on the website https://www.rusprofile.ru/.
        """
        logger.info(f"The processing and filling of data into the dictionary has begun. Data is {sentence}",
                    pid=current_thread().ident)
        data["company_inn"] = inn
        data["sum_count_inn"] = sum_count_inn
        self.join_fts(fts, data, inn, inn_count, num_inn_in_fts, translated)
        data['company_name_rus'] = translated
        data["company_inn_max_rank"] = num_inn_in_fts["company_inn_max_rank"]
        num_inn_in_fts["company_inn_max_rank"] += 1
        if not data["is_fts_found"] and not enforce_get_company:
            return
        company_name, is_cache = provider.get_company_name_by_inn(inn, index)
        data["is_company_name_from_cache"] = is_cache
        data["company_name_unified"] = company_name
        self.compare_different_fuzz(company_name, translated, data)
        logger.info(f"Data was written successfully to the dictionary. Data is {sentence}", pid=current_thread().ident)
        self.write_to_csv(index, data)
        list_inn_in_fts.append(data.copy())

    def get_translated_sentence(self, sentence: str) -> Optional[str]:
        """
        Getting translated sentence.
        """
        sign: str = '/'
        sentence: str = sentence.translate({ord(c): " " for c in r".,!@#$%^&*()[]{};?\|~=_+"})
        sentence = self.replace_quotes(sentence, replaced_str=' ')
        sentence = re.sub(" +", " ", sentence).strip() + sign
        logger.info(f"Try translate sentence to russian. Data is {sentence}", pid=current_thread().ident)
        translated: str = GoogleTranslator(source='en', target='ru').translate(sentence[:4500])
        if translated:
            translated = self.replace_quotes(translated, quotes=['"', '«', '»', sign], replaced_str=' ')
            translated = re.sub(" +", " ", translated).strip()
        return translated

    def get_inn_from_row(self, sentence: str, data: dict, index: int, fts: dict) -> None:
        """
        Full processing of the sentence, including 1). inn search by offer -> company search by inn,
        2). inn search in yandex by request -> company search by inn.
        """
        list_inn: list = []
        logger.info(f"Processing of a row with index {index} begins. Data is {sentence}", pid=current_thread().ident)
        all_list_inn: list = re.findall(r"\d+", sentence)
        cache_inn: LegalEntitiesParser = LegalEntitiesParser()
        for item_inn in all_list_inn:
            with contextlib.suppress(Exception):
                item_inn2 = validate_inn.validate(item_inn)
                list_inn.append(item_inn2)
                logger.info(f"Found INN in sentence. Index is {index}. Data is {sentence}", pid=current_thread().ident)
        logger.info(f"The attempt to find the INN in sentence is completed. Index is {index}. Data is {sentence}",
                    pid=current_thread().ident)
        self.get_company_name_from_internet(list_inn, cache_inn, sentence, data, index, fts)

    def get_company_name_from_internet(
            self, list_inn: list,
            cache_inn: LegalEntitiesParser,
            sentence: str,
            data: dict,
            index: int,
            fts: dict
    ) -> None:
        """
        Getting company name from dadata or get inn from Yandex, then get company name from dadata.
        """
        translated: Optional[str] = self.get_translated_sentence(sentence)
        list_inn_in_fts: List[dict] = []
        num_inn_in_fts: Dict[str, int] = {"num_inn_in_fts": 0, "company_inn_max_rank": 1}
        if list_inn:
            self.get_all_data(fts, cache_inn, data, list_inn[0], sentence, index, num_inn_in_fts, list_inn_in_fts,
                              translated, enforce_get_company=True)
        else:
            cache_name_inn: SearchEngineParser = SearchEngineParser("company_name_and_inn", self.conn)
            if api_inn := cache_name_inn.get_company_name_by_inn(translated, index):
                self.parse_all_found_inn(fts, api_inn, cache_inn, sentence, translated, data, index, num_inn_in_fts,
                                         list_inn_in_fts)
            else:
                self.telegram['errors'].append(cache_name_inn.errors)
                self.get_all_data(fts, cache_inn, data, None, sentence, index, num_inn_in_fts, list_inn_in_fts,
                                  translated, inn_count=0, sum_count_inn=0)
        self.write_existing_inn_from_fts(index, data, list_inn_in_fts, num_inn_in_fts)

    def parse_all_found_inn(
            self,
            fts: dict,
            api_inn: dict,
            cache_inn: LegalEntitiesParser,
            sentence: str,
            translated: str,
            data: dict,
            index: int,
            num_inn_in_fts: dict,
            list_inn_in_fts: list
    ) -> None:
        """
        We extract data on all found INN from Yandex.
        """
        sum_count_inn: int = sum(api_inn.values())
        for inn, inn_count in api_inn.items():
            self.get_all_data(fts, cache_inn, data, inn, sentence, index, num_inn_in_fts, list_inn_in_fts,
                              translated, inn_count, sum_count_inn)
        if not list_inn_in_fts:
            self.get_all_data(fts, cache_inn, data, max(api_inn, key=api_inn.get), sentence, index,
                              num_inn_in_fts, list_inn_in_fts, translated, inn_count=0,
                              sum_count_inn=sum_count_inn, enforce_get_company=True)

    def write_existing_inn_from_fts(self, index: int, data: dict, list_inn_in_fts: list, num_inn_in_fts: dict) -> None:
        """
        Write data inn in files.
        """
        list_is_found_fts: List[bool] = []
        logger.info(f"Check company_name in FTS. Index is {index}. Data is {data}", pid=current_thread().ident)
        for dict_inn in list_inn_in_fts:
            dict_inn["count_inn_in_fts"] = num_inn_in_fts["num_inn_in_fts"]
            if dict_inn["is_fts_found"]:
                self.write_to_json(index, dict_inn)
                list_is_found_fts.append(True)
                break
            else:
                list_is_found_fts.append(False)
        if not list_inn_in_fts or not all(list_is_found_fts):
            max_dict_inn = max(list_inn_in_fts, key=lambda x: x["company_inn_count"]) if list_inn_in_fts else data
            self.write_to_json(index, max_dict_inn)

    @staticmethod
    def join_fts(
            fts: dict,
            data: dict,
            inn: Union[str, None],
            inn_count: int,
            num_inn_in_fts: Dict[str, int],
            translated: str
    ) -> None:
        """
        Join FTS for checking INN.
        """
        data["request_to_yandex"] = f"{translated} ИНН"
        data['company_inn_count'] = inn_count
        data["is_fts_found"] = False
        data["fts_company_name"] = None
        if inn in fts:
            data["is_fts_found"] = True
            num_inn_in_fts["num_inn_in_fts"] += 1
            data["fts_company_name"] = fts[inn]

    def to_csv(self, output_file_path: str, data: dict, operator: str):
        with self.lock:
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
        output_file_path: str = os.path.join(f"{os.path.dirname(self.directory)}/csv",
                                             f'{data["original_file_parsed_on"]}_{basename}')
        if os.path.exists(output_file_path):
            self.to_csv(output_file_path, data, 'a')
        else:
            self.to_csv(output_file_path, data, 'w')
        logger.info(f"Data was written successfully to the file. Index is {index}", pid=current_thread().ident)

    def count_to_telegram(self, data: Dict[str, str]) -> None:
        """
        Count company_unified,is_fts_found
        """
        company_name_unified = data.get('company_name_unified')
        is_fts_found = data.get('is_fts_found')
        if company_name_unified:
            self.telegram['company_name_unified'] += 1
        if is_fts_found is None:
            self.telegram['is_fts_found'] += 1

    def write_to_json(self, index: int, data: dict) -> None:
        """
        Writing data to json.
        """
        self.count_to_telegram(data)
        basename: str = os.path.basename(self.filename)
        output_file_path: str = os.path.join(self.directory, f'{basename}_{index}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            logger.info(f"Data was written successfully to the file. Index is {index}", pid=current_thread().ident)

    def add_index_in_queue(
            self,
            not_parsed_data: List[dict],
            retry_queue: Queue,
            is_queue: bool,
            sentence: str,
            index: int
    ) -> None:
        """
        Adding an index to the queue or writing empty data.
        """
        if not is_queue:
            logger.error(f"An error occurred in which the processor was added to the queue. Index is {index}. "
                         f"Data is {sentence}", pid=current_thread().ident)
            retry_queue.put(index)
        else:
            data_queue: dict = not_parsed_data[index - 2]
            data_queue['original_file_name'] = os.path.basename(self.filename)
            data_queue['original_file_parsed_on'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.write_to_csv(index, data_queue)
            self.write_to_json(index, data_queue)

    def add_new_columns(self, data: dict, start_time_script: str):
        data['is_inn_found_auto'] = True
        data["is_company_name_from_cache"] = False
        data['original_file_name'] = os.path.basename(self.filename)
        data['original_file_parsed_on'] = start_time_script

    def parse_data(
            self,
            not_parsed_data: List[dict],
            index: int,
            data: dict,
            fts: dict,
            start_time_script,
            retry_queue: Queue,
            is_queue: bool = False
    ) -> None:
        """
        Processing each row.
        """
        self.add_new_columns(data, start_time_script)
        sentence: str = data.get("company_name")
        try:
            self.get_inn_from_row(str(sentence), data, index, fts)
        except (IndexError, ValueError, TypeError) as ex:
            logger.error(f'Not found inn INN Yandex. Data is {index, sentence} (most likely a foreign company). '
                         f'Exception - {ex}. Type error - {type(ex)}', pid=current_thread().ident)
            logger_stream.error(f'Not found INN in Yandex. Data is {index, sentence} '
                                f'(most likely a foreign company). Exception - {ex}. Type error - {type(ex)}')
            self.write_to_csv(index, data)
            self.write_to_json(index, data)
        except Exception as ex_full:
            logger.error(f'Unknown errors. Exception is {ex_full}. Data is {index, sentence}',
                         pid=current_thread().ident)
            self.add_index_in_queue(not_parsed_data, retry_queue, is_queue, sentence, index)

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
        dataframe.dropna(inplace=True)
        self.telegram['all_company'] += len(dataframe)
        series: Series = dataframe.iloc[:, 0]
        dataframe = series.to_frame(name="company_name")
        dataframe = dataframe.replace({np.nan: None})
        dataframe['company_name'] = dataframe['company_name'].replace({'_x000D_': ''}, regex=True)
        return dataframe.to_dict('records')

    @staticmethod
    def is_enough_money_to_search_engine():
        """
        Check whether there is enough money in the wallet to process the current file.
        """
        try:
            response_balance: Response = requests.get(f"https://xmlriver.com/api/get_balance/yandex/"
                                                      f"?user={USER_XML_RIVER}&key={KEY_XML_RIVER}")
            response_balance.raise_for_status()
            balance: float = float(response_balance.text)
            if 200.0 > balance >= 100.0:
                telegram(message=f"Баланс в Яндекс кошельке сейчас составляет {balance} рублей.")
            elif balance < 100.0:
                telegram(message='Баланс в Яндекс кошельке меньше 100 рублей. Пополните, пожалуйста, счет.')
                logger.error("There is not enough money to process all the lines. Please top up your account")
                logger_stream.error("не_хватает_денег_для_обработки_файла")
                sys.exit(1)
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred while receiving data from xmlriver. Exception is {e}")
            logger_stream.error("ошибка_при_получении_баланса_яндекса")
            sys.exit(1)

    def start_multiprocessing_with_queue(self, retry_queue: Queue, not_parsed_data: List[dict],
                                         fts_results: dict, start_time: str) -> None:
        """
        Starting queue processing using a multithreading.
        """
        if not retry_queue.empty():
            time.sleep(120)
            logger.info(f"Processing of processes that are in the queue. Size queue is {retry_queue.qsize()}",
                        pid=current_thread().ident)
            with ThreadPoolExecutor(max_workers=COUNT_THREADS) as executor:
                for _ in range(retry_queue.qsize()):
                    index_queue: int = retry_queue.get()
                    executor.submit(self.parse_data, not_parsed_data, index_queue, not_parsed_data[index_queue - 2],
                                    fts_results, start_time, retry_queue, True)

    def start_multiprocessing(self, retry_queue: Queue, not_parsed_data: List[dict], fts_results: dict,
                              start_time: str) -> None:
        """
        Starting processing using a multithreading.
        """
        with ThreadPoolExecutor(max_workers=COUNT_THREADS) as executor:
            for i, dict_data in enumerate(not_parsed_data, 2):
                executor.submit(self.parse_data, not_parsed_data, i, dict_data, fts_results, start_time, retry_queue)

    def send_message(self):
        not_unified = self.telegram.get("all_company") - self.telegram.get("company_name_unified")
        errors = '\n'.join([i for i in self.telegram.get('errors') if i])
        message = (f"Завершена обработка файла: {self.filename.split('/')[-1]}.\n\n"
                   f"Кол-во строк в файле : {self.telegram.get('all_company') + 1}.\n\n"
                   f"Кол-во строк в базе: {self.telegram.get('all_company')}.\n\n"
                   f"Кол-во строк, где значение company_name_unified = НЕ Null : {self.telegram.get('company_name_unified')}\n\n"
                   f"Кол-во строк, где значение company_name_unified = Null : {not_unified}\n\n"
                   f"Кол-во строк, где значение is_fts_found = Null : {self.telegram.get('is_fts_found')}\n\n"
                   f"Ошибки при обработке данных :{errors}")

        telegram(message)

    def main(self):
        """
        The main method that runs the code.
        """
        logging.basicConfig(
            filename=f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging/"
                     f"{datetime.datetime.now().date()}_{os.path.basename(self.filename)}.log",
            format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
            datefmt="%d/%B/%Y %H:%M:%S"
        )
        logger.info("The script has started its work")
        logger.info(f'File is {os.path.basename(self.filename)}')
        self.create_file_for_cache()
        path: str = self.create_file_for_cache()
        self.conn: Connection = sqlite3.connect(path, check_same_thread=False)
        not_parsed_data: List[dict] = self.convert_csv_to_dict()
        client_clickhouse, fts_results = self.connect_to_db()
        retry_queue: Queue = Queue()
        start_time: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.is_enough_money_to_search_engine()
        self.start_multiprocessing(retry_queue, not_parsed_data, fts_results, start_time)
        logger.info(f"All rows have been processed. Is the queue empty? {retry_queue.empty()}",
                    pid=current_thread().ident)
        self.start_multiprocessing_with_queue(retry_queue, not_parsed_data, fts_results, start_time)
        logger.info("Push data to db")
        self.push_data_to_db(start_time)
        logger.info("The script has completed its work")
        self.send_message()


if __name__ == "__main__":
    reference_inn: ReferenceInn = ReferenceInn(os.path.abspath(sys.argv[1]), sys.argv[2])
    reference_inn.main()
