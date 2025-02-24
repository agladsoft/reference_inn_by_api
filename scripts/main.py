import re
import json
import datetime
import numpy as np
import pandas as pd
from queue import Queue
from pathlib import Path
from csv import DictWriter
from fuzzywuzzy import fuzz
from requests import Response
from scripts.__init__ import *
from pandas import DataFrame, Series
from clickhouse_connect import get_client
from threading import current_thread, Lock
from clickhouse_connect.driver import Client
from concurrent.futures import ThreadPoolExecutor
from clickhouse_connect.driver.query import QueryResult
from deep_translator import GoogleTranslator, exceptions
from typing import List, Union, Dict, Optional, Any, Tuple
from scripts.unified_companies import UnifiedCompaniesManager, SearchEngineParser


class ReferenceInn(object):
    def __init__(self, filename, directory):
        self.filename: str = filename
        self.directory: str = directory
        self.russian_companies: list = []
        self.foreign_companies: list = []
        self.unknown_companies: list = []
        self.lock: Lock = Lock()
        self.telegram: Dict[str, Optional[int, str]] = {
            "company_name_unified": 0,
            "is_fts_found": 0,
            "all_company": 0,
            "errors": []
        }

    @staticmethod
    def connect_to_db() -> Tuple[Client, dict]:
        """
        Connecting to clickhouse.
        :return: Client ClickHouse.
        """
        try:
            client: Client = get_client(
                host=get_my_env_var('HOST'),
                database=get_my_env_var('DATABASE'),
                username=get_my_env_var('USERNAME_DB'),
                password=get_my_env_var('PASSWORD')
            )
            fts: QueryResult = client.query(
                "SELECT recipients_tin, senders_tin, name_of_the_recipient, senders_name "
                "FROM fts "
                "GROUP BY recipients_tin, senders_tin, name_of_the_recipient, senders_name"
            )
            # Чтобы проверить, есть ли данные. Так как переменная образуется, но внутри нее могут быть ошибки.
            print(fts.result_rows[0])
            logger.info("Successfully connected to db")
            fts_recipients_inn: dict = {row[0]: row[2] for row in fts.result_rows}
            fts_senders_inn: dict = {row[1]: row[3] for row in fts.result_rows}
            return client, {**fts_recipients_inn, **fts_senders_inn}
        except Exception as ex_connect:
            logger.error(f"Error connection to db {ex_connect}. Type error: {type(ex_connect)}.")
            send_to_telegram('Отсутствует подключение к базе данных при получение данных из таблица fts')
            raise SystemExit("отсутствует_подключение_к_базе") from ex_connect

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
            client.insert_df("reference_inn_all", df, database="default")
        except Exception as ex:
            logger.error(f"Error: {ex}")

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

    def append_data(self, data: dict):
        """
        Append dictionary to list with countries.
        """
        if data.get("country") == "russia":
            self.russian_companies.append(data)
        elif data.get("country") is None:
            self.unknown_companies.append(data)
        else:
            self.foreign_companies.append(data)

    def get_data(
        self,
        fts: dict,
        countries_obj: Optional[Any],
        search_engine: Any,
        data: dict,
        inn: Union[str, None],
        sentence: str,
        index: int,
        num_inn_in_fts: dict,
        list_inn_in_fts: list,
        translated: Optional[str] = None,
        inn_count: int = 1,
        sum_count_inn: int = 1,
        enforce_get_company: bool = False
    ) -> None:
        """
        We get a unified company name from the sentence itself for the found INN. And then we are looking for a company
        on the website https://www.rusprofile.ru/.
        """
        logger.info(
            f"The processing and filling of data into the dictionary has begun. Data: {sentence}",
            pid=current_thread().ident
        )
        data["company_inn"] = inn
        data["sum_count_inn"] = sum_count_inn
        self.join_fts(fts, data, inn, inn_count, num_inn_in_fts)
        data["company_inn_max_rank"] = num_inn_in_fts["company_inn_max_rank"]
        num_inn_in_fts["company_inn_max_rank"] += 1
        if not data["is_fts_found"] and not enforce_get_company:
            self.write_to_csv(index, data)
            return
        companies = list(search_engine.manager.fetch_company_name(countries_obj, inn, index, data.get("company_name")))
        for company_name, country, is_cache in companies:
            if company_name is not None:
                data["is_company_name_from_cache"] = is_cache
                data["company_name_unified"] = company_name
                data["country"] = country
                self.compare_different_fuzz(company_name, translated, data)
        logger.info(f"Data was written successfully to the dictionary. Data: {sentence}", pid=current_thread().ident)
        self.write_to_csv(index, data)
        list_inn_in_fts.append(data)

    def translate_sentence(self, sentence: str, only_russian: bool) -> Optional[str]:
        """
        Getting translated sentence.
        """
        if only_russian:
            sign: str = '/'
            sentence: str = sentence.translate({ord(c): " " for c in r".,!@#$%^&*()[]{};?\|~=_+"})
            sentence = self.replace_quotes(sentence, replaced_str=' ')
            sentence = re.sub(" +", " ", sentence).strip() + sign
            logger.info(f"Try translate sentence to russian. Data: {sentence}", pid=current_thread().ident)
            sentence: str = GoogleTranslator(source='en', target='ru').translate(sentence[:4500]) or ""
            sentence = self.replace_quotes(sentence, quotes=['"', '«', '»', sign], replaced_str=' ')
        sentence: str = sentence.translate({ord(c): " " for c in r"+"})
        return re.sub(" +", " ", sentence).strip()

    def unify_companies(self, sentence, data: dict, index: int, fts: dict, only_russian: bool):
        """

        :param sentence:
        :param data:
        :param index:
        :param fts:
        :param only_russian: bool
        :return:
        """
        list_inn_in_fts: List[dict] = []
        country: Optional[List[object]] = []
        translated: Optional[str] = self.translate_sentence(sentence, only_russian)
        data["request_to_yandex"] = f"{translated} ИНН"
        data['company_name_rus'] = translated
        num_inn_in_fts: Dict[str, int] = {"num_inn_in_fts": 0, "company_inn_max_rank": 1}
        search_engine = SearchEngineParser(country, UnifiedCompaniesManager(only_russian))
        dict_taxpayer_ids, taxpayer_id, from_cache = search_engine.get_taxpayer_id(translated)
        countries = list({
            item
            for inn in dict_taxpayer_ids
            for item in search_engine.manager.get_valid_company(inn)
        })
        if taxpayer_id:
            self.parse_all_found_inn(
                fts, dict_taxpayer_ids, taxpayer_id, countries, search_engine,
                data, sentence, index, num_inn_in_fts, list_inn_in_fts, translated
            )
        else:
            self.get_data(
                fts, None, search_engine, data, taxpayer_id, sentence, index,
                num_inn_in_fts, list_inn_in_fts, translated, inn_count=0, sum_count_inn=0
            )
        self.write_existing_inn_from_fts(search_engine, index, data, list_inn_in_fts, num_inn_in_fts, from_cache)

    def parse_all_found_inn(
        self,
        fts: dict,
        dict_taxpayer_ids: dict,
        taxpayer_id: str,
        countries: Optional[Any],
        search_engine: Any,
        data: dict,
        sentence: str,
        index: int,
        num_inn_in_fts: dict,
        list_inn_in_fts: list,
        translated: str
    ) -> None:
        """
        We extract data on all found INN from Yandex.
        """
        sum_count_inn: int = sum(dict_taxpayer_ids.values())
        for inn, inn_count in dict_taxpayer_ids.items():
            self.get_data(
                fts, countries, search_engine, data.copy(), inn, sentence, index, num_inn_in_fts, list_inn_in_fts,
                translated, inn_count=inn_count, sum_count_inn=sum_count_inn,  enforce_get_company=True
            )
        if not list_inn_in_fts:
            self.get_data(
                fts, countries, search_engine, data, taxpayer_id, sentence, index, num_inn_in_fts,
                list_inn_in_fts, translated, inn_count=0, sum_count_inn=sum_count_inn, enforce_get_company=True
            )

    def write_existing_inn_from_fts(
        self,
        search_engine,
        index: int,
        data: dict,
        list_inn_in_fts: list,
        num_inn_in_fts: dict,
        from_cache: bool
    ) -> None:
        """
        Write data inn in files.
        """
        logger.info(f"Check company_name in FTS. Index: {index}. Data: {data}", pid=current_thread().ident)
        for dict_inn in list_inn_in_fts:
            dict_inn["count_inn_in_fts"] = num_inn_in_fts["num_inn_in_fts"]
            if dict_inn["is_fts_found"]:
                self.append_data(dict_inn)
                if not from_cache:
                    search_engine.cache_add_and_save(
                        dict_inn.get("company_name_rus"),
                        dict_inn.get("company_inn"),
                        dict_inn.get("country")
                    )
                return
        # If no valid INNs found, use the one with the highest company_inn_count or fallback to data
        max_dict_inn = max(list_inn_in_fts, key=lambda x: x["company_inn_count"], default=data)
        self.append_data(max_dict_inn)
        if not from_cache:
            search_engine.cache_add_and_save(
                max_dict_inn.get("company_name_rus"),
                max_dict_inn.get("company_inn"),
                max_dict_inn.get("country")
            )

    @staticmethod
    def join_fts(
        fts: dict,
        data: dict,
        inn: Union[str, None],
        inn_count: int,
        num_inn_in_fts: Dict[str, int]
    ) -> None:
        """
        Join FTS for checking INN.
        """
        data['company_inn_count'] = inn_count
        data["is_fts_found"] = False
        data["fts_company_name"] = None
        if inn in fts and inn is not None:
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
        Writing data to csv.
        """
        basename: str = os.path.basename(self.filename)
        output_file_path: str = os.path.join(
            f"{os.path.dirname(self.directory)}/csv",
            f'{data["original_file_parsed_on"]}_{basename}'
        )
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        if os.path.exists(output_file_path):
            self.to_csv(output_file_path, data, 'a')
        else:
            self.to_csv(output_file_path, data, 'w')
        logger.info(f"Data was written successfully to the file. Index: {index}", pid=current_thread().ident)

    def count_to_telegram(self, data: List[dict]) -> None:
        """
        Count company_unified,is_fts_found
        """
        for row in data:
            company_name_unified = row.get('company_name_unified')
            is_fts_found = row.get('is_fts_found')
            if company_name_unified:
                self.telegram['company_name_unified'] += 1
            if is_fts_found is None:
                self.telegram['is_fts_found'] += 1

    @staticmethod
    def write_to_file(file_path, data):
        """
        Запись данных в файл.
        """
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            logger.info("Данные успешно записаны в файл", pid=current_thread().ident)

    def write_to_json(self) -> None:
        """
        Запись данных в json.
        """
        self.count_to_telegram(self.russian_companies + self.foreign_companies + self.unknown_companies)
        basename: str = os.path.basename(self.filename)
        output_file_path: str = os.path.join(self.directory, f'{basename}_russia.json')
        output_file_path_foreign: str = os.path.join(self.directory, f'{basename}_foreign.json')
        output_file_path_unknown: str = os.path.join(self.directory, f'{basename}_unknown.json')

        self.write_to_file(output_file_path, self.russian_companies)
        self.write_to_file(output_file_path_foreign, self.foreign_companies)
        self.write_to_file(output_file_path_unknown, self.unknown_companies)

    def add_index_in_queue(
        self,
        not_parsed_data: List[dict],
        retry_queue: Queue,
        is_queue: bool,
        sentence: str,
        index: int,
        ex_full: Exception
    ) -> None:
        """
        Adding an index to the queue or writing empty data.
        """
        if not is_queue:
            logger.error(
                f"An error occurred in which the processor was added to the queue. Index: {index}. "
                f"Data: {sentence}", pid=current_thread().ident
            )
            retry_queue.put(index)
        else:
            ERRORS.append(f'Exception: {ex_full}. Data: {index}, {sentence}')
            logger.error(f"Exception: {ex_full}. Data: {index}, {sentence}")
            data_queue: dict = not_parsed_data[index - 2]
            self.write_to_csv(index, data_queue)
            self.append_data(data_queue)

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
        is_queue: bool = False,
        only_russian: bool = True
    ) -> None:
        """
        Processing each row.
        """
        self.add_new_columns(data, start_time_script)
        sentence: str = data.get("company_name")
        try:
            self.unify_companies(sentence, data, index, fts, only_russian)
        except (IndexError, ValueError, TypeError) as ex:
            logger.error(
                f'Not found inn INN Yandex. Data: {index, sentence} (most likely a foreign company). '
                f'Exception - {ex}. Type error - {type(ex)}', pid=current_thread().ident
            )
            self.write_to_csv(index, data)
            self.append_data(data)
        except Exception as ex_full:
            logger.error(f'Unknown errors. Exception: {ex_full}. Data: {index, sentence}', pid=current_thread().ident)
            self.add_index_in_queue(not_parsed_data, retry_queue, is_queue, sentence, index, ex_full)

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

    def convert_file_to_dict(self) -> List[dict]:
        """
        Csv data representation in json.
        """
        dataframe: DataFrame = pd.read_excel(self.filename, dtype=str)
        dataframe = dataframe.dropna()
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
            response_balance: Response = requests.get(
                f"https://xmlriver.com/api/get_balance/yandex/?user={USER_XML_RIVER}&key={KEY_XML_RIVER}"
            )
            response_balance.raise_for_status()
            balance: float = float(response_balance.text)
            if 200.0 > balance >= 100.0:
                send_to_telegram(message=f"Баланс в Яндекс кошельке сейчас составляет {balance} рублей.")
            elif balance < 100.0:
                send_to_telegram(message='Баланс в Яндекс кошельке меньше 100 рублей. Пополните, пожалуйста, счет.')
                logger.error("There is not enough money to process all the lines. Please top up your account")
                raise SystemExit("не_хватает_денег_для_обработки_файла")
        except Exception as e:
            logger.error(f"An error occurred while receiving data from xmlriver. Exception: {e}")
            raise SystemExit("ошибка_при_получении_баланса_яндекса") from e

    def start_multiprocessing_with_queue(
        self,
        retry_queue: Queue,
        not_parsed_data: List[dict],
        fts_results: dict,
        start_time: str,
        only_russian: bool = True
    ) -> None:
        """
        Starting queue processing using a multithreading.
        """
        if not retry_queue.empty():
            time.sleep(120)
            logger.info(f"Processing of processes that are in the queue. Size queue: {retry_queue.qsize()}",
                        pid=current_thread().ident)
            with ThreadPoolExecutor(max_workers=COUNT_THREADS) as executor:
                for _ in range(retry_queue.qsize()):
                    index_queue: int = retry_queue.get()
                    executor.submit(
                        self.parse_data,
                        not_parsed_data,
                        index_queue,
                        not_parsed_data[index_queue - 2],
                        fts_results,
                        start_time,
                        retry_queue,
                        is_queue=True,
                        only_russian=only_russian
                    )

    def start_multiprocessing(
        self,
        retry_queue: Queue,
        not_parsed_data: List[dict],
        fts_results: dict,
        start_time: str,
        only_russian: bool = True
    ) -> None:
        """
        Starting processing using a multithreading.
        """
        with ThreadPoolExecutor(max_workers=COUNT_THREADS) as executor:
            for index, dict_data in enumerate(not_parsed_data, 2):
                executor.submit(
                    self.parse_data,
                    not_parsed_data,
                    index,
                    dict_data,
                    fts_results,
                    start_time,
                    retry_queue,
                    only_russian=only_russian
                )

    def send_message(self, client: Client) -> None:
        """
        Sending a message to the telegram.
        """
        logger.info('Составление сообщения для отправки ботом')
        not_unified = self.telegram["all_company"] - self.telegram["company_name_unified"]
        errors_ = '\n\n'.join(set(ERRORS)) if self.unknown_companies else ''
        count_companies_upload: int = client.query(
            f"SELECT COUNT(*) FROM default.reference_inn "
            f"WHERE original_file_name='{os.path.basename(self.filename)}'"
        ).result_rows[0][0]
        message: str = (
            f"Завершена обработка файла: {self.filename.split('/')[-1]}.\n\n"
            f"Кол-во строк в файле: {self.telegram['all_company']}\n\n"
            f"Кол-во строк в базе: {count_companies_upload}\n\n"
            f"Кол-во строк, где company_name_unified нашлось: {self.telegram['company_name_unified']}\n\n"
            f"Кол-во строк, где company_name_unified НЕ нашлось: {not_unified}\n\n"
            f"Кол-во строк, где is_fts_found НЕ нашлось: {self.telegram['is_fts_found']}\n\n"
            f"Кол-во строк, где country НЕ была найдена: {len(self.unknown_companies)}\n\n"
            f"Ошибки при обработке данных:\n{errors_}"
        )
        logger.info(message)
        send_to_telegram(message)

    def _process_data_with_multiprocessing(
        self,
        retry_queue: Queue,
        data: List[dict],
        fts_results: dict,
        start_time: str,
        only_russian: bool = True
    ):
        """
        Runs multiprocessing for the given data.
        """
        self.start_multiprocessing(retry_queue, data, fts_results, start_time, only_russian)
        self.start_multiprocessing_with_queue(retry_queue, data, fts_results, start_time, only_russian)

    def main(self) -> None:
        """
        The main method that runs the code.
        """
        logging.basicConfig(
            filename=(
                f"{get_my_env_var('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging/"
                f"{datetime.datetime.now().date()}_{os.path.basename(self.filename)}.log"
            )
        )
        logger.info(f"The script has started its work. File: {os.path.basename(self.filename)}")
        client, fts_results = self.connect_to_db()
        not_parsed_data: List[dict] = self.convert_file_to_dict()
        start_time: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        retry_queue: Queue = Queue()

        self.is_enough_money_to_search_engine()
        self._process_data_with_multiprocessing(retry_queue, not_parsed_data, fts_results, start_time)

        logger.info(
            f"All rows have been processed. Is the queue empty? {retry_queue.empty()}", pid=current_thread().ident
        )

        unknown_companies: List[dict] = self.unknown_companies.copy()
        self.unknown_companies = []

        # Повторная обработка ненайденных компаний
        self._process_data_with_multiprocessing(
            retry_queue, unknown_companies, fts_results, start_time, only_russian=False
        )

        self.write_to_json()
        logger.info("Push data to db")
        self.push_data_to_db(start_time)
        time.sleep(300)
        self.send_message(client)
        logger.info(f"The script has completed its work. File: {os.path.basename(self.filename)}")


if __name__ == "__main__":
    reference_inn: ReferenceInn = ReferenceInn(os.path.abspath(sys.argv[1]), sys.argv[2])
    reference_inn.main()
