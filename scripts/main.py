import re
import json
import datetime
import numpy as np
import pandas as pd
from queue import Queue
from csv import DictWriter
from fuzzywuzzy import fuzz
from requests import Response
from scripts.__init__ import *
from pandas import DataFrame, Series
from deep_translator import exceptions
from clickhouse_connect import get_client
from threading import current_thread, Lock
from clickhouse_connect.driver import Client
from scripts.translate import TranslatorFactory
from concurrent.futures import ThreadPoolExecutor
from clickhouse_connect.driver.query import QueryResult
from typing import List, Union, Dict, Optional, Any, Tuple
from scripts.unified_companies import UnifiedCompaniesManager, SearchEngineParser


class ReferenceInn(object):
    def __init__(self, filename: str, directory: str):
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
        Establishes a connection to the database and retrieves data from the 'fts' table.

        This method connects to a Clickhouse database using credentials and host information
        from environment variables. It executes a query to select and group data from the 'fts'
        table, specifically retrieving recipients' and senders' TINs, as well as their respective names.
        The results are logged and returned as a dictionary mapping TINs to names.

        :return: A tuple containing the database client and a dictionary
            where keys are TINs and values are names from the 'fts' table.
        :raises: SystemExit: If there is an error connecting to the database, logs the error and
            sends a notification before terminating the program.
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

    def push_data_to_db(self, start_time_script: str) -> None:
        """
        Pushes data from a CSV file to the database.

        This method reads data from a CSV file generated during the script run,
        processes it to replace NaN and NaT values with None, and inserts the
        data into the 'reference_inn_all' table in the default database.

        :param start_time_script: The timestamp at which the script started,
                                  used to construct the CSV file path.
        :return: None
        :raises Exception: Logs an error message if there is any issue during
                           the database insertion process.
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
        Replaces organization forms and double quotes from a company name.

        This function takes a company name string as an argument and
        replaces the following organization forms: "ООО", "OOO", "OОO",
        "OOО", "ООO", "ОАО", "ЗАО", "3АО", "АО", and double quotes.
        It returns the modified string after stripping any leading or
        trailing whitespace.

        :param company_name: Company name string.
        :return: Modified company name string
        """
        for elem in REPLACED_WORDS:
            company_name: str = company_name.replace(elem, "")
        return company_name.translate({ord(c): "" for c in '"'}).strip()

    @staticmethod
    def replace_quotes(sentence: str, quotes: list = None, replaced_str: str = '"') -> str:
        """
        Replaces specified quote characters in a sentence with a replacement string.

        This static method iterates over a list of quote characters and replaces
        each occurrence in the provided sentence with the specified replacement
        string. By default, it replaces quotes with the standard double quote (").
        If no list of quotes is provided, a predefined list of quote characters
        is used.

        :param sentence: The input string in which quotes are to be replaced.
        :param quotes: A list of quote characters to be replaced. Defaults to None,
                       which uses a predefined list of quotes.
        :param replaced_str: The string to replace each quote character with.
                             Defaults to the standard double quote (").
        :return: The modified sentence with specified quote characters replaced.
        """
        if quotes is None:
            quotes = REPLACED_QUOTES
        for quote in quotes:
            sentence = sentence.replace(quote, replaced_str)
        return sentence

    def compare_different_fuzz(self, company_name: str, translated: Optional[str], data: dict) -> None:
        """
        Compares two company names and sets a confidence rate based on similarity.

        This method takes two company name strings and an empty dictionary as arguments.
        If both company names are not empty, it processes the company name to strip
        leading and trailing whitespace and to remove any extra whitespace between
        words. It then calculates the similarity between the two company names using
        the fuzzywuzzy library. The similarity is a value between 0 and 100, where
        0 means no similarity and 100 means exact match. The method also attempts
        to translate the company name from Russian to English using the Yandex
        translator and compares the translated name with the translated argument.
        Finally, it sets the 'confidence_rate' key in the data dictionary with the
        maximum of the two similarity values calculated.

        :param company_name: Company name string.
        :param translated: Translated company name string.
        :param data: Empty dictionary to store the confidence rate.
        :return: None
        """
        if company_name and translated:
            company_name: str = re.sub(" +", " ", company_name)
            company_name = self.replace_forms_organizations(company_name)
            fuzz_company_name: int = fuzz.partial_ratio(company_name.upper(), translated.upper())
            try:
                translator = TranslatorFactory.get_translator('yandex')
                company_name_en: str = translator.translate(company_name[:4500], source_lang='ru', target_lang='en')
            except exceptions.NotValidPayload:
                company_name_en = company_name
            fuzz_company_name_two: int = fuzz.partial_ratio(company_name_en.upper(), translated.upper())
            data['confidence_rate'] = max(fuzz_company_name, fuzz_company_name_two)

    def append_data(self, data: dict) -> None:
        """
        Appends the given data to the appropriate company list based on the country.

        This method takes a dictionary containing company data and appends it to one
        of the following lists: russian_companies, unknown_companies, or foreign_companies.
        It determines the appropriate list by checking the 'country' key in the data
        dictionary. If the country is 'russia', the data is added to russian_companies.
        If the country is None, the data is added to unknown_companies. Otherwise, the
        data is added to foreign_companies.

        :param data: A dictionary containing company data, which includes the 'country' key.
        :return: None
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
        Processes and fills data into the dictionary.

        This method takes a dictionary containing company data and adds or updates
        several keys with values from other arguments. It sets the 'company_inn' key
        to the given INN, the 'sum_count_inn' key to the sum of the given INN count
        and the 'company_inn_max_rank' key to the value of the 'company_inn_max_rank'
        key in the num_inn_in_fts dictionary, plus one. If the 'is_fts_found' key in
        the data dictionary is False and the enforce_get_company argument is False,
        it writes the data to the CSV file and returns. Otherwise, it fetches a list
        of company names and countries from the search engine and iterates over the
        list, setting the 'is_company_name_from_cache', 'company_name_unified', and
        'country' keys in the data dictionary to the corresponding values from the
        list. It also compares the company name with the translated company name and
        sets the 'confidence_rate' key in the data dictionary to the maximum of the
        two similarity values calculated. Finally, it writes the data to the CSV file
        and appends it to the list_inn_in_fts list.

        :param fts: A dictionary containing FTS data.
        :param countries_obj: An object containing country information.
        :param search_engine: An object containing search engine information.
        :param data: A dictionary containing company data.
        :param inn: The INN of the company.
        :param sentence: The sentence containing the company name.
        :param index: The index of the sentence in the CSV file.
        :param num_inn_in_fts: A dictionary containing the count of INNs in FTS.
        :param list_inn_in_fts: A list to store the data.
        :param translated: The translated company name.
        :param inn_count: The count of the given INN.
        :param sum_count_inn: The sum of the given INN count and the count of INNs in FTS.
        :param enforce_get_company: A flag indicating whether to enforce getting the company name.
        :return: None
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
        companies: List[Tuple[Optional[str], Optional[str], bool]] = list(
            search_engine.manager.fetch_company_name(
                countries_obj, inn, index, data.get("company_name"), self.telegram["errors"]
            )
        )
        for company_name, country, is_cache in companies:
            if company_name is not None:
                data["is_company_name_from_cache"] = is_cache
                data["company_name_unified"] = company_name
                data["country"] = country
                self.compare_different_fuzz(company_name, translated, data)
        logger.info(f"Data was written successfully to the dictionary. Data: {sentence}", pid=current_thread().ident)
        self.write_to_csv(index, data)
        list_inn_in_fts.append(data)

    def translate_sentence(self, sentence: str, with_russian: bool) -> Optional[str]:
        """
        Translates a given sentence into Russian if with_russian is True.

        This method takes a sentence and a boolean indicating if the sentence should be
        translated to Russian. If with_russian is True, it replaces punctuation and
        quotes in the sentence, translates it to Russian using the Yandex translator,
        and strips the result. If with_russian is False, it only strips the sentence.
        The translated sentence is then returned.

        :param sentence: The sentence to be translated.
        :param with_russian: A boolean indicating if the sentence should be translated to Russian.
        :return: The translated sentence or the original sentence if with_russian is False.
        """
        if with_russian:
            sign: str = '/'
            sentence: str = sentence.translate({ord(c): " " for c in r".,!@#$%^&*()[]{};?\|~=_+"})
            sentence = self.replace_quotes(sentence, replaced_str=' ')
            sentence = re.sub(" +", " ", sentence).strip() + sign
            logger.info(f"Try translate sentence to russian. Data: {sentence}", pid=current_thread().ident)
            translator = TranslatorFactory().get_translator('yandex')
            sentence: str = translator.translate(sentence[:4500], source_lang='en', target_lang='ru') or ""
            sentence = self.replace_quotes(sentence, quotes=['"', '«', '»', sign], replaced_str=' ')
        sentence: str = sentence.translate({ord(c): " " for c in r"+"})
        return re.sub(" +", " ", sentence).strip()

    def unify_companies(self, sentence: str, data: dict, index: int, fts: dict, with_russian: bool):
        """
        Unifies companies by getting the company name from the database and FTS.

        This method takes a sentence, data, index, FTS, and a boolean indicating if the sentence
        should be translated to Russian. It translates the sentence to Russian if with_russian is
        True, gets the taxpayer ID from the Yandex parser, and then gets the company name from
        the database and FTS using the UnifiedCompaniesManager. The company name is then written
        to the dictionary and the data is written to the CSV file.

        :param sentence: The sentence to be unified.
        :param data: The data to be written to the CSV file.
        :param index: The index of the current row to be written to the CSV file.
        :param fts: The FTS data.
        :param with_russian: A boolean indicating if the sentence should be translated to Russian.
        :return: None
        """
        list_inn_in_fts: List[dict] = []
        country: Optional[List[object]] = []
        translated: Optional[str] = self.translate_sentence(sentence, with_russian)
        data["request_to_yandex"] = f"{translated} ИНН"
        data['company_name_rus'] = translated
        num_inn_in_fts: Dict[str, int] = {"num_inn_in_fts": 0, "company_inn_max_rank": 1}
        search_engine = SearchEngineParser(country, UnifiedCompaniesManager(with_russian))
        dict_taxpayer_ids, taxpayer_id, from_cache = search_engine.get_taxpayer_id(translated)
        countries: List[callable] = [
            item
            for inn in dict_taxpayer_ids
            for item in search_engine.manager.get_valid_company(inn)
        ]
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
        Processes and fills data into the dictionary for all found INNs.

        This method takes a dictionary containing company data and adds or updates
        several keys with values from other arguments. It processes all found INNs
        and for each one, it fetches a list of company names and countries from the
        search engine and iterates over the list, setting the 'is_company_name_from_cache',
        'company_name_unified', and 'country' keys in the data dictionary to the
        corresponding values from the list. It also compares the company name with
        the translated company name and sets the 'confidence_rate' key in the data
        dictionary to the maximum of the two similarity values calculated. If no
        company was found for any of the INNs, it writes the data to the CSV file and
        appends it to the list_inn_in_fts list.

        :param fts: A dictionary containing FTS data.
        :param dict_taxpayer_ids: A dictionary containing taxpayer IDs and their counts.
        :param taxpayer_id: The taxpayer ID.
        :param countries: An object containing country information.
        :param search_engine: An object containing search engine information.
        :param data: A dictionary containing company data.
        :param sentence: The sentence containing the company name.
        :param index: The index of the sentence in the CSV file.
        :param num_inn_in_fts: A dictionary containing the count of INNs in FTS.
        :param list_inn_in_fts: A list to store the data.
        :param translated: The translated company name.
        :return: None
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
        Check if the company name is in FTS, and if so, write it to the CSV file and append it to the list_inn_in_fts list.
        If not, use the one with the highest company_inn_count or fallback to the data if no valid INNs are found.
        If the data is from the cache, do not write it to the cache.

        :param search_engine: An object containing search engine information.
        :param index: The index of the sentence in the CSV file.
        :param data: A dictionary containing company data.
        :param list_inn_in_fts: A list to store the data.
        :param num_inn_in_fts: A dictionary containing the count of INNs in FTS.
        :param from_cache: A boolean indicating if the data is from the cache.
        :return: None
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
        Join the data with FTS data.

        This method takes a dictionary containing FTS data, a dictionary containing company data,
        an INN, the count of the given INN, and a dictionary containing the count of INNs in FTS.
        It sets the 'company_inn_count' key in the data dictionary to the given INN count,
        sets the 'is_fts_found' key to False, and sets the 'fts_company_name' key to None.
        If the INN is in the FTS dictionary and is not None, it sets the 'is_fts_found' key to True,
        increments the 'num_inn_in_fts' key in the num_inn_in_fts dictionary, and sets the
        'fts_company_name' key to the value of the INN in the FTS dictionary.

        :param fts: A dictionary containing FTS data.
        :param data: A dictionary containing company data.
        :param inn: The INN of the company.
        :param inn_count: The count of the given INN.
        :param num_inn_in_fts: A dictionary containing the count of INNs in FTS.
        :return: None
        """
        data['company_inn_count'] = inn_count
        data["is_fts_found"] = False
        data["fts_company_name"] = None
        if inn in fts and inn is not None:
            data["is_fts_found"] = True
            num_inn_in_fts["num_inn_in_fts"] += 1
            data["fts_company_name"] = fts[inn]

    def to_csv(self, output_file_path: str, data: dict, operator: str) -> None:
        """
        Writes data to a CSV file.

        This method takes a file path, a dictionary containing the data to be written,
        and an operator specifying whether to write or append to the file. It writes
        the data to the file using the DictWriter from the csv module.

        :param output_file_path: The path of the file to be written.
        :param data: A dictionary containing the data to be written.
        :param operator: A string specifying whether to write or append to the file.
        :return: None
        """
        with self.lock:
            with open(output_file_path, operator) as csvfile:
                writer = DictWriter(csvfile, fieldnames=list(data.keys())) # type: ignore
                if operator == 'w':
                    writer.writeheader()
                writer.writerow(data)

    def write_to_csv(self, index: int, data: dict) -> None:
        """
        Writes the provided data to a CSV file.

        This method constructs a file path based on the directory and filename,
        ensuring the directory exists, and writes the data to the CSV file. If the
        file already exists, it appends the data; otherwise, it creates a new file
        and writes the data. Logs the success message with the index.

        :param index: The index of the data to be written, used for logging.
        :param data: A dictionary containing the data to be written to the file.
        :return: None
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
        Counts the number of unified company names and the number of companies that have no INN in FTS.

        This method takes a list of dictionaries, where each dictionary represents a company. It counts
        the number of unified company names (i.e., the number of companies that have a name in the
        'company_name_unified' key) and the number of companies that have no INN in FTS (i.e., the number
        of companies that have a None value in the 'is_fts_found' key). It updates the counters in the
        'telegram' dictionary.

        :param data: A list of dictionaries, where each dictionary represents a company.
        :return: None
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
        Writes data to a file in JSON format.

        This static method takes a file path and data as input, and writes the data
        to the specified file in JSON format with UTF-8 encoding. The method logs
        a success message upon successful writing.

        :param file_path: The path to the file where data will be written.
        :param data: The data to be written to the file, typically a dictionary.
        :return: None
        """
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            logger.info("Данные успешно записаны в файл", pid=current_thread().ident)

    def write_to_json(self) -> None:
        """
        Writes the company data to JSON files.

        This method aggregates the Russian, foreign, and unknown companies, counts
        them for telegram statistics, and writes each category of companies to
        separate JSON files. The output files are named based on the original
        filename, appending '_russia.json', '_foreign.json', and '_unknown.json'
        for the respective categories.

        :return: None
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
        Adds the index to the queue for retry if an error occurs during parsing.

        If the error occurs in the main thread (not in the queue), it logs an error
        message and adds the index to the queue for retry. If the error occurs in a
        separate thread (in the queue), it logs an error message, adds the index to
        the queue for retry, and writes the data to a CSV file.

        :param not_parsed_data: A list of dictionaries containing the data that was
            not parsed.
        :param retry_queue: A Queue object to which the index will be added for retry.
        :param is_queue: A boolean indicating whether the error occurred in the main
            thread (False) or in a separate thread (True).
        :param sentence: A string containing the sentence that was being parsed when
            the error occurred.
        :param index: An integer containing the index of the sentence that was being
            parsed when the error occurred.
        :param ex_full: An Exception object containing the error that occurred.
        :return: None
        """
        if not is_queue:
            logger.error(
                f"An error occurred in which the processor was added to the queue. Index: {index}. "
                f"Data: {sentence}", pid=current_thread().ident
            )
            retry_queue.put(index)
        else:
            self.telegram["errors"].append(f'Exception: {ex_full}. Data: {index}, {sentence}')
            logger.error(f"Exception: {ex_full}. Data: {index}, {sentence}")
            data_queue: dict = not_parsed_data[index - 2]
            self.write_to_csv(index, data_queue)
            self.append_data(data_queue)

    def add_new_columns(self, data: dict, start_time_script: str) -> None:
        """
        Adds new columns to the given data dictionary.

        This method takes a dictionary 'data' and a string 'start_time_script' as input, and adds
        four new key-value pairs to the dictionary. The keys are 'is_inn_found_auto',
        'is_company_name_from_cache', 'original_file_name', and 'original_file_parsed_on', and
        the values are True, False, the base name of the file, and the provided start time script,
        respectively. This method is used to add columns to the data before it is written to a file.

        :param data: A dictionary containing the data to which new columns will be added.
        :param start_time_script: A string containing the start time of the script.
        :return: None
        """
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
        with_russian: bool = True
    ) -> None:
        """
        This method takes a list of dictionaries 'not_parsed_data', an integer 'index', a dictionary 'data',
        a dictionary 'fts', a string 'start_time_script', a Queue object 'retry_queue', a boolean 'is_queue',
        and a boolean 'with_russian' as input, and parses the data in the 'data' dictionary.

        It adds new columns to the 'data' dictionary, then attempts to unify the company name
        using the 'unify_companies' method. If an IndexError, ValueError, or TypeError occurs during
        unification, it logs an error message and writes the data to a CSV file. If any other exception
        occurs, it logs an error message, adds the index to the retry queue, and writes the data to a CSV file.

        :param not_parsed_data: A list of dictionaries containing the data that was not parsed.
        :param index: An integer containing the index of the sentence that is being parsed.
        :param data: A dictionary containing the data to be parsed.
        :param fts: A dictionary containing the FTS data.
        :param start_time_script: A string containing the start time of the script.
        :param retry_queue: A Queue object to which the index will be added for retry if an error occurs.
        :param is_queue: A boolean indicating whether the error occurred in the main thread (False)
            or in a separate thread (True).
        :param with_russian: A boolean indicating whether to unify Russian companies (True) or not (False).
        :return: None
        """
        self.add_new_columns(data, start_time_script)
        sentence: str = data.get("company_name")
        try:
            self.unify_companies(sentence, data, index, fts, with_russian)
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

    def convert_file_to_dict(self) -> List[dict]:
        """
        Converts a file to a dictionary format.

        This method reads an Excel file and converts it into a dictionary format.
        It uses the pandas library to read the Excel file and perform the conversion.
        It also updates the 'all_company' counter in the 'telegram' dictionary.

        :return: A list of dictionaries, with each dictionary containing the company name.
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
        Checks if there is enough money in the Yandex wallet to process all the lines.

        It sends a Telegram message if the balance is between 100 and 200 rubles,
        and raises a SystemExit if the balance is less than 100 rubles.

        :raises SystemExit: If there is not enough money to process all the lines.
        :return: None
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

    def start_multiprocessing(
        self,
        retry_queue: Queue,
        not_parsed_data: List[dict],
        fts_results: dict,
        start_time: str,
        with_russian: bool = True
    ) -> None:
        """
        Initiates multiprocessing for parsing data.

        This method uses a ThreadPoolExecutor to concurrently parse each dictionary
        in the provided list of not_parsed_data. It submits the parse_data method
        for execution, passing the necessary parameters for each entry in the list.

        :param retry_queue: A Queue object used to track indices that need to be retried.
        :param not_parsed_data: A list of dictionaries representing the data to be parsed.
        :param fts_results: A dictionary containing FTS data.
        :param start_time: A string representing the start time of the script.
        :param with_russian: A boolean indicating whether Russian companies should be
            unified (True) or not (False).
        :return: None
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
                    with_russian=with_russian
                )

    def start_multiprocessing_with_queue(
        self,
        retry_queue: Queue,
        not_parsed_data: List[dict],
        fts_results: dict,
        start_time: str,
        with_russian: bool = True
    ) -> None:
        """
        Starts the processing of processes that are in the queue.

        This method is used after the main processing of the data. It processes the
        processes that are in the queue. It waits for 2 minutes before starting the
        processing of the queue. It then starts a ThreadPoolExecutor with a maximum
        of COUNT_THREADS workers and submits the parse_data method for each index
        in the queue.

        :param retry_queue: A Queue object containing the indices of the processes
            that are in the queue.
        :param not_parsed_data: A list of dictionaries containing the data that was
            not parsed.
        :param fts_results: A dictionary containing the FTS data.
        :param start_time: A string containing the start time of the script.
        :param with_russian: A boolean indicating whether to unify Russian companies
            (True) or not (False).
        :return: None
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
                        with_russian=with_russian
                    )

    def send_message(self, client: Client) -> None:
        """
        Composes and sends a summary message to a Telegram bot.

        This method collects data processing statistics, formats a summary message,
        and sends it to a predefined Telegram chat. The statistics include the total
        number of companies processed, the number of unified and non-unified company
        names, and any errors encountered. It also queries the database to count the
        number of companies uploaded.

        :param client: A Client object used to query the database for the count of
            companies uploaded.
        :return: None
        """
        logger.info('Составление сообщения для отправки ботом')
        not_unified: int = self.telegram["all_company"] - self.telegram["company_name_unified"]
        errors_: str = '\n\n'.join(set(self.telegram["errors"])) if self.unknown_companies else ''
        count_companies_upload: int = client.query(
            f"SELECT COUNT(*) FROM default.reference_inn "
            f"WHERE original_file_name='{os.path.basename(self.filename)}'"
        ).result_rows[0][0]
        message: str = (
            f"📈 Завершена обработка файла: {self.filename.split('/')[-1]}.\n\n"
            f"📁 Кол-во строк в файле: {self.telegram['all_company']}\n\n"
            f"🛢️ Кол-во строк в базе: {count_companies_upload}\n\n"
            f"✅ Кол-во строк, где company_name_unified нашлось: {self.telegram['company_name_unified']}\n\n"
            f"⚠️ Кол-во строк, где company_name_unified НЕ нашлось: {not_unified}\n\n"
            f"⚠️ Кол-во строк, где is_fts_found НЕ нашлось: {self.telegram['is_fts_found']}\n\n"
            f"⚠️ Кол-во строк, где country НЕ была найдена: {len(self.unknown_companies)}\n\n"
            f"❌ Ошибки при обработке данных:\n{errors_}"
        )
        logger.info(message)
        send_to_telegram(message)

    def _process_data_with_multiprocessing(
        self,
        retry_queue: Queue,
        data: List[dict],
        fts_results: dict,
        start_time: str,
        with_russian: bool = True
    ):
        """
        Processes data using multiprocessing.

        This method uses multiprocessing to concurrently parse data from the input file.
        It starts two types of multiprocessing, one using a Queue and the other using
        a ThreadPoolExecutor.

        :param retry_queue: A Queue object used to track indices that need to be retried.
        :param data: A list of dictionaries representing the data to be parsed.
        :param fts_results: A dictionary containing FTS data.
        :param start_time: A string representing the start time of the script.
        :param with_russian: A boolean indicating whether Russian companies should be
            unified (True) or not (False).
        :return: None
        """
        self.start_multiprocessing(retry_queue, data, fts_results, start_time, with_russian)
        self.start_multiprocessing_with_queue(retry_queue, data, fts_results, start_time, with_russian)

    def main(self) -> None:
        """
        Executes the main workflow of the script.

        This method initializes logging, connects to the database, and processes data
        from the input file. It first checks the balance to ensure sufficient funds for
        processing. The data is parsed using multiprocessing, with retries for any initially
        unprocessed companies. The results are written to JSON and pushed to the database.
        Finally, a summary message is sent to a Telegram bot after a delay.

        :return: None
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
            retry_queue, unknown_companies, fts_results, start_time, with_russian=False
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
