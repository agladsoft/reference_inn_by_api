import re
import sys
import time
import json
import sqlite3
import contextlib
import numpy as np
import validate_inn
import pandas as pd
from __init__ import *
from pathlib import Path
from fuzzywuzzy import fuzz
from pandas import DataFrame
from sqlite3 import Connection
from typing import List, Tuple, Any
from multiprocessing import Pool, Queue
from pandas.io.parsers import TextFileReader
from deep_translator import GoogleTranslator, exceptions
from inn_api import LegalEntitiesParser, SearchEngineParser, MyError


class ReferenceInn(object):
    def __init__(self, filename, directory):
        self.filename: str = filename
        self.directory = directory

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
            translated: str = GoogleTranslator(source='en', target='ru').translate(sentence[:4500])
        data['is_inn_found_auto'] = True
        data['company_name_rus'] = translated
        inn, company_name = provider.get_company_name_from_cache(inn, index)
        logger.info(f"Transleted is {translated}. Index is {index}", pid=os.getpid())
        data["company_inn"] = inn
        company_name: str = re.sub(" +", " ", company_name)
        data["company_name_unified"] = company_name
        company_name = self.replace_forms_organizations(company_name)
        fuzz_company_name: int = fuzz.partial_ratio(company_name.upper(), translated.upper())
        fuzz_company_name = self.compare_different_fuzz(company_name, translated, fuzz_company_name, data)
        data['confidence_rate'] = fuzz_company_name
        logger.info(f"Data was written successfully to the dictionary. Data is {sentence}", pid=os.getpid())

    def get_company_name_by_sentence(self, provider: SearchEngineParser, sentence: str, index: int,
                                     is_english: bool = False) -> Tuple[str, str]:
        """
        We send the sentence to the Yandex search engine (first we pre-process: translate it into Russian) by the link
        https://xmlriver.com/search_yandex/xml?user=6390&key=e3b3ac2908b2a9e729f1671218c85e12cfe643b0&query=<value> INN
        """
        sign = '/'
        sentence: str = sentence.translate({ord(c): " " for c in r".,!@#$%^&*()[]{};?\|~=_+"})
        if is_english:
            sentence = sentence.replace('"', "")
            inn, translated = provider.get_company_name_from_cache(sentence, index)
            return inn, translated
        sentence = self.replace_quotes(sentence, replaced_str=' ')
        sentence = re.sub(" +", " ", sentence).strip() + sign
        translated: str = GoogleTranslator(source='en', target='ru').translate(sentence[:4500])
        translated = self.replace_quotes(translated, quotes=['"', '«', '»', sign], replaced_str=' ')
        translated = re.sub(" +", " ", translated).strip()
        inn, translated = provider.get_company_name_from_cache(translated, index)
        return inn, translated

    def find_international_company(self, cache_inn: LegalEntitiesParser, sentence: str, data: dict, index: int) -> None:
        """
        Search for international companies.
        """
        for country_and_city in COUNTRIES_AND_CITIES:
            if re.findall(country_and_city, sentence.upper()) and not re.findall("RUSSIA", sentence.upper()):
                data["is_company_name_international"] = True
                self.get_company_name_by_inn(cache_inn, data, inn='None', sentence=sentence, index=index)
        if not data["is_company_name_international"]:
            data["is_company_name_international"] = False

    def get_inn_from_row(self, sentence: str, data: dict, index: int) -> None:
        """
        Full processing of the sentence, including 1). inn search by offer -> company search by inn,
        2). inn search in yandex by request -> company search by inn.
        """
        list_inn: list = []
        logger.info(f"Processing of a row with index {index} begins. Data is {sentence}", pid=os.getpid())
        all_list_inn: list = re.findall(r"\d+", sentence)
        cache_inn: LegalEntitiesParser = LegalEntitiesParser("inn_and_uni_company_name", conn)
        for item_inn in all_list_inn:
            with contextlib.suppress(Exception):
                item_inn2 = validate_inn.validate(item_inn)
                list_inn.append(item_inn2)
        data['original_file_name'] = os.path.basename(self.filename)
        data['original_file_parsed_on'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if list_inn:
            self.get_company_name_by_inn(cache_inn, data, inn=list_inn[0], sentence=sentence, index=index)
        else:
            cache_name_inn: SearchEngineParser = SearchEngineParser("company_name_and_inn", conn)
            inn, translated = self.get_company_name_by_sentence(cache_name_inn, sentence, index)
            self.get_company_name_by_inn(cache_inn, data, inn, sentence, translated=translated, index=index)

    def write_to_json(self, index: int, data: dict) -> None:
        """
        Writing data to json.
        """
        basename: str = os.path.basename(self.filename)
        output_file_path: str = os.path.join(self.directory, f'{basename}_{index}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            logger.info(f"Data was written successfully to the file. Index is {index}. Data is {data}", pid=os.getpid())

    def parse_data(self, index: int, data: dict) -> None:
        """
        Processing each row.
        """
        for key, sentence in data.items():
            try:
                if key == 'company_name':
                    self.get_inn_from_row(sentence, data, index)
            except (IndexError, ValueError, TypeError, sqlite3.OperationalError) as ex:
                logger.error(f'Not found inn INN Yandex. Data is {index, sentence} (most likely a foreign company). '
                             f'Exception - {ex}', pid=os.getpid())
                logger_stream.error(f'Not found INN in Yandex. Data is {index, sentence} '
                                    f'(most likely a foreign company). Exception - {ex}')
            except exceptions.TooManyRequests as ex_translator:
                error_message = f'Too many requests to the translator. Exception - {ex_translator}'
                logger.error(error_message, pid=os.getpid())
                logger_stream.error(f'много_запросов_к_переводчику_на_строке_{index}')
                raise AssertionError(error_message) from ex_translator
        self.write_to_json(index, data)

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
        dataframe: TextFileReader | DataFrame = pd.read_csv(self.filename)
        dataframe.columns = ['company_name']
        # dataframe = dataframe.drop_duplicates(subset='company_name', keep="first")
        dataframe = dataframe.replace({np.nan: None})
        dataframe['company_name'] = dataframe['company_name'].replace({'_x000D_': ''}, regex=True)
        dataframe['company_name_rus'] = None
        dataframe['company_name_unified_en'] = None
        dataframe['company_inn'] = None
        dataframe['company_name_unified'] = None
        dataframe['is_inn_found_auto'] = None
        dataframe['original_file_name'] = None
        dataframe['original_file_parsed_on'] = None
        dataframe['confidence_rate'] = None
        return dataframe.to_dict('records')

    def handle_queue(self, e: Any) -> None:
        """
        Write data to file from queue.
        """
        if type(e) is AssertionError:
            pool.terminate()
        elif type(e) is MyError:
            data_queue: dict = parsed_data[e.index - 2]
            data_queue['original_file_name'] = os.path.basename(self.filename)
            data_queue['original_file_parsed_on'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            self.write_to_json(e.index, data_queue)

    @staticmethod
    def handle_errors(e: Any) -> None:
        """
        Interrupt all processors in case of an error or adding in the queue.
        """
        if type(e) is AssertionError:
            pool.terminate()
        elif type(e) is MyError:
            index: int = e.index
            logger.error(f"An error occured in which the processor was added to the queue. Index is {index}. "
                         f"Data is {e.value}", pid=os.getpid())
            retry_queue.put(index)


if __name__ == "__main__":
    pool: Pool
    procs: list = []
    logger.info("The script has started its work")
    logger.info(f'File is {os.path.basename(sys.argv[1])}')
    reference_inn: ReferenceInn = ReferenceInn(os.path.abspath(sys.argv[1]), sys.argv[2])
    path: str = reference_inn.create_file_for_cache()
    conn: Connection = sqlite3.connect(path)
    parsed_data: List[dict] = reference_inn.convert_csv_to_dict()

    with Pool(processes=WORKER_COUNT) as pool:
        retry_queue: Queue = Queue()
        for i, dict_data in enumerate(parsed_data, 2):
            pool.apply_async(reference_inn.parse_data, (i, dict_data), error_callback=reference_inn.handle_errors)
        pool.close()
        pool.join()

        logger.info(f"All rows have been processed. Is the queue empty? {retry_queue.empty()}")

        if not retry_queue.empty():
            time.sleep(120)
            logger.info("Processing of processes that are in the queue")
            with Pool(processes=WORKER_COUNT) as _pool:
                while not retry_queue.empty():
                    index_queue = retry_queue.get()
                    _pool.apply_async(reference_inn.parse_data, (index_queue, parsed_data[index_queue - 2]),
                                      error_callback=reference_inn.handle_queue)
                _pool.close()
                _pool.join()

    conn.close()
    logger.info("The script has completed its work")
