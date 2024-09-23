import re
import sqlite3
import requests
import contextlib
import validate_inn
from requests import Response
from typing import Union, Tuple
from threading import current_thread
import xml.etree.ElementTree as ElemTree
from __init__ import logger, logger_stream, USER_XML_RIVER, KEY_XML_RIVER, MESSAGE_TEMPLATE, PREFIX_TEMPLATE, ERRORS


class MyError(Exception):
    def __init__(self, error, value, index):
        self.error: str = error
        self.value: str = value
        self.index: int = index


class LegalEntitiesParser(object):

    def get_company_name_by_inn(self, inn: str, index: int) -> \
            Tuple[Union[str, None], Union[bool, None]]:
        """
        Getting the company name unified from the cache, if there is one.
        Otherwise, we are looking for verification of legal entities on websites.
        """
        data: dict = {
            "inn": inn
        }
        try:
            response: Response = requests.post("http://service_inn:8003", json=data)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, str):
                return None, False
            return data[0][0].get('value') if data[0] else None, data[1]
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred during the API request: {str(e)}")
            return None, None


class SearchEngineParser(LegalEntitiesParser):

    def __init__(self, table_name, conn, queue):
        self.table_name: str = table_name
        self.conn: sqlite3.Connection = conn
        self.cur: sqlite3.Cursor = self.load_cache()
        self.queue = queue

    def load_cache(self) -> sqlite3.Cursor:
        """
        Loading the cache.
        """
        cur: sqlite3.Cursor = self.conn.cursor()
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.table_name}(
           key TEXT PRIMARY KEY,
           value TEXT)
        """)
        self.conn.commit()
        return cur

    def cache_add_and_save(self, api_inn: str, api_name: str) -> Tuple[str, str, str]:
        """
        Saving and adding the result to the cache.
        """
        self.cur.executemany(f"INSERT or REPLACE INTO {self.table_name} VALUES(?, ?)", [(api_inn, api_name)])
        self.conn.commit()
        return "Данные записываются в кэш", api_inn, api_name

    @staticmethod
    def log_error(prefix: str, message: str) -> None:
        """
        Recording logs.
        """
        logger.error(message, pid=current_thread().ident)
        logger_stream.error(f"{prefix}")

    @staticmethod
    def get_inn_from_site(dict_inn: dict, values: list, count_inn: int) -> None:
        """
        Parse each site (description and title).
        """
        for item_inn in values:
            with contextlib.suppress(Exception):
                inn: str = validate_inn.validate(item_inn)
                dict_inn[inn] = dict_inn[inn] + 1 if inn in dict_inn else count_inn

    def get_code_error(self, error_code: ElemTree, index: int, value: str) -> None:
        """
        Getting error codes from xml_river.
        """
        if error_code.tag == 'error':
            code: Union[str, None] = error_code.attrib.get('code')
            message: str = MESSAGE_TEMPLATE.get(code, "Not found code error. Index is {}. Exception - {}. "
                                                      "Value - {}. Error code - {}")
            message = message.format(index, error_code.text, value, code)
            prefix: str = PREFIX_TEMPLATE.get(code, "необработанная_ошибка_на_строке_")
            self.log_error(prefix + str(index), message)
            if self.queue:
                ERRORS.append(message)
            if code == '200':
                raise AssertionError(message)
            elif code == '110' or code != '15':
                raise MyError(message, value, index)

    def parse_xml(self, response: Response, index: int, value: str, dict_inn: dict, count_inn: int):
        """
        Parsing xml.
        """
        # Parse the XML data
        root = ElemTree.fromstring(response.text)
        self.get_code_error(root[0][0], index, value)

        # Find all title and passage elements
        for doc in root.findall(".//doc"):
            title = doc.find('title').text if doc.find('title').text is not None else ''
            passage = doc.find('.//passage').text if doc.find('.//passage').text is not None else ''
            inn_text: list = re.findall(r"\d+", passage)
            inn_title: list = re.findall(r"\d+", title)
            self.get_inn_from_site(dict_inn, inn_text, count_inn)
            self.get_inn_from_site(dict_inn, inn_title, count_inn)

    def get_inn_from_search_engine(self, value: str, index: int) -> dict:
        """
        Looking for the INN in the search engine, and then we parse through the sites.
        """
        logger.info(f"Before request. Data is {value}", pid=current_thread().ident)
        try:
            r: Response = requests.get(f"https://xmlriver.com/search_yandex/xml?user={USER_XML_RIVER}"
                                       f"&key={KEY_XML_RIVER}&query={value} ИНН", timeout=120)
        except Exception as e:
            logger.error(f"Run time out. Data is {value}", pid=current_thread().ident)
            raise MyError(f"Run time out. Index is {index}. Exception is {e}. Value - {value}", value, index) from e
        logger.info(f"After request. Data is {value}", pid=current_thread().ident)
        dict_inn: dict = {}
        count_inn: int = 1
        try:
            self.parse_xml(r, index, value, dict_inn, count_inn)
        except (ValueError, KeyError, AttributeError) as ex:
            logger.warning(f"Description {value} not found in the Yandex. Index is {index}. Exception - {ex}",
                           pid=current_thread().ident)
            logger_stream.warning(f"Description {value} not found in the Yandex. Index is {index}. "
                                  f"Exception - {ex}")
        logger.info(f"Dictionary with INN is {dict_inn}. Data is {value}", pid=current_thread().ident)
        return dict_inn

    def get_company_name_by_inn(self, value: str, index: int) -> dict:
        """
        Getting the INN from the cache, if there is one. Otherwise, we search in the search engine.
        """
        api_inn: dict = self.get_inn_from_search_engine(value, index)
        for inn in api_inn.items():
            with contextlib.suppress(Exception):
                if api_inn == 'None':
                    sql_update_query: str = f"""Update {self.table_name} set value = ? where key = ?"""
                    data: Tuple[str, str] = (inn[1], value)
                    self.cur.execute(sql_update_query, data)
                    self.conn.commit()
            self.cache_add_and_save(value, inn[0])
        return api_inn
