import re
import sqlite3
import requests
import contextlib
import validate_inn
from requests import Response
from typing import Union, Tuple
from threading import current_thread
from requests_html import HTMLSession
import xml.etree.ElementTree as ElemTree
from __init__ import logger, logger_stream, USER_XML_RIVER, KEY_XML_RIVER, MESSAGE_TEMPLATE, PREFIX_TEMPLATE, \
    get_my_env_var


class MyError(Exception):
    def __init__(self, error, value, index):
        self.error: str = error
        self.value: str = value
        self.index: int = index


class LegalEntitiesParser(object):

    def get_company_name_by_inn(self, inn: str, index: int) -> \
            Tuple[Union[str, None], Union[str, None], bool]:
        """
        Getting the company name unified from the cache, if there is one.
        Otherwise, we are looking for verification of legal entities on websites.
        """
        data: dict = {
            "inn": inn
        }
        response: Response = requests.post(f"http://{get_my_env_var('HOST')}:8003", json=data)
        if response.status_code == 200:
            data = response.json()
            return inn, data[0][0]['value'], data[1]


class SearchEngineParser(LegalEntitiesParser):

    def __init__(self, table_name, conn):
        self.table_name: str = table_name
        self.conn: sqlite3.Connection = conn
        self.cur: sqlite3.Cursor = self.load_cache()

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

    def get_inn_from_html(self, myroot: ElemTree, index_page: int, results: int, dict_inn: dict, count_inn: int) \
            -> None:
        """
        Parsing the html page of the search engine with the found queries.
        """
        value: str = myroot[0][index_page][0][results][1][3][0].text
        title: str = myroot[0][index_page][0][results][1][1].text
        inn_text: list = re.findall(r"\d+", value)
        inn_title: list = re.findall(r"\d+", title)
        self.get_inn_from_site(dict_inn, inn_text, count_inn)
        self.get_inn_from_site(dict_inn, inn_title, count_inn)

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
            if code == '200':
                raise AssertionError(message)
            elif code == '110' or code != '15':
                raise MyError(message, value, index)

    def parse_xml(self, response: Response, index: int, value: str) -> Tuple[ElemTree.Element, int, int]:
        """
        Parsing xml.
        """
        xml_code: str = response.html.html
        myroot: ElemTree = ElemTree.fromstring(xml_code)
        self.get_code_error(myroot[0][0], index, value)
        index_page: int = 2 if myroot[0][1].tag == 'correct' else 1
        try:
            last_range: int = int(myroot[0][index_page][0][0].attrib['last'])
        except IndexError as index_err:
            logger.warning(f"The request to Yandex has been corrected, so we are shifting the index. Index is {index}. "
                           f"Exception - {index_err}", pid=current_thread().ident)
            index_page += + 1
            last_range = int(myroot[0][index_page][0][0].attrib['last'])
        return myroot, index_page, last_range

    def get_inn_from_search_engine(self, value: str, index: int) -> dict:
        """
        Looking for the INN in the search engine, and then we parse through the sites.
        """
        session: HTMLSession = HTMLSession()
        logger.info(f"Before request. Data is {value}", pid=current_thread().ident)
        try:
            r: Response = session.get(f"https://xmlriver.com/search_yandex/xml?user={USER_XML_RIVER}"
                                      f"&key={KEY_XML_RIVER}&query={value} ИНН", timeout=120)
        except Exception as e:
            logger.error(f"Run time out. Data is {value}", pid=current_thread().ident)
            raise MyError(f"Run time out. Index is {index}. Exception is {e}. Value - {value}", value, index) from e
        logger.info(f"After request. Data is {value}", pid=current_thread().ident)
        myroot, index_page, last_range = self.parse_xml(r, index, value)
        dict_inn: dict = {}
        count_inn: int = 1
        for results in range(1, last_range):
            try:
                self.get_inn_from_html(myroot, index_page, results, dict_inn, count_inn)
            except Exception as ex:
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
        rows: sqlite3.Cursor = self.cur.execute(
            f"""SELECT * FROM "{self.table_name.replace('"', '""')}" WHERE key=?""", (value,),
        )
        list_rows: list = list(rows)
        if list_rows and list_rows[0][1] != "None":
            logger.info(f"Data is {list_rows[0][0]}. INN is {list_rows[0][1]}", pid=current_thread().ident)
            return {list_rows[0][1]: 1}
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
