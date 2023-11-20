import re
import sqlite3
import os
import time
import requests
import contextlib
from bs4 import BeautifulSoup, Tag, ResultSet
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

    def get_company_name_from_legal_entities_parser(self, value: str, _id: str, dict_cache: dict, var_api_name: str = None):
        """
        Looking for a company name unified from the website of legal entities.
        """
        activity = None
        code = None
        if value in dict_cache:
            return value, *dict_cache[value]
        try:
            session: HTMLSession = HTMLSession()
            logger.info(f"Before request. Data is {value}", pid=os.getpid())
            api_inn: Response = session.get(f'https://www.rusprofile.ru/search?query={value}')
            logger.info(f"After request. Data is {value}", pid=os.getpid())
            html_code: str = api_inn.html.html
            html: BeautifulSoup = BeautifulSoup(html_code, 'html.parser')
            page_inn: Tag = html.find('span', attrs={'id': _id})
            page_name: Tag = html.find('h1', attrs={'itemprop': 'name'})
            page_many_inn: ResultSet = html.findAll('span', attrs={'class': 'finded-text'})
            page_many_company: ResultSet = html.findAll("div", attrs={"class": "company-item"})
            company_row: ResultSet = html.findAll("div", attrs={"class": "company-row"})
            for row in company_row:
                main_type = row.find("span", {"class", "company-info__title"})
                if main_type and main_type.text.strip() == "Основной вид деятельности":
                    main_type_text = row.find("span", {"class", "company-info__text"}).text.strip()
                    matches = re.match(r"(.+) \((.+)\)", main_type_text)
                    activity = matches.group(1)
                    code = matches.group(2)
                    print(activity)
                    print(code)
                    break
            for inn, inn_name in zip(page_many_inn, page_many_company):
                if not page_inn and not page_name and inn.text == value:
                    var_api_name: str = inn_name.find("span", {"class", "finded-text"}).parent.parent.parent.parent. \
                        find("a").text.strip()
                    var_api_name = re.sub(" +", " ", var_api_name)
                    break
            if page_name:
                var_api_name = page_name.text.strip()
                logger.info(f"Unified company is {var_api_name}. INN is {value}", pid=os.getpid())
            dict_cache[value] = var_api_name, activity, code
            return value, var_api_name, activity, code
        except (IndexError, ValueError, TypeError):
            return value, var_api_name, activity, code

    def get_company_name_by_inn(self, inn: str, index: int, dict_cache):
        """
        Getting the company name unified from the cache, if there is one.
        Otherwise, we are looking for verification of legal entities on websites.
        """
        api_inn, api_name = None, None
        if inn != 'None':
            if len(inn) == 10:
                api_inn, api_name, activity_main, okved = self.get_company_name_from_legal_entities_parser(inn, 'clip_inn', dict_cache)
            elif len(inn) == 12:
                api_inn, api_name, activity_main, okved = self.get_company_name_from_legal_entities_parser(inn, 'req_inn', dict_cache)
        else:
            logger.error(f"Not found INN {api_inn} in rusprofile. Index is {index}. Unified company name is "
                         f"{api_name}", pid=os.getpid())
            logger_stream.error(f"Not found INN {api_inn} in rusprofile. Index is {index}."
                                f" Unified company name is {api_name}")
        return api_inn, api_name, activity_main, okved, False


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
