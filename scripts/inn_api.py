import os
import re
import sqlite3
import datetime
import requests
import contextlib
import validate_inn
from requests import Response
from typing import Union, Tuple
from requests_html import HTMLSession
import xml.etree.ElementTree as ElemTree
from bs4 import BeautifulSoup, Tag, ResultSet
from __init__ import logger, logger_stream, USER_XML_RIVER, KEY_XML_RIVER, MESSAGE_TEMPLATE, PREFIX_TEMPLATE


class MyError(Exception):
    def __init__(self, error, value, index):
        self.error: str = error
        self.value: str = value
        self.index: int = index


class LegalEntitiesParser(object):
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
        self.cur.executemany(f"INSERT or IGNORE INTO {self.table_name} VALUES(?, ?)", [(api_inn, api_name)])
        self.conn.commit()
        return "Данные записываются в кэш", api_inn, api_name

    @staticmethod
    def get_inn_by_api(value: str, _id: str, var_api_name: str = None) -> Union[Tuple[Union[str, None], str]]:
        """
        Looking for a company name unified from the website of legal entities.
        """
        try:
            session: HTMLSession = HTMLSession()
            logger.info(
                f"Before request (rusprofile). Pid is {os.getpid()}. Time is {datetime.datetime.now()}. Data is {value}")
            api_inn: Response = session.get(f'https://www.rusprofile.ru/search?query={value}')
            logger.info(
                f"After request (rusprofile). Pid is {os.getpid()}. Time is {datetime.datetime.now()}. Data is {value}")
            html_code: str = api_inn.html.html
            html: BeautifulSoup = BeautifulSoup(html_code, 'html.parser')
            page_inn: Tag = html.find('span', attrs={'id': _id})
            page_name: Tag = html.find('h1', attrs={'itemprop': 'name'})
            page_many_inn: ResultSet = html.findAll('span', attrs={'class': 'finded-text'})
            page_many_company: ResultSet = html.findAll("div", attrs={"class": "company-item"})
            for inn, inn_name in zip(page_many_inn, page_many_company):
                if not page_inn and not page_name and inn.text == value:
                    var_api_name: str = inn_name.find("span", {"class", "finded-text"}).parent.parent.parent.parent. \
                        find("a").text.strip()
                    var_api_name = re.sub(" +", " ", var_api_name)
                    break
            if page_name:
                var_api_name = page_name.text.strip()
            return value if value != 'None' else None, var_api_name
        except (IndexError, ValueError, TypeError):
            return value if value != 'None' else None, var_api_name

    def get_inn_from_cache(self, inn: str, index: int) -> Tuple[Union[str, None], Union[str, None]]:
        """
        Getting the company name unified from the cache, if there is one.
        Otherwise, we are looking for verification of legal entities on websites.
        """
        api_inn: Union[str, None]
        api_name: Union[str, None]
        rows: sqlite3.Cursor = self.cur.execute(f"SELECT * FROM {self.table_name} WHERE key = {inn}")
        if list_rows := list(rows):
            return list_rows[0][0], list_rows[0][1]
        for key in [inn]:
            api_inn, api_name = None, None
            if key != 'None':
                if len(key) == 10:
                    api_inn, api_name = self.get_inn_by_api(key, 'clip_inn')
                elif len(key) == 12:
                    api_inn, api_name = self.get_inn_by_api(key, 'req_inn')
            if api_inn is not None and api_name is not None:
                self.cache_add_and_save(api_inn, api_name)
                break
            else:
                logger.error(f"Error: not found inn {api_inn} in rusprofile. Index is {index}. Unified company name is "
                             f"{api_name}")
                logger_stream.error(f"Error: not found inn {api_inn} in rusprofile. Index is {index}."
                                    f" Unified company name is {api_name}")
        return api_inn, api_name


class SearchEngineParser(LegalEntitiesParser):

    @staticmethod
    def log_error(prefix: str, message: str) -> None:
        """
        Recording logs.
        """
        logger.error(message)
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
            message: str = MESSAGE_TEMPLATE.get(code, "Error: not found code error. Index is {}. Exception - {}. "
                                                      "Value - {}. Error code - {}")
            message = message.format(index, error_code.text, value, code)
            prefix: str = PREFIX_TEMPLATE.get(code, "необработанная_ошибка_на_строке_")
            self.log_error(prefix + str(index), message)
            if code == '200':
                raise AssertionError(message)
            elif code == '110' or code != '15':
                raise MyError(message, value, index)

    def get_inn_from_search_engine(self, value: str, index: int) -> str:
        """
        Looking for the INN in the search engine, and then we parse through the sites.
        """
        logger.info(
            f"Before request (yandex). Pid is {os.getpid()}. Time is {datetime.datetime.now()}. Data is {value}")
        r: Response = requests.get(f"https://xmlriver.com/search_yandex/xml?user={USER_XML_RIVER}"
                                   f"&key={KEY_XML_RIVER}&query={value} ИНН")
        logger.info(f"After request (yandex). Pid is {os.getpid()}. Time is {datetime.datetime.now()}. Data is {value}")
        xml_code: str = r.text
        myroot: ElemTree = ElemTree.fromstring(xml_code)
        self.get_code_error(myroot[0][0], index, value)
        index_page: int = 2 if myroot[0][1].tag == 'correct' else 1
        last_range: int = int(myroot[0][index_page][0][0].attrib['last'])
        dict_inn: dict = {}
        count_inn: int = 0
        for results in range(1, last_range):
            try:
                self.get_inn_from_html(myroot, index_page, results, dict_inn, count_inn)
            except Exception as ex:
                logger.warning(
                    f"Warning: description {value} not found in the Yandex. Index is {index}. Exception - {ex}")
                logger_stream.warning(f"Warning: description {value} not found in the Yandex. Index is {index}. "
                                      f"Exception - {ex}")
        return max(dict_inn, key=dict_inn.get) if dict_inn else "None"

    def get_inn_from_cache(self, value: str, index: int) -> Tuple[str, str]:
        """
        Getting the INN from the cache, if there is one. Otherwise, we search in the search engine.
        """
        rows: sqlite3.Cursor = self.cur.execute(
            f"""SELECT * FROM "{self.table_name.replace('"', '""')}" WHERE key=?""", (value,),
        )
        list_rows: list = list(rows)
        if list_rows and list_rows[0][1] != "None":
            return list_rows[0][1], list_rows[0][0]
        for key in [value]:
            api_inn: str = self.get_inn_from_search_engine(key, index)
            with contextlib.suppress(Exception):
                if list_rows[0][1] == 'None':
                    sql_update_query: str = f"""Update {self.table_name} set value = ? where key = ?"""
                    data: tuple[str, str] = (api_inn, value)
                    self.cur.execute(sql_update_query, data)
                    self.conn.commit()
            self.cache_add_and_save(value, api_inn)
        return api_inn, value
