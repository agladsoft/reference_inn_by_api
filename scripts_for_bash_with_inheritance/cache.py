import re
import contextlib
import validate_inn
from typing import Union
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import xml.etree.ElementTree as ElemTree
from __init__ import logger, logger_stream, USER_XML_RIVER, KEY_XML_RIVER, MESSAGE_TEMPLATE, PREFIX_TEMPLATE


class MyError(Exception):
    def __init__(self, error, value, index):
        self.error: str = error
        self.value: str = value
        self.index: int = index


class LegalEntitiesParser(object):
    def __init__(self, table_name, conn):
        self.table_name: str = table_name
        self.conn = conn
        self.cur = self.load_cache()

    def load_cache(self):
        """

        """
        cur = self.conn.cursor()
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.table_name}(
           key TEXT PRIMARY KEY,
           value TEXT)
        """)
        self.conn.commit()
        return cur

    def cache_add_and_save(self, api_inn, api_name):
        """

        """
        self.cur.executemany(f"INSERT or IGNORE INTO {self.table_name} VALUES(?, ?)", [(api_inn, api_name)])
        self.conn.commit()
        return "Данные записываются в кэш", api_inn, api_name

    @staticmethod
    def get_inn_by_api(value, _id, var_api_name=None):
        """

        """
        try:
            session = HTMLSession()
            api_inn = session.get(f'https://www.rusprofile.ru/search?query={value}')
            html_code = api_inn.html.html
            html = BeautifulSoup(html_code, 'html.parser')
            page_inn = html.find('span', attrs={'id': _id})
            page_name = html.find('h1', attrs={'itemprop': 'name'})
            page_many_inn = html.findAll('span', attrs={'class': 'finded-text'})
            page_many_company = html.findAll("div", attrs={"class": "company-item"})
            for inn, inn_name in zip(page_many_inn, page_many_company):
                if not page_inn and not page_name and inn.text == value:
                    var_api_name = inn_name.find("span", {"class", "finded-text"}).parent.parent.parent.parent.find(
                        "a").text.strip()
                    var_api_name = re.sub(" +", " ", var_api_name)
                    break
            if page_name:
                var_api_name = page_name.text.strip()
            return value if value != 'None' else None, var_api_name
        except (IndexError, ValueError, TypeError):
            return value if value != 'None' else None, var_api_name

    def get_inn(self, inn, index):
        """

        """
        rows = self.cur.execute(f"SELECT * FROM {self.table_name} WHERE key = {inn}")
        if rows := list(rows):
            return rows[0][0], rows[0][1]
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

        """
        logger.error(message)
        logger_stream.error(f"{prefix}")

    @staticmethod
    def get_inn_from_site(list_inn, values, count_inn):
        """

        """
        for item_inn in values:
            with contextlib.suppress(Exception):
                inn = validate_inn.validate(item_inn)
                list_inn[inn] = list_inn[inn] + 1 if inn in list_inn else count_inn

    def get_inn_from_html(self, myroot, index_page, results, list_inn, count_inn):
        """

        """
        value = myroot[0][index_page][0][results][1][3][0].text
        title = myroot[0][index_page][0][results][1][1].text
        inn_text = re.findall(r"\d+", value)
        inn_title = re.findall(r"\d+", title)
        self.get_inn_from_site(list_inn, inn_text, count_inn)
        self.get_inn_from_site(list_inn, inn_title, count_inn)

    def get_code_error(self, error_code: ElemTree, index: int, value: str) -> None:
        """

        """
        if error_code.tag == 'error':
            code: Union[str, None] = error_code.attrib.get('code')
            message: str = MESSAGE_TEMPLATE.get(code, "Error: not found code error {}. Index is {}. Exception - {}. "
                                                      "Value - {}")
            message = message.format(index, error_code.text, value, code)
            prefix: str = PREFIX_TEMPLATE.get(code, "необработанная_ошибка_на_строке_")
            self.log_error(prefix + str(index), message)
            if code == '200':
                raise AssertionError(message)
            elif code == '110' or code != '15':
                raise MyError(message, value, index)

    def get_inn_from_search_engine(self, value, index):
        """

        """
        session = HTMLSession()
        r = session.get(f"https://xmlriver.com/search_yandex/xml?user={USER_XML_RIVER}&key={KEY_XML_RIVER}"
                        f"&query={value} ИНН")
        xml_code = r.html.html
        myroot = ElemTree.fromstring(xml_code)
        self.get_code_error(myroot[0][0], index, value)
        index_page = 2 if myroot[0][1].tag == 'correct' else 1
        last_range = int(myroot[0][index_page][0][0].attrib['last'])
        list_inn = {}
        count_inn = 0
        for results in range(1, last_range):
            try:
                self.get_inn_from_html(myroot, index_page, results, list_inn, count_inn)
            except Exception as ex:
                logger.warning(
                    f"Warning: description {value} not found in the Yandex. Index is {index}. Exception - {ex}")
                logger_stream.warning(f"Warning: description {value} not found in the Yandex. Index is {index}. "
                                      f"Exception - {ex}")
        return max(list_inn, key=list_inn.get) if list_inn else "None"

    def get_inn_from_sentence(self, value, index):
        """

        """
        rows = self.cur.execute(
            f"""SELECT * FROM "{self.table_name.replace('"', '""')}" WHERE key=?""", (value,),
        )
        rows = list(rows)
        if rows and rows[0][1] != "None":
            return rows[0][1], rows[0][0]
        for key in [value]:
            api_inn = self.get_inn_from_search_engine(key, index)
            with contextlib.suppress(Exception):
                if rows[0][1] == 'None':
                    sql_update_query = f"""Update {self.table_name} set value = ? where key = ?"""
                    data = (api_inn, value)
                    self.cur.execute(sql_update_query, data)
                    self.conn.commit()
            self.cache_add_and_save(value, api_inn)
        return api_inn, value
