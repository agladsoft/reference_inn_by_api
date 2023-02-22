import re
import contextlib
import validate_inn
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from requests_html import HTMLSession
from __init__ import logger, logger_stream
from __init__ import user_xml_river, key_xml_river


class InnApi:
    def __init__(self, table_name, conn):
        self.table_name = table_name
        self.conn = conn
        self.cur = self.load_cache()

    @staticmethod
    def get_inn_by_api(value, id, var_api_name=None):
        try:
            session = HTMLSession()
            api_inn = session.get(f'https://www.rusprofile.ru/search?query={value}')
            html_code = api_inn.html.html
            html = BeautifulSoup(html_code, 'html.parser')
            page_inn = html.find('span', attrs={'id': id})
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
            return value if value != 'empty' else None, var_api_name
        except Exception:
            return value if value != 'empty' else None, var_api_name

    @staticmethod
    def get_inn_from_site(list_inn, values, count_inn):
        for item_inn in values:
            with contextlib.suppress(Exception):
                inn = validate_inn.validate(item_inn)
                list_inn[inn] = list_inn[inn] + 1 if inn in list_inn else count_inn

    def get_inn_from_html(self, myroot, index_page, results, list_inn, count_inn):
        value = myroot[0][index_page][0][results][1][3][0].text
        title = myroot[0][index_page][0][results][1][1].text
        inn_text = re.findall(r"\d+", value)
        inn_title = re.findall(r"\d+", title)
        self.get_inn_from_site(list_inn, inn_text, count_inn)
        self.get_inn_from_site(list_inn, inn_title, count_inn)

    @staticmethod
    def get_code_error(error_code: ET, index: int, is_var):
        if is_var is True:
            error_code.tag = 'error'
        if error_code.tag == 'error':
            error_code.attrib['code'] = '110'
            if error_code.attrib['code'] == '200':
                error_message: str = f"Error: the money ran out. Index is {index}. Exception - {error_code.text}"
                logger.error(error_message)
                logger_stream.error(f"закончились_деньги_на_строке_{index}")
                raise AssertionError(error_message)
            elif error_code.attrib['code'] == '110':
                error_message = f"Error: there are no free channels for data collection. Index is {index}. " \
                                f"Exception - {error_code.text}"
                logger.error(error_message)
                logger_stream.error(f"нет_свободных_каналов_на_строке_{index}")
                raise AttributeError(error_message)
            elif error_code.attrib['code'] == '15':
                error_message = f"No results found in the search engine. Index is {index}. " \
                                f"Exception - {error_code.text}"
                logger.error(error_message)
                logger_stream.error(f"не_найдено_результатов_{index}")
            else:
                error_message = f"Error: not found code error {error_code.attrib.get('code')}. Index is {index}. " \
                                f"Exception - {error_code.text}"
                logger.error(error_message)
                logger_stream.error(f"необработанная_ошибка_на_строке_{index}")
                raise AssertionError(error_message)

    def get_inn_by_yandex(self, value, index, is_var):
        session = HTMLSession()
        r = session.get(f"https://xmlriver.com/search_yandex/xml?user={user_xml_river}&key={key_xml_river}"
                        f"&query={value} ИНН")
        xml_code = r.html.html
        myroot = ET.fromstring(xml_code)
        self.get_code_error(myroot[0][0], index, is_var)
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
                continue
        return max(list_inn, key=list_inn.get) if list_inn else "empty"

    def load_cache(self):
        cur = self.conn.cursor()
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.table_name}(
           key TEXT PRIMARY KEY,
           value TEXT)
        """)
        self.conn.commit()
        return cur

    def get_inn(self, inn, index):
        rows = self.cur.execute(f"SELECT * FROM {self.table_name} WHERE key = {inn}")
        if rows := list(rows):
            return rows[0][0], rows[0][1]
        for key in [inn]:
            api_inn, api_name = None, None
            if key != 'empty':
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

    def get_inn_from_value(self, value, index, is_var):
        rows = self.cur.execute('SELECT * FROM "{}" WHERE key=?'.format(self.table_name.replace('"', '""')), (value,))
        rows = list(rows)
        if rows and rows[0][1] != "empty":
            return rows[0][1], rows[0][0]
        for key in [value]:
            api_inn = self.get_inn_by_yandex(key, index, is_var)
            with contextlib.suppress(Exception):
                if rows[0][1] == 'empty':
                    sql_update_query = f"""Update {self.table_name} set value = ? where key = ?"""
                    data = (api_inn, value)
                    self.cur.execute(sql_update_query, data)
                    self.conn.commit()
            self.cache_add_and_save(value, api_inn)
        return api_inn, value

    def cache_add_and_save(self, api_inn, api_name):
        self.cur.executemany(f"INSERT or IGNORE INTO {self.table_name} VALUES(?, ?)", [(api_inn, api_name)])
        self.conn.commit()
        return "Данные записываются в кэш", api_inn, api_name
