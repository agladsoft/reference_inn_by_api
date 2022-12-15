import sqlite3
import xml.etree.ElementTree as ET
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import validate_inn
import contextlib
import re
import logging


class GetINNApi:
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
                    var_api_name = inn_name.find("span", {"class", "finded-text"}).parent.parent.parent.parent.find("a").text.strip()
                    var_api_name = re.sub(" +", " ", var_api_name)
                    break
            if page_name:
                var_api_name = page_name.text.strip()
            return value if value != 'empty' else None, var_api_name
        except Exception:
            return value if value != 'empty' else None, var_api_name

    @staticmethod
    def get_inn_from_html(myroot, index_page, results, list_inn, count_inn):
        value = myroot[0][index_page][0][results][1][3][0].text
        title = myroot[0][index_page][0][results][1][1].text
        inn_text = re.findall(r"\d+", value)
        inn_title = re.findall(r"\d+", title)
        for item_inn, item_title_inn in zip(inn_text, inn_title):
            with contextlib.suppress(Exception):
                inn = validate_inn.validate(item_inn) if validate_inn.is_valid(item_inn) else validate_inn.validate(item_title_inn)
                if inn in list_inn:
                    count_inn += 1
                list_inn[inn] = count_inn

    def get_inn_by_yandex(self, value):
        session = HTMLSession()
        r = session.get(f"https://xmlriver.com/search_yandex/xml?user=6390&key=e3b3ac2908b2a9e729f1671218c85e12cfe643b0&query={value} ИНН")
        xml_code = r.html.html
        myroot = ET.fromstring(xml_code)
        index_page = 2 if myroot[0][1].tag == 'correct' else 1
        last_range = int(myroot[0][index_page][0][0].attrib['last'])
        list_inn = {}
        count_inn = 0
        for results in range(1, last_range):
            try:
                self.get_inn_from_html(myroot, index_page, results, list_inn, count_inn)
            except Exception as ex:
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

    def get_inn(self, inn):
        rows = self.cur.execute(f"SELECT * FROM {self.table_name} WHERE key = {inn}")
        if rows := list(rows):
            print(f"Данные есть в кэше: ИНН - {rows[0][0]}, Наименование - {rows[0][1]}")
            return rows[0][0], rows[0][1]
        for key in [inn]:
            api_inn, api_name = None, None
            if key != 'empty':
                if len(key) == 10:
                    api_inn, api_name = self.get_inn_by_api(key, 'clip_inn')
                elif len(key) == 12:
                    api_inn, api_name = self.get_inn_by_api(key, 'req_inn')
            if api_inn is not None and api_name is not None:
                print(self.cache_add_and_save(api_inn, api_name))
                break
            else:
                logging.error(f"Error: {key}")
        return api_inn, api_name

    def get_inn_from_value(self, value):
        rows = self.cur.execute('SELECT * FROM "{}" WHERE key=?'.format(self.table_name.replace('"', '""')), (value,))
        rows = list(rows)
        if rows and rows[0][1] != "empty":
            print(f"Данные есть в кэше: Полное наименование - {rows[0][0]}, ИНН - {rows[0][1]}")
            return rows[0][1], rows[0][0]
        for key in [value]:
            api_inn = self.get_inn_by_yandex(key)
            with contextlib.suppress(Exception):
                if rows[0][1] == 'empty':
                    sql_update_query = f"""Update {self.table_name} set value = ? where key = ?"""
                    data = (api_inn, value)
                    self.cur.execute(sql_update_query, data)
                    self.conn.commit()
            print(self.cache_add_and_save(value, api_inn))
        return api_inn, value

    def cache_add_and_save(self, api_inn, api_name):
        self.cur.executemany(f"INSERT or IGNORE INTO {self.table_name} VALUES(?, ?)", [(api_inn, api_name)])
        self.conn.commit()
        return "Данные записываются в кэш", api_inn, api_name


if __name__ == "__main__":
    conn = sqlite3.connect("cache_inn.db")
    get_inn_api = GetINNApi("inn_and_uni_company_name", conn)
    # print(get_inn_api.get_inn("781310635186"))
    print(get_inn_api.get_inn("1658008723"))
