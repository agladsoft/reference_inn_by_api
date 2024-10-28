import re
import abc
import sqlite3
import functools
import contextlib
from __init__ import *
from pathlib import Path
from functools import reduce
from bs4 import BeautifulSoup
from operator import add, mul
from stdnum.exceptions import *
from requests import Response, Session
from stdnum.util import clean, isdigits
import xml.etree.ElementTree as ElemTree
from typing import Union, List, Optional
from deep_translator import GoogleTranslator


def retry_on_failure(attempts: int = 3, delay: int = 20):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt >= attempts:
                        logger.error(f"All {attempts} attempts failed: {e}")
                        return None  # Возвращаем None после всех неудачных попыток
                    logger.warning(
                        f"Connection failed ({e}), retrying in {delay} seconds... (Attempt {attempt}/{attempts})"
                    )
                    time.sleep(delay)
        return wrapper
    return decorator


class UnifiedCompaniesManager:
    def __init__(self, only_russian):
        if only_russian:
            self.unified_companies = [
                UnifiedRussianCompanies(),
                UnifiedKazakhstanCompanies(),
                UnifiedBelarusCompanies(),
                # UnifiedUzbekistanCompanies()
            ]
        else:
            self.unified_companies = [
                UnifiedKazakhstanCompanies(),
                UnifiedBelarusCompanies(),
                # UnifiedUzbekistanCompanies()
            ]

    def get_valid_company(self, company_data):
        for unified_company in self.unified_companies:
            with contextlib.suppress(Exception):
                if unified_company.is_valid(company_data):
                    yield unified_company

    @staticmethod
    def fetch_company_name(countries, taxpayer_id):
        for country_obj in countries:
            if rows := country_obj.cur.execute(
                f'SELECT * FROM "{country_obj.table_name}" WHERE taxpayer_id=? AND country=?',
                (taxpayer_id, str(country_obj),),
            ).fetchall():
                yield rows[0][1], rows[0][2], True
            else:
                yield country_obj.get_company_by_taxpayer_id(taxpayer_id), str(country_obj), False


class BaseUnifiedCompanies(abc.ABC):
    def __init__(self):
        self.table_name: str = "cache_taxpayer_id"
        self.conn: sqlite3.Connection = sqlite3.connect(self.create_file_for_cache(), check_same_thread=False)
        self.cur: sqlite3.Cursor = self.load_cache()

    @abc.abstractmethod
    def is_valid(self, number: str) -> bool:
        pass

    @abc.abstractmethod
    def get_company_by_taxpayer_id(self, taxpayer_id: str) -> Optional[str]:
        pass

    @staticmethod
    def create_file_for_cache() -> str:
        """
        Creating a file for recording INN caches and sentence.
        """
        path_cache: str = f"{os.environ.get('XL_IDP_ROOT_UNZIPPING')}/cache/cache.db"
        fle: Path = Path(path_cache)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        fle.touch(exist_ok=True)
        return path_cache

    def load_cache(self) -> sqlite3.Cursor:
        """
        Loading the cache.
        """
        cur: sqlite3.Cursor = self.conn.cursor()
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.table_name}(
               taxpayer_id TEXT PRIMARY KEY,
               company_name TEXT,
               country TEXT)
            """)
        self.conn.commit()
        return cur

    @staticmethod
    def get_response(url, country, method="GET", data=None):
        """
        Getting response from site.
        """
        proxy: str = next(CYCLED_PROXIES)
        used_proxy: Optional[str] = None
        try:
            session: Session = requests.Session()
            session.proxies = {"http": proxy}
            if method == "POST":
                response = session.post(url, json=data, timeout=120)
            else:
                response = session.get(url, timeout=120)
            logger.info(f"Статус запроса {response.status_code}. URL - {url}. Country - {country}")
            used_proxy = session.proxies.get('http')  # или 'https', в зависимости от протокола
            logger.info(f'Использованный прокси: {used_proxy}')
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred during the API request - {e}. Proxy - {used_proxy}")
            raise requests.exceptions.RequestException from e

    def cache_add_and_save(self, taxpayer_id: str, company_name: str, country: Union[str, list]) -> None:
        """
        Saving and adding the result to the cache.
        """
        self.cur.executemany(f"INSERT or REPLACE INTO {self.table_name} VALUES(?, ?, ?)",
                             [(taxpayer_id, company_name, str(country))])
        self.conn.commit()


class UnifiedRussianCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "russia"

    def __repr__(self):
        return "russia"

    @staticmethod
    def calc_company_check_digit(number):
        """
        Calculate the check digit for the 10-digit ??? for organisations.
        :param number:
        :return:
        """
        weights = (2, 4, 10, 3, 5, 9, 4, 6, 8)
        return str(sum(w * int(n) for w, n in zip(weights, number)) % 11 % 10)

    @staticmethod
    def calc_personal_check_digits(number):
        """"
        "Calculate the check digits for the 12-digit personal ???.
        :param number:
        :return:
        """
        weights = (7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
        d1 = str(sum(w * int(n) for w, n in zip(weights, number)) % 11 % 10)
        weights = (3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
        d2 = str(sum(w * int(n) for w, n in zip(weights, number[:10] + d1)) % 11 % 10)
        return d1 + d2

    def validate(self, number):
        """
        Check if the number is a valid ???. This checks the length, formatting and check digit.
        :param number:
        :return:
        """
        number = clean(number, ' ').strip()
        if not isdigits(number):
            raise InvalidFormat()
        if len(number) == 10:
            if self.calc_company_check_digit(number) != number[-1]:
                raise InvalidChecksum()
        elif len(number) == 12:
            # persons
            if self.calc_personal_check_digits(number) != number[-2:]:
                raise InvalidChecksum()
        else:
            raise InvalidLength()
        return number

    def is_valid(self, number):
        """Check if the number is a valid ???."""
        try:
            return bool(self.validate(number))
        except ValidationError:
            return False

    @retry_on_failure(attempts=3, delay=5)
    def get_company_by_taxpayer_id(self, taxpayer_id: str):
        """
        Getting the company name unified from the cache, if there is one.
        Otherwise, we are looking for verification of legal entities on websites.
        :param taxpayer_id:
        :return:
        """
        data: dict = {
            "inn": taxpayer_id
        }
        response: Response = requests.post(f"http://{IP_ADDRESS_DADATA}:8003", json=data)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, str):
            return None
        elif data[0]:
            company_name = data[0][0].get('value')
            self.cache_add_and_save(taxpayer_id, company_name, self.__str__())
            return company_name


class UnifiedKazakhstanCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "kazakhstan"

    def __repr__(self):
        return "kazakhstan"

    @staticmethod
    def multiply(weights: List[int], number: str) -> int:
        return reduce(add, map(lambda i: mul(*i), zip(map(int, number), weights)))

    def is_valid(self, number):
        if len(number) != 12:
            return False
        w1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        w2 = [3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2]
        check_sum = self.multiply(w1, number) % 11
        if check_sum == 10:
            check_sum = self.multiply(w2, number) % 11
        return check_sum == int(number[-1])

    @retry_on_failure(attempts=3, delay=5)
    def get_company_by_taxpayer_id(self, taxpayer_id: str):
        """

        :param taxpayer_id:
        :return:
        """
        data = {
            "page": "1",
            "size": 10,
            "value": taxpayer_id
        }
        if response := self.get_response(
            "https://pk.uchet.kz/api/web/company/search/", self.__str__(), method="POST", data=data
        ):
            company_name: Optional[str] = None
            for result in response.json()["results"]:
                company_name = result["name"]
                break
            logger.info(f"Company name is {company_name}. BIN is {taxpayer_id}")
            self.cache_add_and_save(taxpayer_id, company_name, self.__str__())
            return company_name


class UnifiedBelarusCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "belarus"

    def __repr__(self):
        return "belarus"

    def is_valid(self, number):
        """

        :param number:
        :return:
        """
        if len(number) != 9 or number == '000000000':
            return False

        weights = [29, 23, 19, 17, 13, 7, 5, 3]

        checksum = sum(int(d) * w for d, w in zip(number[:-1], weights))
        checksum = checksum % 11
        if checksum == 10:
            checksum = sum(int(d) * w for d, w in zip(number[:-1], weights[1:]))
            checksum = checksum % 11

        return checksum == int(number[-1])

    @retry_on_failure(attempts=3, delay=5)
    def get_company_by_taxpayer_id(self, taxpayer_id: str):
        """

        :param taxpayer_id:
        :return:
        """
        if response := self.get_response(
            f"https://www.portal.nalog.gov.by/grp/getData?unp={taxpayer_id}&charset=UTF-8&type=json", self.__str__()
        ):
            row = response.json()['row']
            data = {'unp': row['vunp'], 'company_name': row['vnaimk']}
            logger.info(f"Company name is {data['company_name']}. UNP is {taxpayer_id}")
            self.cache_add_and_save(taxpayer_id, data['company_name'], self.__str__())
            return data['company_name']


class UnifiedUzbekistanCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "uzbekistan"

    def __repr__(self):
        return "uzbekistan"

    def is_valid(self, number):
        return False if len(number) != 9 else bool(re.match(r'[3-8]', number))

    @retry_on_failure(attempts=3, delay=5)
    def get_company_by_taxpayer_id(self, taxpayer_id: str):
        """

        :param taxpayer_id:
        :return:
        """
        if response := self.get_response(
            f"http://orginfo.uz/en/search/all?q={taxpayer_id}", self.__str__()
        ):
            soup = BeautifulSoup(response.text, "html.parser")
            a = soup.find_all('div', class_='card-body pt-0')[-1]
            if name := a.find_next('h6', class_='card-title'):
                name = name.text.replace('\n', '').strip()
            logger.info(f"Company name is {name}. INN is {taxpayer_id}")
            try:
                company_name: str = GoogleTranslator(source='uz', target='ru').translate(name[:4500])
            except Exception as e:
                logger.error(f"Exception is {e}")
                company_name = name
            self.cache_add_and_save(taxpayer_id, company_name, self.__str__())
            return company_name


class SearchEngineParser(BaseUnifiedCompanies):
    def __init__(self, country, manager):
        super().__init__()
        self.table_name = "search_engine"
        self.cur: sqlite3.Cursor = self.load_cache()
        self.country = country
        self.manager = manager

    def is_valid(self, number: str) -> bool:
        pass

    def get_company_by_taxpayer_id(self, taxpayer_id: str) -> Optional[str]:
        pass

    def get_inn_from_site(self, dict_inn: dict, values: list, count_inn: int) -> None:
        """
        Parse each site (description and title).
        """
        for item_inn in values:
            countries = list(self.manager.get_valid_company(item_inn))
            for country in countries:
                self.country if country in self.country else self.country.append(country)
                dict_inn[item_inn] = dict_inn[item_inn] + 1 if item_inn in dict_inn else count_inn

    @staticmethod
    def get_code_error(error_code: ElemTree, value: str) -> None:
        """
        Getting error codes from xml_river.
        """
        if error_code.tag == 'error':
            code: Union[str, None] = error_code.attrib.get('code')
            message: str = MESSAGE_TEMPLATE.get(code, "Not found code error. Exception - {}. "
                                                      "Value - {}. Error code - {}")
            message = message.format(error_code.text, value, code)
            prefix: str = PREFIX_TEMPLATE.get(code, "необработанная_ошибка_на_строке_")
            logger.error(f"Error code is {code}. Message is {message}. Prefix is {prefix}")
            if code == '200':
                raise AssertionError(message)
            else:
                raise ConnectionRefusedError(message)

    def parse_xml(self, response: Response, value: str, dict_inn: dict, count_inn: int):
        """
        Parsing xml.
        """
        # Parse the XML data
        root = ElemTree.fromstring(response.text)
        self.get_code_error(root[0][0], value)

        # Find all title and passage elements
        for doc in root.findall(".//doc"):
            title = doc.find('title').text or ''
            passage = doc.find('.//passage').text or ''
            inn_text: list = re.findall(r"\d+", passage)
            inn_title: list = re.findall(r"\d+", title)
            self.get_inn_from_site(dict_inn, inn_text, count_inn)
            self.get_inn_from_site(dict_inn, inn_title, count_inn)

    def get_inn_from_search_engine(self, value: str) -> dict:
        """
        Looking for the INN in the search engine, and then we parse through the sites.
        """
        logger.info(f"Before request. Data is {value}")
        try:
            r: Response = requests.get(
                f"https://xmlriver.com/search_yandex/xml?user={USER_XML_RIVER}&key={KEY_XML_RIVER}&query={value} ИНН",
                timeout=120
            )
        except Exception as e:
            logger.error(f"Run time out. Data is {value}. Exception is {e}")
            raise AssertionError from e
        logger.info(f"After request. Data is {value}")
        dict_inn: dict = {}
        count_inn: int = 1
        self.parse_xml(r, value, dict_inn, count_inn)
        logger.info(f"Dictionary with INN is {dict_inn}. Data is {value}")
        return dict_inn

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

    @retry_on_failure(attempts=3, delay=5)
    def get_taxpayer_id(self, value: str):
        """
        Getting the INN from the cache, if there is one. Otherwise, we search in the search engine.
        """
        api_inn: dict = {}
        rows: sqlite3.Cursor = self.cur.execute(f'SELECT * FROM "{self.table_name}" WHERE taxpayer_id=?', (value,), )
        if (list_rows := list(rows)) and list_rows[0][1]:
            logger.info(f"Data is {list_rows[0][0]}. INN is {list_rows[0][1]}")
            return api_inn, list_rows[0][1], True
        api_inn: dict = self.get_inn_from_search_engine(value)
        best_found_inn = max(api_inn, key=api_inn.get, default=None)
        return api_inn, best_found_inn, False


if __name__ == "__main__":
    print(list(UnifiedCompaniesManager(only_russian=True).get_valid_company("302928567")))
