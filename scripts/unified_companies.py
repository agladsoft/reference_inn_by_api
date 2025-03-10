import re
import abc
import sqlite3
import functools
import contextlib
from pathlib import Path
from functools import reduce
from operator import add, mul
from scripts.__init__ import *
from stdnum.exceptions import *
from requests import Response, Session
from stdnum.util import clean, isdigits
import xml.etree.ElementTree as ElemTree
from xml.etree.ElementTree import Element
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup, PageElement
from typing import Union, List, Optional, Any, Generator, Tuple


def retry_on_failure(attempts: int = 3, delay: int = 20) -> Optional[callable]:
    """
    A decorator that retries a function if it raises an exception.

    The function is called up to `attempts` times if it raises an exception. If all
    attempts fail, the last exception is re-raised.

    :param attempts: The number of times to retry the function. Defaults to 3.
    :param delay: The number of seconds to wait between retries. Defaults to 20.
    """
    def decorator(func: callable):
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
                        raise HTTPError(e)
                    logger.warning(
                        f"Connection failed ({e}), retrying in {delay} seconds... (Attempt {attempt}/{attempts})"
                    )
                    time.sleep(delay)
            return None
        return wrapper
    return decorator


class UnifiedCompaniesManager:
    def __init__(self, with_russian: bool):
        if with_russian:
            self.unified_companies: List[callable] = [
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

    def get_valid_company(self, company_data: str) -> Generator:
        """
        Yield valid company instances based on the provided company data.

        This method iterates over the list of unified company objects and checks
        if the given company data is valid for each of them. If the company data
        is valid for a particular unified company, it yields that company instance.

        :param company_data: The company data to be validated against the unified companies.
        :return: A generator yielding valid unified company instances.
        """
        for unified_company in self.unified_companies:
            with contextlib.suppress(Exception):
                if unified_company.is_valid(company_data):
                    yield unified_company

    @staticmethod
    def query_database(country_obj: callable, taxpayer_id: str) -> tuple:
        """
        Query the database for a company entry based on the given taxpayer ID and country object.

        The method executes a SQL query with the provided taxpayer ID and country object as parameters.
        The query is executed on the database connection of the country object, and the results are returned
        as a tuple of all matching entries.

        :param country_obj: The unified company object to query the database for.
        :param taxpayer_id: The taxpayer ID to query the database for.
        :return: A tuple of all matching entries from the database query.
        """
        query: str = f'SELECT * FROM "{country_obj.table_name}" WHERE taxpayer_id=? AND country=?'
        return country_obj.cur.execute(query, (taxpayer_id, str(country_obj))).fetchall()

    @staticmethod
    def handle_valid_taxpayer(country_obj: callable, taxpayer_id: str, index: int, sentence: str) -> tuple:
        """
        Attempt to retrieve and return the company name for a given taxpayer ID using a specific country object.

        This method attempts to fetch the company name associated with a given taxpayer ID by calling
        the `get_company_by_taxpayer_id` method of the provided country object. If successful, it returns
        the company name, the string representation of the country object, and a boolean indicating success.
        If an exception occurs, it logs the error, appends the error information to the global `ERRORS` list,
        and returns a tuple with None values and a boolean indicating failure.

        :param country_obj: The unified company object used to retrieve the company name.
        :param taxpayer_id: The taxpayer ID for which to retrieve the company name.
        :param index: The index used for logging purposes, typically indicating position in a dataset.
        :param sentence: The sentence or context in which the taxpayer ID is being processed.
        :return: A tuple containing the company name (or None if not found), the string representation of the
                 country object (or None in case of an error), and a boolean indicating the success of the operation.
        """
        try:
            company = country_obj.get_company_by_taxpayer_id(taxpayer_id)
            return company, str(country_obj), False
        except Exception as ex:
            ERRORS.append(f'Exception: {ex}. Data: {index}, {sentence}')
            logger.error(f"Exception: {ex}. Data: {index}, {sentence}")
            return None, None, False

    def fetch_company_name(self, countries: Optional[Any], taxpayer_id: str, index: int, sentence: str) -> Generator:
        """
        Generator to fetch company names from the database or through a unified company object.

        This generator iterates over the countries provided and attempts to fetch the company name
        associated with the given taxpayer ID. It first checks if the taxpayer ID is present in the
        database, and if so, yields the company name and the string representation of the country
        object. If not, it checks if the taxpayer ID is valid according to the unified company
        object, and if so, it calls the `handle_valid_taxpayer` method to retrieve the company name.
        If the taxpayer ID is not valid according to the unified company object, it yields None values
        and a boolean indicating failure.

        :param countries: An object containing country information.
        :param taxpayer_id: The taxpayer ID for which to retrieve the company name.
        :param index: The index used for logging purposes, typically indicating position in a dataset.
        :param sentence: The sentence or context in which the taxpayer ID is being processed.
        :return: A generator yielding tuples containing the company name (or None if not found), the string
                 representation of the country object (or None in case of an error), and a boolean indicating
                 the success of the operation.
        """
        for country_obj in countries:
            if rows := self.query_database(country_obj, taxpayer_id):
                yield rows[0][1], rows[0][2], True
            elif country_obj.is_valid(taxpayer_id):
                yield self.handle_valid_taxpayer(country_obj, taxpayer_id, index, sentence)
            else:
                yield None, None, False


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
        Creates a cache file for storing INN data.

        This static method constructs the path for a cache file using the environment
        variable 'XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS'. It ensures that the directory
        exists, creates the cache file if it does not already exist, and returns the path
        to the cache file.

        :return: The path to the created cache file as a string.
        """
        path_cache: str = f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/cache/cache.db"
        fle: Path = Path(path_cache)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        fle.touch(exist_ok=True)
        return path_cache

    def load_cache(self) -> sqlite3.Cursor:
        """
        Initializes the cache by creating a SQLite table if it does not exist.

        This method establishes a cursor to the SQLite database connection and executes
        a SQL statement to create a table named after `self.table_name` if it does not
        already exist. The table consists of three columns: 'taxpayer_id', 'company_name',
        and 'country', where 'taxpayer_id' serves as the primary key. After executing
        the SQL command, the changes are committed to the database.

        :return: A SQLite cursor object for the database connection.
        """
        cur: sqlite3.Cursor = self.conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.table_name}(
            taxpayer_id TEXT PRIMARY KEY,
            company_name TEXT,
            country TEXT)
        """)
        self.conn.commit()
        return cur

    @staticmethod
    def get_response(url, country, method="GET", data=None):
        """
        Sends an HTTP request to the specified URL using a proxy from a cycled list.
    
        This static method initiates an HTTP session with a proxy and sends either a 
        GET or POST request to the given URL. It logs the request status, URL, and 
        country associated with the request. If the request is successful, it returns 
        the response object. If an error occurs, it logs the error and raises an 
        exception.
    
        :param url: The URL to send the request to.
        :param country: The country associated with the request for logging purposes.
        :param method: The HTTP method to use for the request, either "GET" or "POST" (default is "GET").
        :param data: The data to send in the request body if using POST (default is None).
        :return: The response object from the request.
        :raises: requests.exceptions.RequestException if an error occurs during the request.
        """
        proxy: str = next(CYCLED_PROXIES)
        used_proxy: Optional[str] = None
        response: Optional[requests.Response] = None
        try:
            session: Session = requests.Session()
            session.proxies = {"http": proxy}
            if method == "POST":
                response = session.post(url, json=data, timeout=120)
            else:
                response = session.get(url, timeout=120)
            logger.info(f"Статус запроса {response.status_code}. URL - {url}. Country - {country}")
            used_proxy = session.proxies.get('http')  # или 'https', в зависимости от протокола
            logger.info(f"Использованный прокси: {used_proxy}")
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if response.status_code == 400 and response.headers.get("Content-Type") == "application/json":
                with contextlib.suppress(ValueError):
                    error_data = response.json()
                    if error_data.get("message") == "Нет данных по запросу":
                        return None
            logger.error(f"An error occurred during the API request - {e}. Proxy - {used_proxy}")
            raise e

    def cache_add_and_save(self, taxpayer_id: str, company_name: str, country: Union[str, list]) -> None:
        """
        Saves the given taxpayer ID, company name, and country to the cache.
        
        This method takes the given taxpayer ID, company name, and country and adds them to
        the cache. It uses the `executemany` method of the SQLite cursor to insert or replace
        the values in the cache table. After execution, it commits the changes to the database.
        
        :param taxpayer_id: The taxpayer ID to be cached.
        :param company_name: The company name to be cached.
        :param country: The country to be cached, either as a string or a list of strings.
        :return: None
        """
        self.cur.executemany(
            f"INSERT or REPLACE INTO {self.table_name} VALUES(?, ?, ?)",
            [(taxpayer_id, company_name, str(country))]
        )
        self.conn.commit()


class UnifiedRussianCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "russia"

    def __repr__(self):
        return "russia"

    def __eq__(self, other: callable) -> bool:
        return isinstance(other, UnifiedRussianCompanies)

    @staticmethod
    def calc_company_check_digit(taxpayer_id: str) -> str:
        """
        Calculate the check digit for the 10-digit company taxpayer ID.

        This method uses the weights (2, 4, 10, 3, 5, 9, 4, 6, 8) to calculate the check digit
        for the given taxpayer ID. It returns the calculated check digit as a string.

        :param taxpayer_id: The taxpayer ID to be validated.
        :return: The calculated check digit as a string.
        """
        weights: tuple = (2, 4, 10, 3, 5, 9, 4, 6, 8)
        return str(sum(w * int(n) for w, n in zip(weights, taxpayer_id)) % 11 % 10)

    @staticmethod
    def calc_personal_check_digits(taxpayer_id: str) -> str:
        """
        Calculate the two check digits for the 12-digit personal taxpayer ID.

        This method uses the weights (7, 2, 4, 10, 3, 5, 9, 4, 6, 8) to calculate the first check digit
        and the weights (3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8) to calculate the second check digit
        for the given taxpayer ID. It returns the calculated check digits as a string.

        :param taxpayer_id: The taxpayer ID to be validated.
        :return: The calculated check digits as a string.
        """
        weights: tuple = (7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
        d1: str = str(sum(w * int(n) for w, n in zip(weights, taxpayer_id)) % 11 % 10)
        weights: tuple = (3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
        d2: str = str(sum(w * int(n) for w, n in zip(weights, taxpayer_id[:10] + d1)) % 11 % 10)
        return d1 + d2

    def validate(self, taxpayer_id: str) -> str:
        """
        Validate the given taxpayer ID.
    
        This method takes a taxpayer ID as input, cleans and validates it according to the rules
        defined in the `UnifiedRussianCompanies` class, and returns the validated taxpayer ID
        as a string.
    
        Validation rules:
        - The taxpayer ID must be a string of digits.
        - If the taxpayer ID is 10 digits long, the check digit must match the result of the
          `calc_company_check_digit` method.
        - If the taxpayer ID is 12 digits long, the two check digits must match the result of the
          `calc_personal_check_digits` method.
        - If the taxpayer ID is not 10 or 12 digits long, an `InvalidLength` exception is raised.
    
        :param taxpayer_id: The taxpayer ID to be validated.
        :return: The validated taxpayer ID as a string.
        :raises: InvalidFormat, InvalidChecksum, InvalidLength
        """
        taxpayer_id: str = clean(taxpayer_id, ' ').strip()
        if not isdigits(taxpayer_id):
            raise InvalidFormat()
        if len(taxpayer_id) == 10:
            if self.calc_company_check_digit(taxpayer_id) != taxpayer_id[-1]:
                raise InvalidChecksum()
        elif len(taxpayer_id) == 12:
            # persons
            if self.calc_personal_check_digits(taxpayer_id) != taxpayer_id[-2:]:
                raise InvalidChecksum()
        else:
            raise InvalidLength()
        return taxpayer_id

    def is_valid(self, taxpayer_id: str) -> bool:
        """
        Checks if the given taxpayer ID is valid according to the rules defined in the
        `UnifiedRussianCompanies` class.
    
        The `is_valid` method takes a taxpayer ID as input and returns a boolean indicating whether
        the taxpayer ID is valid or not. It uses the `validate` method to validate the taxpayer ID
        and catches any exceptions raised by the `validate` method.
    
        :param taxpayer_id: The taxpayer ID to be validated.
        :return: A boolean indicating whether the taxpayer ID is valid or not.
        """
        try:
            return bool(self.validate(taxpayer_id))
        except ValidationError:
            return False

    @retry_on_failure(attempts=3, delay=5)
    def get_company_by_taxpayer_id(self, taxpayer_id: str) -> Optional[str]:
        """
        Retrieve the company name for a given taxpayer ID.
    
        This method sends a POST request to the specified server to retrieve
        the company name associated with the provided taxpayer ID. It expects
        a JSON response containing the company name. If the response is valid,
        it caches the result using the `cache_add_and_save` method.
    
        :param taxpayer_id: The taxpayer ID for which the company name is to be retrieved.
        :return: The company name if found, otherwise None.
        :raises: HTTPError if the request to the server fails.
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
        return None


class UnifiedKazakhstanCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "kazakhstan"

    def __repr__(self):
        return "kazakhstan"

    def __eq__(self, other: callable) -> bool:
        return isinstance(other, UnifiedKazakhstanCompanies)

    @staticmethod
    def multiply(weights: List[int], number: str) -> int:
        """
        Multiply each digit of the given number with the corresponding weight from the given list of weights
        and sum them up.

        :param weights: The list of weights to use for multiplication.
        :param number: The number to multiply with the weights.
        :return: The sum of the products of the digits of the number and the weights.
        """
        return reduce(add, map(lambda i: mul(*i), zip(map(int, number), weights))) # type: ignore

    def is_valid(self, taxpayer_id: str) -> bool:
        """
        Checks if the given taxpayer ID is valid according to the rules defined in the
        `UnifiedKazakhstanCompanies` class.
    
        The `is_valid` method takes a taxpayer ID as input and returns a boolean indicating whether
        the taxpayer ID is valid or not. It checks if the taxpayer ID is a string of digits and
        if its length is 12. Then it calculates the check digit using the weights (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
        and the weights (3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2) and compares it with the last digit of the
        taxpayer ID.
    
        :param taxpayer_id: The taxpayer ID to be validated.
        :return: A boolean indicating whether the taxpayer ID is valid or not.
        """
        if not isdigits(taxpayer_id):
            return False
        if len(taxpayer_id) != 12:
            return False
        w1: list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        w2: list = [3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2]
        check_sum: int = self.multiply(w1, taxpayer_id) % 11
        if check_sum == 10:
            check_sum = self.multiply(w2, taxpayer_id) % 11
        return check_sum == int(taxpayer_id[-1])

    @retry_on_failure(attempts=3, delay=5)
    def get_company_by_taxpayer_id(self, taxpayer_id: str) -> Optional[str]:
        """
        Retrieve a company name by its taxpayer ID.

        The `get_company_by_taxpayer_id` method takes a taxpayer ID as input and returns
        the corresponding company name. It uses the `pk.uchet.kz` API to retrieve the company
        name. The method is decorated with `retry_on_failure` to retry on failure with a
        delay of 5 seconds and a maximum of 3 attempts.

        The method first constructs a dictionary with the taxpayer ID and sends a POST
        request to the API. It then parses the response JSON and retrieves the first
        result's name and assigns it to `company_name`. The method logs the company name
        and the taxpayer ID and saves it to the cache using `cache_add_and_save`. Finally,
        it returns the company name.

        :param taxpayer_id: The taxpayer ID to retrieve the company name for.
        :return: The company name associated with the given taxpayer ID.
        """
        data: dict = {
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
            logger.info(f"Company name: {company_name}. BIN: {taxpayer_id}")
            self.cache_add_and_save(taxpayer_id, company_name, self.__str__())
            return company_name
        return None


class UnifiedBelarusCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "belarus"

    def __repr__(self):
        return "belarus"

    def __eq__(self, other: callable) -> bool:
        return isinstance(other, UnifiedBelarusCompanies)

    def is_valid(self, taxpayer_id: str) -> bool:
        """
        Validate a taxpayer ID.

        This method takes a taxpayer ID as input and checks if it is valid according to the
        rules defined in the `UnifiedBelarusCompanies` class. It first checks if the
        taxpayer ID is a string of 9 digits and is not equal to '000000000'. It then
        calculates the checksum of the taxpayer ID using the weights [29, 23, 19, 17, 13, 7, 5, 3]
        and checks if it matches the last digit of the taxpayer ID. If the checksum is equal
        to 10, it recalculates the checksum using the weights [23, 19, 17, 13, 7, 5, 3, 2]
        and checks if it matches the last digit of the taxpayer ID.

        :param taxpayer_id: The taxpayer ID to be validated.
        :return: A boolean indicating whether the taxpayer ID is valid or not.
        """
        if not isdigits(taxpayer_id):
            return False
        if len(taxpayer_id) != 9 or taxpayer_id == '000000000':
            return False

        weights: list = [29, 23, 19, 17, 13, 7, 5, 3]

        checksum: int = sum(int(d) * w for d, w in zip(taxpayer_id[:-1], weights))
        checksum %= 11
        if checksum == 10:
            checksum = sum(int(d) * w for d, w in zip(taxpayer_id[:-1], weights[1:]))
            checksum = checksum % 11

        return checksum == int(taxpayer_id[-1])

    @retry_on_failure(attempts=3, delay=5)
    def get_company_by_taxpayer_id(self, taxpayer_id: str) -> Optional[str]:
        """
        Retrieve a company name by its taxpayer ID.

        The `get_company_by_taxpayer_id` method takes a taxpayer ID as input and returns
        the corresponding company name. It uses the `portal.nalog.gov.by` API to retrieve
        the company name. The method is decorated with `retry_on_failure` to retry on failure
        with a delay of 5 seconds and a maximum of 3 attempts.

        The method first constructs a URL with the taxpayer ID and sends a GET request to
        the API. It then parses the response JSON and retrieves the first result's name
        and assigns it to `company_name`. The method logs the company name and the taxpayer
        ID and saves it to the cache using `cache_add_and_save`. Finally, it returns the
        company name.

        :param taxpayer_id: The taxpayer ID to retrieve the company name for.
        :return: The company name associated with the given taxpayer ID, or None if the
            request fails.
        """
        if response := self.get_response(
            f"https://www.portal.nalog.gov.by/grp/getData?unp={taxpayer_id}&charset=UTF-8&type=json", self.__str__()
        ):
            row: dict = response.json()['row']
            data: dict = {'unp': row['vunp'], 'company_name': row['vnaimk']}
            logger.info(f"Company name: {data['company_name']}. UNP: {taxpayer_id}")
            self.cache_add_and_save(taxpayer_id, data['company_name'], self.__str__())
            return data['company_name']
        return None


class UnifiedUzbekistanCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "uzbekistan"

    def __repr__(self):
        return "uzbekistan"

    def __eq__(self, other: callable) -> bool:
        return isinstance(other, UnifiedUzbekistanCompanies)

    def is_valid(self, taxpayer_id: str) -> bool:
        """
        Checks if the given taxpayer ID is valid according to the rules defined in the
        `UnifiedUzbekistanCompanies` class.

        The `is_valid` method takes a taxpayer ID as input and returns a boolean indicating whether
        the taxpayer ID is valid or not. It checks if the taxpayer ID is a string of digits and
        if its length is 9. It also checks if the first digit of the taxpayer ID is in the range
        3 to 8 (inclusive).

        :param taxpayer_id: The taxpayer ID to be validated.
        :return: A boolean indicating whether the taxpayer ID is valid or not.
        """
        if not isdigits(taxpayer_id):
            return False
        return False if len(taxpayer_id) != 9 else bool(re.match(r'[3-8]', taxpayer_id))

    @retry_on_failure(attempts=3, delay=5)
    def get_company_by_taxpayer_id(self, taxpayer_id: str) -> Optional[str]:
        """
        Retrieve a company name by its taxpayer ID.

        The `get_company_by_taxpayer_id` method takes a taxpayer ID as input and returns
        the corresponding company name. It uses the `orginfo.uz` API to retrieve
        the company name. The method is decorated with `retry_on_failure` to retry on failure
        with a delay of 5 seconds and a maximum of 3 attempts.

        The method first constructs a URL with the taxpayer ID and sends a GET request to
        the API. It then parses the response HTML and retrieves the last result's name
        and assigns it to `company_name`. The method logs the company name and the taxpayer
        ID and saves it to the cache using `cache_add_and_save`. Finally, it returns the
        company name.

        :param taxpayer_id: The taxpayer ID to retrieve the company name for.
        :return: The company name associated with the given taxpayer ID, or None if the
            request fails.
        """
        if response := self.get_response(
            f"http://orginfo.uz/en/search/all?q={taxpayer_id}", self.__str__()
        ):
            soup: BeautifulSoup = BeautifulSoup(response.text, "html.parser")
            a: PageElement = soup.find_all('div', class_='card-body pt-0')[-1]
            if name := a.find_next('h6', class_='card-title'):
                company_name: Optional[str] = None if name.text in (None, "None") else name.text.replace('\n', '').strip()
            else:
                company_name = name
            logger.info(f"Company name: {company_name}. INN: {taxpayer_id}")
            self.cache_add_and_save(taxpayer_id, company_name, self.__str__())
            return company_name
        return None


class SearchEngineParser(BaseUnifiedCompanies):
    def __init__(self, country: Optional[List[object]], manager: callable):
        super().__init__()
        self.table_name: str = "search_engine"
        self.cur: sqlite3.Cursor = self.load_cache()
        self.country: Optional[List[object]] = country
        self.manager: callable = manager

    def is_valid(self, number: str) -> bool:
        pass

    def get_company_by_taxpayer_id(self, taxpayer_id: str) -> Optional[str]:
        pass

    def get_inn_from_site(self, dict_inn: dict, values: list, count_inn: int) -> None:
        """
        Method for getting count of INNs from site.

        This method takes a dictionary `dict_inn`, a list of values `values` and a count of INNs `count_inn` as input.
        It iterates over the list of values and for each value, it gets a list of valid companies from the manager.
        It then iterates over the list of valid companies and adds the company to the `country` list if it is not already in it.
        Finally, it increments the value of the item in the `dict_inn` dictionary or adds it to the dictionary if it is not already in it.

        :param dict_inn: A dictionary of INNs.
        :param values: A list of values to be processed.
        :param count_inn: The count of INNs.
        :return: None
        """
        for item_inn in values:
            countries: List[object] = list(self.manager.get_valid_company(item_inn))
            for country in countries:
                self.country if country in self.country else self.country.append(country)
                dict_inn[item_inn] = dict_inn[item_inn] + 1 if item_inn in dict_inn else count_inn

    @staticmethod
    def get_code_error(error_code: ElemTree, value: str) -> None:
        """
        Method for getting code error from xml.

        This method takes an xml element `error_code` and a value `value` as input.
        It then parses the attribute 'code' of the xml element and the value of the attribute 'code'.
        It then constructs a message based on the value of the attribute 'code' and the value `value`.
        It then logs the message and raises an exception based on the value of the attribute 'code'.

        :param error_code: The xml element containing the error code.
        :param value: The value associated with the error code.
        :raises AssertionError: If the code is '200'.
        :raises ConnectionRefusedError: If the code is not '200'.
        :return: None
        """
        if error_code.tag == 'error':
            code: Union[str, None] = error_code.attrib.get('code')
            message: str = MESSAGE_TEMPLATE.get(
                code, "Not found code error. Exception - {}. Value - {}. Error code - {}"
            )
            message = message.format(error_code.text, value, code)
            prefix: str = PREFIX_TEMPLATE.get(code, "необработанная_ошибка_на_строке_")
            logger.error(f"Error code: {code}. Message: {message}. Prefix: {prefix}")
            if code == '200':
                raise AssertionError(message)
            else:
                raise ConnectionRefusedError(message)

    def parse_xml(self, response: Response, value: str, dict_inn: dict, count_inn: int) -> None:
        """
        Method for parsing XML data from the search engine.

        This method takes a requests Response object `response`, a value `value`, a dictionary `dict_inn`, and a count of INNs `count_inn` as input.
        It parses the XML data and checks for errors. If an error is found, it calls the `get_code_error` method to handle the error.
        It then iterates over the XML elements and finds all title and passage elements. For each element, it extracts the INNs and calls the
        `get_inn_from_site` method to add the INNs to the `dict_inn` dictionary.

        :param response: The requests Response object containing the XML data.
        :param value: The value associated with the request.
        :param dict_inn: A dictionary of INNs.
        :param count_inn: The count of INNs.
        :raises AssertionError: If an error is found in the XML data.
        :raises ConnectionRefusedError: If an error is found in the XML data.
        :return: None
        """
        # Parse the XML data
        root: Element = ElemTree.fromstring(response.text)
        self.get_code_error(root[0][0], value)

        # Find all title and passage elements
        for doc in root.findall(".//doc"):
            title: str = doc.find('title').text or ''
            passage: str = doc.find('.//passage').text or ''
            inn_text: list = re.findall(r"\d+", passage)
            inn_title: list = re.findall(r"\d+", title)
            self.get_inn_from_site(dict_inn, inn_text, count_inn)
            self.get_inn_from_site(dict_inn, inn_title, count_inn)

    @retry_on_failure(attempts=3, delay=5)
    def get_inn_from_search_engine(self, value: str) -> dict:
        """
        Method for getting a dictionary of INNs from the search engine.

        This method takes a value `value` as input and returns a dictionary of INNs.
        It sends a GET request to the XML River API with the value and the keyword 'ИНН'.
        It then parses the XML data and checks for errors. If an error is found, it calls the `get_code_error` method to handle the error.
        It then iterates over the XML elements and finds all title and passage elements. For each element, it extracts the INNs and calls the
        `get_inn_from_site` method to add the INNs to the `dict_inn` dictionary.

        The method is decorated with `retry_on_failure` to retry on failure with a delay of 5 seconds and a maximum of 3 attempts.

        :param value: The value associated with the request.
        :return: A dictionary of INNs.
        """
        logger.info(f"Before request. Data: {value}")
        try:
            r: Response = requests.get(
                f"https://xmlriver.com/search_yandex/xml?user={USER_XML_RIVER}&key={KEY_XML_RIVER}&query={value} ИНН",
                timeout=120
            )
        except Exception as e:
            logger.error(f"Run time out. Data: {value}. Exception: {e}")
            raise e
        logger.info(f"After request. Data: {value}")
        dict_inn: dict = {}
        count_inn: int = 1
        self.parse_xml(r, value, dict_inn, count_inn)
        logger.info(f"Dictionary with INN: {dict_inn}. Data: {value}")
        return dict_inn

    def get_taxpayer_id(self, value: str) -> Tuple[dict, Optional[str], bool]:
        """
        Method for getting a taxpayer ID from the database or through the search engine.

        This method takes a value `value` as input and returns a tuple containing a dictionary of INNs,
        the best found INN, and a boolean indicating whether the value was found in the database.

        The method first checks if the value is present in the database, and if so, it returns the
        corresponding INN and a boolean indicating success. If the value is not present in the
        database, it calls the `get_inn_from_search_engine` method to retrieve a dictionary of INNs
        from the search engine. It then finds the best found INN by taking the maximum of the dictionary
        values. Finally, it returns the dictionary of INNs, the best found INN, and a boolean indicating
        failure.

        :param value: The value associated with the request.
        :return: A tuple containing a dictionary of INNs, the best found INN, and a boolean indicating
                 whether the value was found in the database.
        """
        rows: sqlite3.Cursor = self.cur.execute(f'SELECT * FROM "{self.table_name}" WHERE taxpayer_id=?', (value,), )
        if (list_rows := list(rows)) and list_rows[0][1]:
            logger.info(f"Data: {list_rows[0][0]}. INN: {list_rows[0][1]}")
            return {list_rows[0][1]: 1}, list_rows[0][1], True
        api_inn: dict = self.get_inn_from_search_engine(value)
        best_found_inn: Optional[str] = max(api_inn, key=api_inn.get, default=None)
        return api_inn, best_found_inn, False
