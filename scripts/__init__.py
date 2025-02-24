import os
import sys
import time
import logging
import requests
from itertools import cycle
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

load_dotenv()
# os.environ["XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS"] = "."

COUNT_THREADS: int = 3
USER_XML_RIVER: str = "6390"
KEY_XML_RIVER: str = "e3b3ac2908b2a9e729f1671218c85e12cfe643b0"
IP_ADDRESS_DADATA: str = "service_inn"
LOG_FORMAT: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
DATE_FTM: str = "%d/%B/%Y %H:%M:%S"

REPLACED_QUOTES: list = ["<", ">", "«", "»", "’", "‘", "“", "”", "`", "'", '"']
REPLACED_WORDS: list = ["ООО", "OOO", "OОO", "ОOО", "OOО", "ООO", "ОАО", "ИП", "ЗАО", "3АО", "АО"]

MESSAGE_TEMPLATE: dict = {
    '200': "The money ran out. Index is {}. Exception - {}. Value - {}",
    '110': "There are no free channels for data collection. Index is {}. Exception - {}. Value - {}",
    '15': "No results found in the search engine. Index is {}. Exception - {}. Value - {}"
}
PREFIX_TEMPLATE: dict = {
    '200': "закончились_деньги_на_строке_",
    '110': "нет_свободных_каналов_на_строке_",
    '15': "не_найдено_результатов_"
}
ERRORS = []


PROXIES: list = [
    'http://user139922:oulrqa@77.83.194.180:2337',
    'http://user139922:oulrqa@102.165.10.213:2337',
    'http://user139922:oulrqa@77.83.194.99:2337',
    'http://user139922:oulrqa@77.83.194.41:2337',
    'http://user139922:oulrqa@45.87.254.209:2337',
    'http://user139922:oulrqa@102.165.10.243:2337',
    'http://user139922:oulrqa@45.87.254.20:2337',
    'http://user139922:oulrqa@102.165.10.249:2337',
    'http://user139922:oulrqa@102.165.10.154:2337',
    'http://user139922:oulrqa@102.165.10.58:2337'
]
CYCLED_PROXIES: cycle = cycle(PROXIES)


class MissingEnvironmentVariable(Exception):
    pass


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        my_context = kwargs.pop('pid', self.extra['pid'])
        return f'[{my_context}] {msg}', kwargs


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


def get_file_handler(name: str) -> logging.FileHandler:
    log_dir_name: str = f"{get_my_env_var('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging"
    if not os.path.exists(log_dir_name):
        os.mkdir(log_dir_name)
    file_handler = RotatingFileHandler(
        filename=f"{log_dir_name}/{name}.log",
        mode='a',
        maxBytes=10.5 * pow(1024, 2),
        backupCount=3
    )
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FTM))
    return file_handler


def get_stream_handler():
    stream_handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return stream_handler


def get_logger(name: str) -> logging:
    logger_: logging.Logger = logging.getLogger(name)

    if logger_.hasHandlers():
        logger_.handlers.clear()

    # Добавляем обработчики до обертки в CustomAdapter
    logger_.addHandler(get_file_handler(name))
    logger_.addHandler(get_stream_handler())
    logger_.setLevel(logging.INFO)

    # Оборачиваем в CustomAdapter
    return CustomAdapter(logger_, {"pid": None})


logger: logging = get_logger(__name__)


def send_to_telegram(message):
    chat_id = get_my_env_var('CHAT_ID')
    token = get_my_env_var('TOKEN_TELEGRAM')
    topic = get_my_env_var('TOPIC')
    message_id = get_my_env_var('ID')
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    logger.info("Отправка сообщения в телеграмм")
    if len(message) < 4095:
        params = {"chat_id": f"{chat_id}/{topic}", "text": message,
                  'reply_to_message_id': message_id}  # Добавляем /2 для указания второго подканала
        response = requests.get(url, params=params)
        logger.info(response)
    else:
        for n, x in enumerate(range(0, len(message), 4095), 1):
            m = message[x:x + 4095]
            params = {"chat_id": f"{chat_id}/{topic}", "text": m,
                      'reply_to_message_id': message_id}  # Добавляем /2 для указания второго подканала
            response = requests.get(url, params=params)
            logger.info(f'Отправка сообщения #{n}, Статус отправки {response.status_code}')
            time.sleep(2)
