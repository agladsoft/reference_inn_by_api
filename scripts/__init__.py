import os
import time
import logging
import requests
from itertools import cycle
from dotenv import load_dotenv

os.environ["XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS"] = os.path.dirname(os.path.abspath(__file__))

COUNT_THREADS: int = 3
TOKEN_TELEGRAM: str = "6557326533:AAHy6ls9LhTVTGztix8PUSK7BUSaHVEojXc"
CHAT_ID: str = "-4051876751"
USER_XML_RIVER: str = "6390"
KEY_XML_RIVER: str = "e3b3ac2908b2a9e729f1671218c85e12cfe643b0"
IP_ADDRESS_DADATA: str = "service_inn"

TOKEN_DADATA = "baf71b4b95c986ce9148c24f5aa251d94cd9d850"

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


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        my_context = kwargs.pop('pid', self.extra['pid'])
        return f'[{my_context}] {msg}', kwargs


if not os.path.exists(f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging"):
    os.mkdir(f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging")

logger: logging.getLogger = logging.getLogger("file_handler")
if logger.hasHandlers():
    logger.handlers.clear()
logger = CustomAdapter(logger, {"pid": None})
logger.setLevel(logging.INFO)

console_out: logging.StreamHandler = logging.StreamHandler()
logger_stream: logging.getLogger = logging.getLogger("stream")
if logger_stream.hasHandlers():
    logger_stream.handlers.clear()
logger_stream.addHandler(console_out)
logger_stream.setLevel(logging.INFO)

load_dotenv()


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


class MissingEnvironmentVariable(Exception):
    pass


TOKEN_API_YANDEX = get_my_env_var('TOKEN_API_YANDEX')


def telegram(message):
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
