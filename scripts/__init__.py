import os
import time
import logging
import requests
from itertools import cycle
from dotenv import load_dotenv

# os.environ["XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS"] = os.path.dirname(os.path.abspath(__file__))

COUNT_THREADS: int = 3
IP_ADDRESS_DADATA: str = "service_inn"

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
    'http://user139922:oulrqa@166.1.108.56:6573',
    'http://user139922:oulrqa@166.1.108.146:6573',
    'http://user139922:oulrqa@166.1.29.178:6573',
    'http://user139922:oulrqa@166.1.108.52:6573',
    'http://user139922:oulrqa@194.169.161.227:6573',
    'http://user139922:oulrqa@194.169.161.208:6573',
    'http://user139922:oulrqa@185.198.233.102:6573',
    'http://user139922:oulrqa@185.198.233.213:6573'
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

USER_XML_RIVER: str = get_my_env_var('USER_XML_RIVER')
KEY_XML_RIVER: str = get_my_env_var('KEY_XML_RIVER')


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
