import os
import logging
import datetime
from dotenv import load_dotenv


USER_XML_RIVER: str = "6390"
KEY_XML_RIVER: str = "e3b3ac2908b2a9e729f1671218c85e12cfe643b0"

TOKEN_DADATA = "baf71b4b95c986ce9148c24f5aa251d94cd9d850"

REPLACED_QUOTES: list = ["<", ">", "«", "»", "’", "‘", "“", "”", "`", "'", '"']

REPLACED_WORDS: list = ["ООО", "OOO", "OОO", "ОOО", "OOО", "ООO", "ОАО", "ИП", "ЗАО", "3АО", "АО"]

COUNTRIES_AND_CITIES: list = ["UZBEKISTAN", "KAZAKHSTAN", "BELARUS", "POLAND", "CZECH", "AMSTERDAM", "ROTTERDAM",
                              "НИДЕРЛАНД", "HELSINKI", "КАЗАХСТАН", "УЗБЕКИСТАН", "БЕЛАРУСЬ", "ТАШКЕНТ", "SCHERPENZEEL",
                              "ASAKA", "HUNGARY", "KYRGYZSTAN", "BISHKEK", "BANGLADESH", "NETHERLANDS", "BELGIUM",
                              "WARSAW", "POLSKA", "TASHKENT", "ESENTEPE", "BANKASI", "WARSZAWA", "GERMANY",
                              "PHILIPPSBURG", "NEDERLAND", "SCHERPENZEEL", "GDYNIA", "SWIDNICA", "SZCZECIN",
                              "BURGAN BANK", "TURKEY", "ISTANBUL", "UNITED KINGDOM", "ATHENA", "ANGLIA", "SUFFOLK",
                              "CHELMSFORD", "WOLFSBURG", "SLOVAKIA", "ALMATY", "BANGLADESH", "KOREA", "SEOUL",
                              "ZASCIANKI", "TORUN", "MONGOLIA", "FRANCE", "GDYNIA", "BELSK DUZY", "SCHERPENZEEL",
                              "KYRGYZSTAN", "NUR-SULTA", "KYRGYZ REPUBLIC", "TAJIKISTAN", "KILINSKIEGO", "LATVIA",
                              "KOKAND", "GEORGEN AM WALDE", "SAMARKAND", "KARMANA", "MINSK"]

COUNTRY_KAZAKHSTAN = ["KAZAKHSTAN ", "ALMATY", "+7727", "(727)", " BIN "]

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


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        my_context = kwargs.pop('pid', self.extra['pid'])
        return f'[{my_context}] {msg}', kwargs


if not os.path.exists(f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging"):
    os.mkdir(f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging")

logging.basicConfig(
    filename=f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging/"
             f"logging_{datetime.datetime.now().date()}.log",
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%d/%B/%Y %H:%M:%S"
)

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
