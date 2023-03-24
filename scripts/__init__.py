import os
import logging
import datetime

WORKER_COUNT: int = 4

USER_XML_RIVER: str = "6390"
KEY_XML_RIVER: str = "e3b3ac2908b2a9e729f1671218c85e12cfe643b0"

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

MESSAGE_TEMPLATE: dict = {
    '200': "Error: the money ran out. Index is {}. Exception - {}. Value - {}",
    '110': "Error: there are no free channels for data collection. Index is {}. Exception - {}. Value - {}",
    '15': "No results found in the search engine. Index is {}. Exception - {}. Value - {}"
}

PREFIX_TEMPLATE: dict = {
    '200': "закончились_деньги_на_строке_",
    '110': "нет_свободных_каналов_на_строке_",
    '15': "не_найдено_результатов_"
}

if not os.path.exists(f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging"):
    os.mkdir(f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging")

json_handler: logging.FileHandler = logging.FileHandler(
    filename=f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging/"
             f"logging_{datetime.datetime.now().date()}.log")

logger: logging.getLogger = logging.getLogger("file_handler")
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(json_handler)
logger.setLevel(logging.INFO)
logger.info(f'{os.path.basename(__file__)} {datetime.datetime.now()}')

console_out: logging.StreamHandler = logging.StreamHandler()
logger_stream: logging.getLogger = logging.getLogger("stream")
if logger_stream.hasHandlers():
    logger_stream.handlers.clear()
logger_stream.addHandler(console_out)
logger_stream.setLevel(logging.INFO)
