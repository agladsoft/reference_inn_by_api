import os
import json
import logging
import requests

worker_count = 3


def get_translate_from_yandex(text):
    body["texts"] = text
    response = requests.post('https://translate.api.cloud.yandex.net/translate/v2/translate', json=body, headers=header)
    return json.loads(response.text)["translations"][0]["text"]


body = {
    "targetLanguageCode": "ru",
    "folderId": "b1gqnc8iu6nronrd6639",
}

header = {
    "Content-Type": "application/json",
    "Authorization": "Api-Key AQVN1WLLSufpkWuqCkYaT92hB3YuLcSKDooMbBcL"
}

replaced_quotes = ["<", ">", "«", "»", "’", "‘", "“", "”", "`"]

replaced_words = ["ООО", "OOO", "OОO", "ОOО", "OOО", "ООO", "ОАО", "ИП", "ЗАО", "3АО", "АО"]

countries_and_cities = ["UZBEKISTAN", "KAZAKHSTAN", "BELARUS", "POLAND", "CZECH", "AMSTERDAM", "ROTTERDAM", "НИДЕРЛАНД",
                        "HELSINKI", "КАЗАХСТАН", "УЗБЕКИСТАН", "БЕЛАРУСЬ", "ТАШКЕНТ", "SCHERPENZEEL", "ASAKA",
                        "HUNGARY", "KYRGYZSTAN", "BISHKEK", "BANGLADESH", "NETHERLANDS", "BELGIUM", "WARSAW", "POLSKA",
                        "TASHKENT", "ESENTEPE", "BANKASI", "WARSZAWA", "GERMANY", "PHILIPPSBURG", "NEDERLAND",
                        "SCHERPENZEEL", "GDYNIA", "SWIDNICA", "SZCZECIN", "BURGAN BANK", "TURKEY", "ISTANBUL",
                        "UNITED KINGDOM", "ATHENA", "ANGLIA", "SUFFOLK", "CHELMSFORD", "WOLFSBURG", "SLOVAKIA",
                        "ALMATY", "BANGLADESH", "KOREA", "SEOUL", "ZASCIANKI", "TORUN", "MONGOLIA", "FRANCE", "GDYNIA",
                        "BELSK DUZY", "SCHERPENZEEL", "KYRGYZSTAN", "NUR-SULTA", "KYRGYZ REPUBLIC", "TAJIKISTAN",
                        "KILINSKIEGO", "LATVIA", "KOKAND", "GEORGEN AM WALDE", "SAMARKAND"]


if not os.path.exists(f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging"):
    os.mkdir(f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging")

json_handler = logging.FileHandler(filename=f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/logging/"
                                            f"{os.path.basename(__file__)}.log")
logger = logging.getLogger("file_handler")
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(json_handler)
logger.setLevel(logging.INFO)

console_out = logging.StreamHandler()
logger_stream = logging.getLogger("stream")
if logger_stream.hasHandlers():
    logger_stream.handlers.clear()
logger_stream.addHandler(console_out)
logger_stream.setLevel(logging.INFO)
