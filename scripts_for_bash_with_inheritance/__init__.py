import os
import json
import spacy
import logging
import requests

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

worker_count = 6

nlp = spacy.load('en_core_web_sm')

body = {
    "targetLanguageCode": "ru",
    "folderId": "b1gqnc8iu6nronrd6639",
}

header = {
    "Content-Type": "application/json",
    "Authorization": "Api-Key AQVN1WLLSufpkWuqCkYaT92hB3YuLcSKDooMbBcL"
}

replaced_words = ["ООО", "OOO", "OОO", "ОOО", "OOО", "ООO", "ОАО", "ИП", "ЗАО", "3АО", "АО"]


def get_translate_from_yandex(text):
    body["texts"] = text
    response = requests.post('https://translate.api.cloud.yandex.net/translate/v2/translate', json=body, headers=header)
    return json.loads(response.text)["translations"][0]["text"]
