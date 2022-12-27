import os
import spacy
import logging
# os.environ['XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS'] = '/home/timurzav/PycharmWork/docker_project/reference_inn_by_api'
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

worker_count = 1


nlp = spacy.load('en_core_web_sm')

replaced_words = ["ООО", "OOO", "OОO", "ОOО", "OOО", "ООO", "ОАО", "ИП", "ЗАО", "3АО", "АО"]

countries_and_cities = ["UZBEKISTAN", "KAZAKHSTAN", "BELARUS", "POLAND", "CZECH", "AMSTERDAM", "ROTTERDAM", "НИДЕРЛАНД",
                        "HELSINKI", "КАЗАХСТАН", "УЗБЕКИСТАН", "БЕЛАРУСЬ", "ТАШКЕНТ", "SCHERPENZEEL", "ASAKA",
                        "HUNGARY", "KYRGYZSTAN", "BISHKEK"]
