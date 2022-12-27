import re
import sys
import json
import sqlite3
import contextlib
import numpy as np
import validate_inn
import pandas as pd
from __init__ import *
from pathlib import Path
from fuzzywuzzy import fuzz
from cache_inn import GetINNApi
from multiprocessing import Pool
from deep_translator import GoogleTranslator

df = pd.read_csv(os.path.abspath(sys.argv[1]))
df.columns = ['company_name']
df = df.drop_duplicates(subset='company_name', keep="first")
df = df.replace({np.nan: None})
df['company_name_rus'] = None
df['company_name_lemma'] = None
df['company_inn'] = None
df['company_name_unified'] = None
df['is_inn_found_auto'] = None
df['confidence_rate'] = None
parsed_data = df.to_dict('records')


def lemmatize_sentence(company_name_rus):
    docs = nlp(company_name_rus)
    company_name_rus = " ".join([token.lemma_ for token in docs])
    docs = list(nlp.pipe(company_name_rus))
    list_letters = []
    for doc in docs:
        for w in doc:
            if w.like_num or w.lemma_ == '+' or w.lemma_ == '<' or w.lemma_ == '>':
                list_letters.append(" ")
            elif not w.is_punct:
                list_letters.append(w.lower_)
    company_name_rus = "".join(list_letters)
    return re.sub(" +", " ", company_name_rus)


def do_not_count_company_in_quotes(sentence):
    adding_string = ""
    for search_string in ['<<([^"]*)>>', '“([^"]*)”', '"([^"]*)"', "''([^']*)''",
                          "'([^']*)'", '< <([^"]*)> >', '<  <([^"]*)>  >',
                          '<([^"]*)>', '""([^"]*)""']:
        if delete_string := re.findall(search_string, sentence):
            sentence = sentence.replace(delete_string[0], "")
            adding_string = delete_string[0].lower()
            break
    return adding_string, sentence


def get_company_name_by_inn(provider, list_data, inn, sentence, lemmatized_sentence=None):
    if not lemmatized_sentence:
        lemmatized_sentence = lemmatize_sentence(sentence)
        sentence = GoogleTranslator(source='en', target='ru').translate(lemmatized_sentence)
    list_data['is_inn_found_auto'] = True
    list_data['company_name_rus'] = sentence
    list_data['company_name_lemma'] = lemmatized_sentence
    inn, company_name = provider.get_inn(inn)
    company_name = re.sub(" +", " ", company_name)
    list_data["company_inn"] = inn
    list_data["company_name_unified"] = company_name
    list_data['confidence_rate'] = fuzz.partial_ratio(company_name.upper(), sentence.upper())


def get_company_name_by_sentence(provider, sentence, adding_string):
    lemmatized_sentence = lemmatize_sentence(sentence)
    translated = GoogleTranslator(source='en', target='ru').translate(lemmatized_sentence)
    lemmatized_sentence = f"{adding_string} {lemmatized_sentence}".strip()
    translated = f"{adding_string} {translated}".strip()
    inn, translated = provider.get_inn_from_value(translated)
    return inn, translated, lemmatized_sentence


def get_inn_from_row(sentence, data):
    inn = re.findall(r"\d+", sentence)
    cache_inn = GetINNApi("inn_and_uni_company_name", conn)
    list_inn = []
    for item_inn in inn:
        with contextlib.suppress(Exception):
            item_inn2 = validate_inn.validate(item_inn)
            list_inn.append(item_inn2)
    if list_inn:
        get_company_name_by_inn(cache_inn, data, inn=list_inn[0], sentence=sentence)
    else:
        cache_name_inn = GetINNApi("company_name_and_inn", conn)
        adding_string, sentence = do_not_count_company_in_quotes(sentence)
        inn, translated, lemmatized_sentence = get_company_name_by_sentence(cache_name_inn, sentence, adding_string)
        get_company_name_by_inn(cache_inn, data, inn, translated, lemmatized_sentence)


def parse_data(index, data):
    for key, sentence in data.items():
        try:
            if key == 'company_name':
                get_inn_from_row(sentence, data)
        except Exception as ex:
            logger.info(f'Error in inn {ex}: {index} data is {data}')
            logger_stream.info(f'Error in inn {ex}: {index} data is {data}')

    logger.info(f'{index} data is {data}')
    logger_stream.info(f'{index} data is {data}')
    basename = os.path.basename(os.path.abspath(sys.argv[1]))
    output_file_path = os.path.join(sys.argv[2], f'{basename}_{index}.json')
    with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    procs = []
    path = f"{os.environ.get('XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS')}/cache_inn/cache_inn.db"
    fle = Path(path)
    if not os.path.exists(os.path.dirname(fle)):
        os.makedirs(os.path.dirname(fle))
    fle.touch(exist_ok=True)
    conn = sqlite3.connect(path)
    with Pool(processes=worker_count) as pool:
        for i, dict_data in enumerate(parsed_data, 2):
            proc = pool.apply_async(parse_data, (i, dict_data))
            procs.append(proc)
        [proc.get() for proc in procs]
    conn.close()



