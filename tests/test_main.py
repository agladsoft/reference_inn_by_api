import os

os.environ["XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS"] = "."

import csv
import pytest
import requests
import numpy as np
import pandas as pd
from typing import List
from pathlib import PosixPath
from scripts.main import ReferenceInn
from unittest.mock import patch, Mock
from deep_translator import single_detection
from _pytest.logging import LogCaptureFixture
from scripts.unified_companies import SearchEngineParser, UnifiedCompaniesManager


@pytest.fixture
def mock_main(mocker: Mock) -> None:
    mocker.patch("scripts.main.ReferenceInn.main")
    mocker.patch("scripts.main.ReferenceInn.connect_to_db")


# Создаем фикстуру для экземпляра класса ReferenceInn
@pytest.fixture
def reference_inn_instance(mock_main: Mock, tmp_path: PosixPath) -> ReferenceInn:
    return ReferenceInn(f"{tmp_path}/test_file.xlsx", f"{tmp_path}/json")


@patch("pandas.read_excel")
def test_convert_file_to_dict(mock_read_excel: Mock, reference_inn_instance: ReferenceInn) -> None:
    # Act
    mock_data: pd.DataFrame = pd.DataFrame({
        "A": ["Company1", "Company2_x000D_", np.nan, "Company3"]
    })
    mock_read_excel.return_value = mock_data

    result: List[dict] = reference_inn_instance.convert_file_to_dict()
    expected_output: List[dict] = [
        {"company_name": "Company1"},
        {"company_name": "Company2"},
        {"company_name": "Company3"},
    ]

    # Assert
    assert result == expected_output
    assert reference_inn_instance.telegram["all_company"] == 3


@pytest.mark.parametrize(
    "balance_text, expected_message, expected_exit, request_exception",
    [
        ("250.50", None, False, None),  # Достаточно денег
        ("150.00", "Баланс в Яндекс кошельке сейчас составляет 150.0 рублей.", False, None),  # Предупреждение
        (
            "50.00",
            "Баланс в Яндекс кошельке меньше 100 рублей. Пополните, пожалуйста, счет.",
            True,
            None
        ),  # Ошибка из-за низкого баланса
        (
            "invalid_float",
            None,
            True,
            requests.exceptions.RequestException("ошибка_при_получении_баланса_яндекса")
        ),  # Ошибка из-за неправильного формата
    ]
)
@patch("scripts.main.send_to_telegram")
@patch("scripts.main.requests.get")
def test_is_enough_money_to_search_engine(
    mock_get: Mock,
    mock_send_to_telegram: Mock,
    balance_text: str,
    expected_message: str,
    expected_exit: bool,
    request_exception: Exception
):
    # Act
    if request_exception:
        mock_get.side_effect = request_exception  # Имитация ошибки запроса
    else:
        mock_response: Mock = Mock()
        mock_response.text = balance_text
        mock_get.return_value = mock_response

    # Проверяем, что при ошибках выходит SystemExit
    if expected_exit:
        with pytest.raises(SystemExit):
            ReferenceInn.is_enough_money_to_search_engine()
    else:
        ReferenceInn.is_enough_money_to_search_engine()

    # Проверяем отправку сообщений
    if expected_message:
        mock_send_to_telegram.assert_called_once_with(message=expected_message)


@pytest.mark.parametrize(
    "start_time_script, initial_data",
    [
        ("2024-07-27 10:00:00", {}),
        ("2024-07-28 12:30:00", {"existing_key": "existing_value"}),
        ("2024-07-29 15:45:00", {"key1": "value1", "key2": "value2"}),
    ]
)
def test_add_new_columns(reference_inn_instance: ReferenceInn, start_time_script, initial_data):
    # Act
    data: dict = initial_data.copy()
    reference_inn_instance.add_new_columns(data, start_time_script)

    # Assert
    assert data["is_inn_found_auto"] is True
    assert data["is_company_name_from_cache"] is False
    assert data["original_file_name"] == os.path.basename(reference_inn_instance.filename)
    assert data["original_file_parsed_on"] == start_time_script
    for key, value in initial_data.items():
        assert data[key] == value


@pytest.mark.parametrize(
    "index, data",
    [
        (1, {"original_file_parsed_on": "2024-08-03", "col1": "val1", "col2": "val2"}),
        (2, {"original_file_parsed_on": "2024-08-04", "col3": "val3"}),
        (10, {"original_file_parsed_on": "2024-08-05"}),
    ]
)
def test_write_to_csv(reference_inn_instance: ReferenceInn, index: int, data: dict, caplog: LogCaptureFixture) -> None:
    reference_inn_instance.write_to_csv(index, data)
    basename: str = os.path.basename(reference_inn_instance.filename)
    output_dir: str = os.path.join(os.path.dirname(reference_inn_instance.directory), "csv")
    output_file_path: str = os.path.join(output_dir, f'{data["original_file_parsed_on"]}_{basename}')

    # Проверяем, что файл создан
    assert os.path.exists(output_file_path), f"Файл {output_file_path} не был создан"

    # Читаем CSV и проверяем содержимое
    with open(output_file_path, newline='', encoding='utf-8') as csvfile:
        reader: csv.DictReader = csv.DictReader(csvfile)
        rows: list = list(reader)

    assert f"Data was written successfully to the file. Index: {index}" in caplog.text
    assert rows, "Ожидалась хотя бы одна строка в CSV"
    assert data in rows, f"Данные в файле не соответствуют ожидаемым: {rows}"


@pytest.mark.parametrize(
    "data, expected_russian_companies, expected_unknown_companies, expected_foreign_companies",
    [
        ({"country": "russia", "name": "Yandex"}, [{"country": "russia", "name": "Yandex"}], [], []),
        ({"name": "Unknown Company"}, [], [{"name": "Unknown Company"}], []),
        ({"country": "usa", "name": "Google"}, [], [], [{"country": "usa", "name": "Google"}])
    ],
)
def test_append_data(
    reference_inn_instance: ReferenceInn,
    data: dict,
    expected_russian_companies: list,
    expected_unknown_companies: list,
    expected_foreign_companies: list
) -> None:
    # Act
    reference_inn_instance.append_data(data)

    # Assert
    assert reference_inn_instance.russian_companies == expected_russian_companies
    assert reference_inn_instance.unknown_companies == expected_unknown_companies
    assert reference_inn_instance.foreign_companies == expected_foreign_companies


@pytest.mark.parametrize(
    "sentence, only_russian, expected",
    [
        ("This is a test string, with some punctuation!!!", True, "ru"),
        ("This is a test string, with some punctuation!", False, "This is a test string, with some punctuation!"),
        ("++++++++++", False, ""),
        ("", False, "")
    ],
)
def test_translate_sentence(
    reference_inn_instance: ReferenceInn,
    sentence: str,
    only_russian: bool,
    expected: str
) -> None:
    # Act
    actual = reference_inn_instance.translate_sentence(sentence, only_russian)

    # Assert
    if only_russian:
        lang = single_detection(actual, api_key="2b2f53f46c2b7d115d69bf391cfe44c0")
        assert lang == expected
    else:
        assert actual == expected


@pytest.mark.parametrize(
    "is_fts_found, fts, enforce_get_company, mock_companies, expected_data",
    [
        (
            True,  # Флаг, указывающий, что компания найдена в FTS
            {"1234567890": "Company Name"},  # Словарь с INN и названием компании из FTS
            False,  # Не принуждаем к поиску компании в базе, если есть в FTS
            [("Company Name", "RU", True)],  # Результат из мок-данных (название компании, страна, найдено в кэше)
            {
                "company_inn": "1234567890",  # INN компании
                "sum_count_inn": 1,  # Количество встреченных INN
                "is_fts_found": True,  # Успешный поиск в FTS
                "company_inn_max_rank": 1,  # Как часто встречается данный INN в поисковике и на какой позиции находится
                "is_company_name_from_cache": True,  # Компания найдена в кэше
                "company_name_unified": "Company Name",  # Унифицированное название компании
                "country": "RU",  # Страна компании
            }
        ),
        (
            False,  # Компания не найдена в FTS
            {},  # Пустой словарь, так как компания не найдена
            True,  # Принуждаем к поиску компании в базе, несмотря на отсутствие в FTS
            [("Company Name", "RU", False)],  # Мок-данные для поиска в базе (название, страна, не найдено в кэше)
            {
                "company_inn": "1234567890",
                "sum_count_inn": 1,
                "is_fts_found": False,  # Компания не найдена в FTS
                "company_inn_max_rank": 1,
                "is_company_name_from_cache": False,  # Компания не найдена в кэше
                "company_name_unified": "Company Name",
                "country": "RU",
            }
        ),
        (
            False,  # Компания не найдена в FTS
            {},  # Пустой словарь, так как компания не найдена
            False,  # Не принуждаем к поиску компании в базе
            [()],  # Нет данных о компании
            {
                "company_inn": "1234567890",
                "sum_count_inn": 1,
                "is_fts_found": False,  # Компания не найдена в FTS
                "company_inn_max_rank": 1,  # Ранг компании по INN
            }
        ),
        (
            True,  # Компания найдена в FTS
            {"1234567890": "Company Name"},  # Словарь с INN и названием компании из FTS
            False,  # Не принуждаем к поиску компании в базе, если есть в FTS
            [(None, None, False)],  # Нет данных о компании (значение None)
            {
                "company_inn": "1234567890",
                "sum_count_inn": 1,
                "is_fts_found": True,
                "company_inn_max_rank": 1,
            }
        ),
    ],
)
@patch("scripts.unified_companies.UnifiedCompaniesManager.fetch_company_name")
def test_get_data(
    mock_fetch_company_name: Mock,
    reference_inn_instance: ReferenceInn,
    is_fts_found: bool,
    fts: dict,
    enforce_get_company: bool,
    mock_companies: tuple,
    expected_data: dict
) -> None:
    # Arrange
    list_inn_in_fts: list = []
    num_inn_in_fts: dict = {"company_inn_max_rank": 0, "num_inn_in_fts": 0}
    data: dict = {
        "original_file_parsed_on": os.path.basename(reference_inn_instance.filename)
    }
    search_engine = SearchEngineParser([], UnifiedCompaniesManager(True))
    mock_fetch_company_name.return_value = iter(mock_companies)

    # Act
    reference_inn_instance.get_data(
        fts=fts,
        countries_obj=mock_fetch_company_name,
        search_engine=search_engine,
        data=data,
        inn=expected_data["company_inn"],
        sentence="test sentence",
        index=0,
        num_inn_in_fts=num_inn_in_fts,
        list_inn_in_fts=list_inn_in_fts,
        translated="translated sentence",
        enforce_get_company=enforce_get_company
    )

    # Assert
    data.update(num_inn_in_fts)
    for key, value in expected_data.items():
        assert data[key] == value


@pytest.mark.parametrize(
    "company_name, translated, expected_confidence_rate",
    [
        ("ООО Тестовая компания", "Test Company", 100),
        ("ООО Тестовая компания", "ООО Тестовая компания", 100),
        ("ООО Тестовая компания", "", None),
        (None, "Test Company", None),
        ("ООО Тестовая компания", None, None)
    ],
)
@patch("scripts.translate.YandexTranslatorAdapter.translate")
@patch("scripts.translate.GoogleTranslatorAdapter.translate")
def test_compare_different_fuzz(
    mock_google_translate: Mock,  # Первый patch (Google)
    mock_yandex_translate: Mock,  # Второй patch (Yandex)
    reference_inn_instance: ReferenceInn,
    company_name: str,
    translated: str,
    expected_confidence_rate: int
) -> None:
    # Arrange
    data: dict = {}
    mock_google_translate.return_value = "Test Company"
    mock_yandex_translate.return_value = "Test Company"

    # Act
    reference_inn_instance.compare_different_fuzz(company_name, translated, data)

    # Assert
    if expected_confidence_rate is not None:
        assert data["confidence_rate"] == expected_confidence_rate
    else:
        assert "confidence_rate" not in data


@pytest.mark.parametrize(
    "company_name, expected",
    [
        ("ООО Тестовая компания", "Тестовая компания"),
        ("ЗАО Test Company", "Test Company"),
        ("ОАО Test Company", "Test Company"),
        ("", ""),
        ("   ", ""),
        ('""""', ""),
        ("Test Company", "Test Company")
    ],
)
def test_replace_forms_organizations(reference_inn_instance: ReferenceInn, company_name: str, expected: str) -> None:
    # Act
    actual = reference_inn_instance.replace_forms_organizations(company_name)
    # Assert
    assert actual == expected


@pytest.mark.parametrize(
    "list_inn_in_fts, num_inn_in_fts, from_cache, data, expected_append_data_calls",
    [
        (
            [{
                "is_fts_found": True, "company_name_rus": "Company 1",
                "company_inn": "123", "country": "US", "company_inn_count": 1
            }],
            {"num_inn_in_fts": 1},
            False,
            {"company_name_rus": "Original Data", "company_inn": "456", "country": "US"},
            [{
                "is_fts_found": True, "company_name_rus": "Company 1",
                "company_inn": "123", "country": "US", "company_inn_count": 1, "count_inn_in_fts": 1
            }]
        ),
        (
            [{
                "is_fts_found": False, "company_name_rus": "Company 1",
                "company_inn": "123", "country": "US", "company_inn_count": 1
            }],
            {"num_inn_in_fts": 0},
            True,
            {"company_name_rus": "Original Data", "company_inn": "456", "country": "US"},
            [{
                "is_fts_found": False, "company_name_rus": "Company 1",
                "company_inn": "123", "country": "US", "company_inn_count": 1, "count_inn_in_fts": 0
            }]
        ),
        (
            [],
            {"num_inn_in_fts": 0},
            False,
            {"company_name_rus": "Original Data", "company_inn": "456", "country": "US"},
            [{'company_name_rus': 'Original Data', 'company_inn': '456', 'country': 'US'}]
        ),
        (
            [
                {
                    "is_fts_found": False, "company_name_rus": "Company 1",
                    "company_inn": "123", "country": "US", "company_inn_count": 1
                },
                {
                    "is_fts_found": False, "company_name_rus": "Company 2",
                    "company_inn": "456", "country": "US", "company_inn_count": 2
                },
            ],
            {"num_inn_in_fts": 0},
            False,
            {"company_name_rus": "Original Data", "company_inn": "789", "country": "CA"},
            [{
                "is_fts_found": False, "company_name_rus": "Company 2",
                "company_inn": "456", "country": "US", "company_inn_count": 2, "count_inn_in_fts": 0
            }]
        ),
    ],
)
@patch("scripts.unified_companies.BaseUnifiedCompanies.cache_add_and_save")
def test_write_existing_inn_from_fts(
    mock_cache_add_and_save: Mock,
    reference_inn_instance: ReferenceInn,
    list_inn_in_fts: list,
    num_inn_in_fts: dict,
    from_cache: bool,
    data: dict,
    expected_append_data_calls: list
) -> None:
    # Arrange
    search_engine: SearchEngineParser = SearchEngineParser([], UnifiedCompaniesManager(True))
    mock_cache_add_and_save.return_value = None

    # Act
    reference_inn_instance.write_existing_inn_from_fts(search_engine, 0, data, list_inn_in_fts, num_inn_in_fts, from_cache)

    # Assert
    assert reference_inn_instance.foreign_companies == expected_append_data_calls
