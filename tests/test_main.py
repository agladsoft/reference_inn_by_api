import os
import csv
import json
import pytest
import requests
import numpy as np
import pandas as pd
from typing import List
from queue import Queue
from logging import ERROR
from pathlib import PosixPath
from unittest.mock import Mock
from deep_translator import single_detection
from _pytest.logging import LogCaptureFixture

os.environ["XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS"] = os.path.dirname(os.path.dirname(__file__))

from scripts.main import ReferenceInn
from scripts.unified_companies import SearchEngineParser, UnifiedCompaniesManager


use_mock: bool = True


@pytest.fixture
def mock_main(mocker: Mock) -> None:
    mocker.patch("scripts.main.ReferenceInn.main")
    mocker.patch("scripts.main.ReferenceInn.connect_to_db")


# Создаем фикстуру для экземпляра класса ReferenceInn
@pytest.fixture
def reference_inn_instance(mock_main: Mock, tmp_path: PosixPath) -> ReferenceInn:
    return ReferenceInn(f"{tmp_path}/test_file.xlsx", f"{tmp_path}/json")


def test_convert_file_to_dict(mocker: Mock, reference_inn_instance: ReferenceInn) -> None:
    """
    Testing the convert_file_to_dict method of ReferenceInn class.

    This test creates a mock dataframe with a column "A" containing some names and a NaN value.
    The method is called and the result is checked to match the expected output.
    The expected output is a list of dictionaries with the company names as values.
    The test also checks that the counter in the telegram dictionary is incremented correctly.
    :param mocker: Mocker fixture.
    :param reference_inn_instance: ReferenceInn instance.
    :return:
    """
    # Act
    mock_data: pd.DataFrame = pd.DataFrame({
        "A": ["Company1", "Company2_x000D_", np.nan, "Company3"]
    })
    mocker.patch("pandas.read_excel", return_value=mock_data)

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
def test_is_enough_money_to_search_engine(
    mocker: Mock,
    balance_text: str,
    expected_message: str,
    expected_exit: bool,
    request_exception: Exception
) -> None:
    """
    Testing the is_enough_money_to_search_engine method of ReferenceInn class.

    This test checks the behavior of the method under different conditions.
    It uses parametrization to check the following cases:
        - There is enough money in the Yandex wallet.
        - There is a warning because the balance is low.
        - There is an error because the balance is too low.
        - There is an error because the response from the Yandex API is not a valid float.

    The test checks that the correct message is sent to Telegram in each case.
    It also checks that SystemExit is raised when an error occurs.
    :param mocker: Mocker fixture.
    :param balance_text: Balance text.
    :param expected_message: Expected message.
    :param expected_exit: Expected exit.
    :param request_exception: Request exception.
    :return:
    """
    # Act
    mock_get: Mock = mocker.patch("scripts.main.requests.get")
    mock_send_to_telegram: Mock = mocker.patch("scripts.main.send_to_telegram")
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
def test_add_new_columns(reference_inn_instance: ReferenceInn, start_time_script: str, initial_data: dict) -> None:
    """
    Test the add_new_columns method of the ReferenceInn class.

    This test verifies that the add_new_columns method correctly adds new columns
    to the provided data dictionary. It checks that the 'is_inn_found_auto' is set to True,
    'is_company_name_from_cache' is set to False, 'original_file_name' is set to the base
    name of the ReferenceInn instance's filename, and 'original_file_parsed_on' is set to
    the provided start_time_script. Additionally, it ensures that any existing keys in the
    initial data are preserved.

    :param reference_inn_instance: ReferenceInn instance.
    :param start_time_script: The script start time to be added to the data.
    :param initial_data: Initial data dictionary to which new columns will be added.
    """
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
    """
    Test the write_to_csv method of the ReferenceInn class.

    This test verifies that the write_to_csv method correctly writes data to a
    CSV file. It checks that the file is created, that the data is written to the
    file, and that the data is correctly formatted.

    :param reference_inn_instance: ReferenceInn instance.
    :param index: Index of the data to be written.
    :param data: Data to be written.
    :param caplog: LogCaptureFixture fixture.
    """
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
    """
    Testing the append_data method of the ReferenceInn class.

    This test verifies that the append_data method correctly appends the given data
    to the appropriate list. It checks that the Russian companies list contains
    the expected Russian companies, the unknown companies list contains the expected
    unknown companies, and the foreign companies list contains the expected foreign
    companies.

    :param reference_inn_instance: ReferenceInn instance.
    :param data: Data to be appended.
    :param expected_russian_companies: Expected Russian companies.
    :param expected_unknown_companies: Expected unknown companies.
    :param expected_foreign_companies: Expected foreign companies.
    """
    # Act
    reference_inn_instance.append_data(data)

    # Assert
    assert reference_inn_instance.russian_companies == expected_russian_companies
    assert reference_inn_instance.unknown_companies == expected_unknown_companies
    assert reference_inn_instance.foreign_companies == expected_foreign_companies


@pytest.mark.parametrize(
    "sentence, with_russian, expected",
    [
        ("This is a test string, with some punctuation!!!", True, "ru"),
        ("This is a test string, with some punctuation!", False, "This is a test string, with some punctuation!"),
        ("++++++++++", False, ""),
        ("", False, "")
    ],
)
def test_translate_sentence(
    mocker: Mock,
    reference_inn_instance: ReferenceInn,
    sentence: str,
    with_russian: bool,
    expected: str
) -> None:
    """
    Test the translate_sentence method of the ReferenceInn class.

    This test verifies that the translate_sentence method correctly translates
    a given sentence into Russian when with_russian is True. It checks if the
    translation is performed correctly by the Yandex and Google translators using
    mock objects. When with_russian is False, it ensures that the sentence is
    returned without translation and punctuation is correctly removed.

    :param mocker: Mocker fixture.
    :param reference_inn_instance: ReferenceInn instance.
    :param sentence: The sentence to be translated.
    :param with_russian: A boolean indicating if translation to Russian is required.
    :param expected: The expected translated or processed sentence.
    """
    # Act
    if use_mock:
        return_translated = "Это тестовая строка с некоторыми знаками препинания"
        mocker.patch("scripts.translate.YandexTranslatorAdapter.translate", return_value=return_translated)
        mocker.patch("scripts.translate.GoogleTranslatorAdapter.translate", return_value=return_translated)
    actual = reference_inn_instance.translate_sentence(sentence, with_russian)

    # Assert
    if with_russian:
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
def test_get_data(
    mocker: Mock,
    reference_inn_instance: ReferenceInn,
    is_fts_found: bool,
    fts: dict,
    enforce_get_company: bool,
    mock_companies: tuple,
    expected_data: dict
) -> None:
    """
    Test the get_data method of the ReferenceInn class.

    This test verifies that the get_data method correctly processes company
    information and populates the data dictionary based on the input parameters.
    It checks various scenarios such as when a company is found or not found in
    FTS, when a company search is enforced, and when mock company data is provided.
    The test uses mock objects to simulate the behavior of fetching company names
    and to validate the final data structure against expected results.

    :param mocker: Mocker fixture for patching methods.
    :param reference_inn_instance: ReferenceInn instance to test.
    :param is_fts_found: Boolean indicating if the company is found in FTS.
    :param fts: Dictionary containing INN and company names from FTS.
    :param enforce_get_company: Boolean indicating if company search is enforced.
    :param mock_companies: Tuple representing mock company data (name, country, cache status).
    :param expected_data: Expected data dictionary after processing.
    """
    # Arrange
    list_inn_in_fts: list = []
    num_inn_in_fts: dict = {"company_inn_max_rank": 0, "num_inn_in_fts": 0}
    data: dict = {
        "original_file_parsed_on": os.path.basename(reference_inn_instance.filename)
    }
    search_engine = SearchEngineParser([], UnifiedCompaniesManager(True))
    mocker.patch(
        "scripts.unified_companies.UnifiedCompaniesManager.fetch_company_name",
        return_value= iter(mock_companies)
    )

    # Act
    reference_inn_instance.get_data(
        fts=fts,
        countries_obj=mocker,
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
def test_compare_different_fuzz(
    mocker: Mock,
    reference_inn_instance: ReferenceInn,
    company_name: str,
    translated: str,
    expected_confidence_rate: int
) -> None:
    """
    Test the compare_different_fuzz method of the ReferenceInn class.

    This test checks the calculation of confidence rates when comparing company names.
    It uses parameterized inputs to verify various scenarios, including:
    - When both company_name and translated are provided and should result in a 100% confidence rate.
    - When company_name matches translated exactly, resulting in a 100% confidence rate.
    - When either company_name or translated is missing, resulting in no confidence rate being set.

    :param mocker: Mock fixture for patching translator methods.
    :param reference_inn_instance: Instance of ReferenceInn to test.
    :param company_name: The original company name.
    :param translated: The translated company name.
    :param expected_confidence_rate: The expected confidence rate to be asserted.
    """
    # Arrange
    data: dict = {}
    if use_mock:
        return_translated = "Test Company"
        mocker.patch("scripts.translate.YandexTranslatorAdapter.translate", return_value=return_translated)
        mocker.patch("scripts.translate.GoogleTranslatorAdapter.translate", return_value=return_translated)

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
    """
    Test the replace_forms_organizations method of the ReferenceInn class.

    This test verifies that the replace_forms_organizations method correctly removes
    organization forms from the company name. It uses parameterized inputs to cover
    various scenarios, including:
    - Removing "ООО" from the company name.
    - Removing "ЗАО" from the company name.
    - Removing "ОАО" from the company name.
    - Leaving empty strings and strings with only whitespace unchanged.
    - Removing double quotes from the company name.
    - Leaving company names without organization forms unchanged.

    The test asserts that the actual result of calling the method matches the expected
    result for each scenario.

    :param reference_inn_instance: Instance of ReferenceInn to test.
    :param company_name: The original company name.
    :param expected: The expected company name after removing organization forms.
    """
    # Act
    actual = reference_inn_instance.replace_forms_organizations(company_name)
    # Assert
    assert actual == expected


@pytest.mark.parametrize(
    "sentence, expected",
    [
        ('test "test" test', 'test "test" test'),
        ('test `test` test', 'test "test" test'),
        ("test 'test' test", 'test "test" test'),
        ('test <test> test', 'test "test" test'),
        ('test “test” test', 'test "test" test'),
        ('test «test» test', 'test "test" test'),
        ('test ‘test’ test', 'test "test" test')
    ],
)
def test_search_engine_parser_replace_quotes(
    reference_inn_instance: ReferenceInn,
    sentence: str,
    expected: str
) -> None:
    """
    Test the `replace_quotes` method of `ReferenceInn`.

    This test ensures that the `replace_quotes` method correctly replaces various types
    of quotes in a given sentence with the standard double quote ("). It uses parametrized
    inputs to validate different scenarios, each representing a sentence with different
    quote characters.

    The test asserts that the modified sentence matches the expected sentence where all
    quote characters are replaced with double quotes.

    :param reference_inn_instance: The instance of `ReferenceInn` to be tested.
    :param sentence: The sentence containing various quote characters.
    :param expected: The expected sentence with all quotes replaced by double quotes.
    :return: None
    """
    assert reference_inn_instance.replace_quotes(sentence) == expected


@pytest.mark.parametrize(
    "sentence, data, index, fts, with_russian, expected_data",
    [
        (
            "“EUROPAPIER” LLC RUSSIA, 129110, MOSCOW, BOLSHAYA PEREYASLAVSKAYA STR., 46, BLD.2, 4TH FL., "
            "OFFICE 1,ROTEL: +7 495 787 0150 (EXT.154)",
            {
                "company_name": "“EUROPAPIER” LLC RUSSIA, 129110, MOSCOW, BOLSHAYA PEREYASLAVSKAYA STR., 46, BLD.2, "
                                "4TH FL., OFFICE 1,ROTEL: +7 495 787 0150 (EXT.154)",
                "original_file_parsed_on": "2025-03-10"
            },
            2,
            {},
            True,
            {
                'company_name': '“EUROPAPIER” LLC RUSSIA, 129110, MOSCOW, BOLSHAYA PEREYASLAVSKAYA STR., 46, BLD.2, '
                                '4TH FL., OFFICE 1,ROTEL: +7 495 787 0150 (EXT.154)',
                'company_name_rus': 'ООО ЕВРОПАПИР РОССИЯ, 129110, МОСКВА, УЛ. БОЛЬШАЯ ПЕРЕЯСЛАВСКАЯ, д. 46, СТР. 2, '
                                    '4 ЭТАЖ, ОФИС 1, ROTEL: 7 495 787 0150 ДОБ. 154',
                'original_file_parsed_on': '2025-03-10',
                'request_to_yandex': 'ООО ЕВРОПАПИР РОССИЯ, 129110, МОСКВА, УЛ. БОЛЬШАЯ ПЕРЕЯСЛАВСКАЯ, д. 46, СТР. 2, '
                                     '4 ЭТАЖ, ОФИС 1, ROTEL: 7 495 787 0150 ДОБ. 154 ИНН'
            }
        )
    ],
)
def test_unify_companies(
    mocker: Mock,
    reference_inn_instance: ReferenceInn,
    sentence: str,
    data: dict,
    index: int,
    fts: dict,
    with_russian: bool,
    expected_data: dict
) -> None:
    """
    Test the unify_companies method of the ReferenceInn class.

    This test verifies that the unify_companies method correctly unifies company names.
    It uses parameterized inputs to cover various scenarios, including:
    - When both company_name and translated are provided and should result in a 100% confidence rate.
    - When company_name matches translated exactly, resulting in a 100% confidence rate.
    - When either company_name or translated is missing, resulting in no confidence rate being set.

    The test asserts that the actual result of calling the method matches the expected
    result for each scenario.

    :param mocker: Mock fixture for patching translator methods.
    :param reference_inn_instance: Instance of ReferenceInn to test.
    :param sentence: The original sentence to be processed.
    :param data: The original data dictionary.
    :param index: The index of the sentence in the file.
    :param fts: The fts dictionary.
    :param with_russian: Whether to translate the company name to Russian.
    :param expected_data: The expected data dictionary after processing.
    """
    # Arrange
    if use_mock:
        return_translated: str = (
            "ООО ЕВРОПАПИР РОССИЯ, 129110, МОСКВА, УЛ. БОЛЬШАЯ ПЕРЕЯСЛАВСКАЯ, д. 46, СТР. 2, 4 ЭТАЖ, ОФИС 1, "
            "ROTEL: 7 495 787 0150 ДОБ. 154"
        )
        mocker.patch("scripts.translate.YandexTranslatorAdapter.translate", return_value=return_translated)
        mocker.patch("scripts.translate.GoogleTranslatorAdapter.translate", return_value=return_translated)
        mocker.patch(
            "scripts.unified_companies.SearchEngineParser.get_taxpayer_id",
            return_value=({"7708502716": 6}, "7708502716", False)
        )
        mocker.patch(
            "scripts.unified_companies.UnifiedCompaniesManager.fetch_company_name",
            return_value=[('ООО "ЕВРОПАПИР"', "russia", False)]
        )
    # Act
    reference_inn_instance.unify_companies(sentence, data, index, fts, with_russian)

    # Assert
    assert data == expected_data


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
def test_write_existing_inn_from_fts(
    mocker: Mock,
    reference_inn_instance: ReferenceInn,
    list_inn_in_fts: list,
    num_inn_in_fts: dict,
    from_cache: bool,
    data: dict,
    expected_append_data_calls: list
) -> None:
    """
    Test the write_existing_inn_from_fts method of the ReferenceInn class.

    This test verifies that the write_existing_inn_from_fts method correctly processes and appends data
    for existing INNs found in the FTS. It uses parameterized inputs to simulate various scenarios,
    including when an INN is found or not found, and when data should be fetched from cache.

    The test also ensures that the expected data is appended to the foreign_companies attribute of
    the ReferenceInn instance. Mocking is used to patch the cache_add_and_save method to simulate
    caching behavior without affecting the actual cache.

    :param mocker: Mock fixture for patching methods.
    :param reference_inn_instance: Instance of ReferenceInn to test.
    :param list_inn_in_fts: List of dictionaries representing INNs and their associated data.
    :param num_inn_in_fts: Dictionary containing the number of INNs found in FTS.
    :param from_cache: Boolean indicating if data should be fetched from cache.
    :param data: Dictionary containing the original data.
    :param expected_append_data_calls: List of expected dictionaries to be appended to foreign_companies.
    """
    # Arrange
    search_engine: SearchEngineParser = SearchEngineParser([], UnifiedCompaniesManager(True))
    mocker.patch('scripts.unified_companies.BaseUnifiedCompanies.cache_add_and_save')

    # Act
    reference_inn_instance.write_existing_inn_from_fts(
        search_engine, 0, data, list_inn_in_fts, num_inn_in_fts, from_cache
    )

    # Assert
    assert reference_inn_instance.foreign_companies == expected_append_data_calls


@pytest.mark.parametrize("not_parsed_data, sentence, index, ex_full, is_queue, expected_logs, expected_queue_size",
    [
        (
            [{"data": "test", "original_file_parsed_on": "2025-03-10"}], "test sentence", 1, None, False,
            ["An error occurred in which the processor was added to the queue. Index: 1. Data: test sentence"], 1
        ),
        (
            [{"data": "test", "original_file_parsed_on": "2025-03-10"}], "test sentence", 2,
            Exception("test exception"), True, ["Exception: test exception. Data: 2, test sentence"], 0
        ),
        (
            [{"data": "test", "original_file_parsed_on": "2025-03-10"}], "", 1, None, False,
            ["An error occurred in which the processor was added to the queue. Index: 1. Data: "], 1
        ),
        (
            [{"data": "test", "original_file_parsed_on": "2025-03-10"}], "test sentence", 0, None, False,
            ["An error occurred in which the processor was added to the queue. Index: 0. Data: test sentence"], 1
        ),
        (
            [{"original_file_parsed_on": "2025-03-10"}], "test sentence", 1, None, False, 
            ["An error occurred in which the processor was added to the queue. Index: 1. Data: test sentence"], 1
        ),
        (
            [{"original_file_parsed_on": "2025-03-10"}], "test sentence", 2,
            Exception("test exception"), True, ["Exception: test exception. Data: 2, test sentence"], 0
        )
    ]
)
def test_add_index_in_queue(
    reference_inn_instance: ReferenceInn,
    not_parsed_data: List[dict],
    sentence: str,
    index: int,
    ex_full: Exception,
    is_queue: bool,
    expected_logs: List[str],
    expected_queue_size: int,
    caplog: LogCaptureFixture,
) -> None:
    """
    Test the add_index_in_queue method of the ReferenceInn class.

    This test verifies that the add_index_in_queue method correctly processes and logs
    errors when an exception occurs during data processing. It uses parameterized inputs
    to simulate various scenarios, including when an exception is raised or not, and
    whether the error occurred in the main thread or in a separate thread.

    The test also ensures that the expected data is added to the retry queue and that
    the queue size is as expected. Caplog is used to capture the log messages.

    :param reference_inn_instance: Instance of ReferenceInn to test.
    :param not_parsed_data: List of dictionaries representing the data that was not parsed.
    :param sentence: String containing the sentence that was being parsed when the error occurred.
    :param index: Integer containing the index of the sentence that was being parsed when the error occurred.
    :param ex_full: Exception object containing the error that occurred.
    :param is_queue: Boolean indicating whether the error occurred in the main thread (False) or in a separate thread (True).
    :param expected_logs: List of expected log messages.
    :param expected_queue_size: Integer containing the expected size of the retry queue.
    :param caplog: LogCaptureFixture fixture for capturing log messages.
    """
    retry_queue: Queue = Queue()

    # Act
    with caplog.at_level(ERROR):
        reference_inn_instance.add_index_in_queue(not_parsed_data, retry_queue, is_queue, sentence, index, ex_full)

    # Assert
    for expected_log in expected_logs:
        assert expected_log in caplog.text
    assert retry_queue.qsize() == expected_queue_size


@pytest.mark.parametrize(
    "filename, russian_companies, foreign_companies, unknown_companies, expected_filenames",
    [
        (
            "test.xlsx",
            [{"name": "Russian Company"}],
            [{"name": "Foreign Company"}],
            [{"name": "Unknown Company"}],
            ["test_russia.json", "test_foreign.json", "test_unknown.json"],
        ),
        (
            "empty.xlsx",
            [],
            [],
            [],
            ["empty_russia.json", "empty_foreign.json", "empty_unknown.json"],
        ),
        (
            "russian.xlsx",
            [{"name": "Russian Company 1"}, {"name": "Russian Company 2"}],
            [],
            [],
            ["russian_russia.json", "russian_foreign.json", "russian_unknown.json"],
        ),
        (
            "test with spaces.xlsx",
            [{"name": "Company"}],
            [],
            [],
            ["test with spaces_russia.json", "test with spaces_foreign.json", "test with spaces_unknown.json"],
        ),
    ],
)
def test_write_to_json(
    reference_inn_instance: ReferenceInn,
    filename: str,
    russian_companies: list,
    foreign_companies: list,
    unknown_companies: list,
    expected_filenames: list,
    tmp_path: str
) -> None:
    """
    Test the write_to_json method of the ReferenceInn class.

    This test verifies that the write_to_json method correctly writes the Russian,
    foreign, and unknown companies to separate JSON files. The output files are
    named based on the original filename, appending '_russia.json', '_foreign.json',
    and '_unknown.json' for the respective categories.

    It checks that the file is created, that the data is written to the file, and
    that the data is correctly formatted.

    :param reference_inn_instance: ReferenceInn instance.
    :param filename: The filename of the Excel file.
    :param russian_companies: List of dictionaries containing the Russian companies.
    :param foreign_companies: List of dictionaries containing the foreign companies.
    :param unknown_companies: List of dictionaries containing the unknown companies.
    :param expected_filenames: List of strings containing the expected filenames.
    :param tmp_path: The temporary path to which the files are written.
    :return: None
    """
    # Arrange
    reference_inn_instance.filename = filename
    reference_inn_instance.russian_companies = russian_companies
    reference_inn_instance.foreign_companies = foreign_companies
    reference_inn_instance.unknown_companies = unknown_companies

    # Act
    reference_inn_instance.write_to_json()

    # Assert
    for filename in expected_filenames:
        filepath: str = os.path.join(tmp_path, "json", filename)
        assert os.path.exists(filepath)

        with open(filepath, "r") as f:
            if "_russia" in filename:
                assert json.load(f) == russian_companies
            elif "_foreign" in filename:
                assert json.load(f) == foreign_companies
            elif "_unknown" in filename:
                assert json.load(f) == unknown_companies
