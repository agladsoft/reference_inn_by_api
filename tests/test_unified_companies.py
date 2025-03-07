import os

os.environ["XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS"] = "."

import json
import pytest
from typing import Type
from unittest.mock import Mock
from scripts.unified_companies import *

class MockResponse(Response):
    def __init__(self, status_code: int, json_data: Optional[Union[list, dict]] = None, text: Optional[str] = None):
        super().__init__()
        self.status_code = status_code  # Родной атрибут Response

        # Устанавливаем `_content`, чтобы `text` и `json()` работали корректно
        if json_data is not None:
            self._content = json.dumps(json_data).encode('utf-8')  # JSON в байты
        elif text is not None:
            self._content = text.encode('utf-8')  # Обычный текст в байты
        else:
            self._content = b''

    def json(self, **kwargs):
        try:
            return json.loads(self.content)  # `self.content` аналогичен `requests.Response`
        except json.JSONDecodeError as e:
            raise ValueError("No valid JSON data provided") from e

    @property
    def text(self):
        return self.content.decode('utf-8') if self.content else ""


test_sentence: str = "AFRUIT LLC, 192249, SOFIJSKAYA STR., D. 60,  LITER AM, KORPUS 8, P.KAM.3,  ST.PETERSBURG, RUSSIA"
test_response_xml: str = (
    "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
    "<yandexsearch version=\"1.0\">"
    "<response date=\"20250225T123714\">"
    "<found priority=\"all\">3000000</found>"
    "<correct>AFRUIT LLC, 192249, SOFIJSKAYA STR., D. 60, LITER AM, KORPUS 8, P.KAM.3, ST.PETERSBURG, RUSSIA ИНН</correct>"
    "<fixtype>correct</fixtype>"
    "<results>"
    "<grouping>"
    "<page first=\"1\" last=\"14\">1</page>"
    "<group id=\"1\">"
    "<doccount>1</doccount>"
    "<doc>"
    "<url>https://www.rusprofile.ru/id/1227800121779</url>"
    "<title>ООО \"Афрут\" Санкт-Петербург (ИНН 7816734305) адрес...</title>"
    "<contenttype>organic</contenttype>"
    "<data_amp></data_amp>"
    "<passages>"
    "<passage>192249, город Санкт-Петербург, Софийская ул, д. 60 литера АМ, кор. 8 часть камеры 3.... "
    "Генеральный директор ООО \"Афрут\" - Попова Ирина Геннадьевна (ИНН 781118914402).</passage>"
    "</passages>"
    "<tablesnippet></tablesnippet>"
    "</doc>"
    "</group>"
    "</grouping>"
    "</results>"
    "</response>"
    "</yandexsearch>"
)


@pytest.fixture
def mock(mocker: Mock) -> None:
    mocker.patch("scripts.unified_companies.BaseUnifiedCompanies.cache_add_and_save")


@pytest.fixture
def unified_companies_manager():
    return UnifiedCompaniesManager(only_russian=True)


@pytest.fixture
def unified_companies_manager_not_only_russian():
    return UnifiedCompaniesManager(only_russian=False)


@pytest.fixture
def russian_companies(mock):
    return UnifiedRussianCompanies()


@pytest.fixture
def kazakhstan_companies(mock):
    return UnifiedKazakhstanCompanies()


@pytest.fixture
def belarus_companies(mock):
    return UnifiedBelarusCompanies()


@pytest.fixture
def uzbekistan_companies(mock):
    return UnifiedUzbekistanCompanies()


@pytest.fixture
def search_engine_parser(unified_companies_manager):
    return SearchEngineParser(country=[], manager=unified_companies_manager)


@pytest.mark.parametrize(
    "taxpayer_id, expected",
    [
        ("9729133245", True),
        ("6319160313", True),
        ("744800275165", True),
        ("770476437984", True),
        ("1234567890123", False),
        ("1234567891", False),
        ("12345678910", False),
        ("abcdefghijk", False),
    ],
)
def test_russian_companies_is_valid(
    russian_companies: UnifiedRussianCompanies,
    taxpayer_id: str,
    expected: str
) -> None:
    assert russian_companies.is_valid(taxpayer_id) == expected


@pytest.mark.parametrize(
    "taxpayer_id, expected",
    [
        ("921140000433", True),
        ("061040008424", True),
        ("12345678901", False),
        ("1234567890123", False),
        ("abcdefghijkl", False),
    ],
)
def test_kazakhstan_companies_is_valid(
    kazakhstan_companies: UnifiedKazakhstanCompanies,
    taxpayer_id: str,
    expected: str
) -> None:
    assert kazakhstan_companies.is_valid(taxpayer_id) == expected


@pytest.mark.parametrize(
    "taxpayer_id, expected",
    [
        ("790973974", True),
        ("800019585", True),
        ("190491584", True),
        ("1234567890", False),
        ("12345678910", False),
        ("abcdefghi", False),
        ("000000000", False),
    ],
)
def test_belarus_companies_is_valid(
    belarus_companies: UnifiedBelarusCompanies,
    taxpayer_id: str,
    expected: str
) -> None:
    assert belarus_companies.is_valid(taxpayer_id) == expected


@pytest.mark.parametrize(
    "taxpayer_id, expected",
    [
        ("305900252", True),
        ("309053845", True),
        ("923456789", False),
        ("12345678", False),
        ("1234567890", False),
        ("abcdefghi", False),
    ],
)
def test_uzbekistan_companies_is_valid(
    uzbekistan_companies: UnifiedUzbekistanCompanies,
    taxpayer_id: str,
    expected: str
) -> None:
    assert uzbekistan_companies.is_valid(taxpayer_id) == expected


@pytest.mark.parametrize(
    "taxpayer_id, expected",
    [
        ("9729133245", [UnifiedRussianCompanies()]),
        ("921140000433", [UnifiedKazakhstanCompanies()]),
        ("790973974", [UnifiedBelarusCompanies()]),
        # ("305900252", [UnifiedUzbekistanCompanies()])
    ],
)
def test_get_valid_company(
    unified_companies_manager: UnifiedCompaniesManager,
    taxpayer_id: str,
    expected: List[callable]
) -> None:
    assert list(unified_companies_manager.get_valid_company(taxpayer_id)) == expected


@pytest.mark.parametrize(
    "taxpayer_id, expected",
    [
        ("9729133245", []),
        ("921140000433", [UnifiedKazakhstanCompanies()]),
        ("790973974", [UnifiedBelarusCompanies()]),
        # ("305900252", [UnifiedUzbekistanCompanies()])
    ],
)
def test_get_valid_company(
    unified_companies_manager_not_only_russian: UnifiedCompaniesManager,
    taxpayer_id: str,
    expected: List[callable]
) -> None:
    assert list(unified_companies_manager_not_only_russian.get_valid_company(taxpayer_id)) == expected


@pytest.mark.parametrize(
    "taxpayer_id, expected_check_digit",
    [
        ("9729133245", "5"),
        ("6319160313", "3"),
    ],
)
def test_russian_companies_calc_company_check_digit(
    russian_companies: UnifiedRussianCompanies,
    taxpayer_id: str,
    expected_check_digit: str
) -> None:
    assert russian_companies.calc_company_check_digit(taxpayer_id) == expected_check_digit


@pytest.mark.parametrize(
    "taxpayer_id, expected_check_digits",
    [
        ("9729133245", "69"),
        ("6319160313", "89"),
    ],
)
def test_russian_companies_calc_personal_check_digits(
    russian_companies: UnifiedRussianCompanies,
    taxpayer_id: str,
    expected_check_digits: str
) -> None:
    assert russian_companies.calc_personal_check_digits(taxpayer_id) == expected_check_digits


@pytest.mark.parametrize(
    "taxpayer_id, expected_number",
    [
        (" 9729133245 ", "9729133245"),
        ("6319160313\n", "6319160313"),
    ],
)
def test_russian_companies_validate_valid(
    russian_companies: UnifiedRussianCompanies,
    taxpayer_id: str,
    expected_number: str
) -> None:
    assert russian_companies.validate(taxpayer_id) == expected_number


@pytest.mark.parametrize(
    "taxpayer_id, expected_exception",
    [
        ("123456789", InvalidLength),
        ("12345678901", InvalidLength),
        ("123456789a", InvalidFormat),
        ("1234567890123", InvalidLength),
    ],
)
def test_russian_companies_validate_invalid(
    russian_companies: UnifiedRussianCompanies,
    taxpayer_id: str,
    expected_exception: Type[Union[InvalidLength, InvalidFormat]]
) -> None:
    with pytest.raises(expected_exception):
        russian_companies.validate(taxpayer_id)


@pytest.mark.parametrize(
    "taxpayer_id, expected_company",
    [
        ("9729133245", 'ООО "КПДТ"'),
        ("6319160313", 'ООО "СТРОЙСТАНДАРТ"')
    ],
)
def test_russian_companies_get_company_by_taxpayer_id(
    russian_companies: UnifiedRussianCompanies,
    mocker: Mock,
    taxpayer_id: str,
    expected_company: str
) -> None:
    mock_response: MockResponse = MockResponse(
        status_code=200,
        json_data=[[{"value": expected_company, "unrestricted_value": expected_company}]]
    )
    mocker.patch('requests.post', return_value=mock_response)
    assert russian_companies.get_company_by_taxpayer_id(taxpayer_id) == expected_company


@pytest.mark.parametrize(
    "taxpayer_id, expected_company",
    [
        ("921140000433", 'ТОО "Кублей"'),
        ("061040008424", 'ТОО "Альянс-Брок"')
    ],
)
def test_kazakhstan_companies_get_company_by_taxpayer_id(
    kazakhstan_companies: UnifiedKazakhstanCompanies,
    mocker: Mock,
    taxpayer_id: str,
    expected_company: str
) -> None:
    mock_response = MockResponse(200, json_data={"results": [{"name": expected_company}]})
    mocker.patch('scripts.unified_companies.BaseUnifiedCompanies.get_response', return_value=mock_response)
    assert kazakhstan_companies.get_company_by_taxpayer_id(taxpayer_id) == expected_company


@pytest.mark.parametrize(
    "taxpayer_id, expected_company",
    [
        ("790973974", 'ООО "Югум-Древ"'),
        ("800019585", 'СООО "ЗОВ-ЛенЕВРОМЕБЕЛЬ"')
    ],
)
def test_belarus_companies_get_company_by_taxpayer_id(
    belarus_companies: UnifiedBelarusCompanies,
    mocker: Mock,
    taxpayer_id: str,
    expected_company: str
) -> None:
    mock_response = MockResponse(200, json_data={'row': {'vunp': taxpayer_id, 'vnaimk': expected_company}})
    mocker.patch('scripts.unified_companies.BaseUnifiedCompanies.get_response', return_value=mock_response)
    assert belarus_companies.get_company_by_taxpayer_id(taxpayer_id) == expected_company


@pytest.mark.parametrize(
    "taxpayer_id, expected_company",
    [
        ("305900252", '"AUTOMOTIVE AND MACHINERY SOLUTIONS" mas`uliyati cheklangan jamiyati'),
        ("309053845", '"TOSHKENT TRAKTOR ZAVODI" mas`uliyati cheklangan jamiyati')
    ],
)
def test_uzbekistan_companies_get_company_by_taxpayer_id(
    uzbekistan_companies: UnifiedUzbekistanCompanies,
    mocker: Mock,
    taxpayer_id: str,
    expected_company: str
) -> None:
    mock_response = MockResponse(
        status_code=200,
        text=f'<div class="card-body pt-0"><h6 class="card-title">{expected_company}</h6></div>'
    )
    mocker.patch('scripts.unified_companies.BaseUnifiedCompanies.get_response', return_value=mock_response)
    assert uzbekistan_companies.get_company_by_taxpayer_id(taxpayer_id) == expected_company


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
    search_engine_parser: SearchEngineParser,
    sentence: str,
    expected: str
) -> None:
    assert search_engine_parser.replace_quotes(sentence) == expected


@pytest.mark.parametrize(
    "taxpayer_id, expected_count",
    [
        ("9729133245", 1),
        ("921140000433", 1),
        ("790973974", 1)
        # ("305900252", 1)
    ],
)
def test_search_engine_parser_get_inn_from_site(
    search_engine_parser: SearchEngineParser,
    taxpayer_id: str,
    expected_count: int
) -> None:
    dict_inn: dict = {}
    search_engine_parser.get_inn_from_site(dict_inn, [taxpayer_id], expected_count)
    assert dict_inn == {taxpayer_id: expected_count}


def test_search_engine_parser_get_code_error(search_engine_parser: SearchEngineParser) -> None:
    error_code: ElemTree.Element = ElemTree.fromstring('<error code="200">test error</error>')
    with pytest.raises(AssertionError):
        search_engine_parser.get_code_error(error_code, "test_value")

    error_code: ElemTree.Element = ElemTree.fromstring('<error code="400">test error</error>')
    with pytest.raises(ConnectionRefusedError):
        search_engine_parser.get_code_error(error_code, "test_value")


def test_search_engine_parser_parse_xml(search_engine_parser: SearchEngineParser) -> None:
    dict_inn: dict = {}
    response: MockResponse = MockResponse(
        status_code=200,
        text=test_response_xml
    )
    search_engine_parser.parse_xml(response, test_sentence, dict_inn, 1)
    assert dict_inn == {"781118914402": 1, "7816734305": 1}


def test_search_engine_parser_get_inn_from_search_engine(
    search_engine_parser: SearchEngineParser,
    mocker: Mock
) -> None:
    mock_response: MockResponse = MockResponse(200, text=test_response_xml)
    mocker.patch('requests.get', return_value=mock_response)
    assert list(search_engine_parser.get_inn_from_search_engine(test_sentence)) == ['781118914402', '7816734305']


def test_retry_on_failure(mocker):
    mock_func = mocker.Mock(side_effect=[Exception("Test Exception"), 123])
    decorated_func = retry_on_failure(attempts=2, delay=0)(mock_func)
    assert decorated_func() == 123
    assert mock_func.call_count == 2

    mock_func = mocker.Mock(side_effect=Exception("Test Exception"))
    decorated_func = retry_on_failure(attempts=2, delay=0)(mock_func)
    with pytest.raises(HTTPError):
        decorated_func()
    assert mock_func.call_count == 2

