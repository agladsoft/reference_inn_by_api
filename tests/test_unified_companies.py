import pytest
from typing import Type
from unittest.mock import Mock
from scripts.unified_companies import *
from requests.exceptions import HTTPError


class MockResponse:
    def __init__(self, status_code, json_data):
        self.status_code: int = status_code
        self.json_data: list = json_data

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError(f"Mock HTTP error: {self.status_code}")


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
def test_russian_companies_get_company_by_taxpayer_id(russian_companies, mocker, taxpayer_id, expected_company):
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
def test_kazakhstan_companies_get_company_by_taxpayer_id(kazakhstan_companies, mocker, taxpayer_id, expected_company):
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
def test_belarus_companies_get_company_by_taxpayer_id(belarus_companies, mocker, taxpayer_id, expected_company):
    mock_response = MockResponse(200, json_data={'row': {'vunp': taxpayer_id, 'vnaimk': expected_company}})
    mocker.patch('scripts.unified_companies.BaseUnifiedCompanies.get_response', return_value=mock_response)
    assert belarus_companies.get_company_by_taxpayer_id(taxpayer_id) == expected_company


@pytest.mark.parametrize(
    "taxpayer_id, expected_company",
    [
        ("305900252", 'ООО "Югум-Древ"'),
        ("309053845", 'СООО "ЗОВ-ЛенЕВРОМЕБЕЛЬ"')
    ],
)
def test_uzbekistan_companies_get_company_by_taxpayer_id(uzbekistan_companies, mocker, taxpayer_id, expected_company):
    # mock_response = MockResponse(200, text='<div class="card-body pt-0"><h6 class="card-title">test_company_name</h6></div>')
    # mocker.patch('scripts.unified_companies.BaseUnifiedCompanies.get_response', return_value=mock_response)
    # mocker.patch('scripts.unified_companies.GoogleTranslator.translate', return_value="test_company_name")
    assert uzbekistan_companies.get_company_by_taxpayer_id("123456789") == "test_company_name"
#
#
# def test_search_engine_parser_replace_quotes(search_engine_parser):
#     assert search_engine_parser.replace_quotes('test "test" test') == 'test "test" test'
#
#
# def test_search_engine_parser_get_inn_from_site(search_engine_parser):
#     dict_inn = {}
#     search_engine_parser.get_inn_from_site(dict_inn, ["1234567890"], 1)
#     assert dict_inn == {"1234567890": 1}
#
#
# def test_search_engine_parser_get_code_error(search_engine_parser):
#     error_code = ElemTree.fromstring('<error code="200">test error</error>')
#     with pytest.raises(AssertionError):
#         search_engine_parser.get_code_error(error_code, "test_value")
#
#     error_code = ElemTree.fromstring('<error code="400">test error</error>')
#     with pytest.raises(ConnectionRefusedError):
#         search_engine_parser.get_code_error(error_code, "test_value")
#
#
# def test_search_engine_parser_parse_xml(search_engine_parser):
#     dict_inn = {}
#     response = MockResponse(200, text='<root><response><reqid>123</reqid></response><group><doc><title>test title 1234567890</title><passage>test passage 0987654321</passage></doc></group></root>')
#     search_engine_parser.parse_xml(response, "test_value", dict_inn, 1)
#     assert dict_inn == {"1234567890": 1, "0987654321": 1}
#
#
# def test_search_engine_parser_get_inn_from_search_engine(search_engine_parser, mocker):
#     mock_response = MockResponse(200, text='<root><response><reqid>123</reqid></response><group><doc><title>test title 1234567890</title><passage>test passage 0987654321</passage></doc></group></root>')
#     mocker.patch('requests.get', return_value=mock_response)
#     assert search_engine_parser.get_inn_from_search_engine("test_value") == {"1234567890": 1, "0987654321": 1}
#
#
# def test_retry_on_failure(mocker):
#     mock_func = mocker.Mock(side_effect=[Exception("Test Exception"), 123])
#     decorated_func = retry_on_failure(attempts=2, delay=0)(mock_func)
#     assert decorated_func() == 123
#     assert mock_func.call_count == 2
#
#     mock_func = mocker.Mock(side_effect=Exception("Test Exception"))
#     decorated_func = retry_on_failure(attempts=2, delay=0)(mock_func)
#     with pytest.raises(HTTPError):
#         decorated_func()
#     assert mock_func.call_count == 2

