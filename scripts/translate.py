from __init__ import *
from abc import ABC, abstractmethod
from deep_translator import GoogleTranslator, exceptions


class TranslatorStrategy(ABC):
    """Абстрактный класс стратегии перевода."""

    @abstractmethod
    def translate(self, text: str, source_lang: str, target_lang: str) -> str:
        pass

    @staticmethod
    def get_response(url: str, headers: dict, data: dict):
        try:
            response = requests.post(url, json=data, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Ошибка запроса к API: {e}")
            return {}


class YandexTranslator(TranslatorStrategy):
    """Конкретная стратегия перевода через Yandex API."""

    URL = "https://translate.api.cloud.yandex.net/translate/v2/translate"

    def translate(self, text: str, source_lang: str = "en", target_lang: str = "ru") -> str:
        headers = {"Authorization": f"Bearer {TOKEN_YANDEX}"}
        data = {
            "folderId": FOLDER_YANDEX,
            "texts": [text],
            "sourceLanguageCode": source_lang,
            "targetLanguageCode": target_lang
        }
        response_data = self.get_response(self.URL, headers=headers, data=data)

        if "translations" in response_data:
            return response_data["translations"][0]["text"]
        logger.error(f"Ошибка перевода через Yandex API: {response_data}")
        return text


class GoogleTranslatorAdapter(TranslatorStrategy):
    """Конкретная стратегия перевода через Google Translator API."""

    def translate(self, text: str, source_lang: str = "en", target_lang: str = "ru") -> str:
        try:
            return GoogleTranslator(source=source_lang, target=target_lang).translate(text[:4500])
        except (exceptions.NotValidPayload, exceptions.NotValidLength, Exception) as e:
            logger.error(f"Ошибка перевода через Google Translator API: {e}")
            return text


class TranslatorFactory:
    """Фабричный метод для выбора нужного переводчика."""

    TRANSLATORS = {
        "yandex": YandexTranslator,
        "google": GoogleTranslatorAdapter
    }

    @staticmethod
    def get_translator(service: str) -> TranslatorStrategy:
        if translator_class := TranslatorFactory.TRANSLATORS.get(service):
            return translator_class()
        else:
            raise ValueError("Неизвестный сервис перевода")
