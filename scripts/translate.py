from scripts.__init__ import *
from abc import ABC, abstractmethod
from deep_translator import GoogleTranslator, exceptions


class TranslatorStrategy(ABC):
    """An abstract class of translation strategy"""

    @abstractmethod
    def translate(self, text: str, source_lang: str, target_lang: str) -> str:
        """
        Abstract method for translating text from one language to another.
        :param text: Text to be translated.
        :param source_lang: Source language code (e.g. "en" for English).
        :param target_lang: Target language code (e.g. "ru" for Russian).
        :return: Translated text.
        """
        pass

    @staticmethod
    def get_response(url: str, headers: dict, data: dict) -> dict:
        """
        Sends a POST request to the specified URL with the given headers and data.

        This static method attempts to send a POST request to the provided URL using
        the specified headers and JSON data. If the request is successful, it returns
        the JSON response as a dictionary. If an exception occurs during the request,
        it logs the error and returns an empty dictionary.

        :param url: The URL to send the POST request to.
        :param headers: A dictionary of headers to include in the request.
        :param data: A dictionary of data to be sent as JSON in the request body.
        :return: A dictionary containing the JSON response or an empty dictionary if an error occurs.
        """
        try:
            response: requests.Response = requests.post(url, json=data, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Ошибка запроса к API: {e}")
            return {}


class YandexTranslatorAdapter(TranslatorStrategy):
    """A specific translation strategy via the Yandex API"""

    URL: str = "https://translate.api.cloud.yandex.net/translate/v2/translate"

    def translate(self, text: str, source_lang: str = "en", target_lang: str = "ru") -> str:
        """
        Translates the given text from the source language to the target language using the Yandex API.

        This method constructs a request with the provided text, source language, and target language,
        sends it to the Yandex translation API, and returns the translated text. If the translation is
        successful, the translated text is returned. If an error occurs during the API call, it logs
        the error and returns the original text.

        :param text: The text to be translated.
        :param source_lang: The code of the source language (default is "en" for English).
        :param target_lang: The code of the target language (default is "ru" for Russian).
        :return: The translated text or the original text if an error occurs.
        """
        headers: dict = {
            "Content-Type": "application/json",
            "Authorization": f"Api-Key {TOKEN_API_YANDEX}"
        }
        data: dict = {
            "sourceLanguageCode": source_lang,
            "targetLanguageCode": target_lang,
            "texts": [text]
        }
        response_data: dict = self.get_response(self.URL, headers=headers, data=data)

        if "translations" in response_data:
            return response_data["translations"][0]["text"]
        logger.error(f"Ошибка перевода через Yandex API: {response_data}")
        return text


class GoogleTranslatorAdapter(TranslatorStrategy):
    """A specific translation strategy via the Google Translator API"""

    def translate(self, text: str, source_lang: str = "en", target_lang: str = "ru") -> str:
        """
        Translates the given text from the source language to the target language using the Google Translator API.

        This method utilizes the Google Translator API to translate the provided text from the specified source
        language to the target language. It handles exceptions that may occur during the translation process
        and logs errors if the translation fails. The method returns the translated text or the original text
        in case of an error.

        :param text: The text to be translated.
        :param source_lang: The code of the source language (default is "en" for English).
        :param target_lang: The code of the target language (default is "ru" for Russian).
        :return: The translated text or the original text if an error occurs.
        """
        try:
            return GoogleTranslator(source=source_lang, target=target_lang).translate(text[:4500])
        except (exceptions.NotValidPayload, exceptions.NotValidLength, Exception) as e:
            logger.error(f"Ошибка перевода через Google Translator API: {e}")
            return text


class TranslatorFactory:
    """Factory method for selecting the desired translator"""

    TRANSLATORS: dict = {
        "yandex": YandexTranslatorAdapter,
        "google": GoogleTranslatorAdapter
    }

    @staticmethod
    def get_translator(service: str) -> TranslatorStrategy:
        """
        Factory method for selecting the desired translator.
        :param service: Yandex or Google translator.
        :return: class of translator strategy (TranslatorStrategy) or ValueError
                 if unknown service is provided as argument `service`.
        """
        if translator_class := TranslatorFactory.TRANSLATORS.get(service):
            return translator_class()
        else:
            raise ValueError("Неизвестный сервис перевода")
