# Reference INN by API

## 📋 Описание проекта

Система автоматизированного поиска и валидации данных о компаниях по ИНН (Идентификационный номер налогоплательщика) через различные API. Проект предназначен для обработки больших объемов данных о компаниях с возможностью валидации ИНН, поиска информации о компаниях и перевода названий на русский язык.

## 📊 Функциональность

### ⚡ Основные возможности
- **Валидация ИНН** для компаний из России, Казахстана и Беларуси
- **Поиск информации о компаниях** по ИНН через официальные API
- **Автоматический перевод** названий компаний с иностранных языков на русский
- **Унификация названий компаний** с использованием различных алгоритмов сопоставления
- **Кэширование результатов** в SQLite для оптимизации повторных запросов
- **Многопоточная обработка** для ускорения работы с большими объемами данных
- **Интеграция с ClickHouse** для хранения результатов
- **Telegram уведомления** о статусе выполнения
- **Обработка файлов Excel/CSV** с автоматическим конвертированием

### 🌍 Поддерживаемые страны
- **Россия**: Валидация 10-значных (юридические лица) и 12-значных (физические лица) ИНН
- **Казахстан**: Валидация и поиск через API
- **Беларусь**: Валидация и поиск через API
- **Узбекистан**: Валидация и поиск через API (не используется)

### 🔌 Интеграции
- **API переводчиков**: Google Translate, Yandex Translate
- **Поисковые системы**: XML River (Яндекс)
- **Базы данных**: ClickHouse, SQLite (кэш)
- **Уведомления**: Telegram Bot API

## 📁 Структура проекта

```
reference_inn_by_api/
├── scripts/                    # Основные модули системы
│   ├── __init__.py            # Конфигурация, константы, логирование
│   ├── main.py                # Главный модуль с классом ReferenceInn
│   ├── unified_companies.py   # Унификация и поиск компаний по странам
│   ├── validate_inn.py        # Валидация ИНН
├── bash_dir/                  # Bash скрипты
│   ├── reference_inn_by_api.sh # Основной скрипт запуска
│   └── _reference.sh          # Вспомогательный скрипт
├── cache/                     # Кэш базы данных
│   └── cache.db              # SQLite база для кэширования
├── get_difference.py          # Утилита сравнения файлов
├── requirements.txt           # Python зависимости
├── Dockerfile                # Docker конфигурация
└── venv/                     # Виртуальное окружение
```

## 💻 Технические требования

### 🔧 Системные требования
- Python 3.8+
- Linux/Unix система (рекомендуется)
- Минимум 2 GB RAM
- Доступ к интернету для API запросов

### 📦 Зависимости
Основные Python пакеты (см. `requirements.txt`):
- `pandas==1.4.3` - обработка данных
- `requests==2.28.1` - HTTP запросы
- `clickhouse_connect==0.6.13` - подключение к ClickHouse
- `deep-translator==1.9.0` - переводы
- `fuzzywuzzy==0.18.0` - нечеткое сопоставление строк
- `beautifulsoup4==4.11.1` - парсинг HTML/XML
- `python-stdnum==1.17` - валидация номеров
- `csvkit==1.0.7` - работа с CSV

## 🚀 Установка и настройка

### 1. Клонирование репозитория
```bash
git clone <repository-url>
cd reference_inn_by_api
```

### 2. Создание виртуального окружения
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate     # Windows
```

### 3. Установка зависимостей
```bash
pip install -r requirements.txt
```

### 4. Настройка переменных окружения
Создайте файл `.env` в корне проекта:
```env
# ClickHouse настройки
HOST=your_clickhouse_host
DATABASE=your_database_name
USERNAME=your_username
PASSWORD=your_password

# API ключи
TOKEN_API_YANDEX=your_yandex_translate_token
USER_XML_RIVER=your_xml_river_user
KEY_XML_RIVER=your_xml_river_key

# Пути к файлам (Если запускается не через docker-compose)
XL_IDP_PATH_REFERENCE_INN_BY_API=/path/to/excel/files
XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS=/path/to/reference_inn_by_api
XL_IDP_PATH_DOCKER=/path/in/docker

# Telegram (опционально)
TOKEN_TELEGRAM=your_telegram_bot_token
CHAT_ID=your_chat_id
TOPIC=your_topic
ID=your_message_id
```

### 5. Установка дополнительных инструментов
```bash
# Для конвертации Excel файлов
pip install csvkit
```

## ▶️ Запуск

### 🖥️ Локальный запуск
```bash
# Переход в директорию скриптов
cd scripts

# Запуск основного модуля
python main.py
```

### 📜 Запуск через bash скрипт
```bash
# Настройка переменных окружения
export XL_IDP_PATH_REFERENCE_INN_BY_API="/path/to/your/excel/files"

# Запуск
./bash_dir/reference_inn_by_api.sh
```

### 🐳 Docker запуск

```yaml
version: '3.9'
services:
    reference_inn_by_api:
        container_name: reference_inn_by_api
        restart: always
        ports:
          - "8070:8070"
        volumes:
          - ${XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS}:${XL_IDP_PATH_DOCKER}
          - ${XL_IDP_ROOT_REFERENCE_INN_BY_API}:${XL_IDP_PATH_REFERENCE_INN_BY_API}
        environment:
          XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS: ${XL_IDP_PATH_DOCKER}
          XL_IDP_PATH_REFERENCE_INN_BY_API: ${XL_IDP_PATH_REFERENCE_INN_BY_API}
          TOKEN_TELEGRAM: ${TOKEN_TELEGRAM}
        build:
          context: reference_inn_by_api
          dockerfile: ./Dockerfile
          args:
            XL_IDP_PATH_DOCKER: ${XL_IDP_PATH_DOCKER}
        command:
          bash -c "sh ${XL_IDP_PATH_DOCKER}/bash_dir/_reference.sh"
        networks:
          - postgres
```

```bash
docker-compose up -d
```

## 📖 Использование

### 📥 Подготовка входных данных
1. Поместите Excel/CSV файлы в директорию, указанную в `XL_IDP_PATH_REFERENCE_INN_BY_API`
2. Файлы должны содержать колонку с названиями компаний
3. Поддерживаемые форматы: `.xls`, `.xlsx`

### 🔄 Процесс обработки
1. **Конвертация**: Excel файлы автоматически конвертируются в CSV
2. **Парсинг**: Извлекаются названия компаний
3. **Валидация**: Поиск и валидация ИНН в названиях
4. **Поиск**: Запросы к API для получения информации о компаниях
5. **Перевод**: Автоматический перевод названий на русский язык
6. **Сохранение**: Результаты сохраняются в ClickHouse и CSV файлы

### 📤 Выходные данные
- **CSV файлы** с обогащенными данными в папке `csv/`
- **JSON файлы** с метаданными в папке `json/`
- **Обработанные файлы** перемещаются в папку `done/`
- **Данные в ClickHouse** для дальнейшего анализа

## ⚙️ Конфигурация

### 🔧 Основные параметры (scripts/__init__.py)
- `COUNT_THREADS`: Количество потоков для обработки (по умолчанию 3)
- `PROXIES`: Список прокси серверов для запросов
- `REPLACED_QUOTES`: Символы для замены в названиях компаний
- `REPLACED_WORDS`: Слова для замены (ООО, ИП, и т.д.)

### 🔑 Настройка API
- **Yandex Translate**: Требуется `TOKEN_API_YANDEX`
- **Dadata**: Требуется `TOKEN_DADATA`
- **XML River**: Настройки `USER_XML_RIVER` и `KEY_XML_RIVER`

## 📊 Мониторинг и логирование

### 📱 Telegram уведомления
Система отправляет уведомления о:
- Завершении обработки файлов
- Ошибках в процессе работы
- Статистике обработанных записей

### 📝 Логирование
- Детальные логи всех операций
- Отслеживание ошибок API
- Метрики производительности

## 🔧 Устранение неисправностей

### ⚠️ Частые проблемы
1. **Ошибки API**: Проверьте корректность API ключей
2. **Подключение к ClickHouse**: Убедитесь в правильности настроек подключения
3. **Ошибки кодировки**: Используйте UTF-8 для входных файлов
4. **Нехватка места**: Система создает временные файлы, требуется свободное место

### 🐛 Отладка
```bash
# Включение детального логирования
export LOG_LEVEL=DEBUG

# Проверка подключения к ClickHouse
python -c "from scripts.main import ReferenceInn; ReferenceInn.connect_to_db()"
```

## 👨‍💻 Разработка

### 🏗️ Архитектура
- **main.py**: Основной класс `ReferenceInn` с логикой обработки
- **unified_companies.py**: Паттерн Strategy для работы с разными странами
- **translate.py**: Паттерн Strategy для различных переводчиков
- **validate_inn.py**: Утилиты валидации ИНН

### 🚀 Расширение функциональности
1. **Добавление новой страны**: Наследование от `BaseUnifiedCompanies`
2. **Новый переводчик**: Реализация `TranslatorStrategy`
3. **Дополнительные валидации**: Расширение `validate_inn.py`

## 🔒 Безопасность

- API ключи хранятся в переменных окружения
- Использование прокси для анонимизации запросов
- Валидация входных данных
- Ограничение частоты запросов к API

## ⚡ Производительность

- Многопоточная обработка (настраиваемое количество потоков)
- Кэширование результатов в SQLite
- Batch обработка для ClickHouse
- Оптимизированные SQL запросы
