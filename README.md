# Threads Metrics

Инструмент для сбора метрик постов Threads и загрузки их в Google Sheets.

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=src
```

## Конфигурация

Заполните файл `.env` на основе `.env.example`. Критичные переменные:

- `ID_GOOGLE_TABLE` — идентификатор Google-таблицы с данными.
- `URL_GAS_RAZVERTIVANIA` — URL развёрнутого Google Apps Script.
- `GOOGLE_SERVICE_ACCOUNT_JSON` — JSON-ключ сервисного аккаунта Google.
- `THREADS_API_BASE_URL` — базовый URL Threads Graph API (по умолчанию `https://graph.threads.net`).
- `THREADS_REQUEST_TIMEOUT` — таймаут HTTP-запросов в секундах.
- `THREADS_CONCURRENCY` — максимальное число параллельных запросов.
- `THREADS_STATE_FILE` — путь к файлу состояния.
- `THREADS_METRICS_TTL_MIN` — TTL метрик в минутах.

## Запуск

```bash
python -m threads_metrics.main run
```

Команда запуска организует асинхронный сбор постов из Threads по токенам с листа `accounts_threads`,
агрегирует метрики и записывает их на лист `Data_Po_kagdomy_posty`. Прогресс обработки хранится в
файле состояния, а логи выводятся в формате JSON с heartbeat-сообщениями.

## Тесты

```bash
pytest
```
