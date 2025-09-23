# Threads Automation / Автоматизация Threads

## Overview / Обзор
This project contains a Python script that publishes content to Threads based on rows stored in Google Sheets. It keeps the
spreadsheet and Threads API in sync, queues posts, publishes them automatically, and writes back the execution results so that
campaigns can be planned without manual copy-paste. Скрипт автоматизирует публикацию постов в Threads на основе данных из
Google Sheets, синхронизируя очередь постов и статусы их обработки.

## Architecture / Архитектура решения
- **Python script / Python-скрипт** – entry point that fetches rows from Google Sheets, builds the payload, and posts updates to
  the Threads API. Внутри выделены слои работы с HTTP, бизнес-логикой и хранилищем.
- **Google Sheets** – serves as the source of truth for content and statuses. Таблица предоставляет REST‑эндпоинт (через Google
  Apps Script) для чтения и обновления строк.
- **Threads API** – official Meta endpoint that receives posts. Скрипт обрабатывает ответы API и обновляет таблицу.

## Requirements / Требования
- Python 3.11+
- Operating system capable of installing Python dependencies and with outbound Internet access. Операционная система должна
  позволять устанавливать зависимости и иметь доступ к сети Интернет.

### Dependencies / Зависимости
Pinned dependencies are listed in [`requirements.txt`](requirements.txt). Установить их можно командой:

```bash
pip install -r requirements.txt
```

## Environment variables and secrets / Переменные окружения и секреты
| Variable | Description (EN) | Описание (RU) |
| --- | --- | --- |
| `ID_GOOGLE_TABLE` | Identifier of the Google Sheets document that stores the publication queue. | Идентификатор Google Sheets c очередью публикаций. |
| `URL_GAS_RAZVERTIVANIA` | URL of the deployed Google Apps Script Web App exposing the REST interface. | URL развёрнутого Google Apps Script Web App с REST-интерфейсом к таблице. |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | JSON credentials of the Google service account (as a string or path). | JSON с учётными данными сервисного аккаунта Google (строка или путь к файлу). |
| `THREADS_ACCESS_TOKEN` | Threads API access token. | Токен доступа к Threads API. |
| `LOG_LEVEL` | Logging level (`info`, `debug`, `warning`, `error`). | Уровень логирования (`info`, `debug`, `warning`, `error`). |

Create a `.env` file locally or configure CI/CD secrets before running the script. Перед запуском создайте `.env` или
настройте секреты в CI/CD.

## Running the script / Запуск

### Local run / Локально
1. Clone the repository and enter the project directory. Склонируйте репозиторий и перейдите в директорию проекта.
2. Install dependencies: `pip install -r requirements.txt`.
3. Export environment variables or create a `.env`. Создайте `.env` или экспортируйте переменные окружения.
4. Run the entry point, for example:
   ```bash
   python src/main.py
   ```
   The script loads new rows, publishes them to Threads, and updates statuses. Скрипт загрузит новые записи, опубликует их и
   обновит статусы в таблице.

### GitHub Actions
- Configure a workflow with `actions/setup-python`, dependency installation, and the script execution step. Создайте workflow с
  установкой Python и запуском скрипта.
- Add the secrets (`ID_GOOGLE_TABLE`, `URL_GAS_RAZVERTIVANIA`, `GOOGLE_SERVICE_ACCOUNT_JSON`, `THREADS_ACCESS_TOKEN`) to GitHub
  Secrets and expose them through `env`. Секреты добавьте в GitHub Secrets и пробрасывайте через `env`.
- Use `on.schedule` (cron) for periodic runs. Рекомендуем cron `*/15 * * * *`, чтобы проверять новые публикации каждые 15 минут
  без излишней нагрузки.

## Logging and observability / Логирование и наблюдаемость
- Logs are emitted as JSON with `ts`, `level`, `msg`, and `context` fields. Логи выводятся в формате JSON со стандартными
  полями.
- Set `LOG_LEVEL` to `info` for normal runs and `debug` when investigating issues. Используйте `info` для штатной работы и
  `debug` для диагностики.
- Each successful run logs a heartbeat with the processed items count. Для heartbeat предусмотрен лог на уровне `info` с
  количеством обработанных постов.
- Errors go to `stderr`; retries are handled with exponential backoff. Ошибки публикуются в `stderr`, при сбоях выполняются
  повторы с экспоненциальным бэкоффом.

## Rate limits and recommendations / Ограничения и рекомендации по частоте запусков
- Threads API enforces limits on requests and publications per minute—stay below 60 requests/min. Threads API имеет квоты на
  число публикаций и запросов в минуту.
- Google Apps Script is limited by execution time (6 minutes on the free tier) and daily invocations. Google Apps Script
  ограничен по времени выполнения и количеству обращений.
- For large queues, add delays between posts (5–10 seconds) and avoid cron schedules more frequent than every 5 minutes to stay
  within quotas. При большом объёме контента задавайте паузу между публикациями (5–10 секунд) и не запускайте cron чаще, чем раз
  в 5 минут.
- Store tokens securely and rotate credentials regularly. Храните токены в секретах и регулярно ротируйте доступы.
