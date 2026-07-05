# ISS Tracker

Пайплайн на Airflow, который получает текущие координаты ISS через API, обогащает данные информацией о стране через reverse geocoding API и сохраняет результат в ClickHouse.

## Описание проекта

Проект состоит из DAG в Airflow, который выполняется по расписанию каждую минуту:

1. Получает текущие координаты Международной космической станции из API.
2. Отправляет координаты в сервис reverse geocoding.
3. Определяет страну, country code и признак нахождения над сушей.
4. Загружает результат в ClickHouse.

Проект собран в Docker и включает:
- Airflow.
- PostgreSQL для метаданных Airflow.
- ClickHouse для хранения данных.

## Технологии

- Python
- Apache Airflow
- ClickHouse
- PostgreSQL
- Docker
- requests
- clickhouse-driver
- DBeaver

## Структура проекта

```text
.
├── dags/
│   └── iss_tracker.py
├── logs/
├── plugins/
├── docker-compose.yml
├── README.md
└── .gitignore
```

## Как работает pipeline

- `fetch_iss_position` - получает координаты ISS.
- `get_country` - определяет страну по координатам.
- `save_to_clickhouse` - записывает данные в ClickHouse.

## ClickHouse schema

Для хранения данных используется таблица:

- `timestamp` - дата и время получения координат.
- `latitude` - широта.
- `longitude` - долгота.
- `country` - страна.
- `country_code` - код страны.
- `is_over_land` - флаг, находится ли ISS над сушей.

## Как запустить

### 1. Клонировать репозиторий

```bash
git clone <repo-url>
cd <repo-name>
```

### 2. Поднять контейнеры

```bash
docker compose up -d
```

### 3. Открыть Airflow

Перейдите в браузере:

```text
http://localhost:8080
```

### 4. Активировать DAG

В интерфейсе Airflow найдите DAG `iss_tracker` и включите его.

## ClickHouse table

Если таблица ещё не создана, выполните:

```sql
CREATE DATABASE IF NOT EXISTS iss;

CREATE TABLE IF NOT EXISTS iss.positions
(
    `timestamp` DateTime,
    `latitude` Float64,
    `longitude` Float64,
    `country` Nullable(String),
    `country_code` Nullable(String),
    `is_over_land` UInt8
)
ENGINE = MergeTree
ORDER BY `timestamp`;
```

## Notes

- В проекте используется публичное API ISS.
- Если reverse geocoding не возвращает данные, `country` и `country_code` сохраняются как `NULL`.
- `is_over_land` в текущей версии выставляется как `1` при успешном ответе reverse geocoding и `0` при ошибке.
