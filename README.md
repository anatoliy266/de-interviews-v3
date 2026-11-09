# Smart Logistics Data & ML Platform — сквозной ETL/DWH/ML-прототип для аналитики и прогнозирования тарифов грузоперевозок

Проект разворачивает локальный контур **PostgreSQL 17 + Apache Airflow + Python/CatBoost**, который проходит путь от сырых сделок `deals_bigdata` до аналитической витрины рыночного коридора ставок и таблицы пакетного ML-скоринга **«Факт vs Прогноз»**. Данные детерминированы (`as_of = 2026-10-26`, 300 000 сделок по умолчанию), а полный пайплайн можно воспроизвести одной командой `docker compose up` и повторным запуском `entry.py` за нужный период.

## Содержание

1. [Архитектура решения и обоснование стека](#архитектура-решения-и-обоснование-стека)
   - [Схема потока данных](#схема-потока-данных)
   - [Слои и артефакты](#слои-и-артефакты)
   - [Как устроены ETL и витрины](#как-устроены-etl-и-витрины)
   - [Почему PostgreSQL 17](#почему-postgresql-17)
   - [Почему Apache Airflow](#почему-apache-airflow)
   - [Почему CatBoostRegressor](#почему-catboostregressor)
   - [Защита от утечек и временная валидация](#защита-от-утечек-и-временная-валидация)
   - [Ограничения и принятые допущения](#ограничения-и-принятые-допущения)
2. [Требования и пошаговая инструкция запуска](#требования-и-пошаговая-инструкция-запуска)
   - [Системные требования](#системные-требования)
   - [Порты](#порты)
   - [Переменные окружения (.env)](#переменные-окружения-env)
   - [Шаг 1. Запуск инфраструктуры](#шаг-1-запуск-инфраструктуры)
   - [Шаг 2. Проверка инициализации БД](#шаг-2-проверка-инициализации-бд)
   - [Шаг 3. Полный прогон ETL + ML](#шаг-3-полный-прогон-etl--ml)
   - [Шаг 4. Работа через Airflow](#шаг-4-работа-через-airflow)
   - [Шаг 5. Повторный запуск и Backfill](#шаг-5-повторный-запуск-и-backfill)
   - [Шаг 6. Остановка и полный сброс](#шаг-6-остановка-и-полный-сброс)
3. [Проверка витрин (SQL)](#проверка-витрин-sql)
   - [1. Полнота слоёв](#1-полнота-слоёв)
   - [2. Data Quality и карантин](#2-data-quality-и-карантин)
   - [3. Валюты и рублёвая нормализация](#3-валюты-и-рублёвая-нормализация)
   - [4. Дедупликация городов и групп кузова](#4-дедупликация-городов-и-групп-кузова)
   - [5. Витрина `mart.route_card`](#5-витрина-martroute_card)
   - [6. Инварианты коридора, CV, MoM и YoY](#6-инварианты-коридора-cv-mom-и-yoy)
   - [7. Витрина прогнозов `mart.ml_price_predictions`](#7-витрина-прогнозов-martml_price_predictions)
   - [8. Пересчёт метрик ML прямо в SQL](#8-пересчёт-метрик-ml-прямо-в-sql)
4. [Сводка метрик ML](#сводка-метрик-ml)
   - [Итоговая таблица качества](#итоговая-таблица-качества)
   - [Как читать метрики](#как-читать-метрики)
   - [Условия эксперимента](#условия-эксперимента)
   - [Фактический вывод пайплайна обучения](#фактический-вывод-пайплайна-обучения)

## Архитектура решения и обоснование стека

### Схема потока данных

```mermaid
flowchart LR
    subgraph SRC["public — операционный источник"]
        P["prices"]
        C["cities"]
        BT["body_types<br/>price_body_types"]
        PD["price_dates"]
        FX["exchange_rates"]
    end

    subgraph ORCH["Оркестрация"]
        AF["Apache Airflow 3.4<br/>scheduler / api-server / dag-processor"]
        CLI["entry.py<br/>one-shot E2E-прогон"]
    end

    subgraph STG["stg — Core DWH и Data Quality"]
        SP["stg.prices"]
        SQ["stg.dq_quarantine_prices"]
        EC["stg.etl_control<br/>as_of_date"]
        V["SQL-вью<br/>v_dim_cities<br/>v_dim_body_types<br/>v_fact_market_prices<br/>v_price_features_source"]
    end

    subgraph MART["mart / ml — аналитический слой"]
        RC["mart.route_card<br/>materialized view"]
        FE["ml.price_features"]
        PR["mart.ml_price_predictions"]
    end

    subgraph MLBOX["ML-контур"]
        TR["CatBoostRegressor<br/>time-based split"]
        MD[("models/price_regression_v1.cbm")]
    end

    P --> AF
    C --> AF
    BT --> AF
    PD --> AF
    FX --> AF
    AF -->|"DAG: prices_etl_raw_to_stg"| SP
    AF --> SQ
    AF --> EC
    SP --> V
    AF -->|"DAG: prices_etl_stg_to_mart"| RC
    AF -->|"DAG: ml_price_features_generation"| FE
    V --> RC
    V --> FE
    FE --> TR
    TR --> MD
    MD -->|"batch predict"| PR
    CLI -.->|"последовательно запускает 5 шагов"| AF
```

Поток состоит из пяти воспроизводимых шагов:

1. **Raw → STG.** Чтение `public.prices` за окно `loading_date`, конвертация валют в RUB, DQ-валидация, обогащение категорий и идемпотентная загрузка в `stg.prices` / `stg.dq_quarantine_prices`.
2. **STG → Mart.** Пересчёт материализованного представления `mart.route_card` с рыночным коридором, динамикой и Data Trust.
3. **Feature Engineering.** Расчёт календарных и циклических признаков и загрузка в `ml.price_features`.
4. **Model Training.** Хронологическое разделение `train/test`, обучение `CatBoostRegressor`, сохранение модели в `models/price_regression_v1.cbm`.
5. **Batch Prediction.** Скоринг тестового периода и будущих заявок с записью в `mart.ml_price_predictions`.

### Слои и артефакты

| Слой | Объекты | Назначение |
| :--- | :--- | :--- |
| **Source** | `public.prices`, `public.cities`, `public.body_types`, `public.price_body_types`, `public.price_dates`, `public.exchange_rates` | Эталонная операционная схема и детерминированный synthetic-датасет из `sql/01_schema.sql` и `sql/02_synthetic.sql`. |
| **Core / DQ** | `stg.prices`, `stg.dq_quarantine_prices`, `stg.etl_control` | Очищенные сделки, карантин с кодом причины и указатель `as_of_date` для витрин. |
| **Справочные вьюхи** | `stg.v_dim_cities`, `stg.v_dim_body_types`, `stg.v_fact_market_prices`, `stg.v_price_features_source` | Дедупликация измерений, унификация кузовов, единая фактовая выборка для витрины и ML. |
| **Mart** | `mart.route_card` | Витрина `месяц × город отправления × город назначения × группа кузова × весовая категория` с `P25/P50/P75`, `руб./км`, `trips_count`, `carriers_count`, `cv_rate`, `mom_pct`, `yoy_pct`, `reliability_level`. |
| **ML** | `ml.price_features`, `mart.ml_price_predictions`, `models/price_regression_v1.cbm` | Признаковое пространство, результаты batch-скоринга и сериализованная модель CatBoost. |

Ключевые файлы проекта:

```text
.
├── docker-compose.yaml          # PostgreSQL 17 + Airflow + pipeline runner
├── entry.py                     # one-shot оркестратор ETL + ML
├── sql/
│   ├── 00-create-airflow-user.sql
│   ├── 01_schema.sql            # операционная схема public
│   ├── 02_synthetic.sql         # детерминированные данные, as_of = 2026-10-26
│   ├── 03_stg_schema.sql        # stg, DQ-карантин, справочные вьюхи
│   ├── 04_mart_schema.sql       # mart.route_card
│   └── 05_ml_schema.sql         # ml.price_features, mart.ml_price_predictions
├── etl/
│   ├── prices_raw_to_stg.py     # DQ, валюты, идемпотентная загрузка
│   └── prices_stg_to_mart.py    # REFRESH MATERIALIZED VIEW
├── ml/
│   ├── features_stg_to_ml.py    # календарные и циклические признаки
│   ├── train.py                 # Time-Based Split + CatBoost
│   └── predict.py               # Batch Inference
├── dags/                        # 3 Airflow DAG (бонусный контур)
├── utils/                       # подключение, валидация, callbacks, defaults
└── models/                      # сохранённая модель .cbm
```

### Как устроены ETL и витрины

**`etl/prices_raw_to_stg.py`** читает `public.prices` за окно `loading_date` (или CSV), подтягивает справочник городов и курсы ЦБ. Валюта `840`/`978` приводится к RUB по точному совпадению `(currency_code, nearest_rate_date)`; fallback по `loading_date` намеренно не используется. Затем применяются DQ-правила:

- `INVALID_PRICE` — `price <= 0`, а для RUB также `price <= 1000`;
- `INVALID_DISTANCE` — `distance <= 0`;
- `INVALID_WEIGHT` / `INVALID_VOLUME` — отрицательные вес или объём;
- `INVALID_CITY` — города нет в `cities`;
- `MISSING_RATE` — для валютной строки не найден точный курс.

Отклонённые строки не теряются: они пишутся в `stg.dq_quarantine_prices` с `dq_reject_reason`. Для чистых строк заполняются `weight_category` (`G20T`, `FTL`, `10T`, `5T`, `L5T`, `OTH` по весу с округлением до 0.1 т) и `volume_category` (`FTL` при `0 < V <= 120 м³`, иначе `OTH`).

Запись выполняется в одной транзакции: `DELETE` за загружаемое окно → `INSERT` чистых и карантинных строк → `UPSERT` в `stg.etl_control`. Поэтому повторный запуск за тот же период не задваивает данные. В `stg.etl_control` сохраняется `as_of_date`, который потом ограничивает историческую витрину.

**`etl/prices_stg_to_mart.py`** атомарно обновляет материализованное представление `mart.route_card` через `REFRESH MATERIALIZED VIEW`. Витрина строится по `price_dates.price_date`, а не по `prices.loading_date`, поэтому долгосрочная заявка корректно попадает в несколько месяцев. В расчёт входят только `is_fact = true`, `price_type IN (1,2,3,4,5,9)`, `complex = false`, `price > 0`, `distance > 0`, `outlier_mark = false` и `price_date <= as_of_date`. Дедупликация `cities.gc_id` выполняется выбором строки с минимальным `cities.id`; кузова приводятся к стандартным группам через `std_id` (`56` — тент, `4` — рефрижератор, `2` — изотерм, `15` — борт, `0` — не указан, `999` — прочие).

**`ml/features_stg_to_ml.py`** читает `stg.v_price_features_source` и рассчитывает календарные признаки: `year`, `month`, `weekday`, `day_of_year` и гармонические `sin/cos` для месяца, дня недели и дня года. Признаки сохраняются в `ml.price_features` идемпотентно: `DELETE` за окно → `INSERT`.

### Почему PostgreSQL 17

- **Источник уже PostgreSQL 17.** Держать ещё одну СУБД только ради витрин означало бы дублировать справочники, транзакции и инфраструктуру. В проекте один движок обслуживает `public → stg → mart/ml`, что уменьшает число точек отказа и делает запуск действительно однокомандным.
- **Объём данных не требует отдельного OLAP.** 300 000 сделок и несколько тысяч аналитических ячеек — это небольшой объём для PostgreSQL. `PERCENTILE_CONT`, `FILTER`, `DISTINCT ON`, оконные функции и материализованные представления закрывают весь расчёт коридора без внешнего вычислителя.
- **Нужны ACID и транзакционная идемпотентность.** `DELETE + INSERT` за окно, `UPSERT` в `etl_control` и `REFRESH MATERIALIZED VIEW` должны быть предсказуемыми. PostgreSQL даёт это «из коробки»; в ClickHouse пришлось бы отдельно проектировать `ReplacingMergeTree`, дедупликацию и eventual consistency.
- **ClickHouse в задании — бонус, а не обязательство.** Для 300 000 строк он добавил бы второй контейнер, второй SQL-диалект и риск расхождения результатов с обязательной реализацией. PostgreSQL выбран как достаточный и более простой в сопровождении вариант; при росте до сотен миллионов строк аналитический слой можно вынести в ClickHouse без изменения логики расчёта.
- **Богатый SQL для витрины.** `PERCENTILE_CONT(0.25/0.5/0.75)`, `STDDEV_SAMP`, `COUNT(DISTINCT ...) FILTER`, self-join для `MoM/YoY` и `MATERIALIZED VIEW` позволяют держать витрину в БД и не тащить расчёты в Python.

### Почему Apache Airflow

- **Регулярные ETL-шаги описаны как DAG.** В `dags/` есть три DAG: `prices_etl_raw_to_stg`, `prices_etl_stg_to_mart`, `ml_price_features_generation`. Они покрывают загрузку, пересчёт витрины и генерацию признаков.
- **Есть retries, backoff и дедлайны.** В `utils/default_args.py` заданы 3 повтора с экспоненциальной задержкой (5 минут, максимум 30 минут), а в DAG — `DeadlineAlert` на 2 часа и callbacks. Cron или ручной запуск скрипта этого не дают.
- **`max_active_runs=1` защищает от гонок.** Повторный запуск DAG не начнёт второй параллельный прогон поверх того же окна.
- **Airflow 3.4 с `api-server` и `dag-processor`.** Конфигурация соответствует актуальной архитектуре Airflow 3: отдельные scheduler, api-server и dag-processor, UI на `8081`.
- **Полный прогон без UI — через `entry.py`.** Для проверки на чистой машине не нужно настраивать Connections и тыкать кнопки: `entry.py` последовательно запускает пять шагов. Airflow остаётся для регулярного production-режима, `entry.py` — для воспроизводимого one-shot прогона и отладки. Обучение и batch scoring вынесены в `entry.py`; при необходимости они оборачиваются в отдельные DAG без изменения кода.

### Почему CatBoostRegressor

- **Категориальные признаки обрабатываются нативно.** В данных есть `loading_city`, `loading_region`, `unloading_city`, `unloading_region`, `body_group`, `weight_category`, `volume_category`. CatBoost работает с ними через `cat_features` без One-Hot раздувания и без ручного `TargetEncoder`, который легко приводит к утечке.
- **Не нужны масштабирование и импутация.** Деревья инвариантны к масштабу, а пропуски в категориях закрываются явной строкой `__MISSING__`. Это упрощает пайплайн и снижает риск ошибок на редких маршрутах.
- **Лучше бейзлайнов на табличных данных.** По R&D-отчёту `attachments/ml-report.md`: Ridge ≈ `R² 0.84 / MAPE 30%`, DecisionTree ≈ `0.90 / 14%`, CatBoost ≈ `0.93 / 13%`. На нашем тесте CatBoost показал `R² 0.9226` и `MAPE 15.33%` — заметно лучше линейной модели и устойчивее одиночного дерева.
- **Хорошо ловит нелинейные взаимодействия.** Тариф зависит от расстояния, региона, кузова, веса и сезонности совместно; бустинг моделирует эти взаимодействия без ручного конструирования попарных признаков.
- **Гиперпараметры зафиксированы.** `iterations=1500`, `learning_rate=0.05`, `depth=6`, `loss_function='RMSE'`, `eval_metric='MAE'`, `random_seed=42`, `early_stopping_rounds=100`. Ранняя остановка выбрала **296 итераций**.

### Защита от утечек и временная валидация

- **Целевая переменная** — только `price_rub` (ставка за рейс в RUB без НДС).
- **Запрещённые признаки:** `price_with_vat`, `price_rub / distance`, `price_rub / total_weight`, `carrier_cost`, `profit`, `price_group_id`, `avg_price`, `min_km_price`, `max_km_price`, `outlier_mark`. В `ml/train.py` они дополнительно перечислены в `LEAKAGE_COLUMNS` и удаляются из DataFrame.
- **`price_group_stats` не используется как признак.** Этот агрегат считается ночным джобом по сырой цене и может включать текущую предсказываемую сделку.
- **Time-Based Split без shuffle.** При `as_of = 2026-10-26`: `train = [2026-07-26, 2026-10-05)`, `test = [2026-10-05, 2026-10-26]`. Граница `2026-10-05` входит только в тест.
- **Все статистики очистки и импутации** должны считаться только по `train`; в базовой реализации дополнительные IQR-фильтры не применяются, чтобы не усложнять воспроизводимость.
- **География кодируется раздельно:** `loading_region` и `unloading_region` вместо склеенной строки `Город А → Город Б`, что снижает кардинальность и помогает на редких направлениях.

### Ограничения и принятые допущения

- **`outlier_mark` берётся из источника как есть.** Записи с `outlier_mark = true` исключаются из витрины и обучения; флаг не пересчитывается. У самых свежих сделок за `2026-10-26` ночной джоб разметки аномалий ещё не отработал, поэтому `outlier_mark = false` по умолчанию — это ограничение исходных данных, а не признак «чистой» цены. Дополнительный пересчёт по IQR / P5–P95 возможен только как необязательное улучшение и не заменяет входной флаг.
- **Будущие плановые месяцы не попадают в историческую витрину.** `mart.route_card` ограничена `price_date <= as_of_date`; для будущих заявок работает только ML-скоринг по `loading_date > 2026-10-26`.
- **Валютная строка без точного курса** отправляется в карантин (`MISSING_RATE`) и не попадает в нормализованную витрину; fallback по `loading_date` не используется.
- **`cities.gc_id` не уникален.** Дедупликация — `DISTINCT ON (gc_id) ... ORDER BY gc_id, id ASC`, то есть выбирается запись с минимальным `id`.
- **Кузова:** приоритет у `price_body_types`; если связи нет, используется `prices.body_type`; если и его нет — группа `0` («не указан»). Одна цена внутри одной стандартной группы кузова не дублируется.
- **`R²` на уровне отдельного маршрута закономерно ниже**, чем на всей сети: дисперсия тарифа внутри одного направления мала. Для карточки маршрута основные метрики — `MAPE` и `MAE`, а не `R²`.

## Требования и пошаговая инструкция запуска

### Системные требования

| Компонент | Минимум | Рекомендуется |
| :--- | :--- | :--- |
| Docker Engine / Docker Desktop | 24+ с Compose v2 | Docker Desktop 4.30+ |
| ОС | Linux, macOS, Windows с WSL2 | Windows 11 + WSL2 / Ubuntu 22.04+ |
| RAM | 8 ГБ | 16 ГБ (Airflow + PostgreSQL + CatBoost) |
| CPU | 2 vCPU | 4 vCPU |
| Диск | 10 ГБ свободно | 20 ГБ (образы, БД, модель, логи) |
| Сеть | доступ к Docker Hub | стабильный интернет для первой сборки |
| Свободные порты | `5433`, `8081` | — |

Python на хосте не обязателен: весь код выполняется внутри контейнеров. Для локальной отладки нужен Python 3.11+ и пакеты из `python_image/requirements.txt`.

### Порты

| Сервис | Хост | Контейнер | Назначение |
| :--- | :---: | :---: | :--- |
| PostgreSQL 17 | `5433` | `5432` | Основная БД `deals_bigdata` |
| Apache Airflow UI / API | `8081` | `8080` | Веб-интерфейс и REST API Airflow |

Внутри Docker-сети PostgreSQL доступен как `postgres:5432`; именно этот адрес используется в `entry.py` и DAG-подключениях.

### Переменные окружения (.env)

Файл `.env` лежит в корне проекта и подключается ко всем сервисам через `env_file`. Для production замените пароли и секреты; для локального теста значения ниже уже рабочие.

```dotenv
# Python-путь внутри Airflow-контейнеров
PYTHONPATH=/opt/airflow

# Пароль суперпользователя PostgreSQL (используется в строке подключения)
POSTGRES_PASSWORD=test

# Секреты Airflow
AIRFLOW__API_AUTH__JWT_SECRET=change_me_jwt_secret
AIRFLOW__CORE__FERNET_KEY=change_me_fernet_key
AIRFLOW__API__SECRET_KEY=change_me_api_secret

# Администратор Airflow UI
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=YourSuperSecretPassword123!
AIRFLOW_ADMIN_EMAIL=admin@local.dev

# Служебная БД Airflow
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASSWORD=airflow_pass
AIRFLOW_DB_NAME=airflow_db
AIRFLOW_DB_HOST=postgres
AIRFLOW_DB_PORT=5432
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@${AIRFLOW_DB_HOST}:${AIRFLOW_DB_PORT}/${AIRFLOW_DB_NAME}
```

| Переменная | Зачем нужна |
| :--- | :--- |
| `POSTGRES_PASSWORD` | Пароль пользователя `postgres`; должен совпадать с паролем в строке подключения. |
| `AIRFLOW_ADMIN_*` | Создание администратора Airflow при `airflow-init`. |
| `AIRFLOW_DB_*` | Параметры служебной БД Airflow. |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Готовая строка подключения Airflow к PostgreSQL. |
| `AIRFLOW__API_AUTH__JWT_SECRET`, `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__API__SECRET_KEY` | Секреты Airflow; для локального запуска можно оставить тестовые. |

### Шаг 1. Запуск инфраструктуры

Из корня проекта:

```bash
docker compose up -d --build
```

Если используется старая установка Compose, команда может называться `docker-compose up -d --build`. Первый запуск занимает **2–5 минут**: собираются образы, PostgreSQL применяет скрипты из `sql/` (включая генерацию 300 000 сделок), Airflow выполняет `airflow db migrate` и создаёт администратора.

Проверить состояние контейнеров:

```bash
docker compose ps
```

Ожидаемые сервисы: `de_postgres` (healthy), `de_airflow-init` (exited 0), `de_airflow-api-server`, `de_airflow-dag-processor`, `de_airflow-scheduler`, `de_pipeline_runner`.

Посмотреть логи инициализации PostgreSQL:

```bash
docker compose logs -f postgres
```

### Шаг 2. Проверка инициализации БД

```bash
docker exec -it de_postgres psql -U postgres -d deals_bigdata -c "SELECT count(*) AS raw_prices FROM public.prices;"
```

Ожидаемый результат для датасета по умолчанию — `300000`. Также должны существовать схемы `stg`, `mart`, `ml`:

```bash
docker exec -it de_postgres psql -U postgres -d deals_bigdata -c "SELECT nspname FROM pg_namespace WHERE nspname IN ('stg','mart','ml') ORDER BY 1;"
```

Если база уже инициализирована, повторный `docker compose up` **не перезапускает** SQL-скрипты из `docker-entrypoint-initdb.d` — это стандартное поведение PostgreSQL. Для чистого перезапуска удалите volume (см. шаг 6).

### Шаг 3. Полный прогон ETL + ML

`entry.py` последовательно запускает пять шагов: Raw → STG, STG → Mart, Feature Engineering, Training, Batch Prediction.

```bash
docker exec -it de_pipeline_runner python entry.py --connection "postgresql://postgres:test@postgres:5432/deals_bigdata" --model-path "models/price_regression_v1.cbm" --table-name "mart.ml_price_predictions" --provider sql 2023-06-01 2026-10-26
```

Пояснения:

- `--provider sql` обязателен; значение по умолчанию `manual_exec` предназначено только для отладки и не читает данные.
- Начало `2023-06-01` загружает всю историю synthetic-датасета, чтобы в `mart.route_card` были доступны `MoM` и `YoY`. Если нужен только ML-контур, можно начать с `2026-07-26`, но тогда `YoY` в витрине будет `NULL`.
- Конец `2026-10-26` — это `as_of`; он же становится `stg.etl_control.as_of_date` и датой среза для `train/test`.
- Обучение занимает несколько минут; в конце в консоль выводятся метрики и сохраняется модель `models/price_regression_v1.cbm`.

Проверить, что прогнозы появились:

```bash
docker exec -it de_postgres psql -U postgres -d deals_bigdata -c "SELECT count(*) AS predictions, count(*) FILTER (WHERE actual_price_rub IS NOT NULL) AS with_actual, count(*) FILTER (WHERE actual_price_rub IS NULL) AS future FROM mart.ml_price_predictions;"
```

### Шаг 4. Работа через Airflow

Airflow UI доступен по адресу:

```text
http://localhost:8081
```

Логин и пароль — из `.env` (`AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD`). DAG-и используют PostgreSQL Connection с идентификатором `my_postgres_db_conn`; его нужно создать один раз.

Через CLI:

```bash
docker exec -it de_airflow-api-server airflow connections add my_postgres_db_conn --conn-type postgres --conn-host postgres --conn-schema deals_bigdata --conn-login postgres --conn-password test --conn-port 5432
```

Порядок запуска DAG-ов:

1. `prices_etl_raw_to_stg` — загрузка и очистка (params: `date_from_iso`, `date_to_iso`, `data_provider=sql`).
2. `prices_etl_stg_to_mart` — обновление `mart.route_card`.
3. `ml_price_features_generation` — генерация `ml.price_features` (params: `start_date`, `end_date`).

Пример запуска через CLI:

```bash
docker exec -it de_airflow-api-server airflow dags trigger prices_etl_raw_to_stg
docker exec -it de_airflow-api-server airflow dags trigger prices_etl_stg_to_mart
docker exec -it de_airflow-api-server airflow dags trigger ml_price_features_generation --conf "{\"start_date\":\"2026-07-26\",\"end_date\":\"2026-10-26\"}"
```

В UI то же самое делается кнопкой **Trigger DAG** с указанием params. Для регулярного режима достаточно включить расписание; DAG-и уже содержат retries, exponential backoff, `max_active_runs=1`, deadline 2 часа и callbacks.

Обучение и batch scoring в Airflow-контур не включены: они выполняются через `entry.py`, чтобы полный цикл был воспроизводим одной командой. При необходимости шаги 4–5 легко оборачиваются в отдельные DAG-и без изменения ML-кода.

### Шаг 5. Повторный запуск и Backfill

Повторный запуск за тот же период безопасен для ETL и признаков: `stg.prices`, `stg.dq_quarantine_prices` и `ml.price_features` перезаписываются через `DELETE + INSERT` в транзакции, а `mart.route_card` обновляется атомарно. `mart.ml_price_predictions` перед вставкой очищается по `loading_date >= as_of`, поэтому batch scoring также идемпотентен.

Полный прогон за конкретное окно (например, сентябрь 2026):

```bash
docker exec -it de_pipeline_runner python entry.py --connection "postgresql://postgres:test@postgres:5432/deals_bigdata" --model-path "models/price_regression_v1.cbm" --table-name "mart.ml_price_predictions" --provider sql 2026-09-01 2026-09-30
```

Обратите внимание: `entry.py` переобучает модель на окне, оканчивающемся новой датой `as_of`, и перезаписывает `models/price_regression_v1.cbm`. Если нужно обновить только слой DWH без переобучения, запускайте шаги отдельно:

```bash
docker exec -it de_pipeline_runner python etl/prices_raw_to_stg.py --connection "postgresql://postgres:test@postgres:5432/deals_bigdata" --provider sql 2026-09-01 2026-09-30
docker exec -it de_pipeline_runner python etl/prices_stg_to_mart.py --connection "postgresql://postgres:test@postgres:5432/deals_bigdata"
```

Только признаки:

```bash
docker exec -it de_pipeline_runner python ml/features_stg_to_ml.py --connection "postgresql://postgres:test@postgres:5432/deals_bigdata" 2026-07-26 2026-10-26
```

Только обучение и скоринг:

```bash
docker exec -it de_pipeline_runner python ml/train.py --connection "postgresql://postgres:test@postgres:5432/deals_bigdata" --model-path "models/price_regression_v1.cbm" --table-name "mart.ml_price_predictions" 2026-10-26
docker exec -it de_pipeline_runner python ml/predict.py --connection "postgresql://postgres:test@postgres:5432/deals_bigdata" --model-path "models/price_regression_v1.cbm" --table-name "mart.ml_price_predictions" 2026-10-26
```

### Шаг 6. Остановка и полный сброс

Остановить контейнеры, сохранив данные:

```bash
docker compose down
```

Остановить и удалить volume PostgreSQL (полный сброс схемы и данных):

```bash
docker compose down -v
```

После `down -v` следующий `docker compose up -d --build` заново применит `sql/00...sql`–`sql/05...sql` и пересоздаст `deals_bigdata`.

## Проверка витрин (SQL)

Все запросы ниже выполняются в БД `deals_bigdata`. Удобнее открыть интерактивный `psql`:

```bash
docker exec -it de_postgres psql -U postgres -d deals_bigdata
```

Дальше можно вставлять блоки целиком. Значения в комментариях приведены для датасета по умолчанию (`n_prices = 300000`, `as_of = 2026-10-26`).

### 1. Полнота слоёв

```sql
SELECT
    (SELECT count(*) FROM public.prices)             AS raw_prices,
    (SELECT count(*) FROM stg.prices)                AS core_prices,
    (SELECT count(*) FROM stg.dq_quarantine_prices)  AS quarantine_prices,
    (SELECT count(*) FROM stg.v_dim_cities)          AS dim_cities,
    (SELECT count(*) FROM mart.route_card)           AS route_card_rows,
    (SELECT count(*) FROM ml.price_features)         AS ml_features,
    (SELECT count(*) FROM mart.ml_price_predictions) AS predictions;
```

Ориентир для полного окна `2023-06-01 … 2026-10-26`: `raw_prices = 300000`, `core_prices = 299797`, `quarantine_prices = 203`, `dim_cities = 40`. Остальные значения зависят от периода и запуска ML.

Проверить, что ни одна строка не потерялась между источником и Core DWH:

```sql
SELECT
    (SELECT count(*) FROM stg.prices)
  + (SELECT count(*) FROM stg.dq_quarantine_prices) AS accounted_rows,
    (SELECT count(*) FROM public.prices)            AS source_rows;
```

Для полного окна значения должны совпадать. Если ETL запускался за меньшее окно, сравнивайте только строки этого окна по `loading_date`.

### 2. Data Quality и карантин

```sql
SELECT
    dq_reject_reason,
    count(*) AS rows
FROM stg.dq_quarantine_prices
GROUP BY 1
ORDER BY 2 DESC;
```

Ожидаемые причины: `INVALID_PRICE`, `INVALID_DISTANCE`, `INVALID_WEIGHT`, `INVALID_VOLUME`, `INVALID_CITY`, `MISSING_RATE`. Сумма по всем причинам должна быть равна `quarantine_prices` из предыдущего запроса.

Проверить, что в чистый слой не попали явно битые строки:

```sql
SELECT
    count(*) FILTER (WHERE price <= 0)                         AS bad_price,
    count(*) FILTER (WHERE distance <= 0)                      AS bad_distance,
    count(*) FILTER (WHERE total_weight < 0)                   AS bad_weight,
    count(*) FILTER (WHERE total_volume < 0)                   AS bad_volume,
    count(*) FILTER (WHERE loading_city_gcid IS NULL)          AS bad_loading_city,
    count(*) FILTER (WHERE unloading_city_gcid IS NULL)        AS bad_unloading_city
FROM stg.prices;
```

Все значения должны быть `0`.

### 3. Валюты и рублёвая нормализация

```sql
SELECT
    currency_code,
    count(*)                                            AS rows,
    count(*) FILTER (WHERE nearest_rate_date IS NULL)    AS without_rate_date,
    round(min(price)::numeric, 2)                       AS min_price_rub,
    round(max(price)::numeric, 2)                       AS max_price_rub
FROM stg.prices
GROUP BY 1
ORDER BY 1;
```

В `stg.prices.price` уже лежит цена в RUB: исходные `840`/`978` умножены на курс по `nearest_rate_date`, RUB оставлен как есть. Строки без точного курса не должны присутствовать в `stg.prices`; они лежат в карантине:

```sql
SELECT count(*) AS missing_rate_rows
FROM stg.dq_quarantine_prices
WHERE dq_reject_reason = 'MISSING_RATE';
```

### 4. Дедупликация городов и групп кузова

```sql
-- В источнике gc_id не уникален: у Москвы и Казани есть дубли.
SELECT
    count(*)                  AS raw_city_rows,
    count(DISTINCT gc_id)    AS unique_gc_id
FROM public.cities;

SELECT gc_id, count(*) AS duplicates
FROM public.cities
GROUP BY 1
HAVING count(*) > 1
ORDER BY 1;

-- В dim-вьюхе должна остаться ровно одна строка на gc_id.
SELECT count(*) AS dim_rows FROM stg.v_dim_cities;
```

Ориентир: `42` строки в `public.cities`, `40` уникальных `gc_id`, дубли — `1001` (Москва) и `1003` (Казань), `40` строк в `stg.v_dim_cities`.

Проверить унификацию кузовов и отсутствие размножения одной цены внутри одной стандартной группы:

```sql
SELECT body_group_std_id, count(*) AS fact_month_rows
FROM stg.v_fact_market_prices
GROUP BY 1
ORDER BY 1;

SELECT id, loading_month, body_group_std_id, count(*) AS cnt
FROM stg.v_fact_market_prices
GROUP BY 1, 2, 3
HAVING count(*) > 1
LIMIT 20;
```

Вторая выборка должна вернуть **0 строк**. Допустимое исключение — одна и та же цена в разных стандартных группах кузова, если в источнике у неё несколько несвязанных типов; внутри одной группы дублей быть не должно.

### 5. Витрина `mart.route_card`

Обзор витрины:

```sql
SELECT
    count(*)                                              AS cells,
    min(loading_month)                                    AS first_month,
    max(loading_month)                                    AS last_month,
    count(*) FILTER (WHERE reliability_level = 'HIGH')    AS high,
    count(*) FILTER (WHERE reliability_level = 'MEDIUM')  AS medium,
    count(*) FILTER (WHERE reliability_level = 'LOW_DATA') AS low_data,
    round(avg(trips_count)::numeric, 2)                   AS avg_trips_per_cell
FROM mart.route_card;
```

Ориентир: `last_month = 2026-10-01`; будущие месяцы (`price_date > 2026-10-26`) в витрину не попадают.

Карточка конкретного маршрута (Москва → Казань, тент, FTL):

```sql
SELECT
    loading_month,
    origin_city,
    destination_city,
    CASE body_group_std_id
        WHEN 56 THEN 'Тент'
        WHEN 4  THEN 'Рефрижератор'
        WHEN 2  THEN 'Изотерм'
        WHEN 15 THEN 'Борт'
        WHEN 0  THEN 'Не указан'
        ELSE 'Прочие'
    END                                                   AS body_group,
    weight_category,
    trips_count,
    carriers_count,
    round(p25_trip::numeric, 2)                           AS p25_trip,
    round(median_trip::numeric, 2)                        AS median_trip,
    round(p75_trip::numeric, 2)                           AS p75_trip,
    round(p25_km::numeric, 2)                             AS p25_km,
    round(median_km::numeric, 2)                          AS median_km,
    round(p75_km::numeric, 2)                             AS p75_km,
    round(mom_pct::numeric, 2)                            AS mom_pct,
    round(yoy_pct::numeric, 2)                            AS yoy_pct,
    reliability_level
FROM mart.route_card
WHERE origin_city = 'Москва'
  AND destination_city = 'Казань'
  AND body_group_std_id = 56
  AND weight_category = 'FTL'
ORDER BY loading_month DESC
LIMIT 12;
```

Если конкретное направление пустое, уберите фильтры по `body_group_std_id` и `weight_category` — на синтетическом датасете распределение маршрутов неравномерное.

Проверить, что `P25`, `P50` и `P75` не перепутаны:

```sql
SELECT
    count(*) FILTER (WHERE p25_trip > median_trip OR median_trip > p75_trip) AS bad_trip_corridor,
    count(*) FILTER (WHERE p25_km > median_km OR median_km > p75_km)         AS bad_km_corridor,
    count(*) FILTER (WHERE trips_count < 2 AND cv_rate IS NOT NULL)          AS bad_cv_null
FROM mart.route_card;
```

Все три значения должны быть `0`: коридор упорядочен, а `cv_rate` равен `NULL` при менее чем двух наблюдениях.

### 6. Инварианты коридора, CV, MoM и YoY

Проверка `MoM`: если предыдущего календарного месяца нет — `mom_pct IS NULL`; если есть — значение должно совпадать с пересчётом от `median_trip`.

```sql
WITH with_prev AS (
    SELECT
        c.origin_city_gcid,
        c.destination_city_gcid,
        c.body_group_std_id,
        c.weight_category,
        c.loading_month,
        c.median_trip,
        c.mom_pct,
        p.median_trip AS prev_median
    FROM mart.route_card c
    LEFT JOIN mart.route_card p
      ON p.loading_month = (c.loading_month - INTERVAL '1 month')::date
     AND p.origin_city_gcid = c.origin_city_gcid
     AND p.destination_city_gcid = c.destination_city_gcid
     AND p.body_group_std_id = c.body_group_std_id
     AND p.weight_category = c.weight_category
)
SELECT count(*) AS bad_mom
FROM with_prev
WHERE (prev_median IS NULL AND mom_pct IS NOT NULL)
   OR (prev_median IS NOT NULL AND mom_pct IS NULL)
   OR (
        prev_median IS NOT NULL
        AND prev_median <> 0
        AND mom_pct IS NOT NULL
        AND abs(mom_pct - (median_trip / prev_median - 1) * 100) > 1e-6
   );
```

Ожидается `0`. Аналогично можно проверить `YoY`, заменив `INTERVAL '1 month'` на `INTERVAL '1 year'` и `mom_pct` на `yoy_pct`.

Распределение надёжности:

```sql
SELECT reliability_level, count(*) AS cells
FROM mart.route_card
GROUP BY 1
ORDER BY 1;
```

Ожидаемые значения: `HIGH`, `MEDIUM`, `LOW_DATA`. `HIGH` требует одновременно `trips_count >= 30`, `carriers_count >= 5` и свежесть последней даты не более 30 дней относительно `as_of`.

### 7. Витрина прогнозов `mart.ml_price_predictions`

```sql
SELECT
    count(*)                                              AS rows,
    count(*) FILTER (WHERE actual_price_rub IS NOT NULL)  AS actual_rows,
    count(*) FILTER (WHERE actual_price_rub IS NULL)      AS future_rows,
    min(loading_date)                                     AS min_loading_date,
    max(loading_date)                                     AS max_loading_date,
    count(*) FILTER (
        WHERE actual_price_rub IS NULL
          AND (abs_error_rub IS NOT NULL OR ape_pct IS NOT NULL)
    )                                                     AS bad_future_rows
FROM mart.ml_price_predictions;
```

`bad_future_rows` должен быть `0`: для будущих заявок (`loading_date > as_of`) поля `actual_price_rub`, `abs_error_rub` и `ape_pct` обязаны быть `NULL`, даже если в исходной строке была заполнена `price`.

Проверить отсутствие дублей после повторного скоринга:

```sql
SELECT price_id, loading_date, body_group, count(*) AS cnt
FROM mart.ml_price_predictions
GROUP BY 1, 2, 3
HAVING count(*) > 1
LIMIT 20;
```

Ожидается **0 строк**: `predict.py` очищает таблицу по `loading_date >= as_of` перед вставкой.

Пример «Факт vs Прогноз» по историческим сделкам:

```sql
SELECT
    loading_date,
    loading_city_gcid,
    unloading_city_gcid,
    body_group,
    actual_price_rub,
    predicted_price_rub,
    abs_error_rub,
    ape_pct
FROM mart.ml_price_predictions
WHERE actual_price_rub IS NOT NULL
ORDER BY loading_date DESC
LIMIT 10;
```

Пример будущих заявок:

```sql
SELECT
    loading_date,
    loading_city_gcid,
    unloading_city_gcid,
    body_group,
    predicted_price_rub,
    actual_price_rub,
    abs_error_rub,
    ape_pct
FROM mart.ml_price_predictions
WHERE actual_price_rub IS NULL
ORDER BY loading_date
LIMIT 10;
```

### 8. Пересчёт метрик ML прямо в SQL

Этот запрос повторяет метрики обучения по сохранённым прогнозам. Он полезен как независимая проверка того, что в БД лежат корректные `actual_price_rub` / `predicted_price_rub`.

```sql
WITH m AS (
    SELECT
        actual_price_rub::numeric    AS y,
        predicted_price_rub::numeric AS yhat
    FROM mart.ml_price_predictions
    WHERE actual_price_rub IS NOT NULL
      AND predicted_price_rub IS NOT NULL
)
SELECT
    count(*)                                                          AS n,
    round(avg(abs(y - yhat))::numeric, 2)                             AS mae_rub,
    round(sqrt(avg((y - yhat) * (y - yhat)))::numeric, 2)             AS rmse_rub,
    round((1 - sum((y - yhat) * (y - yhat))
             / nullif(sum((y - mean_y) * (y - mean_y)), 0))::numeric, 4) AS r2,
    round(avg(abs((y - yhat) / nullif(y, 0)))::numeric * 100, 2)      AS mape_pct
FROM (
    SELECT y, yhat, avg(y) OVER () AS mean_y
    FROM m
) t;
```

Если `mart.ml_price_predictions` содержит только тестовое окно, результат должен быть близок к метрикам из раздела [Сводка метрик ML](#сводка-метрик-ml). Небольшое расхождение возможно, если после обучения таблица была дополнена прогнозами за другие периоды или если в неё попали строки с `actual_price_rub`, не входившие в тест.

## Сводка метрик ML

### Итоговая таблица качества

Модель `CatBoostRegressor` обучена на исторических завершённых перевозках. Метрики ниже получены на отложенной тестовой выборке `2026-10-05 … 2026-10-26` (граница `2026-10-05` входит только в тест).

| Метрика | Значение | Что показывает |
| :--- | ---: | :--- |
| **$R^2$** | **0.9226** | Модель объясняет **92.26%** дисперсии фактического тарифа по маршрутной сети. |
| **MAPE** | **15.33%** | Средняя относительная ошибка прогноза; удобна для сравнения коротких и дальних маршрутов. |
| **RMSE** | **53 332.83 ₽** | Квадратичная ошибка в рублях; сильнее штрафует крупные промахи на дорогих и дальних рейсах. |
| **MAE** | **27 606.56 ₽** | Средняя абсолютная ошибка в рублях на один рейс; наиболее «физичная» бизнес-метрика. |

Дополнительно: ранняя остановка выбрала **296 итераций** из 1500; модель сохранена в `models/price_regression_v1.cbm`.

### Как читать метрики

- **$R^2 = 0.9226$** — высокое качество для одной глобальной модели на разнородной маршрутной сети. Важно: на уровне отдельного узкого направления $R^2$ закономерно падает, потому что дисперсия тарифа внутри одного маршрута мала. Для карточки маршрута ориентируйтесь на `MAPE` и `MAE`.
- **$MAPE = 15.33\%$** — в среднем модель ошибается на 15% от фактической ставки. Это рабочий уровень для предварительного ориентира в тендере; для финального решения логист всё равно смотрит на коридор `P25–P75` и `reliability_level`.
- **$RMSE = 53 332.83$ ₽** — больше `MAE`, потому что крупные ошибки на дорогих рейсах штрафуются квадратично. Разрыв между `RMSE` и `MAE` — индикатор наличия отдельных крупных промахов.
- **$MAE = 27 606.56$ ₽** — средняя абсолютная ошибка прогноза за рейс. Именно эту величину проще всего обсуждать с бизнесом: «в среднем модель промахивается на ~27.6 тыс. руб. за рейс».

### Условия эксперимента

| Параметр | Значение |
| :--- | :--- |
| Дата среза `as_of` | `2026-10-26` |
| Train | `[2026-07-26, 2026-10-05)` |
| Test | `[2026-10-05, 2026-10-26]` |
| Валидация | Time-Based Split, без `shuffle` |
| Модель | `CatBoostRegressor` |
| Гиперпараметры | `iterations=1500`, `learning_rate=0.05`, `depth=6`, `loss_function='RMSE'`, `eval_metric='MAE'`, `random_seed=42`, `early_stopping_rounds=100` |
| Лучшая итерация | `296` |
| Целевая переменная | `price_rub` — ставка за рейс в RUB без НДС |
| Категориальные признаки | `loading_city`, `loading_region`, `unloading_city`, `unloading_region`, `body_group`, `weight_category`, `volume_category` |
| Числовые признаки | `distance`, `total_weight`, `total_volume`, координаты, год, месяц, день недели, день года и их `sin/cos`-кодирование |
| Исключённые утечки | `price_with_vat`, производные `price/distance` и `price/weight`, `carrier_cost`, `profit`, `price_group_id`, `avg_price`, `min_km_price`, `max_km_price`, `outlier_mark` |

### Фактический вывод пайплайна обучения

Фрагмент консольного лога после успешного обучения — подтверждение метрик и артефакта модели:

```text
Shrink model to first 296 iterations.
2026-11-09 01:21:51 [INFO] __main__: Метрики на тесте: MAE=27606.56, RMSE=53332.83, MAPE=0.1533, R2=0.9226
2026-11-09 01:21:51 [INFO] __main__: Модель успешно сохранена по пути: models\price_regression_v1.cbm
2026-11-09 01:21:51 [INFO] __main__: ML пайплайн обучения успешно завершен.
2026-11-09 01:21:51 [INFO] __main__: Подключение к БД закрыто
2026-11-09 01:21:51 [INFO] __main__: === Пайплайн успешно завершен ===


Результаты обучения:
  R2 Score: 0.9226
  MAE:      27606.56 руб.
```

Интерпретация для бизнеса: модель объясняет более 92% разброса ставок и в среднем ошибается на 15.33% / 27.6 тыс. руб. за рейс. Этого достаточно, чтобы использовать прогноз как предварительный ориентир и сравнивать его с рыночным коридором `P25–P75`; финальное решение по ставке остаётся за логистом с учётом `reliability_level`, срочности и сезонности.



