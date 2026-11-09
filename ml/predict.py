import sys
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from catboost import CatBoostRegressor

from utils.connection import create_db_engine
from utils import defaults
from ml.features_stg_to_ml import build_calendar_features
from etl.prices_raw_to_stg import (
    apply_dq_rules,
    enrich_categories,
    read_reference_data,
    transform_currency,
)

logger = logging.getLogger("orchestrator")

CATEGORICAL_FEATURES = [
    "loading_city", "loading_region",
    "unloading_city", "unloading_region",
    "body_group", "weight_category", "volume_category"
]

NUMERIC_FEATURES = [
    "distance", "total_weight", "total_volume",
    "loading_latitude", "loading_longitude",
    "unloading_latitude", "unloading_longitude",
    "year", "month", "weekday", "day_of_year",
    "day_of_year_sin", "day_of_year_cos",
    "month_sin", "month_cos",
    "weekday_sin", "weekday_cos"
]

FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def prepare_model_data(df: pd.DataFrame) -> pd.DataFrame:
    """Подготовка признаков для CatBoost."""
    features = df[FEATURES].copy()
    for col in CATEGORICAL_FEATURES:
        features[col] = features[col].fillna("__MISSING__").astype(str)
    return features


def _read_historical_features(engine: Engine, start_date, end_date) -> pd.DataFrame:
    """Читает исторические признаки из ml.price_features.

    Если таблица признаков пуста (например, Feature ETL ещё не запускался),
    используется тот же источник, из которого она строится, —
    stg.v_price_features_source.
    """
    query = text("""
        SELECT
            price_id,
            loading_date,
            loading_city_gcid,
            loading_city,
            loading_region,
            loading_latitude,
            loading_longitude,
            unloading_city_gcid,
            unloading_city,
            unloading_region,
            unloading_latitude,
            unloading_longitude,
            distance,
            total_weight,
            total_volume,
            body_group,
            weight_category,
            volume_category,
            price_rub AS actual_price_rub
        FROM ml.price_features
        WHERE loading_date >= :start_date
          AND loading_date <= :end_date
        ORDER BY loading_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={"start_date": start_date, "end_date": end_date},
        )

    if not df.empty:
        return df

    logger.warning(
        "ml.price_features пуст за %s — %s, читаю историю из stg.v_price_features_source",
        start_date,
        end_date,
    )
    return _read_features_from_stg(engine, start_date, end_date)


def _read_features_from_stg(engine: Engine, start_date, end_date) -> pd.DataFrame:
    """Читает исторические признаки напрямую из stg-вьюхи и считает календарь."""
    query = text("""
        SELECT
            price_id,
            loading_date,
            loading_city_gcid,
            loading_city,
            loading_region,
            loading_latitude,
            loading_longitude,
            unloading_city_gcid,
            unloading_city,
            unloading_region,
            unloading_latitude,
            unloading_longitude,
            distance,
            total_weight,
            total_volume,
            body_group,
            weight_category,
            volume_category,
            price_rub AS actual_price_rub
        FROM stg.v_price_features_source
        WHERE loading_date >= :start_date
          AND loading_date <= :end_date
        ORDER BY loading_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={"start_date": start_date, "end_date": end_date},
        )
    if df.empty:
        return df
    return build_calendar_features(df)


def _read_future_features_from_stg(engine: Engine, as_of_date) -> pd.DataFrame:
    """Будущие заявки из stg-вьюхи (если ETL уже загрузил будущий период)."""
    query = text("""
        SELECT
            price_id,
            loading_date,
            loading_city_gcid,
            loading_city,
            loading_region,
            loading_latitude,
            loading_longitude,
            unloading_city_gcid,
            unloading_city,
            unloading_region,
            unloading_latitude,
            unloading_longitude,
            distance,
            total_weight,
            total_volume,
            body_group,
            weight_category,
            volume_category,
            price_rub AS actual_price_rub
        FROM stg.v_price_features_source
        WHERE loading_date > :as_of_date
        ORDER BY loading_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"as_of_date": as_of_date})
    if df.empty:
        return df
    return build_calendar_features(df)


def _read_future_features_from_raw(engine: Engine, as_of_date) -> pd.DataFrame:
    """Строит признаки будущих заявок напрямую из public.prices.

    Повторяет правила Core ETL (конвертация валют, DQ, категории) и логику
    ``stg.v_price_features_source`` (фильтры, маппинг кузова, города).
    Нужен потому, что при штатном запуске ``entry.py`` ETL ограничен
    датой ``as_of`` и будущих строк в stg/ml нет.
    """
    raw_query = text("""
        SELECT *
        FROM public.prices
        WHERE loading_date > :as_of_date
        ORDER BY loading_date
    """)
    with engine.connect() as conn:
        raw = pd.read_sql(raw_query, conn, params={"as_of_date": as_of_date})

    if raw.empty:
        logger.info("В public.prices нет заявок с loading_date > %s", as_of_date)
        return pd.DataFrame()

    cities_ids, rates = read_reference_data(engine)
    raw = transform_currency(raw, rates)
    raw = apply_dq_rules(raw, cities_ids)
    raw = enrich_categories(raw)

    raw = raw[raw["dq_reject_reason"].isna()].copy()
    raw = raw[
        raw["is_fact"].eq(True)
        & raw["price"].gt(0)
        & raw["distance"].gt(0)
        & ~raw["complex"].eq(True)
        & ~raw["outlier_mark"].eq(True)
    ].copy()

    if raw.empty:
        return pd.DataFrame()

    body_query = text("""
        WITH raw_price_bodies AS (
            SELECT
                pbt.price_id,
                pbt.body_type_id AS final_body_type_id
            FROM public.price_body_types pbt
            INNER JOIN public.prices p ON p.id = pbt.price_id
            WHERE p.loading_date > :as_of_date

            UNION ALL

            SELECT
                p.id AS price_id,
                p.body_type AS final_body_type_id
            FROM public.prices p
            WHERE p.loading_date > :as_of_date
              AND p.body_type IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.price_body_types pbt
                  WHERE pbt.price_id = p.id
              )
        )
        SELECT DISTINCT
            rpb.price_id,
            CASE
                WHEN rpb.final_body_type_id IS NULL THEN 0
                ELSE COALESCE(dbt.body_type_std_id, 0)
            END AS body_group
        FROM raw_price_bodies rpb
        LEFT JOIN stg.v_dim_body_types dbt
            ON rpb.final_body_type_id = dbt.raw_body_type_id
    """)
    cities_query = text("""
        SELECT gc_id, name, region_name, latitude, longitude
        FROM stg.v_dim_cities
    """)
    with engine.connect() as conn:
        body_map = pd.read_sql(body_query, conn, params={"as_of_date": as_of_date})
        cities = pd.read_sql(cities_query, conn)

    if body_map.empty:
        logger.warning("Не удалось определить body_group для будущих заявок")
        return pd.DataFrame()

    raw = raw.rename(columns={"id": "price_id"})
    raw = raw.merge(body_map, on="price_id", how="inner")

    loading_cities = cities.rename(columns={
        "gc_id": "loading_city_gcid",
        "name": "loading_city",
        "region_name": "loading_region",
        "latitude": "loading_latitude",
        "longitude": "loading_longitude",
    })
    unloading_cities = cities.rename(columns={
        "gc_id": "unloading_city_gcid",
        "name": "unloading_city",
        "region_name": "unloading_region",
        "latitude": "unloading_latitude",
        "longitude": "unloading_longitude",
    })

    raw = raw.merge(loading_cities, on="loading_city_gcid", how="inner")
    raw = raw.merge(unloading_cities, on="unloading_city_gcid", how="inner")

    future = raw[
        [
            "price_id", "loading_date",
            "loading_city_gcid", "loading_city", "loading_region",
            "loading_latitude", "loading_longitude",
            "unloading_city_gcid", "unloading_city", "unloading_region",
            "unloading_latitude", "unloading_longitude",
            "distance", "total_weight", "total_volume",
            "body_group", "weight_category", "volume_category",
            "price_rub",
        ]
    ].copy()
    future = future.rename(columns={"price_rub": "actual_price_rub"})
    return build_calendar_features(future)


def _read_future_features(engine: Engine, as_of_date) -> pd.DataFrame:
    """Собирает будущие заявки из raw и stg, убирая возможные дубли."""
    parts = []

    try:
        raw_future = _read_future_features_from_raw(engine, as_of_date)
        if not raw_future.empty:
            parts.append(raw_future)
    except Exception:
        logger.warning(
            "Не удалось построить будущие признаки из public.prices",
            exc_info=True,
        )

    try:
        stg_future = _read_future_features_from_stg(engine, as_of_date)
        if not stg_future.empty:
            parts.append(stg_future)
    except Exception:
        logger.warning(
            "Не удалось прочитать будущие признаки из stg.v_price_features_source",
            exc_info=True,
        )

    if not parts:
        return pd.DataFrame()

    future = pd.concat(parts, ignore_index=True)
    future = future.drop_duplicates(subset=["price_id", "body_group"], keep="first")
    return future


def _none_if_na(value):
    """NaN/NaT -> None, чтобы в БД записывался SQL NULL, а не NaN."""
    if value is None or pd.isna(value):
        return None
    return float(value)


def _id_to_str(value):
    """Приводит числовой id к строке, как того требует VARCHAR в витрине."""
    if value is None or pd.isna(value):
        return None
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


def run_batch_prediction(
    db_url: str,
    model_name: str,
    target_table: str,
    as_of_date: str,
    history_days: int = 21,
):
    """Загрузка модели, скоринг истории и будущих заявок, запись в БД."""
    engine = None
    try:
        model_path = Path(model_name)
        if not model_path.exists():
            raise FileNotFoundError(f"Модель не найдена: {model_path}")

        model = CatBoostRegressor()
        model.load_model(str(model_path))
        logger.info("Модель загружена: %s", model_path)

        as_of_dt = datetime.strptime(as_of_date, "%Y-%m-%d").date()
        if history_days < 0:
            raise ValueError("history_days не может быть отрицательным")
        predict_start_dt = as_of_dt - timedelta(days=history_days)

        logger.info(
            "Окно скоринга: история [%s — %s], будущие заявки (> %s)",
            predict_start_dt,
            as_of_dt,
            as_of_dt,
        )

        engine = create_db_engine(db_url)

        historical = _read_historical_features(engine, predict_start_dt, as_of_dt)
        future = _read_future_features(engine, as_of_dt)

        frames = [df for df in (historical, future) if df is not None and not df.empty]
        if not frames:
            logger.warning("Нет данных для скоринга (as_of=%s)", as_of_date)
            return

        df = pd.concat(frames, ignore_index=True)
        df = df.drop_duplicates(subset=["price_id", "body_group"], keep="first")
        df["loading_date"] = pd.to_datetime(df["loading_date"], errors="raise")

        is_future = df["loading_date"] > pd.Timestamp(as_of_dt)
        future_mask = is_future.to_numpy(dtype=bool)

        logger.info(
            "Скоринг %s записей (история: %s, будущие: %s)...",
            len(df),
            int((~future_mask).sum()),
            int(future_mask.sum()),
        )

        X_predict = prepare_model_data(df)
        predicted = np.asarray(model.predict(X_predict), dtype="float64")

        actual = pd.to_numeric(df["actual_price_rub"], errors="coerce").to_numpy(
            dtype="float64"
        )

        actual_for_save = np.where(future_mask, np.nan, actual)

        valid_actual = ~future_mask & ~np.isnan(actual)
        abs_error = np.full(len(df), np.nan, dtype="float64")
        abs_error[valid_actual] = np.abs(
            actual[valid_actual] - predicted[valid_actual]
        )

        ape_pct = np.full(len(df), np.nan, dtype="float64")
        non_zero_actual = valid_actual & (actual != 0)
        ape_pct[non_zero_actual] = (
            np.abs(
                (actual[non_zero_actual] - predicted[non_zero_actual])
                / actual[non_zero_actual]
            )
            * 100.0
        )

        scored_at = datetime.now()

        result_df = pd.DataFrame({
            "price_id": df["price_id"].astype("int64"),
            "loading_date": df["loading_date"].dt.date,
            "loading_city_gcid": [
                _id_to_str(value) for value in df["loading_city_gcid"]
            ],
            "unloading_city_gcid": [
                _id_to_str(value) for value in df["unloading_city_gcid"]
            ],
            "body_group": [_id_to_str(value) for value in df["body_group"]],
            "actual_price_rub": [_none_if_na(value) for value in actual_for_save],
            "predicted_price_rub": [_none_if_na(value) for value in predicted],
            "abs_error_rub": [_none_if_na(value) for value in abs_error],
            "ape_pct": [_none_if_na(value) for value in ape_pct],
            "model_name": model_name,
            "scored_at": scored_at,
        })

        parts = target_table.split(".")
        schema_name = parts[0] if len(parts) > 1 else "public"
        table_raw_name = parts[-1]

        logger.info("Сохранение результатов в таблицу %s...", target_table)

        with engine.begin() as conn:
            deleted = conn.execute(
                text("""
                    DELETE FROM {target_table}
                    WHERE loading_date >= :predict_start_date
                """.format(target_table=target_table)),
                {"predict_start_date": predict_start_dt},
            ).rowcount
            logger.info("Удалено старых прогнозов: %s", deleted)

            result_df.to_sql(
                name=table_raw_name,
                schema=schema_name,
                con=conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )

        logger.info(
            "Batch prediction успешно завершен. Сохранено строк: %s "
            "(из них будущих: %s)",
            len(result_df),
            int(future_mask.sum()),
        )

    except Exception as e:
        logger.exception("Ошибка при выполнении batch prediction: %s", e)
        raise
    finally:
        if engine:
            engine.dispose()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Скрипт пакетного скоринга (Batch Prediction) с будущими заявками."
    )
    parser.add_argument(
        "--connection",
        type=str,
        default=defaults.DB_URL,
        help="строка подключения к БД",
    )
    parser.add_argument(
        "--model_path",
        type=str,
        default=defaults.MODEL_PATH,
        help="Путь к файлу модели .cbm",
    )
    parser.add_argument(
        "--table_name",
        type=str,
        default=defaults.TARGET_TABLE,
        help="имя результирующей таблицы",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=21,
        help="глубина исторического окна скоринга в днях (по умолчанию 21 = 3 недели)",
    )
    parser.add_argument(
        "as_of_date",
        type=str,
        nargs="?",
        default=defaults.AS_OF_DATE,
        help="Дата среза (ГГГГ-ММ-ДД)",
    )

    args = parser.parse_args()

    logger.info(
        "Запуск batch prediction: as_of_date=%s, model_path=%s",
        args.as_of_date,
        args.model_path,
    )

    try:
        run_batch_prediction(
            db_url=args.connection,
            model_name=args.model_path,
            target_table=args.table_name,
            as_of_date=args.as_of_date,
            history_days=args.history_days,
        )
    except Exception as e:
        logger.critical("Сбой пайплайна: %s", e)
        sys.exit(1)
