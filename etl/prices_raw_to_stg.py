import sys
import argparse
import logging
from datetime import date, datetime
from typing import Optional, Tuple, Set

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine

from utils.validation import validate_date_range
from utils.connection import create_db_engine
from utils import defaults

logger = logging.getLogger("orchestrator")


def read_source_data(engine: Engine, start_date: date, end_date: date, provider: str, csv_path: Optional[str]) -> pd.DataFrame:
    """Чтение основных данных из CSV или SQL."""
    try:
        if provider == "csv":
            if not csv_path:
                raise ValueError("Не задан путь для файла csv при provider='csv'")
            df = pd.read_csv(csv_path)
            logger.info("Прочитано строк из CSV: %s", len(df))
            
        elif provider == "sql":
            query = text("""
                SELECT * 
                FROM public.prices 
                WHERE loading_date >= :start_date AND loading_date <= :end_date
            """)
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"start_date": start_date, "end_date": end_date})
            logger.info("Прочитано строк из SQL: %s", len(df))
            
        else:
            raise ValueError(f"Некорректный провайдер: '{provider}'. Ожидалось 'csv' или 'sql'")

        return df

    except Exception:
        logger.exception("Ошибка чтения исходных данных")
        raise


def read_reference_data(engine: Engine) -> Tuple[Set, pd.DataFrame]:
    """Чтение справочных данных (города и курсы валют)."""
    try:
        with engine.connect() as conn:
            cities_df = pd.read_sql(text("SELECT DISTINCT gc_id FROM public.cities"), conn)
            rates_df = pd.read_sql(text("SELECT num_code, value, setting_date FROM public.exchange_rates"), conn)
            
        cities_ids = set(cities_df["gc_id"].dropna().tolist())
        logger.info("Загружены справочные данные: %s городов, %s записей курсов", len(cities_ids), len(rates_df))
        
        return cities_ids, rates_df

    except Exception:
        logger.exception("Ошибка чтения справочных данных")
        raise


def transform_currency(prices_df: pd.DataFrame, rates_df: pd.DataFrame) -> pd.DataFrame:
    """Объединение с курсами валют и расчет цены в рублях."""
    try:
        df = prices_df.merge(
            rates_df, 
            how="left",
            left_on=["currency_code", "nearest_rate_date"],
            right_on=["num_code", "setting_date"],
            validate="many_to_one"
        )

        df["price_rub"] = df["price"]
        not_rub = ~df["currency_code"].eq("643") & ~df["value"].isnull()
        df.loc[not_rub, "price_rub"] = df["price"] * df["value"]
        df["price"] = df["price_rub"]

        logger.info("Конвертация валют выполнена")
        return df

    except Exception:
        logger.exception("Ошибка объединения таблиц и конвертации валют")
        raise


def apply_dq_rules(df: pd.DataFrame, cities_ids: Set) -> pd.DataFrame:
    """Применение правил Data Quality и разметка причин отклонения."""
    try:
        df = df.copy()
        df["dq_reject_reason"] = None

        invalid_prices = (df["price"] <= 0) | ((df["currency_code"] == '643') & (df["price"] <= 1000))
        invalid_distance = df["distance"] <= 0
        invalid_weight = df["total_weight"] < 0
        invalid_volume = df["total_volume"] < 0
        invalid_cities = ~df["loading_city_gcid"].isin(cities_ids) | ~df["unloading_city_gcid"].isin(cities_ids)
        missing_rate_values = ~df["currency_code"].eq("643") & df["value"].isnull()

        # Примечание: при совпадении нескольких условий причина перезаписывается последним сработавшим правилом.
        # Это поведение сохранено из оригинального кода.
        df.loc[invalid_prices, "dq_reject_reason"] = "INVALID_PRICE"
        df.loc[invalid_distance, "dq_reject_reason"] = "INVALID_DISTANCE"
        df.loc[invalid_weight, "dq_reject_reason"] = "INVALID_WEIGHT"
        df.loc[invalid_volume, "dq_reject_reason"] = "INVALID_VOLUME"
        df.loc[invalid_cities, "dq_reject_reason"] = "INVALID_CITY"
        df.loc[missing_rate_values, "dq_reject_reason"] = "MISSING_RATE"

        rejected_count = df["dq_reject_reason"].notnull().sum()
        logger.info("Проверка DQ завершена. Отклонено записей: %s", rejected_count)
        
        return df

    except Exception:
        logger.exception("Ошибка применения правил Data Quality")
        raise


def enrich_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Заполнение пропущенных категорий веса и объема."""
    try:
        df = df.copy()
        
        # Категории веса
        missing_weight = df['weight_category'].isnull()
        rounded_weight = np.round(df['total_weight'], 1)

        df.loc[missing_weight & (rounded_weight >= 30.0), 'weight_category'] = 'G20T'
        df.loc[missing_weight & ((rounded_weight >= 10.0) & (rounded_weight < 30.0)), 'weight_category'] = 'FTL'
        df.loc[missing_weight & ((rounded_weight >= 5.0)  & (rounded_weight < 10.0)), 'weight_category'] = '10T'
        df.loc[missing_weight & ((rounded_weight >= 2.5)  & (rounded_weight < 5.0)), 'weight_category'] = '5T'
        df.loc[missing_weight & ((rounded_weight >= 0.5)  & (rounded_weight < 2.5)), 'weight_category'] = 'L5T'
        df.loc[df['weight_category'].isnull(), 'weight_category'] = 'OTH'

        # Категории объема
        missing_volume = df['volume_category'].isnull()
        df.loc[missing_volume & (df['total_volume'] > 0) & (df['total_volume'] <= 120.0), 'volume_category'] = 'FTL'
        df.loc[df['volume_category'].isnull(), 'volume_category'] = 'OTH'

        logger.info("Обогащение категорий веса и объема завершено")
        return df

    except Exception:
        logger.exception("Ошибка обогащения категорий")
        raise


def save_processed_data(
    engine: Engine, 
    df_clean: pd.DataFrame, 
    df_quarantine: pd.DataFrame, 
    start_date: date, 
    end_date: date, 
    original_cols: list, 
    quarantine_cols: list
):
    """Сохранение очищенных и карантинных данных в БД в рамках одной транзакции."""
    try:
        with engine.begin() as conn:
            logger.info("Начало транзакции записи данных для интервала %s -> %s", start_date, end_date)

            # Очистка старых данных
            conn.execute(
                text("DELETE FROM stg.prices WHERE loading_date >= :start_date AND loading_date <= :end_date"),
                {"start_date": start_date, "end_date": end_date}
            )
            conn.execute(
                text("DELETE FROM stg.dq_quarantine_prices WHERE loading_date >= :start_date AND loading_date <= :end_date"),
                {"start_date": start_date, "end_date": end_date}
            )

            dtype_mapping = {"external_item_uuid": UUID}

            if not df_clean.empty:
                df_clean[original_cols].to_sql(
                    name="prices",
                    schema="stg",
                    con=conn,
                    if_exists="append",
                    index=False,
                    dtype=dtype_mapping,
                    chunksize=1000
                )
                logger.info("Сохранено чистых записей: %s", len(df_clean))

            if not df_quarantine.empty:
                df_quarantine[quarantine_cols].to_sql(
                    name="dq_quarantine_prices",
                    schema="stg",
                    con=conn,
                    if_exists="append",
                    index=False,
                    dtype=dtype_mapping,
                    chunksize=1000
                )
                logger.info("Сохранено записей в карантин: %s", len(df_quarantine))

            conn.execute(
                text("""
                    INSERT INTO stg.etl_control (id, as_of_date)
                    VALUES (TRUE, :as_of_date)
                    ON CONFLICT (id) DO UPDATE SET as_of_date = EXCLUDED.as_of_date
                """),
                {"as_of_date": end_date}
            )

            logger.info("Транзакция записи данных успешно завершена")

    except Exception:
        logger.exception("Ошибка записи в базу данных")
        raise


def run_core_etl(db_url: str, start_date, end_date, provider: str, csv_path: Optional[str] = None):
    """Оркестратор основного ETL-процесса."""
    engine = None
    try:
        start_date, end_date = validate_date_range(
            start_date, 
            end_date, 
            default_start="1970-01-01"
        )

        engine = create_db_engine(db_url)

        prices = read_source_data(engine, start_date, end_date, provider, csv_path)
        original_cols = prices.columns.tolist()
        quarantine_cols = original_cols + ["dq_reject_reason"]

        cities_ids, rates = read_reference_data(engine)

        prices = transform_currency(prices, rates)
        prices = apply_dq_rules(prices, cities_ids)
        prices = enrich_categories(prices)

        prices_clean = prices[prices["dq_reject_reason"].isnull()]
        prices_quarantine = prices[~prices["dq_reject_reason"].isnull()]

        save_processed_data(
            engine=engine,
            df_clean=prices_clean,
            df_quarantine=prices_quarantine,
            start_date=start_date,
            end_date=end_date,
            original_cols=original_cols,
            quarantine_cols=quarantine_cols
        )

        logger.info("Core ETL успешно завершён: %s — %s", start_date, end_date)

    except Exception:
        logger.critical("Ошибка выполнения Core ETL", exc_info=True)
        raise

    finally:
        if engine is not None:
            engine.dispose()
            logger.info("Подключение к БД закрыто")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Скрипт Core ETL: Raw to STG.")
    parser.add_argument("--connection", type=str, default=defaults.DB_URL, help="строка подключения к БД")
    parser.add_argument("--provider", type=str, default="sql", help="провайдер данных")
    parser.add_argument("--csv-path", type=str, default=None, help="путь к локальному CSV файлу")
    parser.add_argument("start_date", type=str, help="Дата начала батча (ГГГГ-ММ-ДД)")
    parser.add_argument("end_date", type=str, help="Дата окончания батча (ГГГГ-ММ-ДД)")

    args = parser.parse_args()

    logger.info("Запуск Core ETL для периода: %s — %s", args.start_date, args.end_date)
    
    try:
        run_core_etl(
            db_url=args.connection,
            start_date=args.start_date,
            end_date=args.end_date,
            provider=args.provider,
            csv_path=args.csv_path
        )
    except Exception as e:
        logger.critical("Сбой Core ETL: %s", e)
        sys.exit(1)