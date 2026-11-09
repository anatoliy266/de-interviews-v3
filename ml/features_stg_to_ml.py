
import sys
import argparse
import logging
from datetime import date, datetime

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text

from utils.validation import validate_date_range
from utils.connection import create_db_engine
from utils import defaults


logger = logging.getLogger(__name__)


def read_source_data(engine, start_date, end_date):
    query = text("""
        SELECT *
        FROM stg.v_price_features_source
        WHERE loading_date BETWEEN :start_date AND :end_date
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                query, conn,
                params={"start_date": start_date, "end_date": end_date}
            )
        logger.info("Получено строк из SQL: %s", len(df))
        return df
    except Exception:
        logger.exception("Ошибка чтения исходных данных")
        raise


def build_calendar_features(df):
    try:
        features = df.copy()

        if "loading_date" not in features.columns:
            raise ValueError("В исходных данных отсутствует loading_date")

        dates = pd.to_datetime(features["loading_date"], errors="raise")
        if dates.isnull().any():
            raise ValueError("В loading_date обнаружены пустые значения")

        features["year"] = dates.dt.year
        features["month"] = dates.dt.month
        features["weekday"] = dates.dt.dayofweek
        features["day_of_year"] = dates.dt.dayofyear

        features["day_of_year_sin"] = np.sin(2 * np.pi * (features["day_of_year"] - 1) / 365.25)
        features["day_of_year_cos"] = np.cos(2 * np.pi * (features["day_of_year"] - 1) / 365.25)
        
        features["month_sin"] = np.sin(2 * np.pi * (features["month"] - 1) / 12)
        features["month_cos"] = np.cos(2 * np.pi * (features["month"] - 1) / 12)
        
        features["weekday_sin"] = np.sin(2 * np.pi * features["weekday"] / 7)
        features["weekday_cos"] = np.cos(2 * np.pi * features["weekday"] / 7)

        logger.info("Календарные признаки рассчитаны")
        return features
    except Exception:
        logger.exception("Ошибка расчёта календарных признаков")
        raise


def save_features(df, engine, start_date, end_date):
    try:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM ml.price_features WHERE loading_date BETWEEN :start_date AND :end_date"),
                {"start_date": start_date, "end_date": end_date}
            )

            if not df.empty:
                df.to_sql(
                    name="price_features",
                    schema="ml",
                    con=conn,
                    if_exists="append",
                    index=False,
                    chunksize=1000
                )
        logger.info("Сохранено строк признаков: %s", len(df))
    except Exception:
        logger.exception("Ошибка записи признаков в БД")
        raise

def run_price_features_etl(db_url, start_date, end_date):
    engine = None
    try:
        # 1. Валидация. 
        start_date, end_date = validate_date_range(start_date, end_date)

        # 2. Подключение через общую утилиту
        engine = create_db_engine(db_url)

        # 3. Извлечение
        prices = read_source_data(engine, start_date, end_date)

        # 4. Трансформация
        features = build_calendar_features(prices)

        # 5. Загрузка
        save_features(features, engine, start_date, end_date)

        logger.info("ML Feature ETL успешно завершён: %s — %s", start_date, end_date)

    except Exception:
        logger.critical("Ошибка выполнения ML Feature ETL", exc_info=True)
        raise
    finally:
        if engine is not None:
            engine.dispose()
            logger.info("Подключение к БД закрыто")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Скрипт инженерии признаков (Feature ETL).")
    parser.add_argument("--connection", type=str, default=defaults.DB_URL, help="Строка подключения к БД")
    parser.add_argument("start_date", type=str, help="Дата начала периода расчета (ГГГГ-ММ-ДД)")
    parser.add_argument("end_date", type=str, help="Дата окончания периода расчета (ГГГГ-ММ-ДД)")

    args = parser.parse_args()

    logger.info("Запуск Feature Pipeline за период: %s по %s", args.start_date, args.end_date)
    
    try:
        run_price_features_etl(
            db_url=args.connection,
            start_date=args.start_date,
            end_date=args.end_date
        )
    except Exception as e:
        logger.critical("Критический сбой Feature Pipeline: %s", e)
        sys.exit(1)