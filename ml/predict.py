import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text
from catboost import CatBoostRegressor

from utils.connection import create_db_engine
from utils import defaults

logger = logging.getLogger(__name__)

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


def run_batch_prediction(db_url: str, model_name: str, target_table: str, as_of_date: str):
    """Загрузка модели, скоринг и сохранение результатов в БД."""
    engine = None
    try:
        model_path = Path(model_name)
        if not model_path.exists():
            raise FileNotFoundError(f"Модель не найдена: {model_path}")
        
        model = CatBoostRegressor()
        model.load_model(str(model_path))
        logger.info("Модель загружена: %s", model_path)

        engine = create_db_engine(db_url)
        query = text("""
            SELECT 
                id,
                price_id,
                loading_date,
                loading_city as loading_city_gcid,
                unloading_city as unloading_city_gcid,
                body_group,
                price_rub as actual_price_rub,
                distance, total_weight, total_volume,
                loading_latitude, loading_longitude,
                unloading_latitude, unloading_longitude,
                year, month, weekday, day_of_year,
                day_of_year_sin, day_of_year_cos,
                month_sin, month_cos,
                weekday_sin, weekday_cos,
                loading_region, unloading_region,
                weight_category, volume_category
            FROM ml.price_features
            WHERE loading_date >= :as_of_date
            ORDER BY loading_date
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"as_of_date": as_of_date})
            
        if df.empty:
            logger.warning(f"Нет данных для скоринга от {as_of_date}")
            return

        logger.info(f"Скоринг {len(df)} записей...")

        X_predict = prepare_model_data(df)
        df["predicted_price_rub"] = model.predict(X_predict)

        ts_test_end = pd.Timestamp(as_of_date)
        future_mask = df["loading_date"] > ts_test_end
        
        df["actual_price_rub_final"] = np.where(future_mask, np.nan, df["actual_price_rub"])
        
        df["abs_error_rub"] = np.where(
            future_mask,
            np.nan,
            np.abs(df["actual_price_rub"] - df["predicted_price_rub"])
        )
        
        df["ape_pct"] = np.where(
            future_mask | (df["actual_price_rub"] == 0),
            np.nan,
            np.abs((df["actual_price_rub"] - df["predicted_price_rub"]) / df["actual_price_rub"]) * 100.0
        )

        df["model_name"] = model_name
        df["scored_at"] = datetime.now()

        columns_to_save = [
            "price_id", "loading_date", "loading_city_gcid", "unloading_city_gcid",
            "body_group", "actual_price_rub_final", "predicted_price_rub",
            "abs_error_rub", "ape_pct", "model_name", "scored_at"
        ]
        
        result_df = df[columns_to_save].copy()
        result_df.rename(columns={"actual_price_rub_final": "actual_price_rub"}, inplace=True)

        logger.info(f"Сохранение результатов в таблицу {target_table}...")
        with engine.connect() as conn:
            result_df.to_sql(target_table, conn, if_exists="append", index=False, method="multi")
            
        logger.info(f"Batch prediction успешно завершен. Сохранено строк: {len(result_df)}")

    except Exception as e:
        logger.exception(f"Ошибка при выполнении batch prediction: {e}")
        raise
    finally:
        if engine:
            engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Скрипт пакетного скоринга (Batch Prediction).")
    parser.add_argument("--connection", type=str, default=defaults.DB_URL, help="строка подключения к БД")
    parser.add_argument("--model-path", type=str, default=defaults.MODEL_PATH, help="Путь к файлу модели .cbm")
    parser.add_argument("--table-name", type=str, default=defaults.TARGET_TABLE, help="имя результирующей таблицы")
    parser.add_argument("as_of_date", type=str, nargs="?", default=defaults.AS_OF_DATE, help="Дата начала скоринга (ГГГГ-ММ-ДД)")

    args = parser.parse_args()

    logger.info("Запуск batch prediction: as_of_date=%s, model_path=%s", args.as_of_date, args.model_path)
    
    try:
        run_batch_prediction(
            db_url=args.connection,
            model_name=args.model_path,
            target_table=args.table_name,
            as_of_date=args.as_of_date
        )
    except Exception as e:
        logger.critical("Сбой пайплайна: %s", e)
        sys.exit(1)
