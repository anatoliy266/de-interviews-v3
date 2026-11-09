import sys
import argparse
import logging
from pathlib import Path
from typing import Tuple, Dict
from datetime import datetime, timedelta
import dateutil.relativedelta

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from catboost import CatBoostRegressor
from sklearn.metrics import (
    r2_score,
    mean_absolute_percentage_error,
    mean_squared_error,
    mean_absolute_error
)

from utils.connection import create_db_engine
from utils import defaults

logger = logging.getLogger("orchestrator")

TARGET = "price_rub"

LEAKAGE_COLUMNS = [
    "price_with_vat", "price_group_id", "avg_price", "min_km_price",
    "max_km_price", "outlier_mark", "carrier_cost", "profit"
]

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


def read_training_data(engine: Engine, train_start: str, test_end: str) -> pd.DataFrame:
    """Чтение готовых признаков из ml.price_features."""
    query = text("""
        SELECT *
        FROM ml.price_features
        WHERE loading_date >= :train_start
          AND loading_date <= :test_end
        ORDER BY loading_date
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"train_start": train_start, "test_end": test_end})
        cols_to_drop = [col for col in LEAKAGE_COLUMNS if col in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            logger.warning("Удалены потенциально опасные колонки: %s", cols_to_drop)

        logger.info("Получено строк для обучения/теста: %s", len(df))
        return df
    except Exception:
        logger.exception("Ошибка чтения данных из ml.price_features")
        raise


def split_dataset(
    df: pd.DataFrame,
    train_start: str,
    train_end: str,
    test_start: str,
    test_end: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Строгое хронологическое разделение (Time-Based Split) без shuffle."""
    data = df.copy()
    data["loading_date"] = pd.to_datetime(data["loading_date"], errors="raise")

    ts_train_start = pd.Timestamp(train_start)
    ts_train_end = pd.Timestamp(train_end)     
    ts_test_start = pd.Timestamp(test_start)   
    ts_test_end = pd.Timestamp(test_end)      

    train_mask = (data["loading_date"] >= ts_train_start) & (data["loading_date"] < ts_train_end)
    test_mask = (data["loading_date"] >= ts_test_start) & (data["loading_date"] <= ts_test_end)

    train = data.loc[train_mask].copy()
    test = data.loc[test_mask].copy()

    if train.empty:
        raise ValueError("Обучающая выборка пуста")
    if test.empty:
        raise ValueError("Тестовая выборка пуста")

    logger.info("Разделение: Train = %s строк, Test = %s строк", len(train), len(test))
    return train, test


def prepare_model_data(df: pd.DataFrame) -> pd.DataFrame:
    """Подготовка признаков для CatBoost."""
    features = df[FEATURES].copy()

    for col in CATEGORICAL_FEATURES:
        features[col] = features[col].fillna("__MISSING__").astype(str)
        
    return features


def calculate_metrics(y_true: pd.Series, y_pred: np.ndarray) -> Dict[str, float]:
    """Расчет бизнес и математических метрик качества регрессии."""
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mape": float(mean_absolute_percentage_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred))
    }


def train_model_pipeline(
    train_df: pd.DataFrame, 
    test_df: pd.DataFrame, 
    model_path: Path
) -> Dict[str, float]:
    """Обучение CatBoostRegressor и оценка качества на тестовом датасете."""
    try:
        X_train = prepare_model_data(train_df)
        y_train = train_df[TARGET]
        
        X_test = prepare_model_data(test_df)
        y_test = test_df[TARGET]
        
        model = CatBoostRegressor(
            iterations=1500,
            learning_rate=0.05,
            depth=6,
            loss_function='RMSE',
            eval_metric='MAE',
            random_seed=42,
            cat_features=CATEGORICAL_FEATURES,
            early_stopping_rounds=100
        )
        
        logger.info("Запуск обучения CatBoostRegressor...")
        model.fit(
            X_train, y_train,
            eval_set=(X_test, y_test),
            verbose=100,
            use_best_model=True
        )
        
        preds = model.predict(X_test)
        metrics = calculate_metrics(y_test, preds)
        
        logger.info(
            "Метрики на тесте: MAE=%.2f, RMSE=%.2f, MAPE=%.4f, R2=%.4f",
            metrics["mae"], metrics["rmse"], metrics["mape"], metrics["r2"]
        )
        
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model.save_model(str(model_path))
        logger.info("Модель успешно сохранена по пути: %s", model_path)
        
        return metrics
    except Exception:
        logger.exception("Ошибка в процессе обучения/валидации модели")
        raise


def run_price_model_training(
    db_url: str,
    train_start: str,
    train_end: str,
    test_start: str,
    test_end: str,
    output_model_path: str = "models/price_regression_v1.cbm"
) -> Dict[str, float]:
    """Основной оркестратор ML-пайплайна обучения."""
    engine = None
    try:
        engine = create_db_engine(db_url)
        
        raw_df = read_training_data(engine, train_start=train_start, test_end=test_end)

        train_data, test_data = split_dataset(
            df=raw_df,
            train_start=train_start,
            train_end=train_end,
            test_start=test_start,
            test_end=test_end
        )
        
        model_path = Path(output_model_path)
        metrics = train_model_pipeline(train_data, test_data, model_path)
        
        logger.info("ML пайплайн обучения успешно завершен.")
        return metrics
        
    except Exception:
        logger.critical("Критический сбой пайплайна обучения модели", exc_info=True)
        raise
    finally:
        if engine is not None:
            engine.dispose()
            logger.info("Подключение к БД закрыто")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Скрипт обучения CatBoost модели на признаках ml.price_features."
    )

    parser.add_argument("--connection", type=str, default=defaults.DB_URL, help="строка подключения к БД")
    parser.add_argument("--model_path", type=str, default=defaults.MODEL_PATH, help="Путь к файлу модели .cbm")
    parser.add_argument("--table_name", type=str, default=defaults.TARGET_TABLE, help="имя таблицы признаков")
    parser.add_argument("as_of_date", type=str, default=defaults.AS_OF_DATE, help="Дата начала обучения (ГГГГ-ММ-ДД)")



    args = parser.parse_args()

    logger.info("Инициализация запуска пайплайна с аргументами командной строки...")

    as_of_dt = datetime.strptime(args.as_of_date, "%Y-%m-%d")
    train_start_dt = as_of_dt - dateutil.relativedelta.relativedelta(months=3)
    split_dt = as_of_dt - timedelta(weeks=3)
    
    train_start = train_start_dt.strftime("%Y-%m-%d")
    train_end = split_dt.strftime("%Y-%m-%d")   
    test_end = args.as_of_date                  

    logger.info("Границы сплита: Train [%s, %s), Test [%s, %s]", 
                train_start, train_end, train_end, test_end)


    try:
        final_metrics = run_price_model_training(
            db_url=args.connection,
            train_start=train_start,
            train_end=train_end,
            test_start=train_end, 
            test_end=test_end,
            output_model_path=args.model_path
        )
        
        logger.info("=== Пайплайн успешно завершен ===")
        print(f"\nРезультаты обучения:")
        print(f"  R2 Score: {final_metrics['r2']:.4f}")
        print(f"  MAE:      {final_metrics['mae']:.2f} руб.")

    except Exception as e:
        logger.critical("Пайплайн завершился ошибкой: %s", e)
        sys.exit(1)