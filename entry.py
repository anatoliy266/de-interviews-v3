import sys
import logging
import argparse
from datetime import datetime, timedelta
import dateutil.relativedelta

from etl.prices_raw_to_stg import run_core_etl
from etl.prices_stg_to_mart import run_mart_etl
from ml.features_stg_to_ml import run_price_features_etl
from ml.train import run_price_model_training
from ml.predict import run_batch_prediction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] orchestrator: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("orchestrator")

def parse_orchestrator_args():
    parser = argparse.ArgumentParser(description="Монолитный CLI оркестратор полного пайплайна.")
    parser.add_argument("--connection", type=str, required=True, help="Строка подключения к БД")
    parser.add_argument("--model-path", type=str, required=True, help="Путь к файлу модели .cbm")
    parser.add_argument("--table-name", type=str, required=True, help="Имя результирующей таблицы")
    parser.add_argument("--provider", type=str, default="sql", help="Провайдер данных для первого шага ETL")
    parser.add_argument("--csv-path", type=str, default=None, help="Путь к файлу CSV (опционально)")
    parser.add_argument("--concurrently", action="store_true", help="Обновлять матпредставление параллельно")
    
    parser.add_argument("start_date", type=str, help="Дата начала батча (ГГГГ-ММ-ДД)")
    parser.add_argument("end_date", type=str, nargs="?", help="Дата окончания батча (Опционально)")
    return parser.parse_args()

def run_pipeline():
    args = parse_orchestrator_args()

    effective_end_date = args.end_date if args.end_date else args.start_date
    
    as_of_dt = datetime.strptime(effective_end_date, "%Y-%m-%d")
    

    feature_start_dt = as_of_dt - dateutil.relativedelta.relativedelta(months=3)
    feature_start_str = feature_start_dt.strftime("%Y-%m-%d")
    split_dt = as_of_dt - timedelta(weeks=3)
    split_date_str = split_dt.strftime("%Y-%m-%d")

    logger.info(f"Старт пайплайна. Период батча ETL: {args.start_date} — {effective_end_date}")
    logger.info(f"ML Окна: Train [{feature_start_str} — {split_date_str}), Test [{split_date_str} — {effective_end_date}]")

    try:
        logger.info(">>> [1/5] Запуск: Raw to STG ETL")
        run_core_etl(
            db_url=args.connection,
            start_date=args.start_date,
            end_date=effective_end_date,
            provider=args.provider,
            csv_path=args.csv_path
        )

        logger.info(">>> [2/5] Запуск: STG to Mart ETL")
        run_mart_etl(
            db_url=args.connection,
            view_name="stg.v_price_features_source", 
            concurrently=args.concurrently
        )

        logger.info(">>> [3/5] Запуск: Feature Engineering")
        run_price_features_etl(
            db_url=args.connection,
            start_date=feature_start_str,
            end_date=effective_end_date
        )

        logger.info(">>> [4/5] Запуск: Model Training")
        run_price_model_training(
            db_url=args.connection,
            train_start=feature_start_str,
            train_end=split_date_str,
            test_start=split_date_str,
            test_end=effective_end_date,
            output_model_path=args.model_path
        )

        logger.info(">>> [5/5] Запуск: Batch Prediction")
        logger.info(f"args.table_name: {args.table_name}")
        run_batch_prediction(
            db_url=args.connection,
            model_name=args.model_path,
            target_table=args.table_name,
            as_of_date=effective_end_date
        )

        logger.info("================================================================")
        logger.info("ПОЛНЫЙ ПАЙПЛАЙН ETL + ML УСПЕШНО ВЫПОЛНЕН!")
        logger.info("================================================================")
        
    except Exception as e:
        logger.critical("================================================================")
        logger.critical(f"[КРИТИЧЕСКИЙ СБОЙ НА ОДНОМ ИЗ ЭТАПОВ]: {e}", exc_info=True)
        logger.critical("================================================================")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
