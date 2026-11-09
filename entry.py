import subprocess
import sys
import os
import logging
import argparse
from datetime import datetime
import dateutil.relativedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] orchestrator: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def parse_orchestrator_args():
    parser = argparse.ArgumentParser(description="Чистый CLI оркестратор полного пайплайна с расширенной обработкой ошибок.")
    parser.add_argument("--connection", type=str, required=True, help="Строка подключения к БД")
    parser.add_argument("--model-path", type=str, required=True, help="Путь к файлу модели .cbm")
    parser.add_argument("--table-name", type=str, required=True, help="Имя результирующей таблицы")
    parser.add_argument("--provider", type=str, default="manual_exec", help="Провайдер данных для первого шага ETL")
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

    logger.info(f"Старт пайплайна. Период батча: {args.start_date} — {effective_end_date}")

    raw_to_stg_args = ["--connection", args.connection, "--provider", args.provider]
    if args.csv_path:
        raw_to_stg_args.extend(["--csv-path", args.csv_path])
    raw_to_stg_args.extend([args.start_date, effective_end_date])

    stg_to_mart_args = ["--connection", args.connection]
    if args.concurrently:
        stg_to_mart_args.append("--concurrently")

    pipeline = [
        {"name": "1. Raw to STG ETL", "path": "etl/prices_raw_to_stg.py", "args": raw_to_stg_args},
        {"name": "2. STG to Mart ETL", "path": "etl/prices_stg_to_mart.py", "args": stg_to_mart_args},
        {"name": "3. Feature Engineering", "path": "ml/features_stg_to_ml.py", "args": ["--connection", args.connection, feature_start_str, effective_end_date]},
        {"name": "4. Model Training", "path": "ml/train.py", "args": ["--connection", args.connection, "--model-path", args.model_path, "--table-name", args.table_name, effective_end_date]},
        {"name": "5. Batch Prediction", "path": "ml/predict.py", "args": ["--connection", args.connection, "--model-path", args.model_path, "--table-name", args.table_name, effective_end_date]}
    ]

    for step in pipeline:
        script = step["path"]
        step_name = step["name"]
        
        if not os.path.exists(script):
            logger.error(f"\n{"="*60}\n[КРИТИЧЕСКАЯ ОШИБКА] Этап: {step_name}\nФайл скрипта не найден по пути: {script}\n{"="*60}")
            sys.exit(1)

        cmd = [sys.executable, script] + step["args"]
        logger.info(f"Выполнение этапа '{step_name}': {' '.join(cmd)}")
        
        # Запускаем процесс, перенаправляя stderr в PIPE для отлова трейсбэков
        process = subprocess.Popen(
            cmd,
            stdout=sys.stdout,  # Логи скрипта сразу летят в консоль в реальном времени
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8"
        )
        
        # Ждем завершения и собираем поток ошибок, если они возникнут
        _, stderr_output = process.communicate()
        
        if process.returncode != 0:
            logger.error(f"\n{"="*80}")
            logger.error(f"[ПАДЕНИЕ ПАЙПЛАЙНА] Сбой на этапе: {step_name}")
            logger.error(f"Скрипт: {script}")
            logger.error(f"Код возврата (Exit Code): {process.returncode}")
            logger.error(f"{"-"*80}")
            logger.error("ОШИБКА И ТРЕЙСБЭК ИЗ СКРИПТА:\n")
            if stderr_output.strip():
                print(stderr_output, file=sys.stderr)
            else:
                logger.error("[Контейнер не вернул stderr. Возможно сбой самого интерпретатора Python или OOM Killer]")
            logger.error(f"{"="*80}\n")
            
            # Выходим с тем же кодом ошибки наружу в docker exec
            sys.exit(process.returncode)
            
        logger.info(f"Этап '{step_name}' успешно завершен.")

    logger.info("Полный пайплайн ETL + ML успешно выполнен!")

if __name__ == "__main__":
    run_pipeline()
