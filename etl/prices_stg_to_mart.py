import sys
import argparse
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from utils.connection import create_db_engine
from utils import defaults

logger = logging.getLogger(__name__)


def refresh_materialized_view(engine: Engine, view_name: str, concurrently: bool = False) -> None:
    """Изолированная атомарная операция обновления материализованного представления."""
    concurrent_str = "CONCURRENTLY" if concurrently else ""
    query = text(f"REFRESH MATERIALIZED VIEW {concurrent_str} {view_name}")
    
    logger.info("Запуск обновления материализованного представления %s...", view_name)
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()
        
    logger.info("Материализованное представление %s успешно обновлено", view_name)

def run_etl(db_url: str, view_name: str, concurrently: bool = False):
    engine = None
    try:
        engine = create_db_engine(db_url)

        refresh_materialized_view(engine, view_name="mart.route_card", concurrently=concurrently)
        
        logger.info("mart ETL успешно завершён")

    except Exception:
        logger.critical("Ошибка выполнения Core ETL", exc_info=True)
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

    parser = argparse.ArgumentParser(description="Скрипт Mart ETL: обновление материализованных представлений.")
    parser.add_argument("--connection", type=str, default=defaults.DB_URL, help="строка подключения к БД")
    parser.add_argument("--view-name", type=str, default="mart.route_card", help="имя матпредставления")
    parser.add_argument("--concurrently", action="store_true", help="обновлять параллельно (CONCURRENTLY)")

    args = parser.parse_args()

    logger.info("Запуск Mart ETL для вьюхи: %s", args.view_name)
    
    try:
        run_etl(
            db_url=args.connection,
            view_name=args.view_name,
            concurrently=args.concurrently
        )
    except Exception as e:
        logger.critical("Сбой Mart ETL: %s", e)
        sys.exit(1)