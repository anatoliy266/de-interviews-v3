# utils/db.py
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

def create_db_engine(db_url: str) -> Engine:
    """Создание и проверка подключения к БД (переиспользуемая функция)."""
    engine = None
    try:
        if not db_url:
            raise ValueError("Не задан URL подключения к БД")

        engine = create_engine(db_url)

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info("Подключение к БД успешно установлено")
        return engine

    except Exception:
        if engine is not None:
            engine.dispose()
        logger.exception("Ошибка подключения к БД")
        raise