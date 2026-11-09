# utils/validators.py
import logging
from datetime import date, datetime
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

def validate_date(value, field_name: str, default: Optional[str] = None) -> date:
    """
    Универсальная валидация даты.
    Если value=None и задан default, вернет дату из default.
    Иначе выбросит ошибку.
    """
    if value is None:
        if default is not None:
            value = default
        else:
            raise ValueError(f"Не задана дата {field_name}")

    try:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Некорректная дата {field_name}: {value}") from exc


def validate_date_range(
    start_date, 
    end_date, 
    default_start: Optional[str] = None
) -> Tuple[date, date]:
    """Валидация диапазона дат с поддержкой дефолтного значения для начала."""
    start = validate_date(start_date, "start_date", default=default_start)
    end = validate_date(end_date, "end_date") # Для end_date дефолт обычно не нужен

    if start > end:
        raise ValueError(
            f"Дата начала должна быть меньше или равна дате конца: "
            f"{start} > {end}"
        )

    return start, end