\c deals_bigdata

CREATE SCHEMA IF NOT EXISTS ml;

CREATE TABLE IF NOT EXISTS ml.price_features (
    id bigserial PRIMARY KEY,
    price_id BIGINT NOT NULL,
    loading_date DATE NOT NULL,

    -- География отправления
    loading_city_gcid integer,
    loading_city TEXT,
    loading_region TEXT,
    loading_latitude DOUBLE PRECISION,
    loading_longitude DOUBLE PRECISION,

    -- География назначения
    unloading_city_gcid integer,
    unloading_city TEXT,
    unloading_region TEXT,
    unloading_latitude DOUBLE PRECISION,
    unloading_longitude DOUBLE PRECISION,

    -- Параметры груза и транспорта
    distance DOUBLE PRECISION,
    total_weight DOUBLE PRECISION,
    total_volume DOUBLE PRECISION,
    body_group INTEGER,
    weight_category TEXT,
    volume_category TEXT,

    -- Календарные признаки
    year INTEGER,
    month INTEGER,
    weekday INTEGER,
    day_of_year INTEGER,

    -- Циклическое кодирование
    day_of_year_sin DOUBLE PRECISION,
    day_of_year_cos DOUBLE PRECISION,
    month_sin DOUBLE PRECISION,
    month_cos DOUBLE PRECISION,
    weekday_sin DOUBLE PRECISION,
    weekday_cos DOUBLE PRECISION,

    -- Целевая переменная
    price_rub double precision,
    CONSTRAINT uq_price_features_price_id_body_group
        UNIQUE (price_id, body_group)
);

CREATE TABLE IF NOT EXISTS mart.ml_price_predictions (
    id bigserial PRIMARY KEY,
    price_id BIGINT,                    
    loading_date DATE,                  -- Дата погрузки
    loading_city_gcid VARCHAR,          -- GCID города погрузки
    unloading_city_gcid VARCHAR,        -- GCID города выгрузки
    body_group VARCHAR,                 -- Тип кузова
    actual_price_rub NUMERIC(15, 2),    -- Фактическая цена (NULL для будущих заявок)
    predicted_price_rub NUMERIC(15, 2), -- Предсказанная моделью цена
    abs_error_rub NUMERIC(15, 2),       -- Абсолютная ошибка |Факт - Прогноз| (NULL для будущих)
    ape_pct NUMERIC(10, 4),             -- Абсолютная процентная ошибка в % (NULL для будущих)
    model_name VARCHAR,                 -- Название модели (например, catboost_price_v1)
    scored_at TIMESTAMP                 -- Время выполнения скоринга
);

-- Индекс для быстрого поиска по дате и ID
CREATE INDEX IF NOT EXISTS idx_ml_price_pred_loading_date 
ON mart.ml_price_predictions (loading_date);