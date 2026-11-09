\c deals_bigdata
create schema if not exists stg;

CREATE TABLE IF NOT EXISTS stg.prices (
    id                  bigserial PRIMARY KEY,
    created_at          timestamp(0),
    updated_at          timestamp(0),
    subscriber_id       integer,
    organization_id     integer,
    loading_date        date             NOT NULL,
    loading_city_gcid   integer          NOT NULL,
    unloading_city_gcid integer          NOT NULL,
    distance            double precision NOT NULL,
    total_weight        double precision NOT NULL,
    total_volume        double precision NOT NULL,
    body_type           integer,
    price               double precision NOT NULL,
    currency_code       varchar(3)       NOT NULL DEFAULT '643',
    complex             boolean          NOT NULL DEFAULT false,
    price_type          smallint         NOT NULL DEFAULT 2,
    item_id             integer,
    is_fact             boolean          NOT NULL DEFAULT true,
    nearest_rate_date   date,
    meta_item_id        bigint,
    end_loading_date    date,
    source              varchar(255),
    external_item_uuid  uuid,
    outlier_mark        boolean          NOT NULL DEFAULT false,
    weight_category     varchar(4),
    volume_category     varchar(4),
    price_group_id      bigint REFERENCES price_group_stats (id)
);
CREATE index IF NOT EXISTS stg_prices_subscriber_id_index ON stg.prices (subscriber_id);
CREATE INDEX IF NOT exists stg_prices_meta_item_id_index ON stg.prices (meta_item_id);
CREATE INDEX IF NOT exists stg_main_prices_index ON stg.prices (loading_city_gcid, unloading_city_gcid, total_weight, total_volume);
CREATE INDEX IF NOT exists stg_main_prices_cat_index ON stg.prices (loading_city_gcid, unloading_city_gcid, weight_category, volume_category, outlier_mark);
CREATE INDEX IF NOT exists stg_price_group_index ON stg.prices (price_group_id);

CREATE TABLE IF NOT EXISTS stg.dq_quarantine_prices (
    id                  bigserial PRIMARY KEY,
    created_at          timestamp(0),
    updated_at          timestamp(0),
    subscriber_id       integer,
    organization_id     integer,
    loading_date        date             NOT NULL,
    loading_city_gcid   integer          NOT NULL,
    unloading_city_gcid integer          NOT NULL,
    distance            double precision NOT NULL,
    total_weight        double precision NOT NULL,
    total_volume        double precision NOT NULL,
    body_type           integer,
    price               double precision NOT NULL,
    currency_code       varchar(3)       NOT NULL DEFAULT '643',
    complex             boolean          NOT NULL DEFAULT false,
    price_type          smallint         NOT NULL DEFAULT 2,
    item_id             integer,
    is_fact             boolean          NOT NULL DEFAULT true,
    nearest_rate_date   date,
    meta_item_id        bigint,
    end_loading_date    date,
    source              varchar(255),
    external_item_uuid  uuid,
    outlier_mark        boolean          NOT NULL DEFAULT false,
    weight_category     varchar(4),
    volume_category     varchar(4),
    price_group_id      bigint REFERENCES price_group_stats (id),
    dq_reject_reason    varchar(255) 
);
CREATE index IF NOT EXISTS stg_quarantine_prices_subscriber_id_index ON stg.dq_quarantine_prices (subscriber_id);
CREATE INDEX IF NOT EXISTS stg_quarantine_prices_meta_item_id_index ON stg.dq_quarantine_prices (meta_item_id);
CREATE INDEX IF NOT EXISTS stg_quarantine_main_prices_index ON stg.dq_quarantine_prices (loading_city_gcid, unloading_city_gcid, total_weight, total_volume);
CREATE INDEX IF NOT EXISTS stg_quarantine_main_prices_cat_index ON stg.dq_quarantine_prices (loading_city_gcid, unloading_city_gcid, weight_category, volume_category, outlier_mark);
CREATE INDEX IF NOT EXISTS stg_quarantine_price_group_index ON stg.dq_quarantine_prices (price_group_id);

CREATE OR REPLACE VIEW stg.v_dim_cities AS
SELECT DISTINCT ON (gc_id)
    gc_id,
    name,
    full_name_local,
    CASE
        WHEN POSITION(',' IN full_name_local) > 0
        THEN NULLIF(
            BTRIM(SUBSTRING(
                full_name_local
                FROM POSITION(',' IN full_name_local) + 1
            )),
            ''
        )
        ELSE NULL
    END AS region_name,
    longitude,
    latitude
FROM public.cities
ORDER BY gc_id, id ASC;


CREATE OR REPLACE VIEW stg.v_dim_body_types AS
SELECT 
    id AS raw_body_type_id,
    CASE 
        WHEN id IN (56, 57, 58, 59, 60, 61) THEN 56 -- Тент
        WHEN id IN (4, 46, 47, 48)          THEN 4  -- Рефрижератор
        WHEN id = 2                         THEN 2  -- Изотерм
        WHEN id IN (1, 15)                  THEN 15 -- Борт
        ELSE 999                                    -- Прочие
    END AS body_type_std_id
FROM public.body_types;

CREATE OR REPLACE VIEW stg.v_fact_market_prices AS
WITH raw_price_bodies AS (
    SELECT
        pbt.price_id,
        pbt.body_type_id AS final_body_type_id
    FROM public.price_body_types pbt
    UNION ALL
    SELECT
        p.id AS price_id,
        p.body_type AS final_body_type_id
    FROM stg.prices p
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.price_body_types pbt
        WHERE pbt.price_id = p.id
    )
),
deduplicated_bodies AS (
    SELECT DISTINCT
        rpb.price_id,
        CASE 
            WHEN rpb.final_body_type_id IS NULL THEN 0
            ELSE COALESCE(dbt.body_type_std_id, 0)
        END AS body_group_std_id
    FROM raw_price_bodies rpb
    LEFT JOIN stg.v_dim_body_types dbt
        ON rpb.final_body_type_id = dbt.raw_body_type_id
)
SELECT
    p.id,
    DATE_TRUNC('month', pd.price_date)::date AS loading_month,
    pd.price_date,
    p.loading_city_gcid,
    p.unloading_city_gcid,
    bt.body_group_std_id,
    p.weight_category,
    p.organization_id,
    p.price_type,
    p.price,
    p.distance,
    p.currency_code,
    p.nearest_rate_date,
    p.created_at
FROM stg.prices p
	INNER JOIN public.price_dates pd ON p.id = pd.price_id
	INNER JOIN deduplicated_bodies bt ON p.id = bt.price_id
WHERE
    p.is_fact = TRUE
    AND p.price_type IN (1, 2, 3, 4, 5, 9)
    AND p.price > 0
    AND p.distance > 0
    AND p.complex = FALSE
    AND p.outlier_mark = FALSE;

CREATE TABLE IF NOT EXISTS stg.etl_control (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id = TRUE),
    as_of_date DATE NOT NULL
);


CREATE OR REPLACE VIEW stg.v_price_features_source AS

WITH raw_price_bodies AS (
    SELECT
        pbt.price_id,
        pbt.body_type_id AS final_body_type_id
    FROM public.price_body_types pbt

    UNION ALL

    SELECT
        p.id AS price_id,
        p.body_type AS final_body_type_id
    FROM stg.prices p
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.price_body_types pbt
        WHERE pbt.price_id = p.id
    )
),

deduplicated_bodies AS (
    SELECT DISTINCT
        rpb.price_id,
        CASE
            WHEN rpb.final_body_type_id IS NULL THEN 0
            ELSE COALESCE(dbt.body_type_std_id, 0)
        END AS body_group
    FROM raw_price_bodies rpb
    LEFT JOIN stg.v_dim_body_types dbt
        ON rpb.final_body_type_id = dbt.raw_body_type_id
)

SELECT
    -- Идентификатор и дата
    p.id AS price_id,
    p.loading_date,

    -- Отправление
    p.loading_city_gcid,
    loading.name AS loading_city,
    loading.region_name AS loading_region,
    loading.latitude AS loading_latitude,
    loading.longitude AS loading_longitude,

    -- Назначение
    p.unloading_city_gcid,
    unloading.name AS unloading_city,
    unloading.region_name AS unloading_region,
    unloading.latitude AS unloading_latitude,
    unloading.longitude AS unloading_longitude,

    -- Характеристики перевозки
    p.distance,
    p.total_weight,
    p.total_volume,
    db.body_group,
    p.weight_category,
    p.volume_category,

    -- Целевая переменная
    p.price AS price_rub

FROM stg.prices p

INNER JOIN deduplicated_bodies db
    ON p.id = db.price_id

INNER JOIN stg.v_dim_cities loading
    ON loading.gc_id = p.loading_city_gcid

INNER JOIN stg.v_dim_cities unloading
    ON unloading.gc_id = p.unloading_city_gcid

WHERE
    p.is_fact = TRUE
    AND p.price > 0
    AND p.distance > 0
    AND p.complex = FALSE
    AND p.outlier_mark = FALSE;


