\c deals_bigdata

CREATE SCHEMA IF NOT EXISTS mart;

DROP materialized view if exists mart.route_card;
CREATE MATERIALIZED VIEW mart.route_card AS
WITH as_of AS (
    SELECT as_of_date
    FROM stg.etl_control
    WHERE id = TRUE
),
monthly_prices AS (
    SELECT 
        id,
        loading_month,
        price_date,
        loading_city_gcid,
        unloading_city_gcid,
        body_group_std_id,
        weight_category,
        organization_id,
        price_type,
        distance,
        price
    FROM stg.v_fact_market_prices f
    CROSS JOIN as_of ao
    WHERE f.price_date <= ao.as_of_date
        AND price > 0
),
-- 3. Агрегируем показатели каждой аналитической ячейки.
monthly_metrics AS (
    SELECT
        loading_month,
        loading_city_gcid,
        unloading_city_gcid,
        body_group_std_id,
        weight_category,
        COUNT(DISTINCT id) AS trips_count,
        COUNT(DISTINCT organization_id)
            FILTER (WHERE price_type IN (3, 9))
            AS carriers_count,
        -- Рыночный коридор за рейс.
        PERCENTILE_CONT(0.25)
            WITHIN GROUP (ORDER BY price) AS p25_trip,
        PERCENTILE_CONT(0.50)
            WITHIN GROUP (ORDER BY price) AS median_trip,
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (ORDER BY price) AS p75_trip,
        AVG(price) AS avg_trip,
        -- Рыночный коридор за километр.
        PERCENTILE_CONT(0.25)
            WITHIN GROUP (
                ORDER BY price / NULLIF(distance, 0)
            ) AS p25_km,
        PERCENTILE_CONT(0.50)
            WITHIN GROUP (
                ORDER BY price / NULLIF(distance, 0)
            ) AS median_km,
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (
                ORDER BY price / NULLIF(distance, 0)
            ) AS p75_km,
        -- Коэффициент вариации.
        CASE
            WHEN COUNT(price) < 2
                OR AVG(price) = 0
                THEN NULL
            ELSE STDDEV_SAMP(price) / AVG(price)
        END AS cv_rate,
        MAX(price_date) AS freshness_date
    FROM monthly_prices
    GROUP BY
        loading_month,
        loading_city_gcid,
        unloading_city_gcid,
        body_group_std_id,
        weight_category
),
-- 4. Сопоставляем с предыдущим календарным месяцем
-- и тем же месяцем прошлого года.
period_comparison AS (
    SELECT
        current_month.*,
        previous_month.median_trip AS previous_month_median,
        previous_year.median_trip AS previous_year_median
    FROM monthly_metrics current_month
    LEFT JOIN monthly_metrics previous_month ON 
        previous_month.loading_month = (current_month.loading_month - INTERVAL '1 month')::date
       	AND previous_month.loading_city_gcid = current_month.loading_city_gcid
       	AND previous_month.unloading_city_gcid = current_month.unloading_city_gcid
       	AND previous_month.body_group_std_id = current_month.body_group_std_id
       	AND previous_month.weight_category = current_month.weight_category
    LEFT JOIN monthly_metrics previous_year ON previous_year.loading_month = (current_month.loading_month - INTERVAL '1 year')::date
       	AND previous_year.loading_city_gcid = current_month.loading_city_gcid
       	AND previous_year.unloading_city_gcid = current_month.unloading_city_gcid
       	AND previous_year.body_group_std_id = current_month.body_group_std_id
       	AND previous_year.weight_category = current_month.weight_category
)
-- 5. Финальная карточка маршрута.
SELECT
    pc.loading_month,
    pc.loading_city_gcid AS origin_city_gcid,
    origin.name AS origin_city,
    origin.full_name_local AS origin_city_full_name,
    pc.unloading_city_gcid AS destination_city_gcid,
    destination.name AS destination_city,
    destination.full_name_local AS destination_city_full_name,
    pc.body_group_std_id,
    pc.weight_category,
    -- За рейс.
    pc.p25_trip,
    pc.median_trip,
    pc.p75_trip,
    pc.avg_trip,
    -- За километр.
    pc.p25_km,
    pc.median_km,
    pc.p75_km,
    -- Data Trust.
    pc.trips_count,
    pc.carriers_count,
    pc.cv_rate,
    -- Динамика медианной ставки за рейс.
    CASE
        WHEN pc.previous_month_median IS NULL
            OR pc.previous_month_median = 0
            THEN NULL
        ELSE
            (pc.median_trip / pc.previous_month_median - 1) * 100
    END AS mom_pct,
    CASE
        WHEN pc.previous_year_median IS NULL
            OR pc.previous_year_median = 0
            THEN NULL
        ELSE
            (pc.median_trip / pc.previous_year_median - 1) * 100
    END AS yoy_pct,
    CASE
        WHEN pc.trips_count >= 30
            AND pc.carriers_count >= 5
            AND pc.freshness_date >= ao.as_of_date - 30
            THEN 'HIGH'
        WHEN pc.trips_count >= 10
            THEN 'MEDIUM'
        ELSE 'LOW_DATA'
    END AS reliability_level
FROM period_comparison pc
	LEFT JOIN stg.v_dim_cities origin
	    ON pc.loading_city_gcid = origin.gc_id
	LEFT JOIN stg.v_dim_cities destination
	    ON pc.unloading_city_gcid = destination.gc_id
CROSS JOIN as_of ao;

