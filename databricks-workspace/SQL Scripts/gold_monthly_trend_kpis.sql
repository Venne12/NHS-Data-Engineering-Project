USE CATALOG adb_nhs_eng_lakehouse;
USE SCHEMA gold;

/* Create Gold Table 4 - Trend-Ready Time-Series KPIs */
CREATE OR REPLACE TABLE gold.monthly_trend_kpis
USING DELTA
AS
WITH base AS (
    SELECT
        period,
        SUM(
            ae_attendances_type_1 +
            ae_attendances_type_2 +
            ae_attendances_other
        ) AS total_attendances,

        SUM(
            attendances_over_4hrs_type_1 +
            attendances_over_4hrs_type_2 +
            attendances_over_4hrs_other
        ) AS total_over_4hrs
    FROM adb_nhs_eng_lakehouse.silver.ae_attendances_silver
    GROUP BY period
)

SELECT
    period,
    total_attendances,
    total_over_4hrs,

    /* Month-on-month change */
    total_attendances
      - LAG(total_attendances) OVER (ORDER BY period)
      AS month_on_month_attendance_change,

    total_over_4hrs
      - LAG(total_over_4hrs) OVER (ORDER BY period)
      AS month_on_month_wait_change,

    /* Rolling average */
    ROUND(
        AVG(total_attendances)
        OVER (ORDER BY period ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
        0
    ) AS rolling_3_month_avg_attendances

FROM base;

/* Validate the table */
SELECT *
FROM gold.monthly_trend_kpis
ORDER BY period;

/* Create an external Gold table from existing one */
CREATE TABLE gold.monthly_trend_kpis_ext
USING DELTA
LOCATION 'abfss://data@nhsdatalakevenz.dfs.core.windows.net/curated/gold/monthly_trend_kpis'
AS
SELECT *
FROM gold.monthly_trend_kpis;

/* Validate tables */
SELECT COUNT(*) FROM gold.monthly_trend_kpis_ext;
SELECT COUNT(*) FROM gold.monthly_trend_kpis;

/* Drop the original table and Rename the external table to original name */
DROP TABLE gold.monthly_trend_kpis;

ALTER TABLE gold.monthly_trend_kpis_ext
RENAME TO gold.monthly_trend_kpis;

/* Verify the Table */
SELECT *
FROM gold.monthly_trend_kpis
ORDER BY period;