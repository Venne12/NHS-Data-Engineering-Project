USE CATALOG adb_nhs_eng_lakehouse;
USE SCHEMA gold;

/* Create Gold Table 3 - Performance Indicators */
CREATE OR REPLACE TABLE gold.monthly_performance_pressure_kpis
USING DELTA
AS
SELECT
    period,

    /* Base totals */
    SUM(
        ae_attendances_type_1 +
        ae_attendances_type_2 +
        ae_attendances_other
    ) AS total_attendances,

    SUM(attendances_over_4hrs_type_1 +
        attendances_over_4hrs_type_2 +
        attendances_over_4hrs_other
    ) AS total_over_4hrs,

    SUM(patients_12hrs_plus_dta) AS total_12hrs_plus,

    SUM(
        emergency_admissions_ae_type_1 +
        emergency_admissions_ae_type_2 +
        emergency_admissions_other_ae +
        other_emergency_admissions
    ) AS total_emergency_admissions,

    SUM(ae_attendances_type_1) AS type1_attendances,

    /* Derived KPIs */
    ROUND(
        (SUM(attendances_over_4hrs_type_1 +
             attendances_over_4hrs_type_2 +
             attendances_over_4hrs_other)
         + SUM(patients_12hrs_plus_dta))
        /
        NULLIF(
            SUM(ae_attendances_type_1 +
                ae_attendances_type_2 +
                ae_attendances_other),
            0
        ), 4
    ) AS pressure_index,

    ROUND(
        SUM(
            emergency_admissions_ae_type_1 +
            emergency_admissions_ae_type_2 +
            emergency_admissions_other_ae +
            other_emergency_admissions
        ) /
        NULLIF(
            SUM(ae_attendances_type_1 +
                ae_attendances_type_2 +
                ae_attendances_other),
            0
        ), 4
    ) AS admission_conversion_rate,

    ROUND(
        SUM(ae_attendances_type_1) /
        NULLIF(
            SUM(ae_attendances_type_1 +
                ae_attendances_type_2 +
                ae_attendances_other),
            0
        ), 4
    ) AS type1_dependency

FROM adb_nhs_eng_lakehouse.silver.ae_attendances_silver
GROUP BY period;

/* Validate the table */
SELECT *
FROM gold.monthly_performance_pressure_kpis
ORDER BY period;

/* Create an external Gold table from existing one */
CREATE TABLE gold.monthly_performance_pressure_kpis_ext
USING DELTA
LOCATION 'abfss://data@nhsdatalakevenz.dfs.core.windows.net/curated/gold/monthly_performance_pressure_kpis'
AS
SELECT *
FROM gold.monthly_performance_pressure_kpis;

/* Validate tables */
SELECT COUNT(*) FROM gold.monthly_performance_pressure_kpis_ext;
SELECT COUNT(*) FROM gold.monthly_performance_pressure_kpis;

/* Drop the original table and Rename the external table to original name */
DROP TABLE gold.monthly_performance_pressure_kpis;

ALTER TABLE gold.monthly_performance_pressure_kpis_ext
RENAME TO gold.monthly_performance_pressure_kpis;

/* Verify the Table */
SELECT *
FROM gold.monthly_performance_pressure_kpis
ORDER BY period;