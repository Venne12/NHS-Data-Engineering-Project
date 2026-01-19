USE CATALOG adb_nhs_eng_lakehouse;
USE SCHEMA gold;

/* Create the Gold KPI Table 1 - Monthly National KPIs */ 
CREATE OR REPLACE TABLE gold.monthly_national_kpis
USING DELTA
AS
SELECT
    period,

    /* Base totals */
    SUM(
        ae_attendances_type_1 +
        ae_attendances_type_2 +
        ae_attendances_other
    ) AS total_ae_attendances,

    SUM(
        emergency_admissions_ae_type_1 +
        emergency_admissions_ae_type_2 +
        emergency_admissions_other_ae +
        other_emergency_admissions
    ) AS total_emergency_admissions,

    SUM(
        attendances_over_4hrs_type_1 +
        attendances_over_4hrs_type_2 +
        attendances_over_4hrs_other
    ) AS total_over_4hrs_attendances,

    SUM(patients_12hrs_plus_dta) AS total_12hrs_plus_patients,

    /* Derived KPIs */
    ROUND(
        SUM(
            attendances_over_4hrs_type_1 +
            attendances_over_4hrs_type_2 +
            attendances_over_4hrs_other
        ) /
        NULLIF(
            SUM(
                ae_attendances_type_1 +
                ae_attendances_type_2 +
                ae_attendances_other
            ), 0
        ) * 100, 2
    ) AS pct_over_4hrs_attendances,

    ROUND(
        SUM(patients_12hrs_plus_dta) /
        NULLIF(
            SUM(
                ae_attendances_type_1 +
                ae_attendances_type_2 +
                ae_attendances_other
            ), 0
        ) * 100, 2
    ) AS pct_12hrs_plus_patients,

    ROUND(
        SUM(
            emergency_admissions_ae_type_1 +
            emergency_admissions_ae_type_2 +
            emergency_admissions_other_ae +
            other_emergency_admissions
        ) /
        NULLIF(
            SUM(
                ae_attendances_type_1 +
                ae_attendances_type_2 +
                ae_attendances_other
            ), 0
        ) * 100, 2
    ) AS admission_conversion_rate

FROM adb_nhs_eng_lakehouse.silver.ae_attendances_silver
GROUP BY period;

/* Verify the Table */
SELECT *
FROM gold.monthly_national_kpis
ORDER BY period;

/* Create an external Gold table from existing one */
CREATE TABLE gold.monthly_national_kpis_ext
USING DELTA
LOCATION 'abfss://data@nhsdatalakevenz.dfs.core.windows.net/curated/gold/monthly_national_kpis'
AS
SELECT *
FROM gold.monthly_national_kpis;

/* Validate tables */
SELECT COUNT(*) FROM gold.monthly_national_kpis_ext;
SELECT COUNT(*) FROM gold.monthly_national_kpis;

/* Drop the original table and Rename the external table to original name */
DROP TABLE gold.monthly_national_kpis;

ALTER TABLE monthly_national_kpis
RENAME TO gold.monthly_national_kpis;

/* Verify the Table */
SELECT *
FROM gold.monthly_national_kpis
ORDER BY period;
