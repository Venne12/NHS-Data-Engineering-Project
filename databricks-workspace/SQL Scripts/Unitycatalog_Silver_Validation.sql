SELECT current_catalog(), current_schema();

SHOW STORAGE CREDENTIALS;

/* Use the catalog and schema */
USE CATALOG adb_nhs_eng_lakehouse;
USE SCHEMA silver;

/* Create external Delta table pointing to your Silver folder */
CREATE TABLE ae_attendances_silver
USING DELTA
LOCATION 'abfss://data@nhsdatalakevenz.dfs.core.windows.net/curated/silver';

/* Verify the table that has been created */
SELECT * FROM ae_attendances_silver LIMIT 10;