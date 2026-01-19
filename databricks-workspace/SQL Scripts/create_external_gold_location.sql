/* Gold layer external location' */

CREATE EXTERNAL LOCATION nhs_gold_location
URL 'abfss://data@nhsdatalakevenz.dfs.core.windows.net/curated/gold'
WITH (STORAGE CREDENTIAL nhs_adls_cred);

/* Verify external locations created */
SHOW EXTERNAL LOCATIONS;