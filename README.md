# NHS A&E Data Engineering Project

## Project Overview
This repository demonstrates an end-to-end **Azure-based Data Engineering pipeline** for NHS Accident & Emergency (A&E) data.  
The pipeline implements **scalable ingestion, lakehouse transformation, governance, incremental KPIs, and dashboard visualization** using modern cloud engineering practices.

The project is divided into four main phases, from raw data ingestion to gold-level KPI generation and orchestration.

---

## Technologies Used
- **Azure Data Lake Storage Gen2 (ADLS)** – Data storage with Bronze, Silver, and Gold layers  
- **Azure Data Factory (ADF)** – Dynamic and parameterized data ingestion pipelines  
- **Azure Databricks** – Data transformation, cleaning, and Delta Lake storage  
- **Azure Key Vault** – Secure secret management for storage keys  
- **Databricks Access Connector & IAM Roles** – Secure access control  
- **Unity Catalog** – Governance and external table management  
- **Databricks Jobs & Pipelines** – Orchestration with retry, notifications, and incremental logic  
- **Dashboards** – KPI visualizations using Databricks SQL

---

## Architecture Overview
![Architecture Diagram](https://github.com/Venne12/NHS-Data-Engineering-Project/blob/main/architecture/Data%20Architecture.png?raw=true)  
*Visual representation of the end-to-end NHS Data Engineering pipeline, including ingestion, lakehouse layers, governance, and dashboards.*

---

## Folder Structure
architecture/
adls-gen2/
azure-data-factory/
azure-key-vault/
databricks-access-connector/
access-control/
databricks-workspace/
dashboard/


---

## Dataset Details

The project ingests **monthly NHS A&E CSV files** from the public NHS website and processes them through the Lakehouse pipeline.  

### Source
- **Format:** CSV (Comma-separated)
- **Source URL:** `https://www.england.nhs.uk` (HTTP linked service)
- **Update Frequency:** Monthly
- **Challenges:** Irregular file names and paths, some months include random hashes

### Columns & Description

| Column Name                               | Data Type | Description |
|-------------------------------------------|-----------|-------------|
| `period`                                  | String    | Month-Year of the data (e.g., "April-2025") |
| `org_code`                                | String    | Unique organization code |
| `parent_org`                              | String    | Parent organization code |
| `org_name`                                | String    | Organization name (nullable) |
| `ae_attendances_type_1`                   | Integer   | Number of type 1 A&E attendances |
| `ae_attendances_type_2`                   | Integer   | Number of type 2 A&E attendances |
| `ae_attendances_other`                     | Integer   | Number of other A&E attendances |
| `ae_attendances_booked_type_1`            | Integer   | Booked type 1 attendances |
| `ae_attendances_booked_type_2`            | Integer   | Booked type 2 attendances |
| `ae_attendances_booked_other`             | Integer   | Booked other attendances |
| `attendances_over_4hrs_type_1`           | Integer   | Type 1 attendances over 4 hours |
| `attendances_over_4hrs_type_2`           | Integer   | Type 2 attendances over 4 hours |
| `attendances_over_4hrs_other`             | Integer   | Other attendances over 4 hours |
| `attendances_over_4hrs_booked_type_1`    | Integer   | Booked type 1 attendances over 4 hours |
| `attendances_over_4hrs_booked_type_2`    | Integer   | Booked type 2 attendances over 4 hours |
| `attendances_over_4hrs_booked_other`     | Integer   | Booked other attendances over 4 hours |
| `patients_4_12hrs_dta`                   | Integer   | Patients waiting 4–12 hours in A&E |
| `patients_12hrs_plus_dta`                | Integer   | Patients waiting over 12 hours in A&E |
| `emergency_admissions_ae_type_1`         | Integer   | Emergency admissions via type 1 A&E |
| `emergency_admissions_ae_type_2`         | Integer   | Emergency admissions via type 2 A&E |
| `emergency_admissions_other_ae`          | Integer   | Emergency admissions via other A&E |
| `other_emergency_admissions`             | Integer   | Other emergency admissions |

**Notes:**
- `TOTAL` rows are removed during transformation
- Columns are standardized in the Silver layer
- Data is partitioned by `Period` in Delta format
- Data feeds **Gold KPI tables** (national, org, performance, trend KPIs)

---

## Phases of Implementation

### **Phase 1 – Data Ingestion (ADF)**
- Created **Resource Group** `nhs-data-eng-rg` and **ADLS Gen2 storage** `nhsdatalakevenz`  
- Configured **ADF linked services**:
  - HTTP source (`ls_nhs_http`)
  - ADLS sink (`ls_nhs_datalake`)  
- Parameterized datasets for month-wise CSV ingestion  
- Implemented **ForEach loops with conditional logic** for irregular months  
- Pipeline: `ingest_nhs_ae_raw` successfully copied files to **raw layer**

### **Phase 2 – Lakehouse Architecture & Transformation**
- Created **Databricks workspace** `adb-nhs-eng-lakehouse` and minimal cluster  
- Configured **Azure Key Vault** and **Access Connector**  
- Mounted ADLS raw container in Databricks  
- Copied raw CSVs → Bronze → Cleaned → Silver layer (Delta format)  
- Transformation steps:
  - Rename columns
  - Drop `TOTAL` rows
  - Add `ingestion_date`
  - Standardize `Period` column

### **Phase 3 – Governance & Gold Tables**
- Unity Catalog: `nhs_catalog` with `bronze`, `silver`, `gold` schemas  
- External silver Delta tables  
- Gold KPI tables:
  - `monthly_national_kpis`
  - `monthly_org_kpis`
  - `monthly_performance_pressure_kpis`
  - `monthly_trend_kpis`  
- Migrated gold tables to **external ADLS location**  
- Incremental updates using **MERGE statements**

### **Phase 4 – Job Orchestration & Dashboard**
- Created **Databricks jobs**:
  - Notebook: `mount_adls_raw` → Silver  
  - Notebook: `nhs_gold_incremental_merge` → Gold  
- Configured retries, email notifications, and cluster assignment  
- Dashboard visualizations:

![Dashboard Example](https://github.com/Venne12/NHS-Data-Engineering-Project/blob/main/dashboard/A&E%20Attendances%20and%20Emergency%20Dashboard.png?raw=true)  
*Interactive KPI dashboard for monthly NHS A&E metrics.*

---

## ❌ Challenges Faced & ✅ How I Fixed Them

### 1. ADF pipeline failures due to unstable Month-wise URL changes → Error 404
- **Problem:** File names changed (`-CSV-revised.csv` → random hashes)  
- **Solution:** Added conditional logic in ADF pipeline to handle irregular months and dynamic file paths

### 2. Databricks access to Key Vault (403 Forbidden)
- **Problem:** `dbutils.secrets.get()` returned permission denied  
- **Solution:** Created **Access Connector**, assigned proper RBAC roles, switched Key Vault to RBAC mode

### 3. CSV schema inference failure
- **Problem:** `UNABLE_TO_INFER_SCHEMA` when reading multiple CSVs  
- **Solution:** Enabled `.option("inferSchema","true")` and `.option("recursiveFileLookup","true")`

### 4. TOTAL rows polluting analytics
- **Problem:** Each CSV had a summary `TOTAL` row  
- **Solution:** Filtered them out with `df.filter(col("period") != "TOTAL")`

### 5. Databricks mount path confusion
- **Problem:** `/mnt/nhs_raw` structure unclear  
- **Solution:** Learned that mount points are aliases; physical folders exist in ADLS

### 6. MERGE errors (column mismatch)
- **Problem:** Incremental MERGE failed due to schema mismatch  
- **Solution:** Aligned source and target columns exactly

---

## Key Takeaways
- Strong **data governance** using Unity Catalog and ADLS security  
- Scalable **parameterized ingestion** for monthly NHS CSVs  
- End-to-end **lakehouse pipeline** from raw → silver → gold  
- Incremental processing ensures **efficient KPI updates**  

---
