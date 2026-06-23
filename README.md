### B142 Data Integration Assignment
# Nursing Home Quality Integration with Spark

This project builds a Databricks and Apache Spark pipeline that integrates four public CMS nursing-home datasets into one facility-level analytical table for descriptive quality analysis.

The main question is:

> What facility-level quality and regulatory patterns become visible when CMS nursing-home staffing, deficiency, penalty, and provider datasets are integrated in a Spark lakehouse?

The pipeline uses a Bronze, Silver, and Gold structure. Raw CSV files are uploaded to a Databricks volume, stored as Bronze Delta tables, cleaned into Silver tables, aggregated to facility grain in Gold, and then analyzed with Spark SQL.

## Data Sources

The project uses public CMS nursing-home data.

| Source | File used | Role in project | Direct download |
| --- | --- | --- | --- |
| Provider Information | `NH_ProviderInfo_May2026.csv` | Facility spine and provider attributes | [CSV](https://data.cms.gov/provider-data/sites/default/files/resources/38f631a211bad946a404d39a1c66d599_1778861765/NH_ProviderInfo_May2026.csv) |
| Health Deficiencies | `NH_HealthCitations_May2026.csv` | Citation counts and severity measures | [CSV](https://data.cms.gov/provider-data/sites/default/files/resources/e269c04e82fe86040cd70b7ba3dd4853_1778861752/NH_HealthCitations_May2026.csv) |
| Penalties | `NH_Penalties_May2026.csv` | Fine and payment-denial summaries | [CSV](https://data.cms.gov/provider-data/sites/default/files/resources/c671cdaa1461db5a685367690785fcb3_1778861763/NH_Penalties_May2026.csv) |
| PBJ Daily Nurse Staffing | `PBJ_dailynursestaffing_CY2025Q4.csv` | Daily staffing hours and HPRD calculations | [CSV](https://data.cms.gov/sites/default/files/2026-04/8f85c7d4-a1f6-4b36-ad20-17abc8aa57d2/PBJ_dailynursestaffing_CY2025Q4.csv) |

Dataset references:

- CMS Provider Data Catalog - Nursing homes topic: https://data.cms.gov/provider-data/topics/nursing-homes
- PBJ Daily Nurse Staffing: https://data.cms.gov/quality-of-care/payroll-based-journal-daily-nurse-staffing
- CMS nursing-home data dictionary: https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/nursing_home/NH_Data_Dictionary.pdf
- PBJ technical specification: https://data.cms.gov/sites/default/files/2023-06/PBJ_PUF_Documentation_July_2023.pdf

## Databricks Project Archive

The Databricks project is provided as a workspace archive:

```text
notebooks/nh-quality-lakehouse.dbc
```

Import the archive into a Databricks workspace. Then run `00 Setup` first. This notebook creates the Bronze, Silver, and Gold schemas plus the raw upload volume:

```text
/Volumes/b142/nh_bronze/raw/
```

After `00 Setup` has created the volume, create the two raw-file subdirectories if they are not already present and upload the raw CSV files into this structure:

```text
/Volumes/b142/nh_bronze/raw/
    care_compare/
        NH_ProviderInfo_May2026.csv
        NH_HealthCitations_May2026.csv
        NH_Penalties_May2026.csv
    pbj/
        PBJ_dailynursestaffing_CY2025Q4.csv
```

The Databricks notebooks use wildcard paths, so a different CMS month or PBJ quarter can be used if the same file naming patterns are kept:

```text
NH_ProviderInfo_*.csv
NH_HealthCitations_*.csv
NH_Penalties_*.csv
PBJ_*.csv
```

If the Databricks catalog is different, update `catalog_name = "b142"` in the project configuration cells before running the pipeline.

Use this setup and run sequence:

```text
00 Setup
Upload raw files into the created volume
01 Bronze Ingestion
02 Silver Cleaning
03 Gold Facility Quality Mart
04 Analysis And Validation
```

## Pipeline Design

The project uses an ELT pattern:

1. Import the Databricks workspace archive.
2. Run `00 Setup` to create the Bronze, Silver, and Gold schemas plus the raw upload volume.
3. Upload raw CMS and PBJ CSV files into the created volume subdirectories.
4. Read raw CSV files as strings and store them as Bronze Delta tables.
5. Clean source-level fields and cast data types in Silver.
6. Aggregate citation, penalty, and daily staffing rows to one row per facility.
7. Left join the rollups to Provider Information to create the Gold `facility_quality` table.
8. Use the Gold table for analysis, validation, and report charts.

Key integration decisions:

- `CMS Certification Number (CCN)` and PBJ `PROVNUM` are standardized into a six-character `ccn` string.
- Raw values are kept as strings in Bronze so type conversion decisions are explicit in Silver.
- Provider Information remains the facility spine.
- Health citations, penalties, and PBJ daily staffing are aggregated before joining to avoid row multiplication.
- Left joins keep all provider facilities and use coverage flags to show which sources matched.

## Main Tables

| Layer | Table | Description |
| --- | --- | --- |
| Bronze | `b142.nh_bronze.provider_info` | Raw Provider Information with safe column names |
| Bronze | `b142.nh_bronze.health_deficiencies` | Raw Health Deficiencies with safe column names |
| Bronze | `b142.nh_bronze.penalties` | Raw Penalties with safe column names |
| Bronze | `b142.nh_bronze.pbj_daily` | Raw PBJ daily staffing with safe column names |
| Silver | `b142.nh_silver.provider_info` | Clean facility-level provider table |
| Silver | `b142.nh_silver.health_deficiencies` | Clean citation-level table with severity helpers |
| Silver | `b142.nh_silver.penalties` | Clean penalty-event table |
| Silver | `b142.nh_silver.pbj_daily` | Clean daily staffing table with HPRD fields |
| Silver | `b142.nh_silver.silver_quality_snapshot` | Row, distinct key, and null key checks |
| Gold | `b142.nh_gold.facility_quality` | Integrated one-row-per-facility analytical table |
| Gold | `b142.nh_gold.join_coverage_summary` | Source match summary for the Gold table |

## Analysis Outputs

The final notebook produces three descriptive analysis views and one validation check:

- How do deficiency patterns differ across staffing quartiles?
- How does penalty burden differ across staffing quartiles?
- How do deficiency and fine patterns differ across facility size bands?
- How close is PBJ-derived RN HPRD to the reported RN staffing field?

Saved report artifacts are stored in `exports/`:

| File | Description |
| --- | --- |
| `Average_Severity_Score_by_Staffing_Quartile.png` | Bar chart for staffing quartiles and severity score |
| `Average_Citations_by_Facility_Size_Band.png` | Bar chart for facility size bands and average citations |
| `Validation_Table.csv` | PBJ-derived RN HPRD validation summary |

## Result Summary

The Gold table keeps all 14,700 provider facilities. Match coverage is high for PBJ staffing records (97.6%) and deficiency records (99.5%), while penalty records are naturally sparser (46.8%).

The main pattern is that lower-staffed facilities show worse average outcomes than higher-staffed facilities. Compared with the highest staffing quartile, the lowest staffing quartile has more average citations (32.33 vs 23.44), a higher severity score (72.40 vs 50.02), more harm-plus citations (2.18 vs 1.09), and higher average computed fines ($45,512 vs $20,239).

Facility size appears to matter as well. Very large facilities average 35.90 citations, compared with 18.78 for small facilities. The RN HPRD validation is used as a comparison check: across 14,071 facilities, reported RN HPRD averages 0.678 and PBJ-derived RN HPRD averages 0.473.

## Scope And Limitations

This project is descriptive and facility-level. It shows patterns in integrated public CMS data, but it does not prove causal effects between staffing, size, deficiencies, and penalties.

Main limits: only one PBJ quarter is used, resident-level data is not available, CMS summary fields and computed rollups may use different definitions, and facility size may partly explain the staffing-quartile pattern. The project uses public facility-level data only and does not include patient-level or private health information.
