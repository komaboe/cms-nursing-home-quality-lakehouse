# Nursing Home Quality Integration with Spark

This project builds a Databricks and Apache Spark pipeline that integrates four public CMS nursing-home datasets into one facility-level analytical table.

The main question is:

> Can separate CMS nursing-home datasets be integrated into one useful facility-level table for descriptive analysis of staffing, deficiencies, and penalties?

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
+-- care_compare/
|   +-- NH_ProviderInfo_May2026.csv
|   +-- NH_HealthCitations_May2026.csv
|   +-- NH_Penalties_May2026.csv
+-- pbj/
    +-- PBJ_dailynursestaffing_CY2025Q4.csv
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
5. Clean and type source-level tables in Silver.
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

The final notebook answers two descriptive questions and runs one validation check:

- How do deficiency patterns differ across staffing quartiles?
- How do deficiency patterns differ across facility size bands?
- How close is PBJ-derived RN HPRD to the reported RN staffing field?

Saved report artifacts are stored in `exports/`:

| File | Description |
| --- | --- |
| `Average_Severity_Score_by_Staffing_Quartile.png` | Bar chart for staffing quartiles and severity score |
| `Average_Citations_by_Facility_Size_Band.png` | Bar chart for facility size bands and average citations |
| `Staffing_Quartiles.csv` | Staffing quartile summary table |
| `Staffing_Comparison_Q1_Q4.csv` | Lowest vs highest staffing quartile comparison |
| `Facility_Size_Bands_Summary.csv` | Facility size band summary table |
| `Validation_Table.csv` | PBJ-derived RN HPRD validation summary |

## Result Summary

The integrated Gold table contains 14,700 provider facilities. Join coverage is high for PBJ staffing records and deficiency records, while penalty records apply to a smaller share of facilities.

The staffing quartile analysis shows that facilities in the lowest staffing quartile have higher average deficiency outcomes than facilities in the highest staffing quartile:

- 37.9% more average citations;
- 44.7% higher average severity score;
- 100.0% more average harm-plus citations.

The same comparison also shows that the lowest-staffing quartile has larger facilities on average, so facility size is a possible confounding factor.

The facility size analysis shows that very large facilities have higher average citations than small facilities:

- small facilities: 18.78 average citations;
- very large facilities: 35.90 average citations.

The RN staffing validation compares the reported RN HPRD field with PBJ-derived RN HPRD. Across 14,071 compared facilities, reported RN HPRD averages 0.678 and PBJ-derived RN HPRD averages 0.473. The difference is treated as a sanity check, not as a perfect reconciliation, because public CMS summary fields and PBJ-derived values can use different reporting windows or definitions.

## Scope And Limitations

The results are descriptive and observational. They show facility-level patterns in public CMS data, but they do not prove that staffing levels cause deficiency outcomes.

Main limitations:

- only one PBJ quarter is used;
- the analysis is facility-level, not resident-level;
- CMS summary fields and computed rollups may use different definitions;
- facility size, ownership, geography, and case mix may affect the observed patterns.

This project uses public facility-level data only. It does not use patient-level data, resident records, or private health information.
