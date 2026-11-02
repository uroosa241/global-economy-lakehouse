# global-economy-lakehouse

## Overview

A medallion lakehouse on Azure Databricks for World Bank economic indicators (currently GDP, `NY.GDP.MKTP.CD`).
Azure Data Factory lands raw API extracts in Bronze; Databricks notebooks curate them into Silver and Gold Delta tables governed by Unity Catalog.

## Architecture: Bronze -> Silver -> Gold

```
World Bank API --(ADF)--> Bronze (NDJSON files, read-only)
                              |
                              v
                 notebooks/02_silver  ->  <catalog>.<silver_schema>
                              |
                              v
                 notebooks/03_gold    ->  <catalog>.<gold_schema>
```

* **Bronze** - raw files written by ADF at `<bronze_root>/<indicator>/<ISO3>/<ISO3>_<indicator>_<YYYY-MM-DD>.json`. Never modified by this project.
* **Silver** (`02_silver`) - parses every line, validates structure and schema drift, reconciles record counts, and appends to history-preserving Delta tables (`wb_indicator_observations`, `wb_load_metadata`, `wb_indicator_quarantine`, `wb_pipeline_run_audit`, view `v_gdp_observations`). Inserts are idempotent; conflicting snapshots go to quarantine and fail the run.
* **Gold** (`03_gold`) - `gdp_country_year`: one row per country and year from the latest load, with year-over-year % change. Refreshed with an idempotent MERGE.

## Parameters

Both notebooks read these widgets (job parameters override the defaults):

| Name | Purpose |
| --- | --- |
| `catalog` | Unity Catalog catalog |
| `bronze_root` | Bronze landing path |
| `silver_schema` | Silver schema |
| `gold_schema` | Gold schema |
| `gold_location` | Managed location for the Gold schema |

## How to run

* **Scheduled:** the Lakeflow Job `global_economy_lakehouse` runs `silver` then `gold` on serverless compute. A file arrival trigger on the Bronze path starts it when ADF lands new files.
* **Manual:** open the job and click *Run now*, or *Run now with different parameters* to override widget values.
* **Interactive:** open a notebook, set the widgets at the top, and run all cells (Silver first, then Gold).

## Prerequisites

* Unity Catalog external locations for the Bronze, Silver and Gold storage paths.
* `READ FILES` on the Bronze location and `CREATE`/`MODIFY` on the target catalog and schemas.
* Do not commit credentials. Use Databricks secret scopes; see `.gitignore`.
