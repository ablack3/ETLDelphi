
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ETLDelphi

**ETLDelphi** transforms Delphi synthetic health data into the OMOP
Common Data Model (CDM) v5.4 in DuckDB. It provides a full ETL pipeline
(source → staging → CDM), configurable type concept IDs, and
mapping-quality tooling.

## What is Delphi synthetic data?

[Delphi](https://github.com/gerstung-lab/Delphi) is a modified GPT-2
model that learns disease progression from electronic health records and
can **generate synthetic patient trajectories**.
[Delphi-2M](https://www.nature.com/articles/s41586-025-09529-3) was
trained on ~400K UK Biobank participants and predicts rates of 1,000+
diseases conditional on past diagnoses; it can also **sample** future
trajectories (e.g. for ages 60–80). The synthetic data preserve
statistical patterns of multi-morbidity and event timing without
disclosing real individuals, and are used here as a Delphi-shaped
**source schema** (enrollment, encounter, problem, medication_orders,
lab_results, etc.) that this package maps to OMOP CDM.

Reference: Shmatko et al., *Learning the natural history of human
disease with generative transformers*, Nature **647**, 248–256 (2025),
<https://doi.org/10.1038/s41586-025-09529-3>.

## Using the Delphi CDM dataset

If you have a pre-built Delphi CDM DuckDB (e.g. from running the ETL
below or from a prepared download), connect with **CDMConnector** and
use the `cdm` object as usual:

``` r
library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("delphi-100k"))
cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", achillesSchema = "main")

# Use cdm with dplyr and OHDSI tools
cdm$person
cdm$condition_occurrence
cdm$drug_exposure
```

Replace `eunomiaDir("delphi-100k")` with the path to your DuckDB file if
you built it locally (e.g. `"~/Desktop/delphi.duckdb"`).

### Deploying the Delphi CDM to another database

Use **CDMConnector::copyCdmTo()** to copy the CDM from DuckDB to another
database (e.g. PostgreSQL, SQL Server):

``` r
library(CDMConnector)
library(DBI)

# Source: Delphi CDM in DuckDB
con_src <- DBI::dbConnect(duckdb::duckdb(), "~/Desktop/delphi.duckdb")
cdm <- cdmFromCon(con_src, cdmSchema = "main", writeSchema = "main")

# Target: your database (example: PostgreSQL)
con_tgt <- DBI::dbConnect(RPostgres::Postgres(), host = "...", dbname = "...", user = "...", password = "...")

# Copy CDM tables to the target (creates tables in writeSchema)
cdm_tgt <- CDMConnector::copyCdmTo(
  con = con_tgt,
  cdm = cdm,
  cdmSchema = "main",      # schema on target for CDM tables
  writeSchema = "main"     # schema on target for any write operations
)

DBI::dbDisconnect(con_src)
DBI::dbDisconnect(con_tgt)
```

Adjust `cdmSchema` and `writeSchema` to match your target database. The
target connection must have write access; vocabulary and clinical tables
will be copied.

## How to run the ETL

1.  **Download the full OMOP vocabulary** from
    [Athena](https://athena.ohdsi.org/) and unzip to a folder
    (e.g. `vocabulary_download_v5`).
2.  **Download the Delphi source data** from \[link to be inserted\] and
    unzip to a folder (e.g. `delphi100k`).
3.  Open **`extras/codeToRun.R`**, set `vocabulary_dir`,
    `delphi_source_dir`, and `duckdb_path`, then **source the file** (or
    run it in R). It will:
    - Create the DuckDB database and load the vocabulary
    - Create the `src` schema and load the Delphi CSVs
    - Run the full ETL into the CDM

No other steps required; the script uses default config (schemas, type
concept IDs). Override `config` in the script or pass a YAML path if
needed.

## More information

- **How to run**: `vignette("how_to_run", package = "ETLDelphi")` —
  prerequisites and step-by-step run.
- **ETL design**: `vignette("etl", package = "ETLDelphi")` — workflow,
  tables, and design choices.
- **Run steps in detail**:
  `vignette("run_etl_steps", package = "ETLDelphi")` — what each ETL
  step does.
- **Package website (vignettes and reference)**: build with
  `pkgdown::build_site()` or see the deployed site if available.
