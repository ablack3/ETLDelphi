# =============================================================================
# codeToRun.R — Load ETLDelphi and run the full ETL with parameterized paths
# =============================================================================
# Set the paths and config below (or pass via environment / command line), then
# source this file or run in R to create the DuckDB, load vocabulary and
# Delphi source data, and execute the ETL.
# =============================================================================

# --- Path parameters (edit these or set via Sys.setenv / command line) -------

# Directory containing vocabulary CSV/TSV files (e.g. CONCEPT.csv, VOCABULARY.csv)
vocabulary_dir <- Sys.getenv("VOCABULARY_DIR", "vocabulary_download_v5")

# Directory containing Delphi source CSV files (e.g. enrollment.csv, encounter.csv)
delphi_source_dir <- Sys.getenv("DELPHI_SOURCE_DIR", "delphi100k")

# Path to the output DuckDB database file (created or overwritten)
duckdb_path <- Sys.getenv("DUCKDB_PATH", "~/Desktop/delphi.duckdb")

file.remove(duckdb_path)

# Optional: vocabulary file delimiter ("," for CSV, "\t" for TSV)
vocab_delimiter <- Sys.getenv("VOCAB_DELIMITER", "\t")

# --- ETL config (schemas, type concept IDs, optional mapping paths) ----------
# Edit this list to override defaults. NULL paths use package defaults.
config <- list(
  schemas = list(src = "src", stg = "stg", cdm = "main"),
  concept_id_unknown = 0L,
  date_formats = c(
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d-%b-%Y", "%Y/%m/%d"
  ),
  type_concept_ids = list(
    visit_type_concept_id = 44818517L,
    default_visit_concept_id = 44813942L,
    condition_type_concept_id = 32818L,
    drug_type_orders = 38000177L,
    drug_type_dispensed = 38000230L,
    drug_type_immunization = 38000280L,
    measurement_type_vitals = 32817L,
    measurement_type_labs = 32827L,
    observation_type_allergy = 32859L,
    note_type_concept_id = 44813942L,
    note_class_concept_id = 44814639L,
    language_concept_id = 4180186L,
    encoding_concept_id = 44815386L,
    procedure_type_concept_id = 38000268L,
    period_type_concept_id = 32821L,
    death_type_concept_id = 32817L
  ),
  prefer_fulfillment = FALSE,
  drug_name_mapping_path = NULL,  # NULL = use package default drug_name_to_concept.csv
  custom_mapping_path = NULL      # NULL = use package default custom_concept_mapping.csv
)

if (file.exists(duckdb_path)) file.remove(duckdb_path)

# --- Load package and connect -------------------------------------------------

library(DBI)
library(duckdb)
library(ETLDelphi)

# Resolve paths relative to current working directory if not absolute
if (!dir.exists(vocabulary_dir)) {
  stop("Vocabulary directory not found: ", vocabulary_dir)
}
if (!dir.exists(delphi_source_dir)) {
  stop("Delphi source directory not found: ", delphi_source_dir)
}

vocabulary_dir <- normalizePath(vocabulary_dir, winslash = "/")
delphi_source_dir <- normalizePath(delphi_source_dir, winslash = "/")
duckdb_path <- normalizePath(duckdb_path, mustWork = FALSE, winslash = "/")

# Connect to DuckDB (file created on first use)
# con <- DBI::dbConnect(duckdb::duckdb(), duckdb_path)
# DBI::dbDisconnect(con)
# --- 1. Initialize CDM DDL and load vocabulary -------------------------------

# debugonce(init_vocabulary_db)

con <- ETLDelphi::init_vocabulary_db(
  duckdb_path,
  vocabulary_dir = vocabulary_dir
)

# cdm <- CDMConnector::cdmFromCon(con, "cdm", "main")
# cdm$concept
#
# cdm$vocabulary


# --- 2. Create src schema and load Delphi CSVs -------------------------------

ETLDelphi::init_source_data(con, delphi_source_dir)

# --- 3. Run the ETL ----------------------------------------------------------

DBI::dbDisconnect(con, shutdown = T)

con <- DBI::dbConnect(duckdb::duckdb(), "~/Desktop/delphi.duckdb")

system.time({
  cdm <- CDMConnector::cdmFromCon(con, "cdm", "main")
})

cdm$person

ETLDelphi::run_etl(con = con, config = config)

DBI::dbListTables(con)

CDMConnector::listTables(con, "cdm")

ETLDelphi::export_unmapped_units(con, output_path = "unmapped_units.csv")
unlink(here::here("inst/shiny/mapping_quality"), recursive = T)
ETLDelphi::analyze_mapping_quality(con, output_dir = "inst/shiny/mapping_quality/mapping_quality_results")

library(CDMConnector)
library(dplyr)

cdm <- CDMConnector::cdmFromCon(con, "main", "main")





DBI::dbDisconnect(con, shutdown = TRUE)
message("ETL complete. DuckDB output: ", duckdb_path)
