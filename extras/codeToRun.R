# =============================================================================
# codeToRun.R — Load ETLDelphi and run the full ETL with parameterized paths
# =============================================================================
# Set the three paths below (or pass via environment / command line), then
# source this file or run in R to create the DuckDB, load vocabulary and
# Delphi source data, and execute the ETL.
# =============================================================================

# --- Parameters (edit these or set via Sys.setenv / command line) ------------

# Directory containing vocabulary CSV/TSV files (e.g. CONCEPT.csv, VOCABULARY.csv)
vocabulary_dir <- Sys.getenv("VOCABULARY_DIR", "vocabulary_download_v5")

# Directory containing Delphi source CSV files (e.g. enrollment.csv, encounter.csv)
delphi_source_dir <- Sys.getenv("DELPHI_SOURCE_DIR", "delphi100k")

# Path to the output DuckDB database file (created or overwritten)
duckdb_path <- Sys.getenv("DUCKDB_PATH", "~/Desktop/delphi.duckdb")

# Optional: vocabulary file delimiter ("," for CSV, "\t" for TSV)
vocab_delimiter <- Sys.getenv("VOCAB_DELIMITER", "\t")

# Optional: path to config YAML; if unset, package default is used
config_path <- Sys.getenv("ETL_CONFIG_PATH", NA_character_)

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

ETLDelphi::run_etl(
  con = con,
  config_path = if (is.na(config_path) || !nzchar(config_path)) NULL else config_path
)

ETLDelphi::export_unmapped_units(con, output_path = "unmapped_units.csv")
ETLDelphi::analyze_mapping_quality(con, output_dir = "mapping_quality_results")

DBI::dbDisconnect(con, shutdown = TRUE)
message("ETL complete. DuckDB output: ", duckdb_path)
