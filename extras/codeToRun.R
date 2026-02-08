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
duckdb_path <- Sys.getenv("DUCKDB_PATH", "delphi_omop.duckdb")

# Optional: vocabulary file delimiter ("," for CSV, "\t" for TSV)
vocab_delimiter <- Sys.getenv("VOCAB_DELIMITER", ",")

# Optional: path to config YAML; if unset, package default is used
config_path <- Sys.getenv("ETL_CONFIG_PATH", NA_character_)

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
con <- DBI::dbConnect(duckdb::duckdb(), duckdb_path)
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

# --- 1. Initialize CDM DDL and load vocabulary -------------------------------

init_vocabulary_db(
  con = con,
  vocabulary_dir = vocabulary_dir,
  delimiter = vocab_delimiter
)

# --- 2. Create src schema and load Delphi CSVs -------------------------------

DBI::dbExecute(con, 'CREATE SCHEMA IF NOT EXISTS src')

# Expected Delphi CSV base names (filename without extension -> src table name)
delphi_tables <- c(
  "allergy", "current_medications", "death", "encounter", "enrollment",
  "immunization", "lab_orders", "lab_results", "medication_fulfillment",
  "medication_orders", "problem", "provider", "therapy_actions",
  "therapy_orders", "vital_sign"
)

for (tbl in delphi_tables) {
  csv_file <- file.path(delphi_source_dir, paste0(tbl, ".csv"))
  if (!file.exists(csv_file)) {
    warning("Delphi CSV not found, skipping: ", csv_file)
    next
  }
  path_sql <- gsub("'", "''", normalizePath(csv_file, winslash = "/"))
  sql <- paste0(
    "CREATE OR REPLACE TABLE src.", tbl, " AS SELECT * FROM read_csv_auto('",
    path_sql, "', header = true)"
  )
  DBI::dbExecute(con, sql)
  message("Loaded src.", tbl)
}

# --- 3. Run the ETL ----------------------------------------------------------

run_etl(
  con = con,
  config_path = if (is.na(config_path) || !nzchar(config_path)) NULL else config_path
)

message("ETL complete. DuckDB output: ", duckdb_path)
