#!/usr/bin/env Rscript
# Run Achilles data characterization on the completed OMOP CDM (DuckDB).
#
# Prerequisites:
#   - ETL has been run so the CDM is populated in the DuckDB database.
#   - R packages: Achilles, DatabaseConnector, duckdb, DBI, yaml
#
# Install OHDSI packages if needed:
#   remotes::install_github("OHDSI/Achilles")
#   remotes::install_github("OHDSI/DatabaseConnector")
#
# Usage:
#   Rscript extras/run_achilles.R [path/to/cdm.duckdb]
#   or from R:
#     source("extras/run_achilles.R"); run_achilles("path/to/cdm.duckdb")
#
# Options (environment variables or arguments):
#   ETLDELPHI_DB_PATH    Path to DuckDB file (default: same as first script arg)
#   config_path          Path to config YAML (optional; else default_etl_config() for schema names)

options(warn = 1)

run_achilles <- function(db_path = NULL,
                         config_path = NULL,
                         cdm_schema = NULL,
                         results_schema = "achilles",
                         scratch_schema = NULL,
                         cdm_version = "5.4",
                         num_threads = 1L,
                         output_folder = "achilles_output",
                         small_cell_count = 5L,
                         verbose = TRUE,
                         create_results_schema = TRUE) {
  db_path <- db_path %||% Sys.getenv("ETLDELPHI_DB_PATH", "")
  if (!nzchar(db_path)) {
    stop("Provide db_path (path to DuckDB file) or set ETLDELPHI_DB_PATH.")
  }
  if (!file.exists(db_path)) {
    stop("Database file not found: ", db_path)
  }

  # Resolve schema names from config if not provided
  if (is.null(cdm_schema)) {
    if (!is.null(config_path) && nzchar(config_path) && file.exists(config_path)) {
      config <- yaml::read_yaml(config_path)
      cdm_schema <- config[["schemas"]][["cdm"]] %||% "cdm"
    } else {
      config <- ETLDelphi::default_etl_config()
      cdm_schema <- config[["schemas"]][["cdm"]] %||% "cdm"
    }
  }
  scratch_schema <- scratch_schema %||% results_schema

  # Create results (and scratch) schema so Achilles can write there
  if (create_results_schema) {
    con <- DBI::dbConnect(duckdb::duckdb(), db_path)
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    for (schema in c(results_schema, scratch_schema)) {
      if (!schema %in% DBI::dbGetQuery(con, "SELECT schema_name FROM duckdb_schemas()")$schema_name) {
        DBI::dbExecute(con, DBI::SQL(paste0('CREATE SCHEMA "', schema, '"')))
        if (verbose) message("Created schema: ", schema)
      }
    }
    on.exit(NULL, add = FALSE)
    DBI::dbDisconnect(con, shutdown = TRUE)
  }

  # DatabaseConnector connection details for DuckDB
  connection_details <- DatabaseConnector::createConnectionDetails(
    dbms = "duckdb",
    server = normalizePath(db_path, mustWork = TRUE)
  )

  if (verbose) {
    message("Running Achilles on CDM in schema: ", cdm_schema)
    message("Results schema: ", results_schema)
    message("Output folder: ", output_folder)
  }

  Achilles::achilles(
    connectionDetails = connection_details,
    cdmDatabaseSchema = cdm_schema,
    resultsDatabaseSchema = results_schema,
    scratchDatabaseSchema = scratch_schema,
    vocabDatabaseSchema = cdm_schema,
    cdmVersion = cdm_version,
    numThreads = num_threads,
    outputFolder = output_folder,
    smallCellCount = small_cell_count,
    verboseMode = verbose,
    createTable = TRUE,
    createIndices = TRUE,
    defaultAnalysesOnly = TRUE
  )
}

`%||%` <- function(x, y) if (is.null(x)) y else x

# Allow running as script: Rscript extras/run_achilles.R [db_path]
if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  db_path <- if (length(args)) args[1] else NULL
  run_achilles(db_path = db_path)
}
