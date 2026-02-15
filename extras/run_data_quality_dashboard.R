#!/usr/bin/env Rscript
# Run OHDSI DataQualityDashboard on the completed OMOP CDM (DuckDB).
#
# Prerequisites:
#   - ETL has been run so the CDM is populated in the DuckDB database.
#   - CDM_SOURCE table should have at least one row (recommended for DQD Shiny viewer).
#   - R packages: DataQualityDashboard, DatabaseConnector, duckdb, DBI, yaml
#
# Install OHDSI packages if needed:
#   remotes::install_github("OHDSI/DataQualityDashboard")
#   remotes::install_github("OHDSI/DatabaseConnector")
#
# Usage:
#   Rscript extras/run_data_quality_dashboard.R [path/to/cdm.duckdb]
#   or from R:
#     source("extras/run_data_quality_dashboard.R"); run_data_quality_dashboard("path/to/cdm.duckdb")
#
# Options (environment variables or arguments):
#   ETLDELPHI_DB_PATH    Path to DuckDB file (default: same as first script arg)
#   config_path          Path to config YAML (optional; else default_etl_config() for schema names)

cd <- DatabaseConnector::createConnectionDetails(
  dbms = "duckdb",
  server = "~/Desktop/delphi.duckdb"
)

con <- DatabaseConnector::connect(cd)

DatabaseConnector::executeSql(con, "create schema results")
DatabaseConnector::executeSql(con, "create schema scratch")
DatabaseConnector::disconnect(con)

results <- DataQualityDashboard::executeDqChecks(
  connectionDetails = cd,
  cdmDatabaseSchema = "main",
  resultsDatabaseSchema = 'results',
  numThreads = 4,
  cdmVersion = "5.4",
  cdmSourceName = "delphi",
  outputFolder = here::here("dqd_results")
)

options(warn = 1)

run_data_quality_dashboard <- function(db_path = NULL,
                                       config_path = NULL,
                                       cdm_schema = NULL,
                                       results_schema = "dqd_results",
                                       cdm_source_name = "ETLDelphi CDM",
                                       cdm_version = "5.4",
                                       num_threads = 1L,
                                       output_folder = "dqd_output",
                                       output_file = "results.json",
                                       check_levels = c("TABLE", "FIELD", "CONCEPT"),
                                       check_severity = c("fatal", "convention", "characterization"),
                                       tables_to_exclude = c(
                                         "CONCEPT", "VOCABULARY", "CONCEPT_ANCESTOR",
                                         "CONCEPT_RELATIONSHIP", "CONCEPT_CLASS", "CONCEPT_SYNONYM",
                                         "RELATIONSHIP", "DOMAIN"
                                       ),
                                       write_to_table = TRUE,
                                       write_table_name = "dqdashboard_results",
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
      cdm_schema <- config[["schemas"]][["cdm"]] %||% "main"
    } else {
      config <- ETLDelphi::default_etl_config()
      cdm_schema <- config[["schemas"]][["cdm"]] %||% "main"
    }
  }

  # Create results schema so DQD can write there
  if (create_results_schema) {
    con <- DBI::dbConnect(duckdb::duckdb(), db_path)
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    schemas <- DBI::dbGetQuery(con, "SELECT schema_name FROM duckdb_schemas()")$schema_name
    if (!results_schema %in% schemas) {
      DBI::dbExecute(con, DBI::SQL(paste0('CREATE SCHEMA "', results_schema, '"')))
      if (verbose) message("Created schema: ", results_schema)
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
    message("Running DataQualityDashboard on CDM in schema: ", cdm_schema)
    message("Results schema: ", results_schema)
    message("Output folder: ", output_folder)
  }

  DataQualityDashboard::executeDqChecks(
    connectionDetails = connection_details,
    cdmDatabaseSchema = cdm_schema,
    resultsDatabaseSchema = results_schema,
    cdmSourceName = cdm_source_name,
    cdmVersion = cdm_version,
    numThreads = num_threads,
    sqlOnly = FALSE,
    sqlOnlyUnionCount = 1L,
    sqlOnlyIncrementalInsert = FALSE,
    outputFolder = output_folder,
    outputFile = output_file,
    verboseMode = verbose,
    writeToTable = write_to_table,
    writeTableName = write_table_name,
    writeToCsv = FALSE,
    csvFile = "",
    checkLevels = check_levels,
    checkSeverity = check_severity,
    tablesToExclude = tables_to_exclude,
    checkNames = c()
  )

  json_path <- file.path(output_folder, cdm_source_name, output_file)
  if (verbose && file.exists(json_path)) {
    message("Results written to: ", json_path)
    message("View in Shiny: DataQualityDashboard::viewDqDashboard(\"", json_path, "\")")
  }
}

`%||%` <- function(x, y) if (is.null(x)) y else x

# Allow running as script: Rscript extras/run_data_quality_dashboard.R [db_path]
if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  db_path <- if (length(args)) args[1] else NULL
  run_data_quality_dashboard(db_path = db_path)
}
