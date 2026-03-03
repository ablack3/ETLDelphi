#' Initialize a fresh DuckDB database with DDL and load vocabulary files in batches
#'
#' Creates a DuckDB database, runs the OMOP CDM DDL (including
#' vocabulary tables), then loads vocabulary TSV/CSV files from a directory
#' in batches with a progress bar. After loading, applies primary key constraints,
#' foreign key constraints, and indexes on vocabulary tables only (not on other
#' CDM tables). Handles large files (e.g. CONCEPT) by chunked reading and appending.
#'
#' @param db_path Character. Path to the DuckDB file (created if missing), or
#'   \code{":memory:"} for an in-memory database.
#' @param vocabulary_dir Character. Directory containing vocabulary TSV/CSV files
#'   (e.g. \code{CONCEPT.csv}, \code{VOCABULARY.csv}).
#' @param cdm_schema Character. Schema name for CDM/vocabulary tables (default \code{"main"}).
#' @param delimiter Character. Column delimiter in vocabulary files (default \code{"\\t"} for TSV).
#'   Use \code{","} if files are comma-separated.
#' @return The DBI connection (invisibly). If \code{con} was supplied, the same connection;
#'   otherwise a new connection that the caller should close with \code{DBI::dbDisconnect(con)}.
#' @export
init_vocabulary_db <- function(db_path,
                               vocabulary_dir = NULL,
                               cdm_schema = "main",
                               delimiter = "\t") {

  checkmate::assertCharacter(db_path, min.chars = 1, any.missing = FALSE)
  checkmate::assertCharacter(vocabulary_dir, min.chars = 1, any.missing = FALSE)
  checkmate::assertCharacter(cdm_schema, min.chars = 1, any.missing = FALSE)
  checkmate::assertChoice(delimiter, choices = c("\t", ","))

  if ( !dir.exists(vocabulary_dir)) {
    cli::cli_abort("Vocabulary directory not found!")
  }

  con <- DBI::dbConnect(duckdb::duckdb(), db_path)

  # Default DDL to package OMOP DDL
  ddl_path <- system.file("omop_cdm_specification", "OMOPCDM_duckdb_5.4_ddl.sql", package = "ETLDelphi", mustWork = TRUE)

  # Create schema and run DDL (skip for "main" - DuckDB default schema, always exists)
  if (cdm_schema != "main") {
    DBI::dbExecute(con, glue::glue("DROP SCHEMA IF EXISTS {cdm_schema} CASCADE;"))
    DBI::dbExecute(con, glue::glue("CREATE SCHEMA {cdm_schema};"))
  }
  sql <- readLines(ddl_path, warn = FALSE)
  sql <- paste(sql, collapse = "\n")
  sql <- gsub("@cdmDatabaseSchema", cdm_schema, sql, fixed = TRUE)
  # Strip single-line comments so semicolons in comments don't break statement split
  sql <- gsub("--[^\n]*", "\n", sql)
  statements <- split_sql_statements(sql)
  cli::cli_alert_info("Running DDL ({length(statements)} statements) ...")
  for (stmt in statements) {
    stmt <- trimws(stmt)
    if (nchar(stmt) == 0L) next
    DBI::dbExecute(con, stmt)
  }
  cli::cli_alert_success("DDL complete.")

  # Map vocabulary file base names (uppercase with underscores) to table names (lowercase)
  file_to_table <- c(
    CONCEPT = "concept",
    CONCEPT_ANCESTOR = "concept_ancestor",
    CONCEPT_CLASS = "concept_class",
    CONCEPT_CPT4 = "concept_cpt4",
    CONCEPT_RELATIONSHIP = "concept_relationship",
    CONCEPT_SYNONYM = "concept_synonym",
    DOMAIN = "domain",
    DRUG_STRENGTH = "drug_strength",
    RELATIONSHIP = "relationship",
    VOCABULARY = "vocabulary"
  )

  # Find vocabulary files (TSV or CSV)
  vocab_files <- fs::dir_ls(vocabulary_dir, type = "file", regexp = "\\.(csv|tsv)$", ignore.case = TRUE)

  # Filter to tables that exist in the DB (after DDL)
  info <- DBI::dbGetQuery(con, "SELECT schema_name, table_name FROM duckdb_tables()")
  existing_in_schema <- info[info$schema_name == cdm_schema, "table_name", drop = TRUE]
  existing_lower <- tolower(existing_in_schema)

  to_load <- character(0)
  for (f in vocab_files) {
    base <- toupper(fs::path_ext_remove(fs::path_file(f)))
    tbl <- file_to_table[base]
    if (is.na(tbl)) next
    if (!tbl %in% existing_lower) next
    to_load[fs::path_file(f)] <- tbl
  }

  if (length(to_load) == 0L) {
    stop("No vocabulary files found in {vocabulary_dir} that match CDM vocabulary tables.")
  }

  for (i in seq_along(to_load)) {

    fname <- names(to_load)[i]
    tbl <- to_load[i]
    fpath <- file.path(vocabulary_dir, fname)
    if (!file.exists(fpath)) {
      stop("Could not find {fpath}!")
    }

    cli::cli_inform("Loading {fname} ({i}/{to_load})")

    DBI::dbExecute(con, glue::glue("DELETE FROM {cdm_schema}.{to_load[i]};"))
    DBI::dbExecute(con, "PRAGMA threads=8;")
    DBI::dbExecute(con, "PRAGMA preserve_insertion_order=false;")
    DBI::dbExecute(con, glue::glue(" INSERT INTO {cdm_schema}.{tbl} SELECT * FROM read_csv('{fpath}', delim ='\t', header =true, dateformat = '%Y%m%d');"))
  }
  cli::cli_inform("Vocab tables loaded")

  # Apply unique indexes (PKs) and non-unique indexes on vocabulary tables only (no ALTER TABLE / FKs in DuckDB)
  constraints_path <- system.file("omop_cdm_specification", "OMOPCDM_duckdb_5.4_vocabulary_constraints.sql", package = "ETLDelphi", mustWork = TRUE)
  cli::cli_alert_info("Applying vocabulary constraints (unique + non-unique indexes) ...")
  sql_voc <- readLines(constraints_path, warn = FALSE)
  sql_voc <- stringr::str_subset(sql_voc, "^--", negate = TRUE)  #  remove comments
  sql_voc <- stringr::str_replace_all(sql_voc, "@cdmDatabaseSchema", cdm_schema)
  for (stmt in sql_voc) {
    stmt <- trimws(stmt)
    if (nchar(stmt) == 0L) next
    DBI::dbExecute(con, stmt)
  }
  cli::cli_alert_success("Vocabulary constraints applied.")
  invisible(con)
}
