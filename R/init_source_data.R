#' Initialize src schema and load Delphi CSV files
#'
#' Drops the src schema if it exists, recreates it, and loads expected Delphi
#' CSV files from a directory into src tables.
#'
#' @param con A DBI connection to a DuckDB database.
#' @param delphi_source_dir Character. Directory containing Delphi source CSV
#'   files (e.g. enrollment.csv, encounter.csv).
#' @return Invisibly returns \code{con}.
#' @export
init_source_data <- function(con, delphi_source_dir) {
  delphi_source_dir <- normalizePath(delphi_source_dir, winslash = "/")
  if (!dir.exists(delphi_source_dir)) {
    stop("Delphi source directory not found: ", delphi_source_dir)
  }

  DBI::dbExecute(con, "DROP SCHEMA IF EXISTS src CASCADE")
  DBI::dbExecute(con, "CREATE SCHEMA src")

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
      "CREATE OR REPLACE TABLE src.", tbl, " AS SELECT * FROM read_csv('",
      path_sql, "', header = true, sample_size = -1)"
    )
    DBI::dbExecute(con, sql)
    message("Loaded src.", tbl)
  }

  invisible(con)
}
