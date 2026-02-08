#' Export entire DuckDB database to Parquet files
#'
#' Writes every table in the database to a folder of Parquet files, one file
#' per table. Tables from different schemas are named \code{<schema>_<table>.parquet}
#' to avoid collisions.
#'
#' @param con DBI connection to DuckDB.
#' @param path Character. Directory path where Parquet files will be written.
#'   Created if it does not exist.
#' @param include_system Logical. If \code{FALSE} (default), tables in
#'   \code{information_schema} and \code{pg_catalog} are skipped.
#' @return Invisible character vector of paths to the written Parquet files.
#' @export
exportToParquet <- function(con, path, include_system = FALSE) {
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }
  path <- normalizePath(path, mustWork = TRUE)

  # All tables across schemas (DuckDB information_schema)
  q <- "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
  if (!include_system) {
    q <- paste0(
      q,
      " AND table_schema NOT IN ('information_schema', 'pg_catalog')"
    )
  }
  tbls <- DBI::dbGetQuery(con, q)
  if (nrow(tbls) == 0) {
    cli::cli_warn("No tables found in the database.")
    return(invisible(character(0)))
  }

  out_files <- character(nrow(tbls))
  for (i in seq_len(nrow(tbls))) {
    schema <- tbls$table_schema[i]
    table <- tbls$table_name[i]
    safe_name <- paste0(schema, "_", table, ".parquet")
    out_path <- file.path(path, safe_name)
    # Use forward slashes in SQL so path is safe in quoted string on all platforms
    out_path_sql <- gsub("\\\\", "/", out_path)
    quoted_table <- glue::glue('"{schema}"."{table}"')
    sql <- glue::glue("COPY {quoted_table} TO '{out_path_sql}' (FORMAT PARQUET)")
    DBI::dbExecute(con, sql)
    out_files[i] <- out_path
    cli::cli_alert_success("{quoted_table} -> {safe_name}")
  }

  cli::cli_alert_info("Exported {nrow(tbls)} table(s) to {path}")
  invisible(out_files)
}
