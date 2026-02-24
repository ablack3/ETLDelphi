#' Export CDM or entire DuckDB database to Parquet files
#'
#' @description
#' **When `x` is a CDM reference object** (from `CDMConnector::cdmFromCon()`):
#' Writes each CDM table to a Parquet file in the given folder. Files are named
#' `{table_name}.parquet`. The folder is created if it does not exist. Default
#' folder is `cdmName(cdm)` in the current working directory.
#'
#' **When `x` is a DBI connection:** Writes every table in the database to a
#' folder of Parquet files, one file per table. Tables from different schemas
#' are named `{schema}_{table}.parquet` to avoid collisions.
#'
#' @param x A **cdm_reference** object (from CDMConnector) or a **DBI connection**
#'   (e.g. DuckDB).
#' @param path Character. Directory path where Parquet files will be written.
#'   For a cdm object, created if it does not exist; defaults to
#'   `file.path(getwd(), cdmName(cdm))`. For a connection, required.
#' @param ... Passed to the method. For the connection method: `include_system`.
#' @return Invisible character vector of paths to the written Parquet files.
#'
#' @export
exportToParquet <- function(x, path = NULL, ...) {
  UseMethod("exportToParquet")
}

#' @rdname exportToParquet
#' @param include_system Logical. If `FALSE` (default), tables in
#'   `information_schema` and `pg_catalog` are skipped. Only used when `x` is a
#'   DBI connection.
#' @export
exportToParquet.DBIConnection <- function(x, path, include_system = FALSE, ...) {
  if (missing(path) || is.null(path)) {
    cli::cli_abort("For a database connection, {.arg path} is required.")
  }
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
  tbls <- DBI::dbGetQuery(x, q)
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
    DBI::dbExecute(x, sql)
    out_files[i] <- out_path
    cli::cli_alert_success("{quoted_table} -> {safe_name}")
  }

  cli::cli_alert_info("Exported {nrow(tbls)} table(s) to {path}")
  invisible(out_files)
}

#' @rdname exportToParquet
#' @export
exportToParquet.cdm_reference <- function(x, path = NULL, ...) {
  if (!requireNamespace("CDMConnector", quietly = TRUE)) {
    stop("Package \"CDMConnector\" is required for exporting a cdm object. Install it with install.packages(\"CDMConnector\").")
  }
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package \"arrow\" is required for exporting a cdm object to Parquet. Install it with install.packages(\"arrow\").")
  }
  if (is.null(path)) {
    path <- file.path(getwd(), CDMConnector::cdmName(x))
  }
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }
  path <- normalizePath(path, mustWork = TRUE)

  tbl_names <- names(x)
  if (length(tbl_names) == 0) {
    cli::cli_warn("CDM object has no tables.")
    return(invisible(character(0)))
  }

  out_files <- character(length(tbl_names))
  for (i in seq_along(tbl_names)) {
    nm <- tbl_names[i]
    out_path <- file.path(path, paste0(nm, ".parquet"))
    df <- dplyr::collect(x[[nm]])
    arrow::write_parquet(df, out_path)
    out_files[i] <- out_path
    cli::cli_alert_success("{nm} -> {nm}.parquet")
  }

  cli::cli_alert_info("Exported {length(tbl_names)} table(s) to {path}")
  invisible(out_files)
}
