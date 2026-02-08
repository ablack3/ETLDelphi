#' Run the Delphi to OMOP CDM ETL
#'
#' Executes all ETL SQL scripts in order: admin -> stage -> keys -> vocab ->
#' cdm_core -> cdm_clinical -> cdm_derived -> qc.
#'
#' @param con DBI connection to DuckDB (source and CDM must exist in DB).
#' @param config_path Path to config YAML; if NULL, uses package default.
#' @param config List override; if provided, config_path is ignored.
#' @param sql_dir Path to inst/sql; if NULL, uses \code{system.file("sql", package = "ETLDelphi")}.
#' @param dry_run If TRUE, only list SQL files that would run.
#' @param from_step Optional: start from this step (e.g. "40_cdm_core").
#' @param to_step Optional: stop after this step.
#' @return Invisible result from \code{run_sql_scripts}.
#' @export
run_etl <- function(con,
                    config_path = NULL,
                    config = NULL,
                    sql_dir = NULL,
                    dry_run = FALSE,
                    from_step = NULL,
                    to_step = NULL) {
  if (is.null(config)) {
    path <- config_path %||% system.file("extdata", "config.yml", package = "ETLDelphi")
    if (!file.exists(path)) stop("Config file not found: ", path)
    config <- yaml::read_yaml(path)
  }

  sql_dir <- sql_dir %||% system.file("sql", package = "ETLDelphi")
  if (!dir.exists(sql_dir)) {
    stop("SQL directory not found. Install package or set sql_dir. Looked at: ", sql_dir)
  }

  withr::with_options(
    list(warn = 1),
    run_sql_scripts(
      con = con,
      sql_dir = sql_dir,
      config = config,
      dry_run = dry_run,
      from_step = from_step,
      to_step = to_step
    )
  )
}

# Simple %||% for default
`%||%` <- function(x, y) if (is.null(x)) y else x
