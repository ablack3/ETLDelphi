#' Default ETL config (schemas, type concept IDs, paths). Used when \code{config} is NULL.
#'
#' Type concept IDs are substituted into SQL via placeholders (e.g. \code{{visit_type_concept_id}}).
#' For the Data Quality Dashboard (DQD) to pass type-concept and domain checks, every ID must exist
#' in your \code{CONCEPT} table and be a standard, valid concept in the correct domain (e.g. Type Concept
#' for \code{*_type_concept_id}, Visit for \code{visit_concept_id}). If DQD fails on NOTE_TYPE_CONCEPT_ID,
#' VISIT_TYPE_CONCEPT_ID, DRUG_TYPE_CONCEPT_ID, PROCEDURE_TYPE_CONCEPT_ID, or OBSERVATION_TYPE_CONCEPT_ID,
#' override \code{type_concept_ids} in your config (or YAML) with concept_id values from your vocabulary.
#' Implausible measurement unit checks are often acceptable for real-world data.
#'
#' @return List suitable for \code{run_etl(con, config = ...)}.
#' @export
default_etl_config <- function() {
  list(
    schemas = list(src = "src", stg = "stg", cdm = "main"),
    concept_id_unknown = 0L,
    date_formats = c(
      "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
      "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d-%b-%Y", "%Y/%m/%d"
    ),
    type_concept_ids = list(
      visit_type_concept_id = 32827L,
      default_visit_concept_id = 9202L,
      condition_type_concept_id = 32840L,
      drug_type_orders = 32838L,
      drug_type_dispensed = 32825L,
      drug_type_immunization = 32818L,
      measurement_type_vitals = 32836L,
      measurement_type_labs = 32856L,
      observation_type_allergy = 32817L,
      note_type_concept_id = 32831L,
      note_class_concept_id = 3000735L,
      language_concept_id = 4180186L,
      encoding_concept_id = 32678L,
      procedure_type_concept_id = 32833L,
      period_type_concept_id = 32821L,
      death_type_concept_id = 32817L
    ),
    prefer_fulfillment = FALSE,
    drug_name_mapping_path = NULL,
    custom_mapping_path = NULL,
    custom_ndc_mapping_path = NULL
  )
}

#' Run the Delphi to OMOP CDM ETL
#'
#' Executes all ETL SQL scripts in order: admin -> stage -> keys -> vocab ->
#' cdm_core -> cdm_clinical -> cdm_derived -> qc.
#'
#' @param con DBI connection to DuckDB (source and CDM must exist in DB).
#' @param config_path Path to config YAML; if NULL, uses \code{default_etl_config()}.
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
    if (!is.null(config_path) && nzchar(trimws(config_path))) {
      if (!file.exists(config_path)) stop("Config file not found: ", config_path)
      config <- yaml::read_yaml(config_path)
    } else {
      config <- default_etl_config()
    }
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
