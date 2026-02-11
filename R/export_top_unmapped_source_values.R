#' Export top unmapped source values to CSV
#'
#' Queries each CDM clinical table for source values where concept_id = 0,
#' aggregates by source_value and record count, and writes to a CSV with
#' \code{source_value}, \code{record_count}, \code{source_table}, and \code{domain}.
#' Domain is inferred from the CDM table (condition, drug, measurement, procedure, observation).
#'
#' @param con DBI connection to the DuckDB database.
#' @param output_path Path to the output CSV file. Default: \code{mapping_quality_results/top_unmapped_source_values.csv}.
#' @param limit Maximum number of rows per domain. Default 500.
#' @param config Optional list with \code{schemas$cdm}; if NULL, uses \code{cdm}.
#' @param template_for_custom_mapping If TRUE, adds empty \code{concept_id} column for use as custom mapping CSV.
#' @return Invisible path to the written file.
#' @export
export_top_unmapped_source_values <- function(con,
                                              output_path = "mapping_quality_results/top_unmapped_source_values.csv",
                                              limit = 500L,
                                              config = NULL,
                                              template_for_custom_mapping = FALSE) {
  cdm <- if (is.null(config) || is.null(config[["schemas"]]) || is.null(config[["schemas"]][["cdm"]])) "cdm" else config[["schemas"]][["cdm"]]

  queries <- list(
    condition_occurrence = list(
      sql = glue::glue(
        "SELECT condition_source_value AS source_value, COUNT(*) AS record_count ",
        "FROM \"{cdm}\".condition_occurrence WHERE condition_concept_id = 0 AND condition_source_value IS NOT NULL ",
        "GROUP BY condition_source_value ORDER BY record_count DESC LIMIT {limit}"
      ),
      domain = "condition"
    ),
    drug_exposure = list(
      sql = glue::glue(
        "SELECT drug_source_value AS source_value, COUNT(*) AS record_count ",
        "FROM \"{cdm}\".drug_exposure WHERE drug_concept_id = 0 AND drug_source_value IS NOT NULL ",
        "GROUP BY drug_source_value ORDER BY record_count DESC LIMIT {limit}"
      ),
      domain = "drug"
    ),
    measurement = list(
      sql = glue::glue(
        "SELECT measurement_source_value AS source_value, COUNT(*) AS record_count ",
        "FROM \"{cdm}\".measurement WHERE measurement_concept_id = 0 AND measurement_source_value IS NOT NULL ",
        "GROUP BY measurement_source_value ORDER BY record_count DESC LIMIT {limit}"
      ),
      domain = "measurement"
    ),
    procedure_occurrence = list(
      sql = glue::glue(
        "SELECT procedure_source_value AS source_value, COUNT(*) AS record_count ",
        "FROM \"{cdm}\".procedure_occurrence WHERE procedure_concept_id = 0 AND procedure_source_value IS NOT NULL ",
        "GROUP BY procedure_source_value ORDER BY record_count DESC LIMIT {limit}"
      ),
      domain = "procedure"
    ),
    observation = list(
      sql = glue::glue(
        "SELECT observation_source_value AS source_value, COUNT(*) AS record_count ",
        "FROM \"{cdm}\".observation WHERE observation_concept_id = 0 AND observation_source_value IS NOT NULL ",
        "GROUP BY observation_source_value ORDER BY record_count DESC LIMIT {limit}"
      ),
      domain = "observation"
    )
  )

  out <- list()
  for (tbl in names(queries)) {
    q <- queries[[tbl]]
    if (!ETLDelphi:::table_exists(con, cdm, tbl)) next
    res <- tryCatch({
      df <- DBI::dbGetQuery(con, q$sql)
      if (nrow(df) > 0) {
        df$source_table <- paste0("cdm.", tbl)
        df$domain <- q$domain
        df[, c("source_value", "record_count", "source_table", "domain")]
      } else {
        NULL
      }
    }, error = function(e) NULL)
    if (!is.null(res)) out[[length(out) + 1L]] <- res
  }

  df <- if (length(out) > 0) do.call(rbind, out) else data.frame(
    source_value = character(), record_count = integer(),
    source_table = character(), domain = character(), stringsAsFactors = FALSE
  )

  if (nrow(df) > 0) {
    df <- df[order(-df$record_count), , drop = FALSE]
  }

  if (template_for_custom_mapping && nrow(df) > 0) {
    df$concept_id <- NA_integer_
    df <- df[, c("source_value", "domain", "concept_id", "record_count", "source_table", drop = FALSE)]
  }

  out_dir <- dirname(output_path)
  if (nzchar(out_dir) && !dir.exists(out_dir)) {
    dir.create(out_dir, recursive = TRUE)
  }
  write.csv(df, output_path, row.names = FALSE)
  message("Wrote ", nrow(df), " unmapped source values to ", output_path)

  invisible(output_path)
}
