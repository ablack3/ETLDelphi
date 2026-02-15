#' Export unmapped categorical measurement values for custom mapping
#'
#' Writes unmapped value + measurement combinations (value_as_concept_id = 0 in
#' cdm.measurement where value_source_value is present) to a CSV so you can map
#' categorical result strings (e.g. "Positive", "Equivocal") to OMOP Meas Value
#' concepts. Run after ETL.
#'
#' @param con DBI connection to the DuckDB database.
#' @param output_path Path to the output CSV file. Default: \code{unmapped_measurement_values.csv} in current directory.
#' @param config Optional list with \code{schemas$cdm}; if NULL, uses \code{main}.
#' @return Invisible path to the written file.
#' @export
export_unmapped_measurement_values <- function(con, output_path = "unmapped_measurement_values.csv", config = NULL) {
  cdm <- if (is.null(config) || is.null(config[["schemas"]]) || is.null(config[["schemas"]][["cdm"]])) "main" else config[["schemas"]][["cdm"]]

  if (!ETLDelphi:::table_exists(con, cdm, "measurement")) {
    stop("cdm.measurement does not exist. Run the ETL first.")
  }

  # Unmapped categorical value + measurement combinations (value_as_concept_id = 0, value_source_value present)
  df <- DBI::dbGetQuery(con, glue::glue(
    'SELECT value_source_value AS source_value, ',
    '       measurement_source_value AS measurement_source_value, ',
    '       COUNT(*) AS n_occurrences, ',
    '       NULL AS target_concept_id ',
    'FROM "{cdm}".measurement ',
    'WHERE value_as_concept_id = 0 ',
    '  AND value_source_value IS NOT NULL AND TRIM(value_source_value) <> \'\' ',
    'GROUP BY value_source_value, measurement_source_value ',
    'ORDER BY value_source_value, measurement_source_value'
  ))
  df <- df[!is.na(df$source_value) & nzchar(trimws(df$source_value)), , drop = FALSE]
  df$target_concept_id <- ""

  write.csv(df, output_path, row.names = FALSE)
  message("Wrote ", nrow(df), " unmapped measurement value-measurement combinations to ", output_path)

  invisible(output_path)
}
