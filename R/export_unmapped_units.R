#' Export unmapped units with their measurements to CSV for manual mapping
#'
#' Writes unmapped unit + measurement combinations (unit_concept_id = 0 in
#' cdm.measurement) to a CSV so you can see which measurements use each unit,
#' which helps identify the correct UCUM concept. Run after ETL.
#'
#' @param con DBI connection to the DuckDB database.
#' @param output_path Path to the output CSV file. Default: \code{unmapped_units.csv} in current directory.
#' @param config Optional list with \code{schemas$stg} and \code{schemas$cdm}; if NULL, uses \code{stg} and \code{main}.
#' @return Invisible path to the written file.
#' @export
export_unmapped_units <- function(con, output_path = "unmapped_units.csv", config = NULL) {
  stg <- resolve_schema(config, "stg")
  cdm <- resolve_schema(config, "cdm")

  if (!table_exists(con, stg, "map_units")) {
    stop("stg.map_units does not exist. Run the ETL first.")
  }
  if (!table_exists(con, cdm, "measurement")) {
    stop("cdm.measurement does not exist. Run the ETL first.")
  }

  # Unmapped unit + measurement combinations from cdm.measurement (unit_concept_id = 0)
  df <- DBI::dbGetQuery(con, glue::glue(
    'SELECT unit_source_value AS source_value, ',
    '       measurement_source_value AS measurement_source_value, ',
    '       COUNT(*) AS n_occurrences, ',
    '       NULL AS target_concept_id ',
    'FROM "{cdm}".measurement ',
    'WHERE unit_concept_id = 0 ',
    '  AND unit_source_value IS NOT NULL AND TRIM(unit_source_value) <> \'\' ',
    'GROUP BY unit_source_value, measurement_source_value ',
    'ORDER BY unit_source_value, measurement_source_value'
  ))
  df <- df[!is.na(df$source_value) & nzchar(trimws(df$source_value)), , drop = FALSE]
  df$target_concept_id <- ""

  utils::write.csv(df, output_path, row.names = FALSE)
  message("Wrote ", nrow(df), " unmapped unit-measurement combinations to ", output_path)

  invisible(output_path)
}
