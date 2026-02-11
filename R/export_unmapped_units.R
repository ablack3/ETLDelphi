#' Export unmapped units to CSV for manual mapping
#'
#' Writes unmapped units (unit_concept_id = 0 in stg.map_units) to a CSV file
#' with columns \code{source_value} and \code{target_concept_id} (empty for you to fill).
#' Run after ETL so stg.map_units is populated.
#'
#' @param con DBI connection to the DuckDB database.
#' @param output_path Path to the output CSV file. Default: \code{unmapped_units.csv} in current directory.
#' @param config Optional list with \code{schemas$stg}; if NULL, uses \code{stg}.
#' @return Invisible path to the written file.
#' @export
export_unmapped_units <- function(con, output_path = "unmapped_units.csv", config = NULL) {
  stg <- if (is.null(config) || is.null(config[["schemas"]]) || is.null(config[["schemas"]][["stg"]])) "stg" else config[["schemas"]][["stg"]]

  if (!ETLDelphi:::table_exists(con, stg, "map_units")) {
    stop("stg.map_units does not exist. Run the ETL first.")
  }

  df <- DBI::dbGetQuery(con, glue::glue(
    'SELECT unit_source_value AS source_value, NULL AS target_concept_id ',
    'FROM "{stg}".map_units WHERE unit_concept_id = 0 ORDER BY source_value'
  ))
  df <- df[!is.na(df$source_value) & nzchar(trimws(df$source_value)), , drop = FALSE]
  df$target_concept_id <- ""

  write.csv(df, output_path, row.names = FALSE)
  message("Wrote ", nrow(df), " unmapped units to ", output_path)

  invisible(output_path)
}
