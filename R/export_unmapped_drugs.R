#' Export unmapped drugs (name, NDC, record counts) for custom mapping
#'
#' Writes drugs that did not map to an OMOP concept (drug_concept_id = 0 in
#' stg.map_drug_order) to a CSV with drug name, NDC (normalized and raw sample),
#' and record counts. Use for custom mapping (e.g. ingredient-level) when NDC
#' is missing, wildcard, or ambiguous. Run after ETL.
#'
#' @param con DBI connection to the DuckDB database.
#' @param output_path Path to the output CSV file. Default: \code{unmapped_drugs.csv} in current directory.
#' @param config Optional list with \code{schemas$stg}; if NULL, uses \code{stg}.
#' @return Invisible path to the written file.
#' @export
export_unmapped_drugs <- function(con, output_path = "unmapped_drugs.csv", config = NULL) {
  stg <- resolve_schema(config, "stg")

  if (!table_exists(con, stg, "map_drug_order")) {
    stop("stg.map_drug_order does not exist. Run the ETL first.")
  }
  if (!table_exists(con, stg, "medication_orders")) {
    stop("stg.medication_orders does not exist. Run the ETL first.")
  }

  # Unmapped drugs from map_drug_order (drug_concept_id = 0) with counts and sample NDC from medication_orders
  df <- DBI::dbGetQuery(con, glue::glue(
    'WITH order_counts AS (',
    '  SELECT ',
    '    CASE WHEN drug_ndc_normalized IS NULL OR TRIM(COALESCE(drug_ndc_normalized, \'\')) = \'\' THEN NULL ELSE drug_ndc_normalized END AS drug_ndc_normalized, ',
    '    drug_name, ',
    '    COUNT(*) AS n_occurrences, ',
    '    MAX(drug_ndc_raw) AS drug_ndc_raw ',
    '  FROM "{stg}".medication_orders ',
    '  GROUP BY 1, 2',
    ') ',
    'SELECT ',
    '  m.drug_name, ',
    '  m.drug_ndc_normalized, ',
    '  o.drug_ndc_raw, ',
    '  o.n_occurrences, ',
    '  NULL AS target_concept_id ',
    'FROM "{stg}".map_drug_order m ',
    'JOIN order_counts o ON (m.drug_ndc_normalized IS NOT DISTINCT FROM o.drug_ndc_normalized) AND m.drug_name = o.drug_name ',
    'WHERE m.drug_concept_id = 0 ',
    'ORDER BY o.n_occurrences DESC'
  ))
  df$target_concept_id <- ""

  write.csv(df, output_path, row.names = FALSE)
  message("Wrote ", nrow(df), " unmapped drug (name/NDC) combinations to ", output_path)

  invisible(output_path)
}
