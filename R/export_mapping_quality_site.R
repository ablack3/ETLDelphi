#' Export mapping quality metrics as JSON
#'
#' Runs all mapping quality queries against the DuckDB database and writes
#' a single JSON file containing all metrics. This is the data file consumed
#' by the static HTML dashboard (\code{inst/site/index.html}).
#'
#' @param con DBI connection to the DuckDB database (post-ETL).
#' @param output_path Path for the JSON output file.
#' @param config Optional ETL config list. Default: \code{NULL} (uses \code{stg}/\code{main} schemas).
#' @return Invisible path to the written JSON file.
#' @export
export_mapping_quality_json <- function(con, output_path = "mapping_quality.json", config = NULL) {
  data <- run_mapping_quality_queries(con, config)
  data$generated_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  json <- jsonlite::toJSON(data, pretty = TRUE, auto_unbox = TRUE, dataframe = "rows")
  writeLines(json, output_path)
  cli::cli_alert_success("Wrote {output_path}")

  invisible(output_path)
}


#' Export a complete static mapping quality site
#'
#' Assembles a deployable directory containing \code{index.html} and
#' \code{mapping_quality.json}. Open \code{index.html} in a browser or
#' push the directory to GitHub Pages.
#'
#' The preferred workflow is to pass \code{json_path} pointing at the
#' \code{mapping_quality.json} file already written by
#' \code{\link{analyze_mapping_quality}}. As a convenience you can still
#' pass a DBI \code{con} to query the database directly.
#'
#' @param json_path Path to an existing \code{mapping_quality.json} file
#'   (e.g. as written by \code{analyze_mapping_quality()}).
#' @param outdir Directory to write the site into. Created if missing.
#' @param con Optional DBI connection to the DuckDB database.
#'   Used only when \code{json_path} is \code{NULL}.
#' @param config Optional ETL config list. Only used with \code{con}.
#' @return Invisible path to \code{outdir}.
#' @export
export_mapping_quality_site <- function(json_path = NULL,
                                        outdir = "docs",
                                        con = NULL,
                                        config = NULL) {
  if (!dir.exists(outdir)) {
    dir.create(outdir, recursive = TRUE)
  }

  if (!is.null(json_path)) {
    # Primary path: copy pre-existing JSON
    if (!file.exists(json_path)) {
      stop("JSON file not found: ", json_path, call. = FALSE)
    }
    file.copy(json_path, file.path(outdir, "mapping_quality.json"), overwrite = TRUE)
  } else if (!is.null(con)) {
    # Convenience path: query DB directly
    export_mapping_quality_json(con, file.path(outdir, "mapping_quality.json"), config)
  } else {
    stop("Provide either json_path or con.", call. = FALSE)
  }

  # Copy static HTML
  html_src <- system.file("site", "index.html", package = "ETLDelphi")
  if (!nzchar(html_src) || !file.exists(html_src)) {
    stop("Static site template not found. Ensure inst/site/index.html exists in the ETLDelphi package.", call. = FALSE)
  }
  file.copy(html_src, file.path(outdir, "index.html"), overwrite = TRUE)

  cli::cli_alert_success("Site exported to {outdir}/")
  cli::cli_alert_info("Open {file.path(outdir, 'index.html')} in a browser, or push to GitHub Pages.")

  invisible(outdir)
}
