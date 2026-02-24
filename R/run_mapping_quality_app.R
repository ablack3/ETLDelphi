#' Launch the interactive mapping quality Shiny app
#'
#' Opens the mapping quality dashboard as an interactive Shiny application.
#' Run after \code{\link{analyze_mapping_quality}} has written results CSV
#' files into \code{results_dir}.
#'
#' For a lightweight static HTML dashboard (no Shiny dependency), see
#' \code{\link{run_mapping_quality_site}}.
#'
#' @param results_dir Path to the folder containing mapping quality CSV files
#'   (written by \code{analyze_mapping_quality()}).
#'   Defaults to \code{"mapping_quality_results"}.
#' @param port Port number for the Shiny server. Default \code{NULL} lets Shiny
#'   pick an available port.
#' @param launch_browser If \code{TRUE} (default), open the app in the system
#'   browser.
#' @return Invisible \code{NULL} (blocks until the app is stopped).
#' @export
run_mapping_quality_app <- function(results_dir = "mapping_quality_results",
                                    port = NULL,
                                    launch_browser = TRUE) {
  if (!requireNamespace("shiny", quietly = TRUE)) {
    stop(
      "Package 'shiny' is required to run the interactive dashboard. ",
      "Install it with: install.packages('shiny')\n",
      "Alternatively, use run_mapping_quality_site() for a static HTML dashboard.",
      call. = FALSE
    )
  }

  # ── Resolve results_dir ──────────────────────────────────────────
  if (is.null(results_dir) || !nzchar(trimws(results_dir))) {
    results_dir <- "mapping_quality_results"
  }
  results_dir <- normalizePath(results_dir, mustWork = FALSE, winslash = "/")

  if (!dir.exists(results_dir)) {
    stop(
      "Results directory not found: '", results_dir, "'. ",
      "Run analyze_mapping_quality() first.",
      call. = FALSE
    )
  }

  # ── Set option that the Shiny app reads ──────────────────────────

  options(etldelphi.mapping_quality_results_dir = results_dir)

  # ── Locate the bundled Shiny app ─────────────────────────────────
  app_dir <- system.file("shiny", "mapping_quality", package = "ETLDelphi")
  if (!nzchar(app_dir) || !dir.exists(app_dir)) {
    stop(
      "Shiny app not found. ",
      "Ensure inst/shiny/mapping_quality/app.R exists in the ETLDelphi package.",
      call. = FALSE
    )
  }

  cli::cli_alert_success("Launching Shiny mapping quality dashboard")
  cli::cli_alert_info("Results directory: {.path {results_dir}}")

  shiny::runApp(
    appDir = app_dir,
    port = port,
    launch.browser = launch_browser
  )

  invisible(NULL)
}
