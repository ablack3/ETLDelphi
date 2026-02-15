#' Run Shiny app to visualize mapping quality results
#'
#' Launches the mapping quality Shiny app. Run after
#' \code{analyze_mapping_quality(con, output_dir = "mapping_quality_results")}
#' and point the app at that directory to view tables and plots.
#'
#' @param results_dir Optional path to the folder containing mapping quality CSV
#'   files (e.g. \code{01_unmapped_concepts_by_table.csv}, \code{02_top_unmapped_source_values.csv}, etc.).
#'   If provided, the app opens with this path pre-filled and will auto-load when the directory exists.
#'   If \code{NULL}, the app uses \code{"mapping_quality_results"} as the default path.
#' @return Invisible NULL (app runs until the user closes it).
#' @export
run_mapping_quality_app <- function(results_dir = NULL) {
  app_dir <- system.file("shiny", "mapping_quality", package = "ETLDelphi")
  if (!nzchar(app_dir) || !dir.exists(app_dir)) {
    stop("Shiny app not found. Install ETLDelphi and ensure inst/shiny/mapping_quality exists.")
  }
  if (!is.null(results_dir)) {
    path <- path.expand(trimws(results_dir))
    if (nzchar(path)) {
      # Use absolute path so the app loads files correctly regardless of its working directory
      if (dir.exists(path)) {
        path <- normalizePath(path, winslash = "/")
      } else {
        # Relative path: resolve against current working directory so app gets an absolute path
        if (!startsWith(path, "/") && !grepl("^[A-Za-z]:", path)) {
          path <- normalizePath(file.path(getwd(), path), mustWork = FALSE, winslash = "/")
        }
      }
    }
    old_opt <- getOption("etldelphi.mapping_quality_results_dir")
    options(etldelphi.mapping_quality_results_dir = path)
    on.exit({
      if (is.null(old_opt)) {
        options(etldelphi.mapping_quality_results_dir = NULL)
      } else {
        options(etldelphi.mapping_quality_results_dir = old_opt)
      }
    }, add = TRUE)
  }
  shiny::runApp(app_dir, display.mode = "normal")
}
