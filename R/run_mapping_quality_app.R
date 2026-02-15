#' Run Shiny app to visualize mapping quality results
#'
#' Launches the mapping quality Shiny app. Run after
#' \code{analyze_mapping_quality(con, output_dir = "mapping_quality_results")}
#' and point the app at that directory to view tables and plots.
#'
#' @return Invisible NULL (app runs until the user closes it).
#' @export
run_mapping_quality_app <- function() {
  app_dir <- system.file("shiny", "mapping_quality", package = "ETLDelphi")
  if (!nzchar(app_dir) || !dir.exists(app_dir)) {
    stop("Shiny app not found. Install ETLDelphi and ensure inst/shiny/mapping_quality exists.")
  }
  shiny::runApp(app_dir, display.mode = "normal")
}
