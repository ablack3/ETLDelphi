#' Mapping Quality Results Viewer
#'
#' Shiny app to visualize outputs from analyze_mapping_quality().
#' Run from package with: ETLDelphi::run_mapping_quality_app()
#' Or run this file with shiny::runApp() after setting the results directory.

library(shiny)
library(DT)

# Default results directory: use mapping_quality_results in current working directory
default_results_dir <- "mapping_quality_results"

# Safe CSV read: return NULL if file missing or empty
read_mq_csv <- function(path) {
  if (!file.exists(path)) return(NULL)
  out <- tryCatch(
    read.csv(path, stringsAsFactors = FALSE),
    error = function(e) NULL
  )
  if (is.null(out) || nrow(out) == 0) return(NULL)
  out
}

ui <- fluidPage(
  titlePanel("Mapping Quality Results"),
  sidebarLayout(
    sidebarPanel(
      width = 3,
      textInput(
        "results_dir",
        "Results directory",
        value = default_results_dir,
        placeholder = "mapping_quality_results"
      ),
      actionButton("load_btn", "Load results", class = "btn-primary"),
      hr(),
      conditionalPanel(
        "output.files_loaded",
        helpText("Data loaded. Use the tabs to explore.")
      ),
      conditionalPanel(
        "!output.files_loaded",
        helpText("Enter the path to the folder containing the mapping quality CSV files, then click Load results.")
      )
    ),
    mainPanel(
      width = 9,
      tabsetPanel(
        id = "tabs",
        tabPanel(
          "Summary",
          fluidRow(
            column(12, h3("Data not mapped summary")),
            column(12, tableOutput("summary_table"))
          ),
          fluidRow(
            column(12, h3("Unmapped concepts by table (overview)")),
            column(12, plotOutput("unmapped_overview_plot", height = "320px")),
            column(12, tableOutput("unmapped_summary_table"))
          )
        ),
        tabPanel(
          "Unmapped by table",
          fluidRow(
            column(12, plotOutput("unmapped_bar", height = "400px")),
            column(12, DTOutput("unmapped_table"))
          )
        ),
        tabPanel(
          "Top unmapped source values",
          fluidRow(
            column(4, uiOutput("top_domain_ui")),
            column(4, numericInput("top_n", "Show top N", value = 25, min = 5, max = 200, step = 5))
          ),
          fluidRow(
            column(12, plotOutput("top_unmapped_bar", height = "420px")),
            column(12, DTOutput("top_unmapped_table"))
          )
        ),
        tabPanel(
          "Record counts by table",
          fluidRow(
            column(12, plotOutput("record_counts_plot", height = "420px")),
            column(12, DTOutput("record_counts_table"))
          )
        ),
        tabPanel(
          "Person count comparison",
          fluidRow(column(12, tableOutput("person_count_table")))
        ),
        tabPanel(
          "One-to-many mappings",
          fluidRow(column(12, DTOutput("one_to_many_table"))),
          fluidRow(column(12, uiOutput("one_to_many_empty_msg")))
        ),
        tabPanel(
          "Many-to-one mappings",
          fluidRow(column(12, DTOutput("many_to_one_table"))),
          fluidRow(column(12, uiOutput("many_to_one_empty_msg")))
        ),
        tabPanel(
          "Reject table row counts",
          fluidRow(
            column(12, plotOutput("reject_plot", height = "400px")),
            column(12, DTOutput("reject_table"))
          )
        )
      )
    )
  )
)

server <- function(input, output, session) {
  # Reactive container for all loaded data
  data <- reactiveVal(list())

  load_results <- function() {
    dir <- trimws(input$results_dir)
    if (!nzchar(dir) || !dir.exists(dir)) {
      if (nzchar(dir)) showNotification("Directory not found.", type = "error")
      return()
    }
    data(list(
      summary = read_mq_csv(file.path(dir, "08_data_not_mapped_summary.csv")),
      unmapped = read_mq_csv(file.path(dir, "01_unmapped_concepts_by_table.csv")),
      top_unmapped = read_mq_csv(file.path(dir, "02_top_unmapped_source_values.csv")),
      record_counts = read_mq_csv(file.path(dir, "03_record_counts_by_table.csv")),
      person_count = read_mq_csv(file.path(dir, "04_person_count_comparison.csv")),
      one_to_many = read_mq_csv(file.path(dir, "05_one_to_many_mappings.csv")),
      many_to_one = read_mq_csv(file.path(dir, "06_many_to_one_mappings.csv")),
      reject = read_mq_csv(file.path(dir, "07_reject_table_row_counts.csv"))
    ))
    showNotification("Results loaded.", type = "message")
  }

  observeEvent(input$load_btn, load_results())

  # Auto-load when default directory exists at startup
  observe({
    dir <- trimws(input$results_dir)
    if (nzchar(dir) && dir.exists(dir) && length(data()) == 0) {
      load_results()
    }
  })

  output$files_loaded <- reactive({
    d <- data()
    any(vapply(d, function(x) !is.null(x) && nrow(x) > 0, logical(1)))
  })
  outputOptions(output, "files_loaded", suspendWhenHidden = FALSE)

  # Summary tab
  output$summary_table <- renderTable({
    d <- data()$summary
    if (is.null(d)) return(data.frame(Message = "No data. Load results from a directory that contains 08_data_not_mapped_summary.csv."))
    d
  }, striped = TRUE)

  output$unmapped_overview_plot <- renderPlot({
    d <- data()$unmapped
    if (is.null(d) || nrow(d) == 0) return(NULL)
    d$table_short <- sub("^[^.]+\\.", "", d$cdm_table)
    par(mar = c(6, 8, 2, 2))
    barplot(
      d$unmapped_pct,
      names.arg = d$table_short,
      horiz = TRUE,
      las = 1,
      col = ifelse(d$unmapped_pct >= 50, "#d95f02", "#7570b3"),
      main = "Unmapped % by CDM table",
      xlab = "Unmapped %"
    )
  })

  output$unmapped_summary_table <- renderTable({
    d <- data()$unmapped
    if (is.null(d)) return(NULL)
    d
  }, striped = TRUE)

  # Unmapped by table tab
  output$unmapped_bar <- renderPlot({
    d <- data()$unmapped
    if (is.null(d) || nrow(d) == 0) return(NULL)
    d$table_short <- sub("^[^.]+\\.", "", d$cdm_table)
    par(mar = c(6, 10, 2, 2))
    barplot(
      d$unmapped_count,
      names.arg = d$table_short,
      horiz = TRUE,
      las = 1,
      col = "#7570b3",
      main = "Unmapped record count by table",
      xlab = "Count"
    )
  })

  output$unmapped_table <- renderDT({
    d <- data()$unmapped
    if (is.null(d)) return(datatable(data.frame()))
    datatable(d, options = list(pageLength = 10))
  })

  # Top unmapped source values
  output$top_domain_ui <- renderUI({
    d <- data()$top_unmapped
    if (is.null(d) || !"domain" %in% names(d)) return(NULL)
    domains <- unique(d$domain)
    selectInput("top_domain", "Domain", choices = c("(all)", domains), selected = "(all)")
  })

  top_unmapped_filtered <- reactive({
    d <- data()$top_unmapped
    if (is.null(d)) return(NULL)
    if (is.null(input$top_domain) || input$top_domain == "(all)") return(d)
    d[d$domain == input$top_domain, , drop = FALSE]
  })

  output$top_unmapped_bar <- renderPlot({
    df <- top_unmapped_filtered()
    if (is.null(df) || nrow(df) == 0) return(NULL)
    n <- min(as.integer(input$top_n %||% 25L), nrow(df))
    df <- head(df, n)
    lbl <- paste0(df$source_value, " (", df$domain, ")")
    par(mar = c(5, max(8, min(20, n * 0.25)), 2, 2))
    barplot(
      df$record_count,
      names.arg = lbl,
      horiz = TRUE,
      las = 1,
      col = "#1b9e77",
      main = paste("Top", n, "unmapped source values"),
      xlab = "Record count"
    )
  })

  output$top_unmapped_table <- renderDT({
    df <- top_unmapped_filtered()
    if (is.null(df)) return(datatable(data.frame()))
    datatable(df, options = list(pageLength = 20))
  })

  # Record counts by table
  output$record_counts_plot <- renderPlot({
    d <- data()$record_counts
    if (is.null(d) || nrow(d) == 0) return(NULL)
    d$table_short <- sub("^[^.]+\\.", "", d$table_name)
    stg <- d[d$layer == "stg", , drop = FALSE]
    cdm <- d[d$layer == "cdm", , drop = FALSE]
    par(mar = c(6, 10, 2, 2))
    all_tables <- d$table_short
    cols <- ifelse(d$layer == "stg", "#d95f02", "#7570b3")
    barplot(
      d$row_count,
      names.arg = all_tables,
      horiz = TRUE,
      las = 1,
      col = cols,
      main = "Record counts by table (orange = stg, purple = cdm)",
      xlab = "Row count"
    )
    legend("bottomright", c("stg", "cdm"), fill = c("#d95f02", "#7570b3"), bty = "n")
  })

  output$record_counts_table <- renderDT({
    d <- data()$record_counts
    if (is.null(d)) return(datatable(data.frame()))
    datatable(d, options = list(pageLength = 25))
  })

  # Person count
  output$person_count_table <- renderTable({
    d <- data()$person_count
    if (is.null(d)) return(data.frame(Message = "No data."))
    d
  }, striped = TRUE)

  # One-to-many
  output$one_to_many_table <- renderDT({
    d <- data()$one_to_many
    if (is.null(d)) return(datatable(data.frame()))
    datatable(d, options = list(pageLength = 20))
  })

  output$one_to_many_empty_msg <- renderUI({
    d <- data()$one_to_many
    if (is.null(d) || nrow(d) == 0) {
      p("No one-to-many mappings found (or file empty).", style = "color: gray;")
    } else {
      NULL
    }
  })

  # Many-to-one
  output$many_to_one_table <- renderDT({
    d <- data()$many_to_one
    if (is.null(d)) return(datatable(data.frame()))
    datatable(d, options = list(pageLength = 20))
  })

  output$many_to_one_empty_msg <- renderUI({
    d <- data()$many_to_one
    if (is.null(d) || nrow(d) == 0) {
      p("No many-to-one mappings found (or file empty).", style = "color: gray;")
    } else {
      NULL
    }
  })

  # Reject tables
  output$reject_plot <- renderPlot({
    d <- data()$reject
    if (is.null(d) || nrow(d) == 0) return(NULL)
    d$table_short <- sub("^reject_", "", d$reject_table)
    par(mar = c(6, 8, 2, 2))
    barplot(
      d$row_count,
      names.arg = d$table_short,
      horiz = TRUE,
      las = 1,
      col = ifelse(d$row_count > 0, "#d95f02", "#1b9e77"),
      main = "Reject table row counts (orange = has rejects)",
      xlab = "Row count"
    )
  })

  output$reject_table <- renderDT({
    d <- data()$reject
    if (is.null(d)) return(datatable(data.frame()))
    datatable(d, options = list(pageLength = 25))
  })
}

shinyApp(ui = ui, server = server)
