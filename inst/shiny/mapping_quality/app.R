#' Mapping Quality Dashboard
#'
#' Shiny app to visualize outputs from analyze_mapping_quality().
#' Run from package with: ETLDelphi::run_mapping_quality_app()

library(shiny)
library(bslib)
library(DT)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
default_results_dir <- getOption("etldelphi.mapping_quality_results_dir", "mapping_quality_results")

read_mq_csv <- function(path) {
  if (!file.exists(path)) return(NULL)
  out <- tryCatch(read.csv(path, stringsAsFactors = FALSE), error = function(e) NULL)
  if (is.null(out) || nrow(out) == 0) return(NULL)
  out
}

fmt_num <- function(x) formatC(x, format = "d", big.mark = ",")
fmt_pct <- function(x) paste0(round(x, 1), "%")

# Color helpers
severity_color <- function(pct) {
  if (is.na(pct)) return("secondary")
  if (pct >= 90) return("success")
  if (pct >= 70) return("primary")
  if (pct >= 50) return("warning")
  "danger"
}

severity_hex <- function(pct) {
  if (is.na(pct)) return("#6c757d")
  if (pct >= 90) return("#198754")
  if (pct >= 70) return("#0d6efd")
  if (pct >= 50) return("#fd7e14")
  "#dc3545"
}

# DT table factory
make_dt <- function(df, page_len = 15, ...) {
  if (is.null(df) || nrow(df) == 0) return(datatable(data.frame(), options = list(dom = "t")))
  datatable(
    df,
    rownames = FALSE,
    class = "compact stripe hover",
    options = list(
      pageLength = page_len,
      scrollX = TRUE,
      autoWidth = TRUE,
      language = list(emptyTable = "No data available")
    ),
    ...
  )
}

# Styled base R bar plot
styled_barplot <- function(values, labels, title = "", xlab = "", col = "#0d6efd",
                           highlight_fn = NULL, ...) {
  if (length(values) == 0) return(invisible(NULL))
  cols <- if (!is.null(highlight_fn)) vapply(values, highlight_fn, character(1)) else rep(col, length(values))
  left_margin <- max(8, min(18, max(nchar(labels), na.rm = TRUE) * 0.55))
  op <- par(mar = c(4, left_margin, 2.5, 1.5), family = "sans", bg = "white")
  on.exit(par(op))
  bp <- barplot(
    rev(values),
    names.arg = rev(labels),
    horiz = TRUE,
    las = 1,
    col = rev(cols),
    border = NA,
    main = title,
    xlab = xlab,
    cex.names = 0.85,
    cex.main = 1.1,
    col.main = "#212529",
    ...
  )
  invisible(bp)
}

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
ui <- page_navbar(
  title = "ETL Mapping Quality",
  theme = bs_theme(
    version = 5,
    bootswatch = "flatly",
    "navbar-bg" = "#2c3e50",
    base_font = font_google("Inter"),
    heading_font = font_google("Inter"),
    font_scale = 0.95
  ),
  fillable = FALSE,

  # --- Settings bar below navbar ---
  header = tags$div(
    class = "container-fluid py-2",
    style = "background: #f8f9fa; border-bottom: 1px solid #dee2e6;",
    tags$div(
      class = "row align-items-center gx-2",
      tags$div(
        class = "col-auto",
        tags$span(class = "text-muted small", "Results directory:")
      ),
      tags$div(
        class = "col",
        style = "max-width: 600px;",
        textInput("results_dir", label = NULL, value = default_results_dir,
                  placeholder = "mapping_quality_results", width = "100%")
      ),
      tags$div(
        class = "col-auto",
        actionButton("load_btn", "Load", class = "btn-sm btn-primary")
      ),
      tags$div(
        class = "col-auto",
        uiOutput("load_status_badge")
      )
    )
  ),

  # =====================================================================
  # Dashboard tab
  # =====================================================================
  nav_panel(
    title = "Dashboard",
    icon = icon("gauge-high"),

    # KPI row
    layout_columns(
      col_widths = c(3, 3, 3, 3),
      uiOutput("kpi_overall_coverage"),
      uiOutput("kpi_total_records"),
      uiOutput("kpi_unmapped_records"),
      uiOutput("kpi_rejected_records")
    ),

    # Domain coverage cards
    tags$h5("Coverage by Domain", class = "mt-4 mb-3"),
    layout_columns(
      col_widths = c(4, 4, 4),
      uiOutput("domain_card_condition"),
      uiOutput("domain_card_drug"),
      uiOutput("domain_card_measurement")
    ),
    layout_columns(
      col_widths = c(4, 4, 4),
      class = "mt-3",
      uiOutput("domain_card_procedure"),
      uiOutput("domain_card_observation"),
      uiOutput("domain_card_person")
    ),

    # Overview chart
    card(
      class = "mt-4",
      card_header("Mapping Coverage by Table"),
      card_body(
        plotOutput("dashboard_coverage_plot", height = "380px")
      )
    )
  ),

  # =====================================================================
  # Unmapped Details tab
  # =====================================================================
  nav_panel(
    title = "Unmapped Details",
    icon = icon("triangle-exclamation"),

    layout_columns(
      col_widths = c(6, 6),

      card(
        card_header("Unmapped Records by Table"),
        card_body(plotOutput("unmapped_count_plot", height = "340px"))
      ),

      card(
        card_header("Unmapped Percentage by Table"),
        card_body(plotOutput("unmapped_pct_plot", height = "340px"))
      )
    ),

    card(
      class = "mt-3",
      card_header("Unmapped Concepts Detail"),
      card_body(DTOutput("unmapped_detail_table"))
    )
  ),

  # =====================================================================
  # Top Unmapped Source Values tab
  # =====================================================================
  nav_panel(
    title = "Top Unmapped Values",
    icon = icon("list-ol"),

    layout_columns(
      col_widths = c(4, 4, 4),
      card(
        card_body(
          class = "p-2",
          uiOutput("top_domain_selector")
        )
      ),
      card(
        card_body(
          class = "p-2",
          numericInput("top_n", "Show top N", value = 25, min = 5, max = 200, step = 5, width = "100%")
        )
      ),
      card(
        card_body(
          class = "p-2",
          uiOutput("top_unmapped_summary_text")
        )
      )
    ),

    card(
      class = "mt-3",
      card_header("Top Unmapped Source Values"),
      card_body(plotOutput("top_unmapped_bar", height = "500px"))
    ),

    card(
      class = "mt-3",
      card_header("Full Table"),
      card_body(DTOutput("top_unmapped_table"))
    )
  ),

  # =====================================================================
  # Record Counts tab
  # =====================================================================
  nav_panel(
    title = "Record Counts",
    icon = icon("database"),

    layout_columns(
      col_widths = c(6, 6),

      card(
        card_header("Staging Tables"),
        card_body(plotOutput("stg_counts_plot", height = "400px"))
      ),

      card(
        card_header("CDM Tables"),
        card_body(plotOutput("cdm_counts_plot", height = "400px"))
      )
    ),

    card(
      class = "mt-3",
      card_header("All Record Counts"),
      card_body(DTOutput("record_counts_table"))
    )
  ),

  # =====================================================================
  # Mapping Cardinality tab
  # =====================================================================
  nav_panel(
    title = "Mapping Cardinality",
    icon = icon("arrows-split-up-and-left"),

    layout_columns(
      col_widths = c(6, 6),

      card(
        card_header(
          class = "d-flex justify-content-between align-items-center",
          tags$span("One-to-Many Mappings"),
          uiOutput("otm_badge")
        ),
        card_body(
          tags$p(class = "text-muted small mb-2", "One source code maps to multiple standard concepts."),
          DTOutput("one_to_many_table")
        )
      ),

      card(
        card_header(
          class = "d-flex justify-content-between align-items-center",
          tags$span("Many-to-One Mappings"),
          uiOutput("mto_badge")
        ),
        card_body(
          tags$p(class = "text-muted small mb-2", "Multiple source codes map to the same standard concept."),
          DTOutput("many_to_one_table")
        )
      )
    )
  ),

  # =====================================================================
  # Domain Conformance tab
  # =====================================================================
  nav_panel(
    title = "Domain Conformance",
    icon = icon("arrows-rotate"),

    # KPI row
    layout_columns(
      col_widths = c(4, 4, 4),
      uiOutput("kpi_domain_conformance"),
      uiOutput("kpi_routed_records"),
      uiOutput("kpi_tables_conforming")
    ),

    # Conformance by table
    card(
      class = "mt-3",
      card_header("Domain Conformance by CDM Table"),
      card_body(plotOutput("domain_conf_plot", height = "340px"))
    ),

    layout_columns(
      col_widths = c(6, 6),
      class = "mt-3",

      card(
        card_header("Conformance Detail"),
        card_body(
          tags$p(class = "text-muted small mb-2",
                 "Records by CDM table and concept domain. Concept_id = 0 excluded."),
          DTOutput("domain_conf_table")
        )
      ),

      card(
        card_header("Domain Routing Log"),
        card_body(
          tags$p(class = "text-muted small mb-2",
                 "Records moved between CDM tables during domain routing step."),
          DTOutput("routing_log_table")
        )
      )
    )
  ),

  # =====================================================================
  # Data Quality tab
  # =====================================================================
  nav_panel(
    title = "Data Quality",
    icon = icon("clipboard-check"),

    layout_columns(
      col_widths = c(6, 6),

      card(
        card_header("Person Count Comparison"),
        card_body(
          tags$p(class = "text-muted small mb-2",
                 "Verifies no patients were lost during transformation."),
          uiOutput("person_check_result"),
          tableOutput("person_count_table")
        )
      ),

      card(
        card_header("Reject Summary"),
        card_body(
          uiOutput("reject_summary_kpi"),
          tags$hr(),
          tags$p(class = "text-muted small mb-2",
                 "Records rejected during staging due to invalid dates, missing keys, etc.")
        )
      )
    ),

    card(
      class = "mt-3",
      card_header("Reject Table Details"),
      card_body(
        plotOutput("reject_plot", height = "380px")
      )
    ),

    card(
      class = "mt-3",
      card_body(DTOutput("reject_detail_table"))
    )
  )
)

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------
server <- function(input, output, session) {
  data <- reactiveVal(list())

  load_results <- function() {
    dir <- trimws(input$results_dir)
    if (!nzchar(dir)) return()
    if (dir.exists(dir)) {
      dir <- normalizePath(dir, winslash = "/")
    } else if (!startsWith(dir, "/") && !grepl("^[A-Za-z]:", dir)) {
      dir <- normalizePath(file.path(getwd(), dir), mustWork = FALSE, winslash = "/")
    }
    if (!dir.exists(dir)) {
      showNotification("Directory not found.", type = "error", duration = 4)
      return()
    }
    data(list(
      summary    = read_mq_csv(file.path(dir, "08_data_not_mapped_summary.csv")),
      unmapped   = read_mq_csv(file.path(dir, "01_unmapped_concepts_by_table.csv")),
      top_unmapped = read_mq_csv(file.path(dir, "02_top_unmapped_source_values.csv")),
      record_counts = read_mq_csv(file.path(dir, "03_record_counts_by_table.csv")),
      person_count  = read_mq_csv(file.path(dir, "04_person_count_comparison.csv")),
      one_to_many   = read_mq_csv(file.path(dir, "05_one_to_many_mappings.csv")),
      many_to_one   = read_mq_csv(file.path(dir, "06_many_to_one_mappings.csv")),
      reject        = read_mq_csv(file.path(dir, "07_reject_table_row_counts.csv")),
      domain_conf   = read_mq_csv(file.path(dir, "09_domain_conformance.csv")),
      routing_log   = read_mq_csv(file.path(dir, "10_domain_routing_log.csv"))
    ))
    showNotification("Results loaded successfully.", type = "message", duration = 3)
  }

  observeEvent(input$load_btn, load_results())

  observe({
    dir <- trimws(input$results_dir)
    if (!nzchar(dir) || length(data()) > 0) return()
    if (dir.exists(dir)) dir <- normalizePath(dir, winslash = "/")
    else if (!startsWith(dir, "/") && !grepl("^[A-Za-z]:", dir))
      dir <- normalizePath(file.path(getwd(), dir), mustWork = FALSE, winslash = "/")
    if (dir.exists(dir)) load_results()
  })

  files_loaded <- reactive({
    d <- data()
    length(d) > 0 && any(vapply(d, function(x) !is.null(x) && nrow(x) > 0, logical(1)))
  })

  output$load_status_badge <- renderUI({
    if (files_loaded()) {
      tags$span(class = "badge bg-success", "Loaded")
    } else {
      tags$span(class = "badge bg-secondary", "No data")
    }
  })

  # -----------------------------------------------------------------------
  # Derived metrics
  # -----------------------------------------------------------------------
  # Primary domain rows only (exclude unit_concept_id and value_as_concept_id)
  primary_unmapped <- reactive({
    d <- data()$unmapped
    if (is.null(d)) return(NULL)
    primary_cols <- c("condition_concept_id", "drug_concept_id", "measurement_concept_id",
                      "procedure_concept_id", "observation_concept_id")
    d[d$concept_column %in% primary_cols, , drop = FALSE]
  })

  overall_coverage <- reactive({
    d <- primary_unmapped()
    if (is.null(d) || nrow(d) == 0) return(NA_real_)
    total <- sum(d$total_rows, na.rm = TRUE)
    unmapped <- sum(d$unmapped_count, na.rm = TRUE)
    if (total == 0) return(NA_real_)
    100 * (total - unmapped) / total
  })

  total_cdm_records <- reactive({
    d <- primary_unmapped()
    if (is.null(d)) return(NA_integer_)
    sum(d$total_rows, na.rm = TRUE)
  })

  total_unmapped_records <- reactive({
    d <- data()$summary
    if (is.null(d)) return(NA_integer_)
    row <- d[d$metric == "total_cdm_records_with_concept_id_0", , drop = FALSE]
    if (nrow(row) == 0) return(NA_integer_)
    row$value[1]
  })

  total_rejected <- reactive({
    d <- data()$summary
    if (is.null(d)) return(NA_integer_)
    row <- d[d$metric == "total_rows_in_reject_tables", , drop = FALSE]
    if (nrow(row) == 0) return(NA_integer_)
    row$value[1]
  })

  domain_metrics <- reactive({
    d <- primary_unmapped()
    if (is.null(d) || nrow(d) == 0) return(NULL)
    d$domain <- sub("_concept_id$", "", d$concept_column)
    d$mapped_count <- d$total_rows - d$unmapped_count
    d$mapped_pct <- ifelse(d$total_rows > 0, 100 * d$mapped_count / d$total_rows, NA_real_)
    d
  })

  # -----------------------------------------------------------------------
  # Dashboard: KPI value boxes
  # -----------------------------------------------------------------------
  output$kpi_overall_coverage <- renderUI({
    cov <- overall_coverage()
    value_box(
      title = "Overall Mapping Coverage",
      value = if (is.na(cov)) "N/A" else fmt_pct(cov),
      showcase = icon("chart-pie"),
      theme = if (is.na(cov)) "secondary" else severity_color(cov),
      p(class = "small mb-0", "Primary concept columns only")
    )
  })

  output$kpi_total_records <- renderUI({
    n <- total_cdm_records()
    value_box(
      title = "Total CDM Records",
      value = if (is.na(n)) "N/A" else fmt_num(n),
      showcase = icon("database"),
      theme = "primary",
      p(class = "small mb-0", "Across 5 clinical tables")
    )
  })

  output$kpi_unmapped_records <- renderUI({
    n <- total_unmapped_records()
    value_box(
      title = "Unmapped Records",
      value = if (is.na(n)) "N/A" else fmt_num(n),
      showcase = icon("circle-exclamation"),
      theme = if (!is.na(n) && n > 0) "warning" else "success",
      p(class = "small mb-0", "concept_id = 0")
    )
  })

  output$kpi_rejected_records <- renderUI({
    n <- total_rejected()
    value_box(
      title = "Rejected Records",
      value = if (is.na(n)) "N/A" else fmt_num(n),
      showcase = icon("filter-circle-xmark"),
      theme = if (!is.na(n) && n > 0) "danger" else "success",
      p(class = "small mb-0", "Invalid dates, missing keys, etc.")
    )
  })

  # Domain coverage cards helper
  make_domain_card <- function(domain_label, concept_col_prefix) {
    dm <- domain_metrics()
    if (is.null(dm)) return(card(card_body(tags$p(class = "text-muted", "No data"))))
    row <- dm[dm$domain == concept_col_prefix, , drop = FALSE]
    if (nrow(row) == 0) return(card(card_body(tags$p(class = "text-muted", "No data"))))
    row <- row[1, ]
    cov_pct <- row$mapped_pct
    col <- severity_hex(cov_pct)
    card(
      card_body(
        class = "p-3",
        tags$div(
          class = "d-flex justify-content-between align-items-start",
          tags$div(
            tags$h6(class = "text-muted mb-1", domain_label),
            tags$h3(class = "mb-0", style = paste0("color:", col), fmt_pct(cov_pct))
          ),
          tags$div(
            class = "text-end",
            tags$div(class = "small text-muted", paste(fmt_num(row$mapped_count), "mapped")),
            tags$div(class = "small text-muted", paste(fmt_num(row$unmapped_count), "unmapped")),
            tags$div(class = "small text-muted", paste(fmt_num(row$total_rows), "total"))
          )
        ),
        tags$div(
          class = "progress mt-2",
          style = "height: 6px;",
          tags$div(
            class = paste0("progress-bar bg-", severity_color(cov_pct)),
            role = "progressbar",
            style = paste0("width:", round(cov_pct, 1), "%"),
            `aria-valuenow` = round(cov_pct, 1),
            `aria-valuemin` = "0",
            `aria-valuemax` = "100"
          )
        )
      )
    )
  }

  output$domain_card_condition <- renderUI(make_domain_card("Conditions", "condition"))
  output$domain_card_drug <- renderUI(make_domain_card("Drugs", "drug"))
  output$domain_card_measurement <- renderUI(make_domain_card("Measurements", "measurement"))
  output$domain_card_procedure <- renderUI(make_domain_card("Procedures", "procedure"))
  output$domain_card_observation <- renderUI(make_domain_card("Observations", "observation"))

  output$domain_card_person <- renderUI({
    d <- data()$person_count
    if (is.null(d)) return(card(card_body(tags$p(class = "text-muted", "No data"))))
    src_count <- d[d$source == "stg_enrollment_distinct_member_id", "count"]
    cdm_count <- d[d$source == "cdm_person", "count"]
    if (length(src_count) == 0 || length(cdm_count) == 0)
      return(card(card_body(tags$p(class = "text-muted", "No data"))))
    match <- identical(as.integer(src_count), as.integer(cdm_count))
    card(
      card_body(
        class = "p-3",
        tags$div(
          class = "d-flex justify-content-between align-items-start",
          tags$div(
            tags$h6(class = "text-muted mb-1", "Person Integrity"),
            tags$h3(class = "mb-0", style = paste0("color:", if (match) "#198754" else "#dc3545"),
                    if (match) "Match" else "Mismatch")
          ),
          tags$div(
            class = "text-end",
            tags$div(class = "small text-muted", paste(fmt_num(src_count), "source")),
            tags$div(class = "small text-muted", paste(fmt_num(cdm_count), "CDM"))
          )
        ),
        tags$div(
          class = "progress mt-2",
          style = "height: 6px;",
          tags$div(
            class = paste0("progress-bar bg-", if (match) "success" else "danger"),
            role = "progressbar",
            style = "width: 100%"
          )
        )
      )
    )
  })

  # Dashboard: coverage chart
  output$dashboard_coverage_plot <- renderPlot({
    d <- data()$unmapped
    if (is.null(d) || nrow(d) == 0) return(NULL)

    d$label <- paste0(
      sub("^[^.]+\\.", "", d$cdm_table), " (", d$concept_column, ")"
    )
    d$mapped_pct <- 100 - d$unmapped_pct

    left_margin <- max(10, max(nchar(d$label), na.rm = TRUE) * 0.55)
    op <- par(mar = c(4, left_margin, 2, 1), family = "sans", bg = "white")
    on.exit(par(op))

    n <- nrow(d)
    ypos <- barplot(
      rev(d$mapped_pct),
      names.arg = rev(d$label),
      horiz = TRUE,
      las = 1,
      col = vapply(rev(d$mapped_pct), function(p) severity_hex(p), character(1)),
      border = NA,
      xlim = c(0, 105),
      xlab = "Mapped %",
      cex.names = 0.8,
      cex.main = 1.1
    )

    # Add percentage labels
    text(
      x = rev(d$mapped_pct) + 1.5,
      y = ypos,
      labels = fmt_pct(rev(d$mapped_pct)),
      adj = 0,
      cex = 0.75,
      col = "#495057"
    )
  })

  # -----------------------------------------------------------------------
  # Unmapped Details tab
  # -----------------------------------------------------------------------
  output$unmapped_count_plot <- renderPlot({
    d <- data()$unmapped
    if (is.null(d) || nrow(d) == 0) return(NULL)
    d$label <- paste0(sub("^[^.]+\\.", "", d$cdm_table), "\n(", d$concept_column, ")")
    styled_barplot(
      d$unmapped_count, d$label,
      title = "Unmapped Record Count",
      xlab = "Records",
      highlight_fn = function(v) if (v > 100000) "#dc3545" else if (v > 10000) "#fd7e14" else "#0d6efd"
    )
  })

  output$unmapped_pct_plot <- renderPlot({
    d <- data()$unmapped
    if (is.null(d) || nrow(d) == 0) return(NULL)
    d$label <- paste0(sub("^[^.]+\\.", "", d$cdm_table), "\n(", d$concept_column, ")")
    styled_barplot(
      d$unmapped_pct, d$label,
      title = "Unmapped Percentage",
      xlab = "% Unmapped",
      highlight_fn = function(v) severity_hex(100 - v)
    )
  })

  output$unmapped_detail_table <- renderDT({
    d <- data()$unmapped
    if (is.null(d)) return(make_dt(data.frame()))
    d$mapped_count <- d$total_rows - d$unmapped_count
    d$mapped_pct <- round(100 - d$unmapped_pct, 2)
    d$unmapped_pct <- round(d$unmapped_pct, 2)
    names(d) <- c("CDM Table", "Concept Column", "Total Rows", "Unmapped Count",
                   "Unmapped %", "Mapped Count", "Mapped %")
    make_dt(d) |>
      formatCurrency(c("Total Rows", "Unmapped Count", "Mapped Count"),
                     currency = "", digits = 0) |>
      formatStyle("Unmapped %",
                  backgroundColor = styleInterval(c(10, 25, 50),
                                                  c("#d4edda", "#fff3cd", "#f8d7da", "#f5c6cb")))
  })

  # -----------------------------------------------------------------------
  # Top Unmapped Source Values tab
  # -----------------------------------------------------------------------
  output$top_domain_selector <- renderUI({
    d <- data()$top_unmapped
    if (is.null(d) || !"domain" %in% names(d)) return(NULL)
    domains <- sort(unique(d$domain))
    selectInput("top_domain", "Domain", choices = c("All Domains" = "(all)", domains),
                selected = "(all)", width = "100%")
  })

  top_unmapped_filtered <- reactive({
    d <- data()$top_unmapped
    if (is.null(d)) return(NULL)
    if (is.null(input$top_domain) || input$top_domain == "(all)") return(d)
    d[d$domain == input$top_domain, , drop = FALSE]
  })

  output$top_unmapped_summary_text <- renderUI({
    df <- top_unmapped_filtered()
    if (is.null(df) || nrow(df) == 0) return(tags$p(class = "text-muted mb-0 mt-2", "No unmapped values."))
    total_vals <- nrow(df)
    total_recs <- sum(df$record_count, na.rm = TRUE)
    tags$div(
      class = "mt-2",
      tags$p(class = "mb-1", tags$strong(fmt_num(total_vals)), " distinct unmapped values"),
      tags$p(class = "mb-0 text-muted small", "Affecting ", tags$strong(fmt_num(total_recs)), " records")
    )
  })

  output$top_unmapped_bar <- renderPlot({
    df <- top_unmapped_filtered()
    if (is.null(df) || nrow(df) == 0) return(NULL)
    n <- min(as.integer(input$top_n %||% 25L), nrow(df))
    df <- head(df[order(-df$record_count), ], n)

    domain_colors <- c(
      condition = "#dc3545", drug = "#fd7e14", measurement = "#0d6efd",
      procedure = "#6610f2", observation = "#20c997",
      measurement_unit = "#6c757d", measurement_value = "#adb5bd"
    )
    cols <- ifelse(df$domain %in% names(domain_colors),
                   domain_colors[df$domain], "#0d6efd")

    lbl <- ifelse(
      nchar(df$source_value) > 40,
      paste0(substr(df$source_value, 1, 37), "..."),
      df$source_value
    )

    left_margin <- max(10, max(nchar(lbl), na.rm = TRUE) * 0.52)
    op <- par(mar = c(4, left_margin, 2.5, 5), family = "sans", bg = "white")
    on.exit(par(op))

    ypos <- barplot(
      rev(df$record_count),
      names.arg = rev(lbl),
      horiz = TRUE,
      las = 1,
      col = rev(cols),
      border = NA,
      xlab = "Record count",
      cex.names = 0.7,
      cex.main = 1.1,
      main = paste("Top", n, "unmapped source values")
    )

    # Count labels
    text(
      x = rev(df$record_count),
      y = ypos,
      labels = fmt_num(rev(df$record_count)),
      adj = -0.15,
      cex = 0.65,
      col = "#495057"
    )

    # Legend
    present_domains <- unique(df$domain)
    present_colors <- domain_colors[present_domains]
    present_colors <- present_colors[!is.na(present_colors)]
    if (length(present_colors) > 0) {
      legend("bottomright",
             legend = names(present_colors),
             fill = present_colors,
             border = NA,
             bty = "n",
             cex = 0.8)
    }
  })

  output$top_unmapped_table <- renderDT({
    df <- top_unmapped_filtered()
    if (is.null(df)) return(make_dt(data.frame()))
    df <- df[order(-df$record_count), , drop = FALSE]
    names(df) <- c("Domain", "Source Value", "Record Count")
    make_dt(df, page_len = 20, filter = "top") |>
      formatCurrency("Record Count", currency = "", digits = 0)
  })

  # -----------------------------------------------------------------------
  # Record Counts tab
  # -----------------------------------------------------------------------
  output$stg_counts_plot <- renderPlot({
    d <- data()$record_counts
    if (is.null(d)) return(NULL)
    stg <- d[d$layer == "stg", , drop = FALSE]
    stg <- stg[!is.na(stg$row_count), , drop = FALSE]
    if (nrow(stg) == 0) return(NULL)
    stg$label <- sub("^[^.]+\\.", "", stg$table_name)
    styled_barplot(stg$row_count, stg$label, title = "Staging Tables",
                   xlab = "Row count", col = "#fd7e14")
  })

  output$cdm_counts_plot <- renderPlot({
    d <- data()$record_counts
    if (is.null(d)) return(NULL)
    cdm <- d[d$layer == "cdm", , drop = FALSE]
    cdm <- cdm[!is.na(cdm$row_count), , drop = FALSE]
    if (nrow(cdm) == 0) return(NULL)
    cdm$label <- sub("^[^.]+\\.", "", cdm$table_name)
    styled_barplot(cdm$row_count, cdm$label, title = "CDM Tables",
                   xlab = "Row count", col = "#0d6efd")
  })

  output$record_counts_table <- renderDT({
    d <- data()$record_counts
    if (is.null(d)) return(make_dt(data.frame()))
    names(d) <- c("Table", "Row Count", "Layer")
    make_dt(d) |>
      formatCurrency("Row Count", currency = "", digits = 0) |>
      formatStyle("Layer",
                  backgroundColor = styleEqual(c("stg", "cdm"), c("#fff3cd", "#cfe2ff")))
  })

  # -----------------------------------------------------------------------
  # Mapping Cardinality tab
  # -----------------------------------------------------------------------
  output$one_to_many_table <- renderDT({
    d <- data()$one_to_many
    if (is.null(d) || nrow(d) == 0) {
      return(make_dt(data.frame(Message = "No one-to-many mappings found.")))
    }
    make_dt(d, page_len = 10)
  })

  output$many_to_one_table <- renderDT({
    d <- data()$many_to_one
    if (is.null(d) || nrow(d) == 0) {
      return(make_dt(data.frame(Message = "No many-to-one mappings found.")))
    }
    make_dt(d, page_len = 10)
  })

  output$otm_badge <- renderUI({
    d <- data()$one_to_many
    n <- if (!is.null(d) && nrow(d) > 0) nrow(d) else 0
    tags$span(class = paste0("badge bg-", if (n > 0) "warning" else "success"), n)
  })

  output$mto_badge <- renderUI({
    d <- data()$many_to_one
    n <- if (!is.null(d) && nrow(d) > 0) nrow(d) else 0
    tags$span(class = paste0("badge bg-", if (n > 0) "info" else "success"), n)
  })

  # -----------------------------------------------------------------------
  # Domain Conformance tab
  # -----------------------------------------------------------------------
  # Summarise conformance per table: one row per table with conformance %
  domain_conf_summary <- reactive({
    d <- data()$domain_conf
    if (is.null(d) || nrow(d) == 0) return(NULL)
    # Conforming rows are where concept_domain == expected_domain
    conf_rows <- d[d$conforming == TRUE, , drop = FALSE]
    tables <- unique(d$cdm_table)
    result <- data.frame(
      cdm_table = character(),
      expected_domain = character(),
      total_mapped = integer(),
      conforming_count = integer(),
      conformance_pct = numeric(),
      stringsAsFactors = FALSE
    )
    for (tbl in tables) {
      tbl_data <- d[d$cdm_table == tbl, , drop = FALSE]
      total <- sum(tbl_data$record_count, na.rm = TRUE)
      conf <- sum(tbl_data$record_count[tbl_data$conforming == TRUE], na.rm = TRUE)
      expected <- tbl_data$expected_domain[1]
      result <- rbind(result, data.frame(
        cdm_table = tbl, expected_domain = expected,
        total_mapped = total, conforming_count = conf,
        conformance_pct = if (total > 0) round(100 * conf / total, 1) else NA_real_,
        stringsAsFactors = FALSE
      ))
    }
    result
  })

  overall_conformance <- reactive({
    s <- domain_conf_summary()
    if (is.null(s) || nrow(s) == 0) return(NA_real_)
    total <- sum(s$total_mapped, na.rm = TRUE)
    conf <- sum(s$conforming_count, na.rm = TRUE)
    if (total == 0) return(NA_real_)
    round(100 * conf / total, 1)
  })

  output$kpi_domain_conformance <- renderUI({
    pct <- overall_conformance()
    value_box(
      title = "Domain Conformance",
      value = if (is.na(pct)) "N/A" else fmt_pct(pct),
      showcase = icon("arrows-rotate"),
      theme = if (is.na(pct)) "secondary" else severity_color(pct),
      p(class = "small mb-0", "Mapped records in correct table")
    )
  })

  output$kpi_routed_records <- renderUI({
    d <- data()$routing_log
    n <- if (!is.null(d) && nrow(d) > 0) sum(d$record_count, na.rm = TRUE) else 0L
    value_box(
      title = "Records Routed",
      value = fmt_num(n),
      showcase = icon("shuffle"),
      theme = if (n > 0) "info" else "success",
      p(class = "small mb-0", "Moved to correct CDM table")
    )
  })

  output$kpi_tables_conforming <- renderUI({
    s <- domain_conf_summary()
    if (is.null(s) || nrow(s) == 0) return(value_box(title = "Tables at 100%", value = "N/A",
                                                       showcase = icon("check-double"), theme = "secondary"))
    n_perfect <- sum(s$conformance_pct >= 100, na.rm = TRUE)
    n_total <- nrow(s)
    value_box(
      title = "Tables at 100%",
      value = paste0(n_perfect, "/", n_total),
      showcase = icon("check-double"),
      theme = if (n_perfect == n_total) "success" else "warning",
      p(class = "small mb-0", "Fully domain-conformant tables")
    )
  })

  output$domain_conf_plot <- renderPlot({
    s <- domain_conf_summary()
    if (is.null(s) || nrow(s) == 0) return(NULL)
    s$label <- paste0(sub("_", "\n", s$cdm_table), "\n(", s$expected_domain, ")")
    styled_barplot(
      s$conformance_pct, s$label,
      title = "Domain Conformance by CDM Table",
      xlab = "Conformance %",
      highlight_fn = function(v) severity_hex(v)
    )
  })

  output$domain_conf_table <- renderDT({
    d <- data()$domain_conf
    if (is.null(d) || nrow(d) == 0) return(make_dt(data.frame()))
    display <- d[, c("cdm_table", "expected_domain", "concept_domain", "record_count",
                      "total_mapped", "conformance_pct"), drop = FALSE]
    names(display) <- c("CDM Table", "Expected Domain", "Concept Domain", "Records",
                         "Total Mapped", "% of Table")
    make_dt(display, page_len = 15) |>
      formatCurrency(c("Records", "Total Mapped"), currency = "", digits = 0) |>
      formatStyle("Concept Domain",
                  backgroundColor = styleEqual(
                    display$`Expected Domain`,
                    rep("#d4edda", nrow(display))
                  ))
  })

  output$routing_log_table <- renderDT({
    d <- data()$routing_log
    if (is.null(d) || nrow(d) == 0) {
      return(make_dt(data.frame(Message = "No records were routed between tables.")))
    }
    names(d) <- c("From Table", "To Domain", "Records Moved")
    make_dt(d, page_len = 10) |>
      formatCurrency("Records Moved", currency = "", digits = 0)
  })

  # -----------------------------------------------------------------------
  # Data Quality tab
  # -----------------------------------------------------------------------
  output$person_check_result <- renderUI({
    d <- data()$person_count
    if (is.null(d)) return(tags$p(class = "text-muted", "No data loaded."))
    src <- d[d$source == "stg_enrollment_distinct_member_id", "count"]
    cdm <- d[d$source == "cdm_person", "count"]
    if (length(src) == 0 || length(cdm) == 0) return(tags$p(class = "text-muted", "Incomplete data."))
    match <- identical(as.integer(src), as.integer(cdm))
    tags$div(
      class = paste0("alert alert-", if (match) "success" else "danger", " py-2"),
      if (match)
        tags$span(icon("circle-check"), " Person counts match: ", tags$strong(fmt_num(src)))
      else
        tags$span(icon("circle-xmark"), " Mismatch: Source = ", fmt_num(src), ", CDM = ", fmt_num(cdm))
    )
  })

  output$person_count_table <- renderTable({
    d <- data()$person_count
    if (is.null(d)) return(NULL)
    d$count <- fmt_num(d$count)
    names(d) <- c("Source", "Count")
    d
  }, striped = TRUE, hover = TRUE, bordered = TRUE, width = "100%")

  output$reject_summary_kpi <- renderUI({
    d <- data()$reject
    if (is.null(d)) return(tags$p(class = "text-muted", "No data loaded."))
    total <- sum(d$row_count, na.rm = TRUE)
    n_tables <- sum(d$row_count > 0, na.rm = TRUE)
    tags$div(
      class = paste0("alert alert-", if (total == 0) "success" else "warning", " py-2"),
      if (total == 0)
        tags$span(icon("circle-check"), " No rejected records across all ", nrow(d), " checks")
      else
        tags$span(icon("triangle-exclamation"),
                  tags$strong(fmt_num(total)),
                  " rejected records across ",
                  tags$strong(n_tables),
                  " tables")
    )
  })

  output$reject_plot <- renderPlot({
    d <- data()$reject
    if (is.null(d) || nrow(d) == 0) return(NULL)
    # Only show non-zero or show all if all zero
    if (any(d$row_count > 0)) d <- d[d$row_count > 0, , drop = FALSE]
    if (nrow(d) == 0) return(NULL)
    d$label <- sub("^reject_", "", d$reject_table)
    styled_barplot(
      d$row_count, d$label,
      title = "Reject Table Row Counts",
      xlab = "Row count",
      highlight_fn = function(v) if (v > 0) "#dc3545" else "#198754"
    )
  })

  output$reject_detail_table <- renderDT({
    d <- data()$reject
    if (is.null(d)) return(make_dt(data.frame()))
    d$status <- ifelse(d$row_count == 0, "OK", "Has Rejects")
    names(d) <- c("Reject Table", "Row Count", "Status")
    make_dt(d) |>
      formatCurrency("Row Count", currency = "", digits = 0) |>
      formatStyle("Status",
                  color = styleEqual(c("OK", "Has Rejects"), c("#198754", "#dc3545")),
                  fontWeight = "bold")
  })
}

shinyApp(ui = ui, server = server)
