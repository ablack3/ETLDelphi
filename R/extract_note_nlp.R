# extract_note_nlp.R
# Extract structured clinical entities from parsed SOAP notes and load into
# cdm.note_nlp and supplemental CDM fact tables. Uses regex-based extraction
# (from SQL) with vocabulary lookup + LLM fallback for concept mapping.

#' Extract structured data from SOAP notes and populate note_nlp
#'
#' Parses SOAP notes into sections, extracts clinical entities (conditions,
#' prescriptions, procedures, screenings) using regex, maps them to OMOP
#' concepts via vocabulary lookup and LLM, then loads into \code{cdm.note_nlp}
#' and supplements \code{cdm.condition_occurrence}, \code{cdm.drug_exposure},
#' and \code{cdm.procedure_occurrence}.
#'
#' @section Pipeline integration:
#' \preformatted{
#' # 1. Run ETL (includes note parsing + entity extraction in 35_nlp step)
#' run_etl(con)
#'
#' # 2. Map entities and load note_nlp + supplemental facts
#' extract_note_nlp(con)
#' }
#'
#' @param con DBI connection to the DuckDB database (post-ETL).
#' @param config ETL config list. Default: \code{default_etl_config()}.
#' @param log_path Path for the concept mapping log CSV.
#' @param provider LLM provider for unmatched entities: \code{"anthropic"} or
#'   \code{"openai"}. Default: auto-detected.
#' @param model LLM model name. Default: from env var.
#' @param api_key LLM API key. Default: from env var.
#' @param hecate Hecate client object. Default: created from env vars.
#' @param skip_llm If TRUE, only use vocabulary lookup (no LLM calls).
#' @param skip_load If TRUE, only do concept mapping (don't load CDM tables).
#' @return Invisible data.frame of mapping results.
#' @export
extract_note_nlp <- function(con,
                              config = NULL,
                              log_path = "note_nlp_mapping_log.csv",
                              provider = NULL,
                              model = NULL,
                              api_key = NULL,
                              hecate = NULL,
                              skip_llm = FALSE,
                              skip_load = FALSE) {
  config <- config %||% default_etl_config()
  stg <- config$schemas$stg %||% "stg"
  cdm <- config$schemas$cdm %||% "main"

  # ── Check prerequisite staging tables exist ──────────────────────
  required_tables <- c("note_parsed", "nlp_conditions", "nlp_prescriptions",
                       "nlp_procedures", "nlp_screenings")
  existing <- DBI::dbGetQuery(con, glue::glue(
    "SELECT table_name FROM information_schema.tables
     WHERE table_schema = '{stg}'
       AND table_name IN ({paste0(\"'\", required_tables, \"'\", collapse = ',')})"
  ))$table_name

  missing <- setdiff(required_tables, existing)
  if (length(missing) > 0) {
    cli::cli_alert_warning(
      "Missing staging tables: {paste(missing, collapse = ', ')}. ",
      "Running note parsing and extraction SQL first..."
    )
    run_note_parsing_sql(con, config)
  }

  # ── Gather unique entities needing concept mapping ───────────────
  cli::cli_h1("Gathering unique entities from parsed notes")

  entities <- gather_unique_entities(con, stg)
  cli::cli_alert_info("Found {nrow(entities)} unique entity texts to map")

  if (nrow(entities) == 0) {
    cli::cli_alert_success("No entities to map.")
    return(invisible(data.frame()))
  }

  # ── Step 1: Vocabulary lookup ────────────────────────────────────
  cli::cli_h2("Step 1: Vocabulary lookup")
  mapped <- vocabulary_lookup(con, entities, cdm)

  n_vocab <- sum(!is.na(mapped$concept_id) & mapped$concept_id != 0)
  n_remaining <- sum(is.na(mapped$concept_id) | mapped$concept_id == 0)
  cli::cli_alert_success("Vocabulary lookup mapped {n_vocab} / {nrow(entities)} entities")
  cli::cli_alert_info("{n_remaining} entities remain unmapped")

  # ── Step 2: LLM mapping for unmatched entities ───────────────────
  if (!skip_llm && n_remaining > 0) {
    cli::cli_h2("Step 2: LLM concept mapping ({n_remaining} entities)")
    mapped <- llm_map_entities(mapped, log_path = log_path, provider = provider,
                                model = model, api_key = api_key, hecate = hecate)
    n_llm <- sum(!is.na(mapped$concept_id) & mapped$concept_id != 0) - n_vocab
    cli::cli_alert_success("LLM mapped {n_llm} additional entities")
  } else if (skip_llm && n_remaining > 0) {
    cli::cli_alert_info("Skipping LLM (skip_llm = TRUE). {n_remaining} entities remain unmapped.")
  }

  # ── Write mapping table to staging ───────────────────────────────
  cli::cli_h2("Writing concept mapping to staging")
  DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS \"{stg}\".note_nlp_concept_map"))
  DBI::dbWriteTable(con, DBI::Id(schema = stg, table = "note_nlp_concept_map"),
                    mapped, overwrite = TRUE)
  cli::cli_alert_success("Wrote {nrow(mapped)} mappings to {stg}.note_nlp_concept_map")

  # ── Step 3: Load CDM tables ──────────────────────────────────────
  if (!skip_load) {
    cli::cli_h2("Step 3: Loading CDM tables")
    load_note_nlp(con, stg, cdm)
    supplement_cdm_facts(con, stg, cdm)
  }

  # ── Summary ──────────────────────────────────────────────────────
  n_final <- sum(!is.na(mapped$concept_id) & mapped$concept_id != 0)
  cli::cli_h1("Note NLP extraction complete")
  cli::cli_alert_success("{n_final} / {nrow(mapped)} entities mapped to OMOP concepts")

  invisible(mapped)
}


# ── Helper: Run note parsing SQL if staging tables are missing ─────────
run_note_parsing_sql <- function(con, config) {
  stg <- config$schemas$stg %||% "stg"
  sql_dir <- system.file("sql", "35_nlp", package = "ETLDelphi")
  if (!nzchar(sql_dir) || !dir.exists(sql_dir)) {
    stop("NLP SQL directory not found. Ensure inst/sql/35_nlp/ exists.", call. = FALSE)
  }

  sql_files <- sort(list.files(sql_dir, pattern = "\\.sql$", full.names = TRUE))
  # Only run extraction SQL (01, 02), not loading
  sql_files <- sql_files[grepl("0[12]_", basename(sql_files))]

  for (f in sql_files) {
    cli::cli_alert_info("Running {basename(f)} ...")
    sql <- paste(readLines(f, warn = FALSE), collapse = "\n")
    # Schema substitution
    sql <- gsub("\\bstg\\.", paste0('"', stg, '".'), sql)
    sql <- gsub("\\bcdm\\.", paste0('"', config$schemas$cdm %||% "main", '".'), sql)

    stmts <- strsplit(sql, ";(?=(?:[^']*'[^']*')*[^']*$)", perl = TRUE)[[1]]
    for (stmt in stmts) {
      stmt <- trimws(stmt)
      if (nzchar(stmt) && !grepl("^\\s*--", stmt)) {
        DBI::dbExecute(con, stmt)
      }
    }
  }
  cli::cli_alert_success("Note parsing and extraction complete")
}


# ── Helper: Gather unique entity texts from all extraction tables ──────
gather_unique_entities <- function(con, stg) {
  sql <- glue::glue("
    SELECT DISTINCT lexical_variant AS entity_text, 'condition' AS entity_type
    FROM \"{stg}\".nlp_conditions
    WHERE lexical_variant IS NOT NULL AND TRIM(lexical_variant) != ''

    UNION

    SELECT DISTINCT drug_name AS entity_text, 'drug' AS entity_type
    FROM \"{stg}\".nlp_prescriptions
    WHERE drug_name IS NOT NULL AND TRIM(drug_name) != ''

    UNION

    SELECT DISTINCT procedure_name AS entity_text, 'procedure' AS entity_type
    FROM \"{stg}\".nlp_procedures
    WHERE procedure_name IS NOT NULL AND TRIM(procedure_name) != ''
  ")
  entities <- DBI::dbGetQuery(con, sql)
  entities$concept_id <- NA_integer_
  entities$concept_name <- NA_character_
  entities$vocabulary_id <- NA_character_
  entities$match_type <- NA_character_
  entities
}


# ── Helper: Try vocabulary lookup for entity texts ─────────────────────
vocabulary_lookup <- function(con, entities, cdm) {
  for (i in seq_len(nrow(entities))) {
    text <- entities$entity_text[i]
    etype <- entities$entity_type[i]

    # Map entity_type to OMOP domain_id
    domain_filter <- switch(etype,
      condition = "'Condition'",
      drug = "'Drug'",
      procedure = "'Procedure'",
      "'Condition', 'Observation'"
    )

    # Try exact name match (case-insensitive) to standard concept
    result <- DBI::dbGetQuery(con, glue::glue("
      SELECT c.concept_id, c.concept_name, c.vocabulary_id
      FROM \"{cdm}\".concept c
      WHERE LOWER(c.concept_name) = LOWER('{gsub(\"'\", \"''\", text)}')
        AND c.standard_concept = 'S'
        AND c.domain_id IN ({domain_filter})
      LIMIT 1
    "))

    if (nrow(result) > 0) {
      entities$concept_id[i] <- result$concept_id[1]
      entities$concept_name[i] <- result$concept_name[1]
      entities$vocabulary_id[i] <- result$vocabulary_id[1]
      entities$match_type[i] <- "vocab_exact"
      next
    }

    # Try synonym match
    result <- DBI::dbGetQuery(con, glue::glue("
      SELECT c.concept_id, c.concept_name, c.vocabulary_id
      FROM \"{cdm}\".concept_synonym cs
      JOIN \"{cdm}\".concept c ON c.concept_id = cs.concept_id
      WHERE LOWER(cs.concept_synonym_name) = LOWER('{gsub(\"'\", \"''\", text)}')
        AND c.standard_concept = 'S'
        AND c.domain_id IN ({domain_filter})
      LIMIT 1
    "))

    if (nrow(result) > 0) {
      entities$concept_id[i] <- result$concept_id[1]
      entities$concept_name[i] <- result$concept_name[1]
      entities$vocabulary_id[i] <- result$vocabulary_id[1]
      entities$match_type[i] <- "vocab_synonym"
    }
  }

  entities
}


# ── Helper: LLM concept mapping for unmatched entities ─────────────────
llm_map_entities <- function(mapped, log_path, provider, model, api_key, hecate) {
  unmapped_idx <- which(is.na(mapped$concept_id) | mapped$concept_id == 0)
  if (length(unmapped_idx) == 0) return(mapped)

  # Resolve LLM provider
  provider <- provider %||% detect_llm_provider()
  provider <- match.arg(provider, c("anthropic", "openai"))

  if (provider == "anthropic") {
    model <- model %||% Sys.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
    api_key <- api_key %||% Sys.getenv("ANTHROPIC_API_KEY")
  } else {
    model <- model %||% Sys.getenv("OPENAI_MODEL", "gpt-4o")
    api_key <- api_key %||% Sys.getenv("OPENAI_API_KEY")
  }
  cli::cli_alert_info("Using LLM: {provider} ({model})")

  # Initialize Hecate client
  hc <- hecate %||% hecate_client()
  handlers <- build_tool_handlers(hc, oh = NULL, domain = NULL)
  tools <- mapping_tools(domain = "condition")  # Generic tools

  system_prompt <- paste0(
    "You are a clinical terminology specialist. Your task is to map a clinical ",
    "term from a medical note to the most appropriate OMOP standard concept.\n\n",
    "Use the search_concepts tool to find matching OMOP concepts. ",
    "Prefer SNOMED concepts for conditions and procedures, RxNorm for drugs.\n\n",
    "CRITICAL: Your final response MUST be a single JSON object with these fields:\n",
    "- concept_id: integer (the OMOP concept_id, or 0 if no match)\n",
    "- concept_name: string (the concept name)\n",
    "- vocabulary_id: string (e.g., 'SNOMED', 'RxNorm')\n",
    "- confidence: number 0-1 (1.0 = exact match, 0.7+ = good match)\n",
    "- reasoning: string (brief explanation)\n\n",
    "Do NOT include any text before or after the JSON."
  )

  # Load existing log to skip already-processed
  existing_log <- if (file.exists(log_path) && file.size(log_path) > 0) {
    tryCatch(read.csv(log_path, stringsAsFactors = FALSE), error = function(e) data.frame())
  } else {
    data.frame()
  }

  # Write log header if new
  if (!file.exists(log_path) || file.size(log_path) == 0) {
    writeLines("entity_text,entity_type,concept_id,concept_name,vocabulary_id,confidence,reasoning,timestamp,model",
               log_path)
  }

  chat_fn <- if (provider == "anthropic") anthropic_tool_loop else openai_tool_loop

  for (idx in unmapped_idx) {
    text <- mapped$entity_text[idx]
    etype <- mapped$entity_type[idx]

    # Skip if already in log
    if (nrow(existing_log) > 0 && text %in% existing_log$entity_text) {
      log_row <- existing_log[existing_log$entity_text == text, ][1, ]
      if (!is.na(log_row$concept_id) && log_row$concept_id != 0) {
        mapped$concept_id[idx] <- log_row$concept_id
        mapped$concept_name[idx] <- log_row$concept_name
        mapped$vocabulary_id[idx] <- log_row$vocabulary_id
        mapped$match_type[idx] <- "llm_cached"
        next
      }
    }

    cli::cli_alert_info("LLM mapping: \"{text}\" ({etype})")

    user_msg <- paste0(
      "Map this clinical term to an OMOP standard concept:\n",
      "Term: \"", text, "\"\n",
      "Domain: ", etype, "\n",
      "Search for the best matching concept and return JSON."
    )

    messages <- list(
      list(role = "system", content = system_prompt),
      list(role = "user", content = user_msg)
    )

    result <- tryCatch({
      resp <- chat_fn(messages = messages, tools = tools,
                       tool_handlers = handlers, model = model,
                       api_key = api_key)

      # Parse JSON from response
      reply_text <- if (is.list(resp$final_message)) {
        paste(sapply(resp$final_message, function(b) {
          if (is.list(b) && !is.null(b$text)) b$text else ""
        }), collapse = "")
      } else {
        resp$final_message
      }

      parsed <- try_parse_llm_json(reply_text)
      if (!is.null(parsed) && !is.null(parsed$concept_id)) {
        parsed
      } else {
        list(concept_id = 0L, concept_name = NA, vocabulary_id = NA,
             confidence = 0, reasoning = "Failed to parse LLM response")
      }
    },
    rate_limit_error = function(e) {
      cli::cli_alert_danger("Rate limit reached. Stopping LLM mapping.")
      return(NULL)
    },
    error = function(e) {
      cli::cli_warn("  LLM error: {conditionMessage(e)}")
      list(concept_id = 0L, concept_name = NA, vocabulary_id = NA,
           confidence = 0, reasoning = paste("Error:", conditionMessage(e)))
    })

    if (is.null(result)) break  # Rate limited

    # Update mapping
    if (!is.null(result$concept_id) && result$concept_id != 0) {
      mapped$concept_id[idx] <- as.integer(result$concept_id)
      mapped$concept_name[idx] <- result$concept_name %||% NA_character_
      mapped$vocabulary_id[idx] <- result$vocabulary_id %||% NA_character_
      mapped$match_type[idx] <- "llm"
    }

    # Append to log
    log_line <- paste(
      gsub(",", ";", text),
      etype,
      result$concept_id %||% 0,
      gsub(",", ";", result$concept_name %||% ""),
      result$vocabulary_id %||% "",
      result$confidence %||% 0,
      gsub(",", ";", result$reasoning %||% ""),
      format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
      model,
      sep = ","
    )
    cat(log_line, "\n", file = log_path, append = TRUE)
  }

  mapped
}


# ── Helper: Load note_nlp from mapped entities ─────────────────────────
load_note_nlp <- function(con, stg, cdm) {
  # Get max existing note_nlp_id
  max_id <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COALESCE(MAX(note_nlp_id), 0) AS max_id FROM \"{cdm}\".note_nlp"
  ))$max_id

  sql <- glue::glue("
    INSERT INTO \"{cdm}\".note_nlp (
      note_nlp_id, note_id, section_concept_id, snippet, \"offset\",
      lexical_variant, note_nlp_concept_id, note_nlp_source_concept_id,
      nlp_system, nlp_date, nlp_datetime, term_exists, term_temporal, term_modifiers
    )

    -- Conditions from Assessment
    SELECT
      {max_id} + ROW_NUMBER() OVER (ORDER BY e.note_id, e.lexical_variant) AS note_nlp_id,
      e.note_id,
      40763911 AS section_concept_id,  -- Subjective Narrative (closest for Assessment)
      SUBSTR(e.lexical_variant, 1, 250) AS snippet,
      NULL AS \"offset\",
      SUBSTR(e.lexical_variant, 1, 250) AS lexical_variant,
      COALESCE(m.concept_id, 0) AS note_nlp_concept_id,
      NULL AS note_nlp_source_concept_id,
      'ETLDelphi-regex-v1' AS nlp_system,
      CURRENT_DATE AS nlp_date,
      CURRENT_TIMESTAMP AS nlp_datetime,
      e.term_exists,
      e.term_temporal,
      NULL AS term_modifiers
    FROM \"{stg}\".nlp_conditions e
    LEFT JOIN \"{stg}\".note_nlp_concept_map m
      ON LOWER(m.entity_text) = LOWER(e.lexical_variant) AND m.entity_type = 'condition'
    WHERE NOT EXISTS (
      SELECT 1 FROM \"{cdm}\".note_nlp n
      WHERE n.note_id = e.note_id AND n.nlp_system = 'ETLDelphi-regex-v1'
        AND LOWER(n.lexical_variant) = LOWER(SUBSTR(e.lexical_variant, 1, 250))
    )

    UNION ALL

    -- Prescriptions from Plan
    SELECT
      {max_id} + (SELECT COUNT(*) FROM \"{stg}\".nlp_conditions) +
        ROW_NUMBER() OVER (ORDER BY e.note_id, e.drug_name),
      e.note_id,
      706300 AS section_concept_id,  -- Plan of care
      SUBSTR(e.drug_name || ' - ' || COALESCE(e.dose, ''), 1, 250),
      NULL,
      SUBSTR(e.drug_name, 1, 250),
      COALESCE(m.concept_id, 0),
      NULL,
      'ETLDelphi-regex-v1',
      CURRENT_DATE,
      CURRENT_TIMESTAMP,
      'Y',
      'current',
      CASE WHEN e.dose IS NOT NULL AND e.dose != '' THEN '{{\"dose\": \"' || REPLACE(e.dose, '\"', '') || '\"}}' ELSE NULL END
    FROM \"{stg}\".nlp_prescriptions e
    LEFT JOIN \"{stg}\".note_nlp_concept_map m
      ON LOWER(m.entity_text) = LOWER(e.drug_name) AND m.entity_type = 'drug'
    WHERE NOT EXISTS (
      SELECT 1 FROM \"{cdm}\".note_nlp n
      WHERE n.note_id = e.note_id AND n.nlp_system = 'ETLDelphi-regex-v1'
        AND LOWER(n.lexical_variant) = LOWER(SUBSTR(e.drug_name, 1, 250))
    )

    UNION ALL

    -- Procedures from Plan
    SELECT
      {max_id} + (SELECT COUNT(*) FROM \"{stg}\".nlp_conditions) +
        (SELECT COUNT(*) FROM \"{stg}\".nlp_prescriptions) +
        ROW_NUMBER() OVER (ORDER BY e.note_id, e.procedure_name),
      e.note_id,
      706300,
      SUBSTR(e.procedure_name, 1, 250),
      NULL,
      SUBSTR(e.procedure_name, 1, 250),
      COALESCE(m.concept_id, 0),
      NULL,
      'ETLDelphi-regex-v1',
      CURRENT_DATE,
      CURRENT_TIMESTAMP,
      'Y',
      'current',
      '{{\"action_type\": \"' || e.action_type || '\"}}'
    FROM \"{stg}\".nlp_procedures e
    LEFT JOIN \"{stg}\".note_nlp_concept_map m
      ON LOWER(m.entity_text) = LOWER(e.procedure_name) AND m.entity_type = 'procedure'
    WHERE NOT EXISTS (
      SELECT 1 FROM \"{cdm}\".note_nlp n
      WHERE n.note_id = e.note_id AND n.nlp_system = 'ETLDelphi-regex-v1'
        AND LOWER(n.lexical_variant) = LOWER(SUBSTR(e.procedure_name, 1, 250))
    )
  ")

  n_before <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM \"{cdm}\".note_nlp"
  ))$n

  DBI::dbExecute(con, sql)

  n_after <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM \"{cdm}\".note_nlp"
  ))$n

  cli::cli_alert_success("Loaded {n_after - n_before} rows into cdm.note_nlp (total: {n_after})")
}


# ── Helper: Supplement CDM fact tables with note-derived records ────────
supplement_cdm_facts <- function(con, stg, cdm) {
  # -- Conditions from Assessment --
  max_co_id <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COALESCE(MAX(condition_occurrence_id), 0) AS max_id FROM \"{cdm}\".condition_occurrence"
  ))$max_id

  n_cond <- DBI::dbExecute(con, glue::glue("
    INSERT INTO \"{cdm}\".condition_occurrence (
      condition_occurrence_id, person_id, condition_concept_id,
      condition_start_date, condition_start_datetime,
      condition_type_concept_id, visit_occurrence_id,
      condition_source_value, condition_source_concept_id
    )
    SELECT
      {max_co_id} + ROW_NUMBER() OVER (ORDER BY e.note_id),
      e.person_id,
      m.concept_id,
      e.note_date,
      e.note_date::TIMESTAMP,
      32858,  -- NLP type concept
      e.visit_occurrence_id,
      SUBSTR(e.lexical_variant, 1, 50),
      0
    FROM \"{stg}\".nlp_conditions e
    JOIN \"{stg}\".note_nlp_concept_map m
      ON LOWER(m.entity_text) = LOWER(e.lexical_variant) AND m.entity_type = 'condition'
    WHERE m.concept_id IS NOT NULL AND m.concept_id != 0
      AND e.term_exists = 'Y'
      AND NOT EXISTS (
        SELECT 1 FROM \"{cdm}\".condition_occurrence co
        WHERE co.person_id = e.person_id
          AND co.condition_concept_id = m.concept_id
          AND co.condition_start_date = e.note_date
      )
  "))
  cli::cli_alert_success("Supplemented condition_occurrence with {n_cond} NLP-derived rows")

  # -- Drugs from Prescriptions --
  max_de_id <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COALESCE(MAX(drug_exposure_id), 0) AS max_id FROM \"{cdm}\".drug_exposure"
  ))$max_id

  n_drug <- DBI::dbExecute(con, glue::glue("
    INSERT INTO \"{cdm}\".drug_exposure (
      drug_exposure_id, person_id, drug_concept_id,
      drug_exposure_start_date, drug_exposure_end_date,
      drug_type_concept_id, visit_occurrence_id,
      drug_source_value, drug_source_concept_id
    )
    SELECT
      {max_de_id} + ROW_NUMBER() OVER (ORDER BY e.note_id),
      e.person_id,
      m.concept_id,
      e.note_date,
      e.note_date,
      32858,  -- NLP type concept
      e.visit_occurrence_id,
      SUBSTR(e.drug_name, 1, 50),
      0
    FROM \"{stg}\".nlp_prescriptions e
    JOIN \"{stg}\".note_nlp_concept_map m
      ON LOWER(m.entity_text) = LOWER(e.drug_name) AND m.entity_type = 'drug'
    WHERE m.concept_id IS NOT NULL AND m.concept_id != 0
      AND NOT EXISTS (
        SELECT 1 FROM \"{cdm}\".drug_exposure de
        WHERE de.person_id = e.person_id
          AND de.drug_concept_id = m.concept_id
          AND de.drug_exposure_start_date = e.note_date
      )
  "))
  cli::cli_alert_success("Supplemented drug_exposure with {n_drug} NLP-derived rows")

  # -- Procedures from Plan --
  max_po_id <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COALESCE(MAX(procedure_occurrence_id), 0) AS max_id FROM \"{cdm}\".procedure_occurrence"
  ))$max_id

  n_proc <- DBI::dbExecute(con, glue::glue("
    INSERT INTO \"{cdm}\".procedure_occurrence (
      procedure_occurrence_id, person_id, procedure_concept_id,
      procedure_date, procedure_datetime,
      procedure_type_concept_id, visit_occurrence_id,
      procedure_source_value, procedure_source_concept_id
    )
    SELECT
      {max_po_id} + ROW_NUMBER() OVER (ORDER BY e.note_id),
      e.person_id,
      m.concept_id,
      e.note_date,
      e.note_date::TIMESTAMP,
      32858,  -- NLP type concept
      e.visit_occurrence_id,
      SUBSTR(e.procedure_name, 1, 50),
      0
    FROM \"{stg}\".nlp_procedures e
    JOIN \"{stg}\".note_nlp_concept_map m
      ON LOWER(m.entity_text) = LOWER(e.procedure_name) AND m.entity_type = 'procedure'
    WHERE m.concept_id IS NOT NULL AND m.concept_id != 0
      AND NOT EXISTS (
        SELECT 1 FROM \"{cdm}\".procedure_occurrence po
        WHERE po.person_id = e.person_id
          AND po.procedure_concept_id = m.concept_id
          AND po.procedure_date = e.note_date
      )
  "))
  cli::cli_alert_success("Supplemented procedure_occurrence with {n_proc} NLP-derived rows")
}
