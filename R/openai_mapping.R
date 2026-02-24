# openai_mapping.R
# Internal: OpenAI chat completions client with function-calling loop for
# vocabulary mapping. Not exported; called by improve_mappings().

# Single OpenAI chat completion call via httr2.
# Returns parsed response body (list).
# Signals a `rate_limit_error` condition on HTTP 429 so callers can stop the loop.
openai_chat <- function(messages,
                        tools = NULL,
                        model = NULL,
                        api_key = NULL,
                        base_url = NULL,
                        temperature = 0.1) {
  model <- model %||% Sys.getenv("OPENAI_MODEL", "gpt-4o")
  api_key <- api_key %||% Sys.getenv("OPENAI_API_KEY")
  base_url <- base_url %||% Sys.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

  if (!nzchar(api_key)) stop("OPENAI_API_KEY not set.", call. = FALSE)

  body <- list(
    model = model,
    messages = messages,
    temperature = temperature
  )
  if (!is.null(tools) && length(tools) > 0) {
    body$tools <- tools
  }

  resp <- tryCatch(
    httr2::request(paste0(base_url, "/chat/completions")) |>
      httr2::req_headers(
        Authorization = paste("Bearer", api_key),
        `Content-Type` = "application/json"
      ) |>
      httr2::req_body_json(body, auto_unbox = TRUE) |>
      httr2::req_timeout(120) |>
      httr2::req_retry(max_tries = 3, backoff = ~ 5) |>
      httr2::req_perform(),
    error = function(e) {
      msg <- conditionMessage(e)
      if (grepl("429", msg, fixed = TRUE) || grepl("Too Many Requests", msg, fixed = TRUE) ||
          grepl("rate limit", msg, ignore.case = TRUE)) {
        stop(rate_limit_error(msg))
      }
      stop(e)
    }
  )

  httr2::resp_body_json(resp)
}

# Custom condition constructor for rate limit errors
rate_limit_error <- function(message) {
  structure(
    class = c("rate_limit_error", "error", "condition"),
    list(message = message)
  )
}

# Iterative function-calling loop. Sends messages to OpenAI, processes tool_calls,
# executes handlers, appends results, repeats until model returns a text response.
# Returns list(final_message, messages, tool_calls_made).
openai_tool_loop <- function(messages,
                             tools,
                             tool_handlers,
                             max_iterations = 10L,
                             model = NULL,
                             api_key = NULL,
                             base_url = NULL,
                             temperature = 0.1) {
  total_tool_calls <- 0L

  for (iter in seq_len(max_iterations)) {
    response <- openai_chat(
      messages = messages,
      tools = tools,
      model = model,
      api_key = api_key,
      base_url = base_url,
      temperature = temperature
    )

    choice <- response$choices[[1]]
    assistant_msg <- choice$message

    # Append assistant message to conversation
    messages <- c(messages, list(assistant_msg))

    # If no tool calls, model is done
    if (is.null(assistant_msg$tool_calls) || length(assistant_msg$tool_calls) == 0) {
      return(list(
        final_message = assistant_msg$content %||% "",
        messages = messages,
        tool_calls_made = total_tool_calls
      ))
    }

    # Process each tool call
    for (tc in assistant_msg$tool_calls) {
      fn_name <- tc$`function`$name
      fn_args_raw <- tc$`function`$arguments

      fn_args <- tryCatch(
        jsonlite::fromJSON(fn_args_raw, simplifyVector = FALSE),
        error = function(e) list()
      )

      handler <- tool_handlers[[fn_name]]
      if (is.null(handler)) {
        result_str <- jsonlite::toJSON(
          list(error = paste("Unknown tool:", fn_name)),
          auto_unbox = TRUE
        )
      } else {
        result_str <- tryCatch(
          handler(fn_args),
          error = function(e) {
            jsonlite::toJSON(list(error = conditionMessage(e)), auto_unbox = TRUE)
          }
        )
      }

      # Append tool result message
      messages <- c(messages, list(list(
        role = "tool",
        tool_call_id = tc$id,
        content = as.character(result_str)
      )))
      total_tool_calls <- total_tool_calls + 1L
    }
  }

  cli::cli_warn("Tool-calling loop reached max iterations ({max_iterations})")
  list(
    final_message = assistant_msg$content %||% "",
    messages = messages,
    tool_calls_made = total_tool_calls
  )
}

# OpenAI function-calling tool definitions.
# Base tools (Hecate) are always available; drug-specific tools (OMOPHub NDC) added for drug domain.
mapping_tools <- function(domain = NULL) {
  base_tools <- list(
    list(
      type = "function",
      `function` = list(
        name = "search_concepts",
        description = paste(
          "Search the OMOP Standardized Vocabulary for concepts using semantic search.",
          "Best for name/text searches. Returns concept_id, concept_name, domain_id,",
          "vocabulary_id, concept_class_id, standard_concept, and similarity score."
        ),
        parameters = list(
          type = "object",
          properties = list(
            query = list(type = "string", description = "Search text (concept name, code, or description)"),
            vocabulary_id = list(type = "string", description = "Filter by vocabulary: SNOMED, ICD10CM, ICD9CM, RxNorm, LOINC, CPT4, HCPCS, UCUM, NDC, etc."),
            domain_id = list(type = "string", description = "Filter by domain: Condition, Drug, Procedure, Measurement, Observation, Device, Unit"),
            standard_concept = list(type = "string", enum = list("S", "C"), description = "S = Standard concepts only, C = Classification only"),
            limit = list(type = "integer", description = "Max results (default 25, max 100)")
          ),
          required = list("query")
        )
      )
    ),
    list(
      type = "function",
      `function` = list(
        name = "get_concept",
        description = "Get detailed information about a specific OMOP concept by its concept_id.",
        parameters = list(
          type = "object",
          properties = list(
            concept_id = list(type = "integer", description = "The OMOP concept_id to look up")
          ),
          required = list("concept_id")
        )
      )
    ),
    list(
      type = "function",
      `function` = list(
        name = "get_concept_relationships",
        description = paste(
          "Get relationships for a concept (Maps to, Is a, Subsumes, etc.).",
          "Useful for finding standard equivalents of non-standard concepts."
        ),
        parameters = list(
          type = "object",
          properties = list(
            concept_id = list(type = "integer", description = "The OMOP concept_id to get relationships for")
          ),
          required = list("concept_id")
        )
      )
    )
  )

  # Drug-specific tools: OMOPHub NDC lookup and mapping
  drug_tools <- list(
    list(
      type = "function",
      `function` = list(
        name = "lookup_ndc",
        description = paste(
          "Look up an NDC (National Drug Code) in the OMOP vocabulary by exact code.",
          "Automatically tries multiple normalized variants: digits-only, 11-digit padded,",
          "hyphenated formats (5-4-1, 5-3-2, 4-4-2), and * replaced with 0.",
          "Returns the NDC concept AND the standard RxNorm concept it maps to.",
          "Use this FIRST for any source value that looks like an NDC code (mostly digits,",
          "possibly with hyphens, asterisks, or other separators)."
        ),
        parameters = list(
          type = "object",
          properties = list(
            ndc_code = list(type = "string", description = "The raw NDC code string (e.g. '00944262001', '58914*01310', '00078-0538-15')")
          ),
          required = list("ndc_code")
        )
      )
    ),
    list(
      type = "function",
      `function` = list(
        name = "search_ndc",
        description = paste(
          "Search OMOPHub for NDC concepts by text query.",
          "Good for searching NDC codes that may have unusual formatting.",
          "Returns matching concepts from the NDC vocabulary."
        ),
        parameters = list(
          type = "object",
          properties = list(
            query = list(type = "string", description = "NDC code or drug text to search for"),
            page_size = list(type = "integer", description = "Max results (default 10)")
          ),
          required = list("query")
        )
      )
    )
  )

  if (identical(domain, "drug")) {
    c(base_tools, drug_tools)
  } else {
    base_tools
  }
}

# Build named list of tool handler functions.
# Base handlers call Hecate; drug domain adds OMOPHub NDC handlers.
# Each handler takes a list of arguments and returns a JSON string.
build_tool_handlers <- function(hc, oh = NULL, domain = NULL) {
  handlers <- list(
    search_concepts = function(args) {
      Sys.sleep(0.3)
      result <- hecate_search(
        query = args$query,
        vocabulary_id = args$vocabulary_id,
        domain_id = args$domain_id,
        standard_concept = args$standard_concept,
        limit = args$limit %||% 25L,
        client = hc
      )
      jsonlite::toJSON(result, auto_unbox = TRUE, pretty = FALSE)
    },
    get_concept = function(args) {
      Sys.sleep(0.3)
      result <- hecate_get_concept(args$concept_id, client = hc)
      jsonlite::toJSON(result, auto_unbox = TRUE, pretty = FALSE)
    },
    get_concept_relationships = function(args) {
      Sys.sleep(0.3)
      result <- hecate_get_relationships(args$concept_id, client = hc)
      jsonlite::toJSON(result, auto_unbox = TRUE, pretty = FALSE)
    }
  )

  # Drug domain: add OMOPHub NDC handlers
  if (identical(domain, "drug") && !is.null(oh)) {
    handlers$lookup_ndc <- function(args) {
      result <- lookup_ndc_smart(args$ndc_code, oh_client = oh, hc_client = hc)
      jsonlite::toJSON(result, auto_unbox = TRUE, pretty = FALSE)
    }
    handlers$search_ndc <- function(args) {
      Sys.sleep(0.3)
      result <- omophub_search(
        query = args$query,
        vocabulary_ids = "NDC",
        domain_ids = "Drug",
        page_size = args$page_size %||% 10L,
        client = oh
      )
      jsonlite::toJSON(result, auto_unbox = TRUE, pretty = FALSE)
    }
  }

  handlers
}

# Domain-aware system prompt for GPT-4 vocabulary mapping.
build_mapping_system_prompt <- function(domain) {
  domain_desc <- switch(domain,
    condition = "diagnosis, problem, or medical condition",
    drug = "medication, drug, or immunization",
    measurement = "lab test, vital sign, or clinical measurement",
    measurement_value = "categorical lab result value (e.g., Positive, Normal, Pass)",
    procedure = "clinical procedure or therapy",
    observation = "clinical observation, allergy, or finding",
    "clinical concept"
  )

  domain_hints <- switch(domain,
    condition = paste(
      "You will receive both a problem code AND a text description when available.",
      "STRATEGY: Search by code first - it may be an ICD-10-CM, ICD-9-CM, or SNOMED code.",
      "Use search_concepts with the code and the appropriate vocabulary_id filter.",
      "Use the text description to verify your match or disambiguate between candidates.",
      "If the code search fails, search semantically by the problem description in SNOMED",
      "with domain_id='Condition' and standard_concept='S'.",
      "Use your medical knowledge to infer the correct SNOMED standard concept.",
      "For ICD codes, find the 'Maps to' standard SNOMED concept via get_concept_relationships."
    ),
    drug = paste(
      "You will receive BOTH a drug name AND an NDC code when available.",
      "STRATEGY: Use lookup_ndc with the NDC code FIRST - it automatically tries multiple",
      "normalizations (digits-only, 11-digit padded, 5-4-1, 5-3-2, 4-4-2 hyphenation).",
      "Common NDC issues in this data: * used instead of hyphens, missing hyphens, leading zeros stripped.",
      "If lookup_ndc finds the NDC, verify the result by checking that the returned drug concept",
      "matches the drug name provided. This cross-check ensures accuracy.",
      "If lookup_ndc fails, search by drug name in RxNorm with domain_id='Drug', standard_concept='S'.",
      "Prefer Ingredient-level RxNorm concepts. If the source is a brand name, find the generic ingredient.",
      "Use your pharmacological knowledge to identify drug classes, brand/generic equivalences,",
      "and common drug name variants (e.g., 'OMS' = oral morphine sulfate)."
    ),
    measurement = paste(
      "You will receive both a LOINC code AND a test name when available.",
      "STRATEGY: Search by LOINC code first using search_concepts with vocabulary_id='LOINC'.",
      "Use the test name to verify your match.",
      "If no LOINC code is available, search semantically by test name in LOINC vocabulary.",
      "Use domain_id='Measurement' and standard_concept='S'.",
      "Use your clinical laboratory knowledge to interpret test names and find the correct",
      "LOINC concept (e.g., 'CBC' maps to specific LOINC panel codes)."
    ),
    procedure = paste(
      "You will receive a procedure code, name, and source vocabulary when available.",
      "STRATEGY: Search by code in the specified vocabulary first (e.g., CPT4, HCPCS, SNOMED).",
      "Use search_concepts with vocabulary_id filter matching the source vocabulary.",
      "Use the procedure name to verify or search semantically if code lookup fails.",
      "Use domain_id='Procedure' and standard_concept='S'.",
      "Use your clinical knowledge to interpret procedure names and map to standard concepts."
    ),
    measurement_value = paste(
      "You will receive a categorical lab result value (like 'right ear pass', 'reactive', '20/20').",
      "Test LOINC and test name are provided as context to help understand the meaning.",
      "STRATEGY: Search for Meas Value concepts in the 'Meas Value' vocabulary first.",
      "Common Meas Value concepts: Normal (4069590), Abnormal (4135493), Positive (9191),",
      "Negative (9189), Present (4181412), Absent (4132135), High (4328749), Low (4267416),",
      "Pass (4077375), Trace (9192), Reactive (9191), Non-reactive (9190), Equivocal (45877994).",
      "If no Meas Value concept fits, search SNOMED with domain_id='Meas Value' or 'Observation'.",
      "The concept should represent the RESULT VALUE, not the test itself.",
      "Use your clinical knowledge to interpret result descriptions in the context of the test."
    ),
    observation = paste(
      "You will receive an allergen name and/or a drug code with vocabulary when available.",
      "STRATEGY: Search by allergen name in SNOMED with domain_id='Observation', standard_concept='S'.",
      "If a drug code and vocabulary are provided (CVX, NDC), search by code first.",
      "Use your clinical knowledge to map allergy descriptions to SNOMED standard concepts.",
      "Allergies to drugs should map to SNOMED allergy concepts, not drug concepts."
    ),
    "Search with standard_concept='S'."
  )

  # Drug domain gets extra tool descriptions
  tools_desc <- if (domain == "drug") {
    paste0(
      "## Your Tools\n",
      "You have access to two vocabulary APIs:\n\n",
      "**NDC Lookup (OMOPHub) - use for code-based lookups:**\n",
      "- lookup_ndc: Smart NDC lookup - tries multiple normalized variants automatically.\n",
      "  Returns the NDC concept AND the standard RxNorm concept it maps to.\n",
      "- search_ndc: Search NDC vocabulary by text query.\n\n",
      "**Semantic Search (Hecate) - use for name-based and general searches:**\n",
      "- search_concepts: Semantic search across all OMOP vocabularies\n",
      "- get_concept: Look up a specific concept by ID\n",
      "- get_concept_relationships: Find related concepts (Maps to, Is a, etc.)\n"
    )
  } else {
    paste0(
      "## Your Tools\n",
      "You have access to the Hecate OMOP vocabulary search API:\n",
      "- search_concepts: Search by text across all OMOP vocabularies\n",
      "  (supports filters: vocabulary_id, domain_id, standard_concept, limit)\n",
      "- get_concept: Look up a specific concept by ID\n",
      "- get_concept_relationships: Find related concepts (Maps to, Is a, etc.)\n"
    )
  }

  paste0(
    "You are an OMOP CDM vocabulary mapping specialist with deep clinical coding knowledge. ",
    "Your task is to find the best standard OMOP concept_id for a given source value from ",
    "a clinical data system.\n\n",
    "## Context\n",
    "- The source value comes from the \"", domain, "\" domain of a healthcare ETL\n",
    "- It represents a ", domain_desc, " that could not be automatically mapped\n",
    "- You are given rich context (code + name + vocabulary) when available\n\n",
    tools_desc, "\n",
    "## Instructions\n",
    "1. Analyze ALL provided context (code, name, description, vocabulary) to understand ",
    "what clinical concept this represents\n",
    "2. ", domain_hints, "\n",
    "3. If the first search yields no good results, try alternative terms, synonyms, ",
    "abbreviations, brand/generic names, or broader/narrower terms\n",
    "4. Use your medical coding knowledge to make inferences when the source value is ",
    "ambiguous or abbreviated\n",
    "5. Verify your chosen concept with get_concept to confirm it is Standard ",
    "(standard_concept='S') and in the correct domain\n",
    "6. If the concept is non-standard, use get_concept_relationships to find the ",
    "'Maps to' standard concept\n",
    "7. When both code and name are available, cross-check: the mapped concept should ",
    "be consistent with both pieces of information\n\n",
    "## Required Output Format\n",
    "Respond with EXACTLY this JSON (no markdown, no extra text):\n",
    "{\n",
    "  \"concept_id\": <integer or null if no mapping found>,\n",
    "  \"concept_name\": \"<name of the chosen concept>\",\n",
    "  \"vocabulary_id\": \"<vocabulary of the chosen concept>\",\n",
    "  \"confidence\": <float 0.0 to 1.0>,\n",
    "  \"reasoning\": \"<1-2 sentence explanation>\",\n",
    "  \"source_is_ndc\": <true/false - set true if the source value is an NDC code>,\n",
    "  \"ndc_normalized\": \"<the normalized NDC code if source_is_ndc is true, else null>\"\n",
    "}\n\n",
    "## Confidence Guidelines\n",
    "- 1.0: Exact code match verified against name (both code and name confirm the same concept)\n",
    "- 0.9: Very high confidence - code or name matches with confirmed standard concept\n",
    "- 0.7-0.8: Good semantic match from name/description, correct domain and vocabulary\n",
    "- 0.5-0.6: Partial match, may be broader/narrower than ideal, or only name OR code matched\n",
    "- 0.3-0.4: Weak match based on inference, uncertain\n",
    "- 0.0: No mapping found"
  )
}
