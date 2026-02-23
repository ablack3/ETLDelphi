# openai_mapping.R
# Internal: OpenAI chat completions client with function-calling loop for
# vocabulary mapping. Not exported; called by improve_mappings().

# Single OpenAI chat completion call via httr2.
# Returns parsed response body (list).
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

  resp <- httr2::request(paste0(base_url, "/chat/completions")) |>
    httr2::req_headers(
      Authorization = paste("Bearer", api_key),
      `Content-Type` = "application/json"
    ) |>
    httr2::req_body_json(body, auto_unbox = TRUE) |>
    httr2::req_timeout(120) |>
    httr2::req_retry(max_tries = 3, backoff = ~ 2) |>
    httr2::req_perform()

  httr2::resp_body_json(resp)
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

# OpenAI function-calling tool definitions for Hecate vocabulary search.
mapping_tools <- function() {
  list(
    list(
      type = "function",
      `function` = list(
        name = "search_concepts",
        description = paste(
          "Search the OMOP Standardized Vocabulary for concepts.",
          "Returns concept_id, concept_name, domain_id, vocabulary_id,",
          "concept_class_id, standard_concept, and similarity score."
        ),
        parameters = list(
          type = "object",
          properties = list(
            query = list(type = "string", description = "Search text (concept name, code, or description)"),
            vocabulary_id = list(type = "string", description = "Filter by vocabulary: SNOMED, ICD10CM, ICD9CM, RxNorm, LOINC, CPT4, HCPCS, UCUM, etc."),
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
}

# Build named list of tool handler functions that call Hecate API.
# Each handler takes a list of arguments and returns a JSON string.
build_tool_handlers <- function(hc) {
  list(
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
}

# Domain-aware system prompt for GPT-4 vocabulary mapping.
build_mapping_system_prompt <- function(domain) {
  domain_desc <- switch(domain,
    condition = "diagnosis, problem, or medical condition",
    drug = "medication, drug, or immunization",
    measurement = "lab test, vital sign, or clinical measurement",
    procedure = "clinical procedure or therapy",
    observation = "clinical observation, allergy, or finding",
    "clinical concept"
  )

  domain_hints <- switch(domain,
    condition = "Search SNOMED with domain_id='Condition' and standard_concept='S'. If the source looks like an ICD code, search for it by code first.",
    drug = "Search RxNorm with domain_id='Drug' and standard_concept='S'. Prefer Ingredient-level concepts. If source is a brand name, find the generic ingredient.",
    measurement = "Search LOINC or SNOMED with domain_id='Measurement' and standard_concept='S'. For lab tests, LOINC is preferred.",
    procedure = "Search SNOMED, CPT4, or HCPCS with domain_id='Procedure' and standard_concept='S'.",
    observation = "Search SNOMED with domain_id='Observation' and standard_concept='S'.",
    "Search with standard_concept='S'."
  )

  paste0(
    "You are an OMOP CDM vocabulary mapping specialist. Your task is to find the best ",
    "standard OMOP concept_id for a given source value from a clinical data system.\n\n",
    "## Context\n",
    "- The source value comes from the \"", domain, "\" domain of a healthcare ETL\n",
    "- It represents a ", domain_desc, " that could not be automatically mapped\n\n",
    "## Your Tools\n",
    "You have access to the Hecate OMOP vocabulary search API:\n",
    "- search_concepts: Search by text across all OMOP vocabularies\n",
    "- get_concept: Look up a specific concept by ID\n",
    "- get_concept_relationships: Find related concepts (Maps to, Is a, etc.)\n\n",
    "## Instructions\n",
    "1. Analyze the source value to understand what clinical concept it represents\n",
    "2. ", domain_hints, "\n",
    "3. If the first search yields no good results, try alternative terms, synonyms, ",
    "or broader/narrower terms\n",
    "4. Verify your chosen concept with get_concept to confirm it is Standard ",
    "(standard_concept='S') and in the correct domain\n",
    "5. If the concept is non-standard, use get_concept_relationships to find the ",
    "'Maps to' standard concept\n\n",
    "## Required Output Format\n",
    "Respond with EXACTLY this JSON (no markdown, no extra text):\n",
    "{\n",
    "  \"concept_id\": <integer or null if no mapping found>,\n",
    "  \"concept_name\": \"<name of the chosen concept>\",\n",
    "  \"vocabulary_id\": \"<vocabulary of the chosen concept>\",\n",
    "  \"confidence\": <float 0.0 to 1.0>,\n",
    "  \"reasoning\": \"<1-2 sentence explanation>\"\n",
    "}\n\n",
    "## Confidence Guidelines\n",
    "- 1.0: Exact code match (source code directly maps to OMOP concept)\n",
    "- 0.9: Very high confidence name/code match with confirmed standard concept\n",
    "- 0.7-0.8: Good semantic match, correct domain and vocabulary\n",
    "- 0.5-0.6: Partial match, may be broader/narrower than ideal\n",
    "- 0.3-0.4: Weak match, uncertain\n",
    "- 0.0: No mapping found"
  )
}
