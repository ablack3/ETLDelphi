#!/usr/bin/env Rscript
library(CDMConnector)
library(dplyr)

con <- DBI::dbConnect(duckdb::duckdb(), "~/Desktop/delphi.duckdb")
cdm <- cdmFromCon(con, "cdm", "main")

# Ancestor concept
ancestor_id <- 439392L

# Get ancestor concept name
ancestor_name <- cdm$concept %>%
  filter(concept_id == ancestor_id) %>%
  pull(concept_name)
cat("=== Ancestor concept 439392 ===\n")
cat(paste0(ancestor_id, ": ", coalesce(ancestor_name, "(unknown)"), "\n\n"))

# All descendants (including self)
descendants <- cdm$concept_ancestor %>%
  filter(ancestor_concept_id == ancestor_id) %>%
  left_join(select(cdm$concept, concept_id, concept_name, domain_id), 
            by = c("descendant_concept_id" = "concept_id")) %>%
  select(descendant_concept_id, concept_name, domain_id) %>%
  collect()

cat("=== Descendant count ===\n")
cat(nrow(descendants), "descendants (including self)\n\n")

# Record counts in condition_occurrence for these concepts (copy=TRUE for local df)
counts <- cdm$condition_occurrence %>%
  inner_join(
    descendants %>% select(descendant_concept_id),
    by = c("condition_concept_id" = "descendant_concept_id"),
    copy = TRUE
  ) %>%
  left_join(
    descendants %>% select(descendant_concept_id, concept_name),
    by = c("condition_concept_id" = "descendant_concept_id"),
    copy = TRUE
  ) %>%
  count(condition_concept_id, concept_name, sort = TRUE) %>%
  collect()

cat("=== Record counts in condition_occurrence (descendants of 439392) ===\n")
print(counts)
cat("\nTotal records:", sum(counts$n), "\n")

DBI::dbDisconnect(con, shutdown = TRUE)
cat("\nDone.\n")
