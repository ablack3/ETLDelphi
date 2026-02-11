#!/usr/bin/env Rscript
library(CDMConnector)
library(dplyr)

con <- DBI::dbConnect(duckdb::duckdb(), "~/Desktop/delphi.duckdb")
cdm <- cdmFromCon(con, "cdm", "main")

TARGET <- 439392L

# Get all ancestors of 439392 (includes self via min_levels_of_separation = 0)
ancestors <- cdm$concept_ancestor %>%
  filter(descendant_concept_id == TARGET) %>%
  select(ancestor_concept_id) %>%
  pull(ancestor_concept_id) %>%
  unique()

# Include 439392 itself in case it's not in concept_ancestor (self-ancestor)
ancestor_ids <- unique(c(ancestors, TARGET))

cat("=== Ancestors of", TARGET, "(count:", length(ancestor_ids), ") ===\n")

# Get concept names for these ancestors
ancestor_names <- cdm$concept %>%
  filter(concept_id %in% ancestor_ids) %>%
  select(concept_id, concept_name, domain_id) %>%
  collect()

print(ancestor_names)

# Get condition_occurrence counts for these concepts
counts <- cdm$condition_occurrence %>%
  filter(condition_concept_id %in% !!ancestor_ids) %>%
  count(condition_concept_id, sort = TRUE) %>%
  collect()

# Join to concept names
result <- counts %>%
  left_join(ancestor_names, by = c("condition_concept_id" = "concept_id")) %>%
  arrange(desc(n))

cat("\n=== Condition occurrence counts for ancestor concepts ===\n")
print(result)

cat("\n=== Summary ===\n")
cat("Total condition rows matching ancestor concepts:", sum(result$n), "\n")

DBI::dbDisconnect(con, shutdown = TRUE)
cat("\nDone.\n")
