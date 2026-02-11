#!/usr/bin/env Rscript
library(CDMConnector)
library(dplyr)

con <- DBI::dbConnect(duckdb::duckdb(), "~/Desktop/delphi.duckdb")
cdm <- cdmFromCon(con, "cdm", "main")

cat("=== 1. Total condition_occurrence rows ===\n")
print(cdm$condition_occurrence %>% tally())

cat("\n=== 2. Top condition types (by concept_id) ===\n")
top_conds <- cdm$condition_occurrence %>%
  left_join(select(cdm$concept, concept_id, concept_name, domain_id), 
            by = c("condition_concept_id" = "concept_id")) %>%
  filter(condition_concept_id > 0) %>%
  count(condition_concept_id, concept_name, sort = TRUE)
print(head(top_conds, 20))

cat("\n=== 3. Cancer via concept_ancestor (4134596 Malignant neoplastic disease) ===\n")
cancer_ancestor <- tryCatch({
  cdm$condition_occurrence %>%
    inner_join(
      cdm$concept_ancestor %>%
        filter(ancestor_concept_id == 4134596) %>%
        select(descendant_concept_id),
      by = c("condition_concept_id" = "descendant_concept_id")
    ) %>%
    tally()
}, error = function(e) { tibble(n = NA); cat("Error:", conditionMessage(e), "\n") })
print(cancer_ancestor)

cat("\n=== 4. ICD-10 malignant neoplasms (source starts with C) ===\n")
icd10_c <- cdm$condition_occurrence %>%
  filter(substr(condition_source_value, 1, 1) == "C") %>%
  count(condition_source_value, sort = TRUE)
print(head(icd10_c, 20))
cat("Total rows with C* codes:", nrow(icd10_c), "\n")
cat("Total count:", sum(icd10_c$n), "\n")

DBI::dbDisconnect(con, shutdown = TRUE)
cat("\nDone.\n")
