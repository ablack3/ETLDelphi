-- DuckDB OMOP CDM 5.4: Non-unique indexes for VOCABULARY TABLES ONLY.
-- Apply after loading vocabulary data. Primary keys are defined inline in the DDL (column PRIMARY KEY).
-- No ALTER TABLE / foreign keys (not supported in DuckDB). Schema placeholder: @cdmDatabaseSchema (e.g. cdm).

-- ========== Non-unique indexes (vocab tables only) ==========
CREATE INDEX idx_concept_code ON @cdmDatabaseSchema.concept(concept_code);
CREATE INDEX idx_concept_vocabulary_id ON @cdmDatabaseSchema.concept(vocabulary_id);
CREATE INDEX idx_concept_domain_id ON @cdmDatabaseSchema.concept(domain_id);
CREATE INDEX idx_concept_class_id ON @cdmDatabaseSchema.concept(concept_class_id);
CREATE INDEX idx_concept_relationship_id_1 ON @cdmDatabaseSchema.concept_relationship(concept_id_1);
CREATE INDEX idx_concept_relationship_id_2 ON @cdmDatabaseSchema.concept_relationship(concept_id_2);
CREATE INDEX idx_concept_relationship_id_3 ON @cdmDatabaseSchema.concept_relationship(relationship_id);
CREATE INDEX idx_concept_synonym_id ON @cdmDatabaseSchema.concept_synonym(concept_id);
CREATE INDEX idx_concept_ancestor_id_1 ON @cdmDatabaseSchema.concept_ancestor(ancestor_concept_id);
CREATE INDEX idx_concept_ancestor_id_2 ON @cdmDatabaseSchema.concept_ancestor(descendant_concept_id);
CREATE INDEX idx_drug_strength_id_1 ON @cdmDatabaseSchema.drug_strength(drug_concept_id);
CREATE INDEX idx_drug_strength_id_2 ON @cdmDatabaseSchema.drug_strength(ingredient_concept_id);
