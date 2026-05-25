import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------
# Read Source Tables from S3
# ----------------------------
population_person_demographics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/population_person_demographics_silver.{FILE_FORMAT}/")
)
population_person_demographics_silver_df.createOrReplaceTempView(
    "population_person_demographics_silver"
)

population_disease_events_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/population_disease_events_silver.{FILE_FORMAT}/")
)
population_disease_events_silver_df.createOrReplaceTempView(
    "population_disease_events_silver"
)

genomics_variants_observed_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomics_variants_observed_silver.{FILE_FORMAT}/")
)
genomics_variants_observed_silver_df.createOrReplaceTempView(
    "genomics_variants_observed_silver"
)

genomics_variant_reference_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomics_variant_reference_silver.{FILE_FORMAT}/")
)
genomics_variant_reference_silver_df.createOrReplaceTempView(
    "genomics_variant_reference_silver"
)

sequencing_runs_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_runs_silver.{FILE_FORMAT}/")
)
sequencing_runs_silver_df.createOrReplaceTempView("sequencing_runs_silver")

labs_biomarkers_results_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/labs_biomarkers_results_silver.{FILE_FORMAT}/")
)
labs_biomarkers_results_silver_df.createOrReplaceTempView(
    "labs_biomarkers_results_silver"
)

# ---------------------------------------------------------
# Target: gold.gold_population_person_demographics (gppd)
# Source: silver.population_person_demographics_silver ppd_slv
# ---------------------------------------------------------
gold_population_person_demographics_df = spark.sql(
    """
SELECT
  TRIM(ppd_slv.patient_id)                             AS person_id,
  CAST(ppd_slv.date_of_birth AS date)                  AS birth_date,
  TRIM(ppd_slv.gender)                                 AS gender_identity,
  TRIM(ppd_slv.ethnicity)                              AS ethnicity,
  TRIM(ppd_slv.country)                                AS country,
  TRIM(ppd_slv.state)                                  AS state_province,
  TRIM(ppd_slv.city)                                   AS city
FROM population_person_demographics_silver ppd_slv
"""
)

(
    gold_population_person_demographics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_population_person_demographics.csv")
)

# ------------------------------------------------------
# Target: gold.gold_population_disease_events (gpde)
# Source: silver.population_disease_events_silver pde_slv
# Note: No column mappings provided in UDT for this target.
# ------------------------------------------------------
gold_population_disease_events_df = spark.sql(
    """
SELECT
  TRIM(pde_slv.disease_event_id)                        AS disease_event_id,
  TRIM(pde_slv.patient_id)                              AS person_id,
  TRIM(pde_slv.diagnosis)                               AS condition_name,
  CAST(pde_slv.registration_date AS date)               AS event_date
FROM population_disease_events_silver pde_slv
"""
)

(
    gold_population_disease_events_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_population_disease_events.csv")
)

# -------------------------------------------------------------------------
# Target: gold.gold_genomics_variants_observed (ggvo)
# Source: silver.genomics_variants_observed_silver gvo_slv
#         INNER JOIN silver.sequencing_runs_silver sr_slv ON gvo_slv.run_id = sr_slv.run_id
# -------------------------------------------------------------------------
gold_genomics_variants_observed_df = spark.sql(
    """
SELECT
  TRIM(gvo_slv.variant_id)                              AS variant_id,
  TRIM(gvo_slv.patient_id)                              AS person_id,
  TRIM(gvo_slv.run_id)                                  AS test_id,
  TRIM(gvo_slv.chromosome)                              AS chromosome,
  CAST(gvo_slv.genomic_position AS int)                 AS position_hg38,
  TRIM(gvo_slv.reference_allele)                        AS ref_allele,
  TRIM(gvo_slv.alternate_allele)                        AS alt_allele,
  TRIM(gvo_slv.gene_name)                               AS gene_symbol,
  TRIM(gvo_slv.variant_type)                            AS variant_class,
  TRIM(gvo_slv.clinical_significance)                   AS clinical_significance,
  CAST(gvo_slv.pathogenicity_score AS float)            AS pathogenicity,
  CAST(gvo_slv.detected_date AS date)                   AS interpretation_date
FROM genomics_variants_observed_silver gvo_slv
INNER JOIN sequencing_runs_silver sr_slv
  ON gvo_slv.run_id = sr_slv.run_id
"""
)

(
    gold_genomics_variants_observed_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_genomics_variants_observed.csv")
)

# --------------------------------------------------------
# Target: gold.gold_genomics_variant_reference (ggvr)
# Source: silver.genomics_variant_reference_silver gvr_slv
# Note: No column mappings provided in UDT for this target.
# --------------------------------------------------------
gold_genomics_variant_reference_df = spark.sql(
    """
SELECT
  *
FROM genomics_variant_reference_silver gvr_slv
"""
)

(
    gold_genomics_variant_reference_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_genomics_variant_reference.csv")
)

# ---------------------------------------------
# Target: gold.gold_sequencing_runs (gsr)
# Source: silver.sequencing_runs_silver sr_slv
# ---------------------------------------------
gold_sequencing_runs_df = spark.sql(
    """
SELECT
  TRIM(sr_slv.run_id)                                   AS run_id,
  TRIM(sr_slv.patient_id)                               AS person_id,
  TRIM(sr_slv.sample_id)                                AS specimen_id,
  TRIM(sr_slv.run_id)                                   AS test_id,
  TRIM(sr_slv.sequencing_platform)                      AS platform,
  CAST(sr_slv.read_length AS int)                       AS read_length,
  CAST(sr_slv.coverage_depth AS double)                 AS coverage_mean,
  CAST(sr_slv.alignment_rate AS double)                 AS mapping_rate,
  CAST(sr_slv.run_date AS date)                         AS run_date,
  TRIM(sr_slv.sequencing_center)                        AS lab_site,
  TRIM(sr_slv.processing_status)                        AS pass_fail_flag
FROM sequencing_runs_silver sr_slv
"""
)

(
    gold_sequencing_runs_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sequencing_runs.csv")
)

# ---------------------------------------------------
# Target: gold.gold_labs_biomarkers_results (glbr)
# Source: silver.labs_biomarkers_results_silver lbr_slv
# ---------------------------------------------------
gold_labs_biomarkers_results_df = spark.sql(
    """
SELECT
  TRIM(lbr_slv.result_id)                               AS lab_result_id,
  TRIM(lbr_slv.patient_id)                              AS person_id,
  TRIM(lbr_slv.sample_id)                               AS specimen_id,
  CAST(lbr_slv.collection_date AS date)                 AS collection_date,
  CAST(lbr_slv.result_date AS date)                     AS result_date,
  TRIM(lbr_slv.biomarker)                               AS analyte_name,
  TRIM(lbr_slv.test_result)                             AS result_value_text,
  TRIM(lbr_slv.unit)                                    AS unit,
  TRIM(lbr_slv.reference_range)                         AS reference_range_high,
  TRIM(lbr_slv.lab_name)                                AS lab_site
FROM labs_biomarkers_results_silver lbr_slv
"""
)

(
    gold_labs_biomarkers_results_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_labs_biomarkers_results.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_population_variant_prevalence_daily (gpvpd)
# Source: silver.genomics_variants_observed_silver gvo_slv
#         INNER JOIN silver.population_person_demographics_silver ppd_slv ON gvo_slv.patient_id = ppd_slv.patient_id
# Note: No column mappings provided in UDT for this target.
# -----------------------------------------------------------------------------------
gold_population_variant_prevalence_daily_df = spark.sql(
    """
SELECT
  gvo_slv.*,
  ppd_slv.date_of_birth AS ppd_date_of_birth,
  ppd_slv.gender AS ppd_gender,
  ppd_slv.ethnicity AS ppd_ethnicity,
  ppd_slv.country AS ppd_country,
  ppd_slv.state AS ppd_state,
  ppd_slv.city AS ppd_city
FROM genomics_variants_observed_silver gvo_slv
INNER JOIN population_person_demographics_silver ppd_slv
  ON gvo_slv.patient_id = ppd_slv.patient_id
"""
)

(
    gold_population_variant_prevalence_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_population_variant_prevalence_daily.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_disease_variant_correlation (gdvc)
# Source: silver.population_disease_events_silver pde_slv
#         INNER JOIN silver.genomics_variants_observed_silver gvo_slv ON pde_slv.patient_id = gvo_slv.patient_id
#         INNER JOIN silver.population_person_demographics_silver ppd_slv ON pde_slv.patient_id = ppd_slv.patient_id
# Note: No column mappings provided in UDT for this target.
# -----------------------------------------------------------------------------------
gold_disease_variant_correlation_df = spark.sql(
    """
SELECT
  pde_slv.*,
  gvo_slv.variant_id AS gvo_variant_id,
  gvo_slv.run_id AS gvo_run_id,
  gvo_slv.chromosome AS gvo_chromosome,
  gvo_slv.genomic_position AS gvo_genomic_position,
  gvo_slv.reference_allele AS gvo_reference_allele,
  gvo_slv.alternate_allele AS gvo_alternate_allele,
  gvo_slv.gene_name AS gvo_gene_name,
  gvo_slv.variant_type AS gvo_variant_type,
  gvo_slv.clinical_significance AS gvo_clinical_significance,
  gvo_slv.pathogenicity_score AS gvo_pathogenicity_score,
  gvo_slv.detected_date AS gvo_detected_date,
  ppd_slv.date_of_birth AS ppd_date_of_birth,
  ppd_slv.gender AS ppd_gender,
  ppd_slv.ethnicity AS ppd_ethnicity,
  ppd_slv.country AS ppd_country,
  ppd_slv.state AS ppd_state,
  ppd_slv.city AS ppd_city
FROM population_disease_events_silver pde_slv
INNER JOIN genomics_variants_observed_silver gvo_slv
  ON pde_slv.patient_id = gvo_slv.patient_id
INNER JOIN population_person_demographics_silver ppd_slv
  ON pde_slv.patient_id = ppd_slv.patient_id
"""
)

(
    gold_disease_variant_correlation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_disease_variant_correlation.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_high_risk_population_segments (ghrps)
# Source: silver.genomics_variants_observed_silver gvo_slv
#         INNER JOIN silver.population_person_demographics_silver ppd_slv ON gvo_slv.patient_id = ppd_slv.patient_id
# Note: No column mappings provided in UDT for this target.
# -----------------------------------------------------------------------------------
gold_high_risk_population_segments_df = spark.sql(
    """
SELECT
  gvo_slv.*,
  ppd_slv.date_of_birth AS ppd_date_of_birth,
  ppd_slv.gender AS ppd_gender,
  ppd_slv.ethnicity AS ppd_ethnicity,
  ppd_slv.country AS ppd_country,
  ppd_slv.state AS ppd_state,
  ppd_slv.city AS ppd_city
FROM genomics_variants_observed_silver gvo_slv
INNER JOIN population_person_demographics_silver ppd_slv
  ON gvo_slv.patient_id = ppd_slv.patient_id
"""
)

(
    gold_high_risk_population_segments_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_high_risk_population_segments.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_regional_disease_trends_monthly (grdtm)
# Source: silver.population_disease_events_silver pde_slv
# Note: No column mappings provided in UDT for this target.
# -----------------------------------------------------------------------------------
gold_regional_disease_trends_monthly_df = spark.sql(
    """
SELECT
  *
FROM population_disease_events_silver pde_slv
"""
)

(
    gold_regional_disease_trends_monthly_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_regional_disease_trends_monthly.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_regional_biomarker_abnormality_monthly (grbam)
# Source: silver.labs_biomarkers_results_silver lbr_slv
#         INNER JOIN silver.population_person_demographics_silver ppd_slv ON lbr_slv.patient_id = ppd_slv.patient_id
# Note: No column mappings provided in UDT for this target.
# -----------------------------------------------------------------------------------
gold_regional_biomarker_abnormality_monthly_df = spark.sql(
    """
SELECT
  lbr_slv.*,
  ppd_slv.date_of_birth AS ppd_date_of_birth,
  ppd_slv.gender AS ppd_gender,
  ppd_slv.ethnicity AS ppd_ethnicity,
  ppd_slv.country AS ppd_country,
  ppd_slv.state AS ppd_state,
  ppd_slv.city AS ppd_city
FROM labs_biomarkers_results_silver lbr_slv
INNER JOIN population_person_demographics_silver ppd_slv
  ON lbr_slv.patient_id = ppd_slv.patient_id
"""
)

(
    gold_regional_biomarker_abnormality_monthly_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_regional_biomarker_abnormality_monthly.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_public_health_dashboard_metrics_daily (gphdmd)
# Source: silver.population_disease_events_silver pde_slv
#         INNER JOIN silver.population_person_demographics_silver ppd_slv ON pde_slv.patient_id = ppd_slv.patient_id
#         LEFT JOIN silver.genomics_variants_observed_silver gvo_slv ON ppd_slv.patient_id = gvo_slv.patient_id
#         LEFT JOIN silver.labs_biomarkers_results_silver lbr_slv ON ppd_slv.patient_id = lbr_slv.patient_id
# Note: No column mappings provided in UDT for this target.
# -----------------------------------------------------------------------------------
gold_public_health_dashboard_metrics_daily_df = spark.sql(
    """
SELECT
  pde_slv.*,
  ppd_slv.date_of_birth AS ppd_date_of_birth,
  ppd_slv.gender AS ppd_gender,
  ppd_slv.ethnicity AS ppd_ethnicity,
  ppd_slv.country AS ppd_country,
  ppd_slv.state AS ppd_state,
  ppd_slv.city AS ppd_city,
  gvo_slv.variant_id AS gvo_variant_id,
  gvo_slv.run_id AS gvo_run_id,
  gvo_slv.chromosome AS gvo_chromosome,
  gvo_slv.genomic_position AS gvo_genomic_position,
  gvo_slv.reference_allele AS gvo_reference_allele,
  gvo_slv.alternate_allele AS gvo_alternate_allele,
  gvo_slv.gene_name AS gvo_gene_name,
  gvo_slv.variant_type AS gvo_variant_type,
  gvo_slv.clinical_significance AS gvo_clinical_significance,
  gvo_slv.pathogenicity_score AS gvo_pathogenicity_score,
  gvo_slv.detected_date AS gvo_detected_date,
  lbr_slv.result_id AS lbr_result_id,
  lbr_slv.sample_id AS lbr_sample_id,
  lbr_slv.collection_date AS lbr_collection_date,
  lbr_slv.result_date AS lbr_result_date,
  lbr_slv.biomarker AS lbr_biomarker,
  lbr_slv.test_result AS lbr_test_result,
  lbr_slv.unit AS lbr_unit,
  lbr_slv.reference_range AS lbr_reference_range,
  lbr_slv.lab_name AS lbr_lab_name
FROM population_disease_events_silver pde_slv
INNER JOIN population_person_demographics_silver ppd_slv
  ON pde_slv.patient_id = ppd_slv.patient_id
LEFT JOIN genomics_variants_observed_silver gvo_slv
  ON ppd_slv.patient_id = gvo_slv.patient_id
LEFT JOIN labs_biomarkers_results_silver lbr_slv
  ON ppd_slv.patient_id = lbr_slv.patient_id
"""
)

(
    gold_public_health_dashboard_metrics_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_public_health_dashboard_metrics_daily.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_data_lineage_quality_audit (gdla)
# Source: silver.population_person_demographics_silver ppd_slv
#         INNER JOIN silver.population_disease_events_silver pde_slv ON ppd_slv.patient_id = pde_slv.patient_id
#         INNER JOIN silver.genomics_variants_observed_silver gvo_slv ON ppd_slv.patient_id = gvo_slv.patient_id
#         INNER JOIN silver.sequencing_runs_silver sr_slv ON gvo_slv.run_id = sr_slv.run_id
#         INNER JOIN silver.labs_biomarkers_results_silver lbr_slv ON ppd_slv.patient_id = lbr_slv.patient_id
# Note: No column mappings provided in UDT for this target.
# -----------------------------------------------------------------------------------
gold_data_lineage_quality_audit_df = spark.sql(
    """
SELECT
  ppd_slv.*,
  pde_slv.disease_event_id AS pde_disease_event_id,
  pde_slv.diagnosis AS pde_diagnosis,
  pde_slv.registration_date AS pde_registration_date,
  gvo_slv.variant_id AS gvo_variant_id,
  gvo_slv.run_id AS gvo_run_id,
  gvo_slv.chromosome AS gvo_chromosome,
  gvo_slv.genomic_position AS gvo_genomic_position,
  gvo_slv.reference_allele AS gvo_reference_allele,
  gvo_slv.alternate_allele AS gvo_alternate_allele,
  gvo_slv.gene_name AS gvo_gene_name,
  gvo_slv.variant_type AS gvo_variant_type,
  gvo_slv.clinical_significance AS gvo_clinical_significance,
  gvo_slv.pathogenicity_score AS gvo_pathogenicity_score,
  gvo_slv.detected_date AS gvo_detected_date,
  sr_slv.run_id AS sr_run_id,
  sr_slv.sample_id AS sr_sample_id,
  sr_slv.sequencing_platform AS sr_sequencing_platform,
  sr_slv.read_length AS sr_read_length,
  sr_slv.coverage_depth AS sr_coverage_depth,
  sr_slv.alignment_rate AS sr_alignment_rate,
  sr_slv.run_date AS sr_run_date,
  sr_slv.sequencing_center AS sr_sequencing_center,
  sr_slv.processing_status AS sr_processing_status,
  lbr_slv.result_id AS lbr_result_id,
  lbr_slv.sample_id AS lbr_sample_id,
  lbr_slv.collection_date AS lbr_collection_date,
  lbr_slv.result_date AS lbr_result_date,
  lbr_slv.biomarker AS lbr_biomarker,
  lbr_slv.test_result AS lbr_test_result,
  lbr_slv.unit AS lbr_unit,
  lbr_slv.reference_range AS lbr_reference_range,
  lbr_slv.lab_name AS lbr_lab_name
FROM population_person_demographics_silver ppd_slv
INNER JOIN population_disease_events_silver pde_slv
  ON ppd_slv.patient_id = pde_slv.patient_id
INNER JOIN genomics_variants_observed_silver gvo_slv
  ON ppd_slv.patient_id = gvo_slv.patient_id
INNER JOIN sequencing_runs_silver sr_slv
  ON gvo_slv.run_id = sr_slv.run_id
INNER JOIN labs_biomarkers_results_silver lbr_slv
  ON ppd_slv.patient_id = lbr_slv.patient_id
"""
)

(
    gold_data_lineage_quality_audit_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_lineage_quality_audit.csv")
)

job.commit()
