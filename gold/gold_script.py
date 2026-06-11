import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------
# Read source tables from S3
# -----------------------------
patient_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_silver.{FILE_FORMAT}/")
)

specimen_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/specimen_silver.{FILE_FORMAT}/")
)

sequencing_run_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_run_silver.{FILE_FORMAT}/")
)

genomic_variant_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomic_variant_silver.{FILE_FORMAT}/")
)

lab_result_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_result_silver.{FILE_FORMAT}/")
)

# -----------------------------
# Create temp views
# -----------------------------
patient_silver_df.createOrReplaceTempView("patient_silver")
specimen_silver_df.createOrReplaceTempView("specimen_silver")
sequencing_run_silver_df.createOrReplaceTempView("sequencing_run_silver")
genomic_variant_silver_df.createOrReplaceTempView("genomic_variant_silver")
lab_result_silver_df.createOrReplaceTempView("lab_result_silver")

# ============================================================
# Target: gold_patient_profile
# ============================================================
gold_patient_profile_df = spark.sql(
    """
    SELECT
      CAST(ps.patient_id AS STRING)        AS patient_id,
      CAST(ps.first_name AS STRING)        AS first_name,
      CAST(ps.last_name AS STRING)         AS last_name,
      CAST(ps.date_of_birth AS DATE)       AS date_of_birth,
      CAST(ps.gender AS STRING)            AS sex_at_birth,
      CAST(ps.ethnicity AS STRING)         AS ethnicity,
      CAST(ps.state AS STRING)             AS address_state,
      CAST(ps.country AS STRING)           AS address_country
    FROM patient_silver ps
    """
)

(
    gold_patient_profile_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_profile.csv")
)

# ============================================================
# Target: gold_specimen
# ============================================================
gold_specimen_df = spark.sql(
    """
    SELECT
      CAST(ss.sample_id AS STRING)  AS source_specimen_id,
      CAST(ss.patient_id AS STRING) AS patient_id
    FROM specimen_silver ss
    INNER JOIN patient_silver ps
      ON ss.patient_id = ps.patient_id
    """
)

(
    gold_specimen_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_specimen.csv")
)

# ============================================================
# Target: gold_sequencing_run
# ============================================================
gold_sequencing_run_df = spark.sql(
    """
    SELECT
      CAST(srs.run_id AS STRING)                AS source_run_id,
      CAST(srs.patient_id AS STRING)            AS patient_id,
      CAST(srs.sample_id AS STRING)             AS specimen_id,
      CAST(srs.sequencing_platform AS STRING)   AS platform,
      CAST(srs.run_date AS DATE)                AS run_datetime,
      CAST(srs.processing_status AS STRING)     AS run_status
    FROM sequencing_run_silver srs
    INNER JOIN patient_silver ps
      ON srs.patient_id = ps.patient_id
    INNER JOIN specimen_silver ss
      ON srs.sample_id = ss.sample_id
    """
)

(
    gold_sequencing_run_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sequencing_run.csv")
)

# ============================================================
# Target: gold_genomic_variant
# ============================================================
gold_genomic_variant_df = spark.sql(
    """
    SELECT
      CAST(gvs.variant_id AS STRING)            AS variant_id,
      CAST(gvs.patient_id AS STRING)            AS patient_id,
      CAST(gvs.run_id AS STRING)                AS sequencing_run_id,
      CAST(srs.sample_id AS STRING)             AS specimen_id,
      CAST(gvs.chromosome AS STRING)            AS chromosome,
      CAST(gvs.genomic_position AS INT)         AS position,
      CAST(gvs.reference_allele AS STRING)      AS reference_allele,
      CAST(gvs.alternate_allele AS STRING)      AS alternate_allele,
      CAST(gvs.gene_name AS STRING)             AS gene_symbol,
      CAST(gvs.variant_type AS STRING)          AS variant_type,
      CAST(gvs.clinical_significance AS STRING) AS clinical_significance,
      CAST(gvs.detected_date AS DATE)           AS detected_datetime
    FROM genomic_variant_silver gvs
    INNER JOIN patient_silver ps
      ON gvs.patient_id = ps.patient_id
    INNER JOIN sequencing_run_silver srs
      ON gvs.run_id = srs.run_id
    LEFT JOIN specimen_silver ss
      ON srs.sample_id = ss.sample_id
    """
)

(
    gold_genomic_variant_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_genomic_variant.csv")
)

# ============================================================
# Target: gold_lab_result
# ============================================================
gold_lab_result_df = spark.sql(
    """
    SELECT
      CAST(lrs.result_id AS STRING)         AS source_result_id,
      CAST(lrs.patient_id AS STRING)        AS patient_id,
      CAST(lrs.sample_id AS STRING)         AS specimen_id,
      CAST(lrs.test_name AS STRING)         AS test_name,
      CAST(lrs.biomarker AS STRING)         AS analyte,
      CAST(lrs.test_result AS STRING)       AS result_value,
      CAST(lrs.unit AS STRING)              AS result_unit,
      CAST(lrs.reference_range AS STRING)   AS reference_range_low,
      CAST(lrs.reference_range AS STRING)   AS reference_range_high,
      CAST(lrs.approval_status AS STRING)   AS result_status,
      CAST(lrs.collection_date AS DATE)     AS collected_datetime,
      CAST(lrs.result_date AS DATE)         AS result_datetime
    FROM lab_result_silver lrs
    INNER JOIN patient_silver ps
      ON lrs.patient_id = ps.patient_id
    LEFT JOIN specimen_silver ss
      ON lrs.sample_id = ss.sample_id
    """
)

(
    gold_lab_result_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_lab_result.csv")
)

# ============================================================
# Target: gold_biomarker_trend
# ============================================================
gold_biomarker_trend_df = spark.sql(
    """
    SELECT
      CAST(lrs.patient_id AS STRING)      AS patient_id,
      CAST(lrs.biomarker AS STRING)       AS analyte,
      CAST(MIN(lrs.collection_date) AS DATE) AS trend_start_datetime,
      CAST(MAX(lrs.result_date) AS DATE)     AS trend_end_datetime,
      CAST(MIN(lrs.test_result) AS STRING)   AS first_value,
      CAST(MAX(lrs.test_result) AS STRING)   AS last_value,
      CAST(MIN(lrs.test_result) AS STRING)   AS min_value,
      CAST(MAX(lrs.test_result) AS STRING)   AS max_value,
      CAST(MAX(lrs.result_date) AS DATE)     AS latest_result_datetime
    FROM lab_result_silver lrs
    INNER JOIN patient_silver ps
      ON lrs.patient_id = ps.patient_id
    GROUP BY
      lrs.patient_id,
      lrs.biomarker
    """
)

(
    gold_biomarker_trend_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_biomarker_trend.csv")
)

# ============================================================
# Target: gold_mutation_prevalence
# ============================================================
gold_mutation_prevalence_df = spark.sql(
    """
    SELECT
      CAST(gvs.gene_name AS STRING)             AS gene_symbol,
      CAST(gvs.clinical_significance AS STRING) AS clinical_significance,
      CAST(
        CONCAT(
          gvs.chromosome, ':',
          CAST(gvs.genomic_position AS STRING), ':',
          gvs.reference_allele, '>',
          gvs.alternate_allele
        ) AS STRING
      ) AS variant_key,
      CAST(COUNT(DISTINCT gvs.patient_id) AS INT) AS n_patients,
      CAST(COUNT(gvs.variant_id) AS INT)          AS n_variants
    FROM genomic_variant_silver gvs
    INNER JOIN patient_silver ps
      ON gvs.patient_id = ps.patient_id
    GROUP BY
      gvs.gene_name,
      gvs.clinical_significance,
      gvs.chromosome,
      gvs.genomic_position,
      gvs.reference_allele,
      gvs.alternate_allele
    """
)

(
    gold_mutation_prevalence_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_mutation_prevalence.csv")
)

# ============================================================
# Target: gold_disease_pattern_summary
# ============================================================
gold_disease_pattern_summary_df = spark.sql(
    """
    SELECT
      CAST(gvs.gene_name AS STRING)             AS gene_symbol,
      CAST(gvs.clinical_significance AS STRING) AS clinical_significance,
      CAST(COUNT(DISTINCT gvs.patient_id) AS INT) AS n_patients,
      CAST(COUNT(DISTINCT gvs.run_id) AS INT)     AS n_runs,
      CAST(COUNT(gvs.variant_id) AS INT)          AS n_variants
    FROM genomic_variant_silver gvs
    INNER JOIN patient_silver ps
      ON gvs.patient_id = ps.patient_id
    INNER JOIN sequencing_run_silver srs
      ON gvs.run_id = srs.run_id
    GROUP BY
      gvs.gene_name,
      gvs.clinical_significance
    """
)

(
    gold_disease_pattern_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_disease_pattern_summary.csv")
)

# ============================================================
# Target: gold_variant_alert
# ============================================================
gold_variant_alert_df = spark.sql(
    """
    SELECT
      CAST(gvs.variant_id AS STRING)            AS variant_id,
      CAST(gvs.patient_id AS STRING)            AS patient_id,
      CAST(gvs.run_id AS STRING)                AS sequencing_run_id,
      CAST(gvs.detected_date AS DATE)           AS alert_datetime,
      CAST(gvs.clinical_significance AS STRING) AS clinical_significance_at_alert
    FROM genomic_variant_silver gvs
    INNER JOIN patient_silver ps
      ON gvs.patient_id = ps.patient_id
    INNER JOIN sequencing_run_silver srs
      ON gvs.run_id = srs.run_id
    """
)

(
    gold_variant_alert_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_variant_alert.csv")
)

# ============================================================
# Target: gold_data_audit_lineage
# (Only columns provided in UDT "columns" list)
# ============================================================
gold_data_audit_lineage_df = spark.sql(
    """
    SELECT
      CAST(ps.patient_id AS STRING) AS record_business_key,
      CAST(ss.sample_id AS STRING)  AS source_record_id
    FROM patient_silver ps
    LEFT JOIN specimen_silver ss
      ON ps.patient_id = ss.patient_id
    LEFT JOIN sequencing_run_silver srs
      ON ps.patient_id = srs.patient_id
    LEFT JOIN lab_result_silver lrs
      ON ps.patient_id = lrs.patient_id
    LEFT JOIN genomic_variant_silver gvs
      ON ps.patient_id = gvs.patient_id
    """
)

(
    gold_data_audit_lineage_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_audit_lineage.csv")
)

job.commit()