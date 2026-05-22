import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------

srpds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_run_performance_daily_silver.{FILE_FORMAT}/")
)
pvfs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_variant_fact_silver.{FILE_FORMAT}/")
)
lrts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_trend_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------

srpds_df.createOrReplaceTempView("sequencing_run_performance_daily_silver")
pvfs_df.createOrReplaceTempView("patient_variant_fact_silver")
lrts_df.createOrReplaceTempView("lab_results_trend_silver")

# ------------------------------------------------------------------------------
# TABLE: gold.gold_sequencing_run_performance_daily
# ------------------------------------------------------------------------------
gold_sequencing_run_performance_daily_df = spark.sql(
    """
    SELECT
        CAST(srpds.run_id AS STRING) AS run_id,
        DATE(srpds.run_date) AS run_date,
        CAST(srpds.lab_id AS STRING) AS lab_id,
        CAST(srpds.instrument_id AS STRING) AS instrument_id,
        CAST(srpds.sample_count AS STRING) AS sample_count,
        CAST(srpds.mean_read_depth AS DOUBLE) AS mean_read_depth,
        CAST(srpds.pct_reads_q30 AS DOUBLE) AS pct_reads_q30,
        CAST(srpds.pct_bases_covered_20x AS DOUBLE) AS pct_bases_covered_20x,
        CAST(srpds.failure_flag AS STRING) AS failure_flag,
        CAST(srpds.data_quality_score AS DOUBLE) AS data_quality_score
    FROM sequencing_run_performance_daily_silver srpds
    """
)

(
    gold_sequencing_run_performance_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sequencing_run_performance_daily.csv")
)

# ------------------------------------------------------------------------------
# TABLE: gold.gold_patient_variant_fact
# ------------------------------------------------------------------------------
gold_patient_variant_fact_df = spark.sql(
    """
    SELECT
        CAST(pvfs.patient_id AS STRING) AS patient_id,
        CAST(pvfs.run_id AS STRING) AS run_id,
        CAST(pvfs.variant_id AS STRING) AS variant_id,
        CAST(pvfs.chromosome AS STRING) AS chromosome,
        CAST(pvfs.position AS INT) AS position,
        CAST(pvfs.reference_allele AS STRING) AS reference_allele,
        CAST(pvfs.alternate_allele AS STRING) AS alternate_allele,
        CAST(pvfs.gene_symbol AS STRING) AS gene_symbol,
        CAST(pvfs.variant_type AS STRING) AS variant_type,
        CAST(pvfs.zygosity AS STRING) AS zygosity,
        CAST(pvfs.clinical_significance AS STRING) AS clinical_significance,
        CAST(pvfs.variant_quality_score AS FLOAT) AS variant_quality_score,
        DATE(pvfs.variant_call_date) AS variant_call_date
    FROM patient_variant_fact_silver pvfs
    """
)

(
    gold_patient_variant_fact_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_variant_fact.csv")
)

# ------------------------------------------------------------------------------
# TABLE: gold.gold_lab_results_trend
# ------------------------------------------------------------------------------
gold_lab_results_trend_df = spark.sql(
    """
    SELECT
        CAST(lrts.patient_id AS STRING) AS patient_id,
        CAST(lrts.result_id AS STRING) AS result_id,
        CAST(lrts.test_code AS STRING) AS test_code,
        CAST(lrts.test_name AS STRING) AS test_name,
        CAST(lrts.result_value AS STRING) AS result_value,
        CAST(lrts.result_unit AS STRING) AS result_unit,
        CAST(lrts.reference_range_low AS STRING) AS reference_range_low,
        CAST(lrts.reference_range_high AS STRING) AS reference_range_high,
        CAST(lrts.abnormal_flag AS STRING) AS abnormal_flag,
        DATE(lrts.result_date) AS result_date
    FROM lab_results_trend_silver lrts
    """
)

(
    gold_lab_results_trend_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_lab_results_trend.csv")
)

# ------------------------------------------------------------------------------
# TABLE: gold.gold_patient_genomics_profile
# ------------------------------------------------------------------------------
gold_patient_genomics_profile_df = spark.sql(
    """
    SELECT
        CAST(pvfs.patient_id AS STRING) AS patient_id,
        CAST(MAX_BY(pvfs.run_id, DATE(pvfs.variant_call_date)) AS STRING) AS latest_sequencing_run_id,
        DATE(MAX_BY(srpds.run_date, DATE(pvfs.variant_call_date))) AS latest_sequencing_run_date,
        DATE(MAX(lrts.result_date)) AS latest_lab_result_date,
        CAST(COUNT(pvfs.variant_id) AS INT) AS variant_count_total,
        CAST(COUNT(CASE WHEN pvfs.clinical_significance = 'Pathogenic' THEN pvfs.variant_id END) AS INT) AS variant_count_pathogenic,
        CAST(COUNT(CASE WHEN pvfs.clinical_significance = 'Likely pathogenic' THEN pvfs.variant_id END) AS INT) AS variant_count_likely_pathogenic,
        CAST(COUNT(CASE WHEN pvfs.clinical_significance = 'VUS' THEN pvfs.variant_id END) AS INT) AS variant_count_vus,
        CAST(AVG(COALESCE(CAST(pvfs.variant_quality_score AS DOUBLE), CAST(srpds.data_quality_score AS DOUBLE))) AS DOUBLE) AS data_quality_score,
        DATE(MAX(pvfs.variant_call_date)) AS record_effective_date
    FROM patient_variant_fact_silver pvfs
    LEFT JOIN sequencing_run_performance_daily_silver srpds
        ON pvfs.run_id = srpds.run_id
    LEFT JOIN lab_results_trend_silver lrts
        ON pvfs.patient_id = lrts.patient_id
    GROUP BY
        pvfs.patient_id
    """
)

(
    gold_patient_genomics_profile_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_genomics_profile.csv")
)

# ------------------------------------------------------------------------------
# TABLE: gold.gold_clinical_trial_eligibility_screening
# NOTE: ELIGIBILITY_RULE / ELIGIBILITY_STATUS / ELIGIBILITY_REASON / RULE_VERSION
# are represented exactly as provided in UDT and must exist as Spark SQL functions.
# ------------------------------------------------------------------------------
gold_clinical_trial_eligibility_screening_df = spark.sql(
    """
    SELECT
        CAST(pvfs.patient_id AS STRING) AS patient_id,
        CAST(pvfs.variant_id AS STRING) AS supporting_variant_id,
        CAST(lrts.result_id AS STRING) AS supporting_lab_result_id,
        DATE(MAX(pvfs.variant_call_date)) AS evaluation_date,
        CAST(ELIGIBILITY_RULE(pvfs.clinical_significance, lrts.abnormal_flag) AS STRING) AS trial_id,
        CAST(ELIGIBILITY_STATUS(pvfs.clinical_significance, lrts.abnormal_flag) AS STRING) AS eligibility_status,
        CAST(ELIGIBILITY_REASON(pvfs.variant_id, lrts.result_id) AS STRING) AS eligibility_reason,
        CAST(RULE_VERSION(pvfs.variant_type) AS STRING) AS rule_version
    FROM patient_variant_fact_silver pvfs
    LEFT JOIN lab_results_trend_silver lrts
        ON pvfs.patient_id = lrts.patient_id
    GROUP BY
        pvfs.patient_id,
        pvfs.variant_id,
        lrts.result_id,
        pvfs.clinical_significance,
        lrts.abnormal_flag,
        pvfs.variant_type
    """
)

(
    gold_clinical_trial_eligibility_screening_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_eligibility_screening.csv")
)

job.commit()