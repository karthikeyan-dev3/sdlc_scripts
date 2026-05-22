import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ============================================================
# Source: sequencing_run_performance_daily_bronze (srpdb)
# Target: sequencing_run_performance_daily_silver
# ============================================================
srpdb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_run_performance_daily_bronze.{FILE_FORMAT}/")
)
srpdb_df.createOrReplaceTempView("sequencing_run_performance_daily_bronze")

sequencing_run_performance_daily_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    srpdb.*,
    ROW_NUMBER() OVER (
      PARTITION BY srpdb.run_id
      ORDER BY srpdb.upload_timestamp DESC
    ) AS rn
  FROM sequencing_run_performance_daily_bronze srpdb
  WHERE srpdb.run_id IS NOT NULL
    AND srpdb.run_date IS NOT NULL
)
SELECT
  CAST(srpdb.run_id AS STRING) AS run_id,
  DATE(srpdb.run_date) AS run_date,
  CAST(srpdb.sequencing_center AS STRING) AS lab_id,
  CAST(srpdb.sequencing_platform AS STRING) AS instrument_id,
  CAST(srpdb.sample_id AS STRING) AS sample_count,
  CAST(srpdb.coverage_depth AS DOUBLE) AS mean_read_depth,
  CAST(srpdb.quality_score AS DOUBLE) AS pct_reads_q30,
  CAST(srpdb.alignment_rate AS DOUBLE) AS pct_bases_covered_20x,
  CAST(srpdb.processing_status AS STRING) AS failure_flag,
  CAST(srpdb.quality_score AS DOUBLE) AS data_quality_score
FROM ranked srpdb
WHERE srpdb.rn = 1
""")

(
    sequencing_run_performance_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sequencing_run_performance_daily_silver.csv")
)

# ============================================================
# Source: patient_variant_fact_bronze (pvfb)
# Target: patient_variant_fact_silver
# ============================================================
pvfb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_variant_fact_bronze.{FILE_FORMAT}/")
)
pvfb_df.createOrReplaceTempView("patient_variant_fact_bronze")

patient_variant_fact_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    pvfb.*,
    ROW_NUMBER() OVER (
      PARTITION BY pvfb.patient_id, pvfb.run_id, pvfb.variant_id
      ORDER BY pvfb.detected_date DESC
    ) AS rn
  FROM patient_variant_fact_bronze pvfb
  WHERE pvfb.variant_id IS NOT NULL
    AND pvfb.patient_id IS NOT NULL
    AND pvfb.run_id IS NOT NULL
)
SELECT
  CAST(pvfb.patient_id AS STRING) AS patient_id,
  CAST(pvfb.run_id AS STRING) AS run_id,
  CAST(pvfb.variant_id AS STRING) AS variant_id,
  CAST(pvfb.chromosome AS STRING) AS chromosome,
  CAST(pvfb.genomic_position AS INT) AS position,
  CAST(pvfb.reference_allele AS STRING) AS reference_allele,
  CAST(pvfb.alternate_allele AS STRING) AS alternate_allele,
  CAST(pvfb.gene_name AS STRING) AS gene_symbol,
  CAST(pvfb.variant_type AS STRING) AS variant_type,
  CAST(pvfb.validation_status AS STRING) AS zygosity,
  CAST(pvfb.clinical_significance AS STRING) AS clinical_significance,
  CAST(pvfb.pathogenicity_score AS FLOAT) AS variant_quality_score,
  DATE(pvfb.detected_date) AS variant_call_date
FROM ranked pvfb
WHERE pvfb.rn = 1
""")

(
    patient_variant_fact_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_variant_fact_silver.csv")
)

# ============================================================
# Source: lab_results_trend_bronze (lrtb)
# Target: lab_results_trend_silver
# ============================================================
lrtb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_trend_bronze.{FILE_FORMAT}/")
)
lrtb_df.createOrReplaceTempView("lab_results_trend_bronze")

lab_results_trend_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    lrtb.*,
    ROW_NUMBER() OVER (
      PARTITION BY lrtb.result_id
      ORDER BY lrtb.result_date DESC, lrtb.collection_date DESC
    ) AS rn
  FROM lab_results_trend_bronze lrtb
  WHERE lrtb.result_id IS NOT NULL
    AND lrtb.patient_id IS NOT NULL
    AND lrtb.result_date IS NOT NULL
)
SELECT
  CAST(lrtb.patient_id AS STRING) AS patient_id,
  CAST(lrtb.result_id AS STRING) AS result_id,
  CAST(lrtb.biomarker AS STRING) AS test_code,
  CAST(lrtb.test_name AS STRING) AS test_name,
  CAST(lrtb.test_result AS STRING) AS result_value,
  CAST(lrtb.unit AS STRING) AS result_unit,
  CAST(lrtb.reference_range AS STRING) AS reference_range_low,
  CAST(lrtb.reference_range AS STRING) AS reference_range_high,
  CAST(lrtb.interpretation AS STRING) AS abnormal_flag,
  DATE(lrtb.result_date) AS result_date
FROM ranked lrtb
WHERE lrtb.rn = 1
""")

(
    lab_results_trend_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_results_trend_silver.csv")
)

job.commit()