import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# 1) Read source tables from S3
# -------------------------------------------------------------------
patient_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_silver.{FILE_FORMAT}/")
)

sequencing_run_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_run_silver.{FILE_FORMAT}/")
)

variant_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/variant_silver.{FILE_FORMAT}/")
)

lab_result_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_result_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# 2) Create temp views
# -------------------------------------------------------------------
patient_silver_df.createOrReplaceTempView("patient_silver")
sequencing_run_silver_df.createOrReplaceTempView("sequencing_run_silver")
variant_silver_df.createOrReplaceTempView("variant_silver")
lab_result_silver_df.createOrReplaceTempView("lab_result_silver")

# -------------------------------------------------------------------
# TARGET: gold.gold_patient_risk_score_history
# Mapping: silver.patient_silver ps LEFT JOIN silver.sequencing_run_silver srs ON ps.patient_id = srs.patient_id
#          LEFT JOIN silver.variant_silver vs ON ps.patient_id = vs.patient_id
#          LEFT JOIN silver.lab_result_silver lrs ON ps.patient_id = lrs.patient_id
# Columns per UDT: patient_id
# -------------------------------------------------------------------
gold_patient_risk_score_history_df = spark.sql(
    """
SELECT
  CAST(ps.patient_id AS STRING) AS patient_id
FROM patient_silver ps
LEFT JOIN sequencing_run_silver srs
  ON ps.patient_id = srs.patient_id
LEFT JOIN variant_silver vs
  ON ps.patient_id = vs.patient_id
LEFT JOIN lab_result_silver lrs
  ON ps.patient_id = lrs.patient_id
"""
)

(
    gold_patient_risk_score_history_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_risk_score_history.csv")
)

gold_patient_risk_score_history_df.createOrReplaceTempView("gold_patient_risk_score_history")

# -------------------------------------------------------------------
# TARGET: gold.gold_patient_risk_score_current
# Mapping: gold.gold_patient_risk_score_history gprsh INNER JOIN silver.patient_silver ps ON gprsh.patient_id = ps.patient_id
#          LEFT JOIN silver.sequencing_run_silver srs ON ps.patient_id = srs.patient_id
#          LEFT JOIN silver.variant_silver vs ON ps.patient_id = vs.patient_id
#          LEFT JOIN silver.lab_result_silver lrs ON ps.patient_id = lrs.patient_id
# Columns per UDT: patient_id
# -------------------------------------------------------------------
gold_patient_risk_score_current_df = spark.sql(
    """
SELECT
  CAST(ps.patient_id AS STRING) AS patient_id
FROM gold_patient_risk_score_history gprsh
INNER JOIN patient_silver ps
  ON gprsh.patient_id = ps.patient_id
LEFT JOIN sequencing_run_silver srs
  ON ps.patient_id = srs.patient_id
LEFT JOIN variant_silver vs
  ON ps.patient_id = vs.patient_id
LEFT JOIN lab_result_silver lrs
  ON ps.patient_id = lrs.patient_id
"""
)

(
    gold_patient_risk_score_current_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_risk_score_current.csv")
)

gold_patient_risk_score_current_df.createOrReplaceTempView("gold_patient_risk_score_current")

# -------------------------------------------------------------------
# TARGET: gold.gold_patient_risk_trend_daily
# Mapping: gold.gold_patient_risk_score_history gprsh INNER JOIN silver.patient_silver ps ON gprsh.patient_id = ps.patient_id
# Columns per UDT: patient_id
# -------------------------------------------------------------------
gold_patient_risk_trend_daily_df = spark.sql(
    """
SELECT
  CAST(ps.patient_id AS STRING) AS patient_id
FROM gold_patient_risk_score_history gprsh
INNER JOIN patient_silver ps
  ON gprsh.patient_id = ps.patient_id
"""
)

(
    gold_patient_risk_trend_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_risk_trend_daily.csv")
)

# -------------------------------------------------------------------
# TARGET: gold.gold_risk_alerts
# Mapping: gold.gold_patient_risk_score_history gprsh INNER JOIN silver.patient_silver ps ON gprsh.patient_id = ps.patient_id
# Columns per UDT: patient_id
# -------------------------------------------------------------------
gold_risk_alerts_df = spark.sql(
    """
SELECT
  CAST(ps.patient_id AS STRING) AS patient_id
FROM gold_patient_risk_score_history gprsh
INNER JOIN patient_silver ps
  ON gprsh.patient_id = ps.patient_id
"""
)

(
    gold_risk_alerts_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_risk_alerts.csv")
)

# -------------------------------------------------------------------
# TARGET: gold.gold_patient_prioritization_queue
# Mapping: gold.gold_patient_risk_score_current gprsc INNER JOIN silver.patient_silver ps ON gprsc.patient_id = ps.patient_id
# Columns per UDT: patient_id
# -------------------------------------------------------------------
gold_patient_prioritization_queue_df = spark.sql(
    """
SELECT
  CAST(ps.patient_id AS STRING) AS patient_id
FROM gold_patient_risk_score_current gprsc
INNER JOIN patient_silver ps
  ON gprsc.patient_id = ps.patient_id
"""
)

(
    gold_patient_prioritization_queue_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_prioritization_queue.csv")
)

# -------------------------------------------------------------------
# TARGET: gold.gold_high_risk_population_summary
# Mapping: gold.gold_patient_risk_score_current gprsc INNER JOIN silver.patient_silver ps ON gprsc.patient_id = ps.patient_id
# Columns per UDT: patient_id
# -------------------------------------------------------------------
gold_high_risk_population_summary_df = spark.sql(
    """
SELECT
  CAST(ps.patient_id AS STRING) AS patient_id
FROM gold_patient_risk_score_current gprsc
INNER JOIN patient_silver ps
  ON gprsc.patient_id = ps.patient_id
"""
)

(
    gold_high_risk_population_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_high_risk_population_summary.csv")
)

# -------------------------------------------------------------------
# TARGET: gold.gold_variant_pathogenic_evidence_by_patient
# Mapping: silver.variant_silver vs INNER JOIN silver.patient_silver ps ON vs.patient_id = ps.patient_id
#          LEFT JOIN silver.sequencing_run_silver srs ON vs.run_id = srs.run_id
# Columns per UDT: patient_id, gene_symbol, variant_id, variant_classification, classification_date
# -------------------------------------------------------------------
gold_variant_pathogenic_evidence_by_patient_df = spark.sql(
    """
SELECT
  CAST(vs.patient_id AS STRING) AS patient_id,
  CAST(vs.gene_name AS STRING) AS gene_symbol,
  CAST(vs.variant_id AS STRING) AS variant_id,
  CAST(vs.clinical_significance AS STRING) AS variant_classification,
  CAST(vs.detected_date AS DATE) AS classification_date
FROM variant_silver vs
INNER JOIN patient_silver ps
  ON vs.patient_id = ps.patient_id
LEFT JOIN sequencing_run_silver srs
  ON vs.run_id = srs.run_id
"""
)

(
    gold_variant_pathogenic_evidence_by_patient_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_variant_pathogenic_evidence_by_patient.csv")
)

# -------------------------------------------------------------------
# TARGET: gold.gold_risk_score_data_quality
# Mapping: gold.gold_patient_risk_score_history gprsh INNER JOIN silver.patient_silver ps ON gprsh.patient_id = ps.patient_id
#          LEFT JOIN silver.sequencing_run_silver srs ON ps.patient_id = srs.patient_id
#          LEFT JOIN silver.variant_silver vs ON ps.patient_id = vs.patient_id
#          LEFT JOIN silver.lab_result_silver lrs ON ps.patient_id = lrs.patient_id
# Columns per UDT: (only patient_id provided for this table in UDT)
# -------------------------------------------------------------------
gold_risk_score_data_quality_df = spark.sql(
    """
SELECT
  CAST(ps.patient_id AS STRING) AS patient_id
FROM gold_patient_risk_score_history gprsh
INNER JOIN patient_silver ps
  ON gprsh.patient_id = ps.patient_id
LEFT JOIN sequencing_run_silver srs
  ON ps.patient_id = srs.patient_id
LEFT JOIN variant_silver vs
  ON ps.patient_id = vs.patient_id
LEFT JOIN lab_result_silver lrs
  ON ps.patient_id = lrs.patient_id
"""
)

(
    gold_risk_score_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_risk_score_data_quality.csv")
)
