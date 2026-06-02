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

# -------------------------------------------------------------------
# Read Source Tables (S3)
# -------------------------------------------------------------------
patient_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_silver.{FILE_FORMAT}/")
)
patient_silver_df.createOrReplaceTempView("patient_silver")

sequencing_run_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_run_silver.{FILE_FORMAT}/")
)
sequencing_run_silver_df.createOrReplaceTempView("sequencing_run_silver")

lab_test_result_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_test_result_silver.{FILE_FORMAT}/")
)
lab_test_result_silver_df.createOrReplaceTempView("lab_test_result_silver")

variant_observation_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/variant_observation_silver.{FILE_FORMAT}/")
)
variant_observation_silver_df.createOrReplaceTempView("variant_observation_silver")

# -------------------------------------------------------------------
# Target: gold_patient_registration_kpi_daily
# -------------------------------------------------------------------
gold_patient_registration_kpi_daily_sql = """
SELECT
  CAST(ps.registration_date AS DATE) AS kpi_date,
  CAST(COUNT(ps.patient_id) AS BIGINT) AS total_registrations,
  CAST(COUNT(DISTINCT ps.patient_id) AS BIGINT) AS unique_patients_registered
FROM patient_silver ps
GROUP BY CAST(ps.registration_date AS DATE)
"""
gold_patient_registration_kpi_daily_df = spark.sql(gold_patient_registration_kpi_daily_sql)

(
    gold_patient_registration_kpi_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_registration_kpi_daily.csv")
)

# -------------------------------------------------------------------
# Target: gold_sequencing_quality_run_metrics
# -------------------------------------------------------------------
gold_sequencing_quality_run_metrics_sql = """
SELECT
  TRIM(CAST(srs.run_id AS STRING)) AS run_id,
  CAST(srs.run_date AS DATE) AS run_date,
  TRIM(CAST(srs.sequencing_center AS STRING)) AS lab_name,
  TRIM(CAST(srs.sequencing_center AS STRING)) AS lab_id,
  TRIM(CAST(srs.processing_status AS STRING)) AS run_status,
  CAST(COUNT(DISTINCT srs.sample_id) AS BIGINT) AS total_samples
FROM sequencing_run_silver srs
GROUP BY
  TRIM(CAST(srs.run_id AS STRING)),
  CAST(srs.run_date AS DATE),
  TRIM(CAST(srs.sequencing_center AS STRING)),
  TRIM(CAST(srs.processing_status AS STRING))
"""
gold_sequencing_quality_run_metrics_df = spark.sql(gold_sequencing_quality_run_metrics_sql)

(
    gold_sequencing_quality_run_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sequencing_quality_run_metrics.csv")
)

# -------------------------------------------------------------------
# Target: gold_sequencing_quality_sample_metrics
# -------------------------------------------------------------------
gold_sequencing_quality_sample_metrics_sql = """
SELECT
  TRIM(CAST(srs.sample_id AS STRING)) AS sample_id,
  TRIM(CAST(srs.patient_id AS STRING)) AS patient_id,
  CAST(ltrs.collection_date AS DATE) AS collection_date,
  CAST(ltrs.result_date AS DATE) AS received_date,
  TRIM(CAST(srs.run_id AS STRING)) AS run_id
FROM sequencing_run_silver srs
LEFT JOIN lab_test_result_silver ltrs
  ON srs.sample_id = ltrs.sample_id
"""
gold_sequencing_quality_sample_metrics_df = spark.sql(gold_sequencing_quality_sample_metrics_sql)

(
    gold_sequencing_quality_sample_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sequencing_quality_sample_metrics.csv")
)

# -------------------------------------------------------------------
# Target: gold_variant_distribution_daily
# -------------------------------------------------------------------
gold_variant_distribution_daily_sql = """
SELECT
  CAST(vos.detected_date AS DATE) AS observation_date,
  TRIM(CAST(vos.reporting_lab AS STRING)) AS lab_id,
  TRIM(CAST(ps.diagnosis AS STRING)) AS disease_area,
  TRIM(CAST(vos.gene_name AS STRING)) AS gene,
  TRIM(CAST(vos.variant_id AS STRING)) AS variant_id,
  TRIM(CAST(vos.variant_type AS STRING)) AS variant_type,
  TRIM(CAST(vos.clinical_significance AS STRING)) AS clinical_significance,
  CAST(COUNT(vos.variant_id) AS BIGINT) AS variant_count,
  CAST(COUNT(DISTINCT vos.patient_id) AS BIGINT) AS unique_patients_with_variant
FROM variant_observation_silver vos
LEFT JOIN patient_silver ps
  ON vos.patient_id = ps.patient_id
GROUP BY
  CAST(vos.detected_date AS DATE),
  TRIM(CAST(vos.reporting_lab AS STRING)),
  TRIM(CAST(ps.diagnosis AS STRING)),
  TRIM(CAST(vos.gene_name AS STRING)),
  TRIM(CAST(vos.variant_id AS STRING)),
  TRIM(CAST(vos.variant_type AS STRING)),
  TRIM(CAST(vos.clinical_significance AS STRING))
"""
gold_variant_distribution_daily_df = spark.sql(gold_variant_distribution_daily_sql)

(
    gold_variant_distribution_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_variant_distribution_daily.csv")
)

# -------------------------------------------------------------------
# Target: gold_laboratory_performance_daily
# -------------------------------------------------------------------
gold_laboratory_performance_daily_sql = """
SELECT
  CAST(ltrs.result_date AS DATE) AS performance_date,
  TRIM(CAST(ltrs.lab_name AS STRING)) AS lab_name,
  TRIM(CAST(ltrs.lab_name AS STRING)) AS lab_id,
  CAST(COUNT(DISTINCT ltrs.sample_id) AS BIGINT) AS total_samples_received,
  CAST(COUNT(DISTINCT ltrs.sample_id) AS BIGINT) AS total_samples_completed,
  CAST(AVG(DATEDIFF(CAST(ltrs.result_date AS DATE), CAST(ltrs.collection_date AS DATE)) * 24) AS DOUBLE) AS avg_turnaround_time_hours,
  CAST(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY (DATEDIFF(CAST(ltrs.result_date AS DATE), CAST(ltrs.collection_date AS DATE)) * 24)) AS DOUBLE) AS p90_turnaround_time_hours
FROM lab_test_result_silver ltrs
GROUP BY
  CAST(ltrs.result_date AS DATE),
  TRIM(CAST(ltrs.lab_name AS STRING))
"""
gold_laboratory_performance_daily_df = spark.sql(gold_laboratory_performance_daily_sql)

(
    gold_laboratory_performance_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_laboratory_performance_daily.csv")
)

# -------------------------------------------------------------------
# Target: gold_disease_trend_daily
# -------------------------------------------------------------------
gold_disease_trend_daily_sql = """
SELECT
  CAST(ltrs.result_date AS DATE) AS trend_date,
  TRIM(CAST(ps.diagnosis AS STRING)) AS disease_name,
  TRIM(CAST(ltrs.lab_name AS STRING)) AS lab_id,
  CAST(COUNT(ltrs.result_id) AS BIGINT) AS total_tests_count,
  CAST(COUNT(DISTINCT ltrs.patient_id) AS BIGINT) AS unique_patients_tested
FROM patient_silver ps
LEFT JOIN lab_test_result_silver ltrs
  ON ps.patient_id = ltrs.patient_id
GROUP BY
  CAST(ltrs.result_date AS DATE),
  TRIM(CAST(ps.diagnosis AS STRING)),
  TRIM(CAST(ltrs.lab_name AS STRING))
"""
gold_disease_trend_daily_df = spark.sql(gold_disease_trend_daily_sql)

(
    gold_disease_trend_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_disease_trend_daily.csv")
)

# -------------------------------------------------------------------
# Target: gold_geographic_health_insights_monthly
# -------------------------------------------------------------------
gold_geographic_health_insights_monthly_sql = """
SELECT
  CAST(DATE_TRUNC('month', CAST(ps.registration_date AS DATE)) AS DATE) AS month_start_date,
  TRIM(CAST(ps.diagnosis AS STRING)) AS disease_name,
  CAST(COUNT(ltrs.result_id) AS BIGINT) AS tests_count,
  CAST(COUNT(DISTINCT ps.patient_id) AS BIGINT) AS unique_patients_count
FROM patient_silver ps
LEFT JOIN lab_test_result_silver ltrs
  ON ps.patient_id = ltrs.patient_id
GROUP BY
  CAST(DATE_TRUNC('month', CAST(ps.registration_date AS DATE)) AS DATE),
  TRIM(CAST(ps.diagnosis AS STRING))
"""
gold_geographic_health_insights_monthly_df = spark.sql(gold_geographic_health_insights_monthly_sql)

(
    gold_geographic_health_insights_monthly_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_geographic_health_insights_monthly.csv")
)

# -------------------------------------------------------------------
# Target: gold_operational_sla_daily
# -------------------------------------------------------------------
gold_operational_sla_daily_sql = """
SELECT
  CAST(ltrs.result_date AS DATE) AS sla_date,
  TRIM(CAST(ltrs.lab_name AS STRING)) AS lab_id,
  CAST(AVG(DATEDIFF(CAST(ltrs.result_date AS DATE), CAST(ltrs.collection_date AS DATE)) * 24) AS DOUBLE) AS avg_cycle_time_hours,
  CAST(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY (DATEDIFF(CAST(ltrs.result_date AS DATE), CAST(ltrs.collection_date AS DATE)) * 24)) AS DOUBLE) AS p90_cycle_time_hours
FROM lab_test_result_silver ltrs
GROUP BY
  CAST(ltrs.result_date AS DATE),
  TRIM(CAST(ltrs.lab_name AS STRING))
"""
gold_operational_sla_daily_df = spark.sql(gold_operational_sla_daily_sql)

(
    gold_operational_sla_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_operational_sla_daily.csv")
)

job.commit()
