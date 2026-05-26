import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# Read source tables (S3) and create temp views
# -------------------------------------------------------------------
patient_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_silver.{FILE_FORMAT}/")
)
patient_silver_df.createOrReplaceTempView("patient_silver")

patient_lab_events_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_lab_events_silver.{FILE_FORMAT}/")
)
patient_lab_events_silver_df.createOrReplaceTempView("patient_lab_events_silver")

sequencing_runs_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_runs_silver.{FILE_FORMAT}/")
)
sequencing_runs_silver_df.createOrReplaceTempView("sequencing_runs_silver")

lab_test_results_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_test_results_silver.{FILE_FORMAT}/")
)
lab_test_results_silver_df.createOrReplaceTempView("lab_test_results_silver")

patient_variant_events_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_variant_events_silver.{FILE_FORMAT}/")
)
patient_variant_events_silver_df.createOrReplaceTempView("patient_variant_events_silver")

# -------------------------------------------------------------------
# Target: gold_patient_registration_daily
# Sources: silver.patient_silver ps
# -------------------------------------------------------------------
gold_patient_registration_daily_df = spark.sql(
    """
    SELECT
        CAST(ps.patient_id AS STRING) AS patient_id,
        DATE(ps.registration_date) AS registration_date,
        CAST(ps.region_code AS STRING) AS region_code
    FROM patient_silver ps
    """
)

(
    gold_patient_registration_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_registration_daily.csv")
)

# -------------------------------------------------------------------
# Target: gold_patient_geo_disease_daily
# Sources: silver.patient_lab_events_silver ples
# -------------------------------------------------------------------
gold_patient_geo_disease_daily_df = spark.sql(
    """
    SELECT
        DATE(ples.result_date) AS event_date,
        CAST(ples.region_code AS STRING) AS region_code,
        CAST(ples.facility_id AS STRING) AS facility_id,
        CAST(ples.disease_code AS STRING) AS disease_code
    FROM patient_lab_events_silver ples
    """
)

(
    gold_patient_geo_disease_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_geo_disease_daily.csv")
)

# -------------------------------------------------------------------
# Target: gold_sequencing_run_quality
# Sources: silver.sequencing_runs_silver srs
# -------------------------------------------------------------------
gold_sequencing_run_quality_df = spark.sql(
    """
    SELECT
        CAST(srs.run_id AS STRING) AS run_id,
        DATE(srs.run_date) AS run_date,
        CAST(srs.facility_id AS STRING) AS facility_id,
        CAST(srs.instrument_id AS STRING) AS instrument_id,
        CAST(srs.coverage_depth AS DOUBLE) AS mean_coverage,
        CAST(srs.processing_status AS STRING) AS run_status
    FROM sequencing_runs_silver srs
    """
)

(
    gold_sequencing_run_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sequencing_run_quality.csv")
)

# -------------------------------------------------------------------
# Target: gold_lab_performance_daily
# Sources: silver.lab_test_results_silver ltrs
# -------------------------------------------------------------------
gold_lab_performance_daily_df = spark.sql(
    """
    SELECT
        DATE(ltrs.result_date) AS lab_date,
        CAST(ltrs.lab_name AS STRING) AS facility_id,
        CAST(ltrs.lab_name AS STRING) AS lab_id,
        CAST(ltrs.test_name AS STRING) AS test_type
    FROM lab_test_results_silver ltrs
    """
)

(
    gold_lab_performance_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_lab_performance_daily.csv")
)

# -------------------------------------------------------------------
# Target: gold_variant_distribution_daily
# Sources: silver.patient_variant_events_silver pves
# -------------------------------------------------------------------
gold_variant_distribution_daily_df = spark.sql(
    """
    SELECT
        DATE(pves.detected_date) AS variant_date,
        CAST(pves.disease_code AS STRING) AS disease_code,
        CAST(pves.gene_symbol AS STRING) AS gene_symbol,
        CAST(pves.variant_class AS STRING) AS variant_class
    FROM patient_variant_events_silver pves
    """
)

(
    gold_variant_distribution_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_variant_distribution_daily.csv")
)

# -------------------------------------------------------------------
# Targets present in UDT tables list but with no column mappings provided:
# - gold_operational_sla_tracking
# - gold_operational_alerts
# - gold_reporting_kpis_daily
# - gold_data_lineage_audit
# - gold_access_audit
# No output generated for these due to missing target column definitions.
# -------------------------------------------------------------------
