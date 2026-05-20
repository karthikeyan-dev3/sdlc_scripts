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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------
# Read source tables (S3)
# -------------------------
pis_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_identity_silver.{FILE_FORMAT}/")
)

pas_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_analytics_silver.{FILE_FORMAT}/")
)

pces_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_clinical_events_silver.{FILE_FORMAT}/")
)

pdms_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_dashboard_metrics_silver.{FILE_FORMAT}/")
)

rls_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/report_latency_silver.{FILE_FORMAT}/")
)

dpms_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_process_monitor_silver.{FILE_FORMAT}/")
)

# -------------------------
# Create temp views
# -------------------------
pis_df.createOrReplaceTempView("pis")
pas_df.createOrReplaceTempView("pas")
pces_df.createOrReplaceTempView("pces")
pdms_df.createOrReplaceTempView("pdms")
rls_df.createOrReplaceTempView("rls")
dpms_df.createOrReplaceTempView("dpms")

# ============================================================
# Target: gold.gold_patient_entity
# mapping: silver.patient_identity_silver pis LEFT JOIN silver.patient_analytics_silver pas ON pis.patient_id = pas.patient_id
# ============================================================
gold_patient_entity_df = spark.sql("""
WITH pas_ranked AS (
  SELECT
    pas.patient_id AS patient_id,
    ROW_NUMBER() OVER (
      PARTITION BY pas.patient_id
      ORDER BY CAST(pas.record_timestamp AS timestamp) DESC
    ) AS rn
  FROM pas
)
SELECT
  CAST(pis.patient_id AS string) AS patient_id,
  CAST(pis.healthcare_system AS string) AS healthcare_system
FROM pis
LEFT JOIN pas_ranked
  ON CAST(pis.patient_id AS string) = CAST(pas_ranked.patient_id AS string)
 AND pas_ranked.rn = 1
""")

(
    gold_patient_entity_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_entity.csv")
)

# ============================================================
# Target: gold.gold_clinical_data_entity
# mapping: silver.patient_identity_silver pis LEFT JOIN silver.patient_clinical_events_silver pces ON pis.patient_id = pces.patient_id LEFT JOIN silver.patient_dashboard_metrics_silver pdms ON pis.patient_id = pdms.patient_id
# ============================================================
gold_clinical_data_entity_df = spark.sql("""
SELECT
  CAST(pis.patient_id AS string) AS patient_id,
  CAST(pis.healthcare_system AS string) AS healthcare_system,
  CAST(pces.record_timestamp AS timestamp) AS record_timestamp
FROM pis
LEFT JOIN pces
  ON CAST(pis.patient_id AS string) = CAST(pces.patient_id AS string)
LEFT JOIN pdms
  ON CAST(pis.patient_id AS string) = CAST(pdms.patient_id AS string)
""")

(
    gold_clinical_data_entity_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_data_entity.csv")
)

# ============================================================
# Target: gold.gold_analytics_entity
# mapping: silver.patient_identity_silver pis LEFT JOIN silver.patient_clinical_events_silver pces ON pis.patient_id = pces.patient_id LEFT JOIN silver.report_latency_silver rls ON pis.patient_id = rls.patient_id LEFT JOIN silver.data_process_monitor_silver dpms ON dpms.pipeline_id = 'patient_360_build'
# ============================================================
gold_analytics_entity_df = spark.sql("""
SELECT
  CAST(pis.patient_id AS string) AS patient_id,
  CAST(pces.record_timestamp AS timestamp) AS record_timestamp,
  CAST(dpms.data_updated_timestamp AS timestamp) AS data_updated_timestamp
FROM pis
LEFT JOIN pces
  ON CAST(pis.patient_id AS string) = CAST(pces.patient_id AS string)
LEFT JOIN rls
  ON CAST(pis.patient_id AS string) = CAST(rls.patient_id AS string)
LEFT JOIN dpms
  ON CAST(dpms.pipeline_id AS string) = 'patient_360_build'
""")

(
    gold_analytics_entity_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_analytics_entity.csv")
)

# ============================================================
# Target: gold.gold_monitoring_entity
# mapping: silver.data_process_monitor_silver dpms
# ============================================================
gold_monitoring_entity_df = spark.sql("""
SELECT
  CAST(dpms.data_updated_timestamp AS timestamp) AS data_updated_timestamp
FROM dpms
""")

(
    gold_monitoring_entity_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_monitoring_entity.csv")
)

job.commit()
