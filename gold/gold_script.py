import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------
# 1) Read source tables from S3
# --------------------------------------------------------------------
sequencing_runs_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_runs_silver.{FILE_FORMAT}/")
)

lab_work_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_work_items_silver.{FILE_FORMAT}/")
)

pending_approvals_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/pending_approvals_silver.{FILE_FORMAT}/")
)

patient_diagnostic_results_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_diagnostic_results_silver.{FILE_FORMAT}/")
)

pathogenic_variant_alerts_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/pathogenic_variant_alerts_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------
sequencing_runs_silver_df.createOrReplaceTempView("sequencing_runs_silver")
lab_work_items_silver_df.createOrReplaceTempView("lab_work_items_silver")
pending_approvals_silver_df.createOrReplaceTempView("pending_approvals_silver")
patient_diagnostic_results_silver_df.createOrReplaceTempView("patient_diagnostic_results_silver")
pathogenic_variant_alerts_silver_df.createOrReplaceTempView("pathogenic_variant_alerts_silver")

# --------------------------------------------------------------------
# Target: gold.gold_sequencing_run_monitoring
# Source: silver.sequencing_runs_silver srs
# --------------------------------------------------------------------
gold_sequencing_run_monitoring_df = spark.sql(
    """
    SELECT
        CAST(srs.run_id AS STRING) AS run_id,
        CAST(srs.processing_status AS STRING) AS current_status,
        CAST(srs.upload_timestamp AS TIMESTAMP) AS data_ingested_ts
    FROM sequencing_runs_silver srs
    """
)

(
    gold_sequencing_run_monitoring_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sequencing_run_monitoring.csv")
)

# --------------------------------------------------------------------
# Target: gold.gold_lab_work_item_monitoring
# Source: silver.lab_work_items_silver lwis
# --------------------------------------------------------------------
gold_lab_work_item_monitoring_df = spark.sql(
    """
    SELECT
        CAST(lwis.result_id AS STRING) AS work_item_id,
        CAST(lwis.sample_id AS STRING) AS sample_id,
        CAST(lwis.patient_id AS STRING) AS patient_id,
        CAST(lwis.test_name AS STRING) AS test_name,
        CAST(lwis.collection_date AS DATE) AS received_ts,
        CAST(lwis.result_date AS DATE) AS processing_end_ts
    FROM lab_work_items_silver lwis
    """
)

(
    gold_lab_work_item_monitoring_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_lab_work_item_monitoring.csv")
)

# --------------------------------------------------------------------
# Target: gold.gold_pending_approval_monitoring
# Source: silver.pending_approvals_silver pas
# --------------------------------------------------------------------
gold_pending_approval_monitoring_df = spark.sql(
    """
    SELECT
        CAST(pas.result_id AS STRING) AS entity_id,
        CAST(pas.patient_id AS STRING) AS patient_id,
        CAST(pas.approval_status AS STRING) AS approval_status,
        CAST(pas.result_date AS DATE) AS status_ts
    FROM pending_approvals_silver pas
    """
)

(
    gold_pending_approval_monitoring_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_pending_approval_monitoring.csv")
)

# --------------------------------------------------------------------
# Target: gold.gold_patient_diagnostic_result_monitoring
# Mapping columns reference lwis + pdrs (as provided in UDT columns)
# --------------------------------------------------------------------
gold_patient_diagnostic_result_monitoring_df = spark.sql(
    """
    SELECT
        CAST(lwis.result_id AS STRING) AS result_id,
        CAST(pdrs.patient_id AS STRING) AS patient_id,
        CAST(lwis.test_name AS STRING) AS test_name,
        CAST(lwis.test_result AS STRING) AS result_value_text,
        CAST(lwis.unit AS STRING) AS result_unit,
        CAST(lwis.result_date AS DATE) AS result_ts
    FROM lab_work_items_silver lwis
    INNER JOIN patient_diagnostic_results_silver pdrs
        ON pdrs.patient_id = lwis.patient_id
    """
)

(
    gold_patient_diagnostic_result_monitoring_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_diagnostic_result_monitoring.csv")
)

# --------------------------------------------------------------------
# Target: gold.gold_pathogenic_variant_alert_facts
# Source: silver.pathogenic_variant_alerts_silver pvas
#        INNER JOIN silver.sequencing_runs_silver srs ON pvas.run_id = srs.run_id
# --------------------------------------------------------------------
gold_pathogenic_variant_alert_facts_df = spark.sql(
    """
    SELECT
        CAST(pvas.variant_id AS STRING) AS variant_id,
        CAST(pvas.patient_id AS STRING) AS patient_id,
        CAST(pvas.run_id AS STRING) AS run_id,
        CAST(srs.sample_id AS STRING) AS sample_id,
        CAST(pvas.gene_name AS STRING) AS gene,
        CAST(pvas.clinical_significance AS STRING) AS variant_classification,
        CAST(pvas.detected_date AS DATE) AS detected_ts
    FROM pathogenic_variant_alerts_silver pvas
    INNER JOIN sequencing_runs_silver srs
        ON pvas.run_id = srs.run_id
    """
)

(
    gold_pathogenic_variant_alert_facts_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_pathogenic_variant_alert_facts.csv")
)

# --------------------------------------------------------------------
# Target: gold.gold_clinical_alert_events
# Source: silver.pending_approvals_silver pas
#        INNER JOIN silver.lab_work_items_silver lwis ON pas.result_id = lwis.result_id
#        INNER JOIN silver.pathogenic_variant_alerts_silver pvas ON pvas.patient_id = pas.patient_id
# --------------------------------------------------------------------
gold_clinical_alert_events_df = spark.sql(
    """
    SELECT
        CAST(pas.result_id AS STRING) AS entity_id,
        CAST(pas.patient_id AS STRING) AS patient_id,
        CAST(pvas.run_id AS STRING) AS run_id
    FROM pending_approvals_silver pas
    INNER JOIN lab_work_items_silver lwis
        ON pas.result_id = lwis.result_id
    INNER JOIN pathogenic_variant_alerts_silver pvas
        ON pvas.patient_id = pas.patient_id
    """
)

(
    gold_clinical_alert_events_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_alert_events.csv")
)

# --------------------------------------------------------------------
# Target: gold.gold_monitoring_dataset_metadata
# Source joins per UDT mapping_details, columns per UDT
# --------------------------------------------------------------------
gold_monitoring_dataset_metadata_df = spark.sql(
    """
    SELECT
        CAST(srs.run_id AS STRING) AS source_object_name,
        CAST(srs.upload_timestamp AS TIMESTAMP) AS last_ingested_ts
    FROM sequencing_runs_silver srs
    INNER JOIN lab_work_items_silver lwis
        ON srs.sample_id = lwis.sample_id
    INNER JOIN pending_approvals_silver pas
        ON pas.patient_id = lwis.patient_id
    INNER JOIN patient_diagnostic_results_silver pdrs
        ON pdrs.patient_id = pas.patient_id
    INNER JOIN pathogenic_variant_alerts_silver pvas
        ON pvas.run_id = srs.run_id
    """
)

(
    gold_monitoring_dataset_metadata_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_monitoring_dataset_metadata.csv")
)
