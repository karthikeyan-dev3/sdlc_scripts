import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------
# Read Source Tables (Bronze)
# ----------------------------
patient_data_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_bronze.{FILE_FORMAT}/")
)

lab_test_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_test_results_bronze.{FILE_FORMAT}/")
)

genomics_sequencing_runs_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomics_sequencing_runs_bronze.{FILE_FORMAT}/")
)

genomic_variants_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomic_variants_bronze.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
patient_data_bronze_df.createOrReplaceTempView("patient_data_bronze")
lab_test_results_bronze_df.createOrReplaceTempView("lab_test_results_bronze")
genomics_sequencing_runs_bronze_df.createOrReplaceTempView("genomics_sequencing_runs_bronze")
genomic_variants_bronze_df.createOrReplaceTempView("genomic_variants_bronze")

# ============================================================
# Target: patient_silver
# Mapping: bronze.patient_data_bronze pdb
# ============================================================
patient_silver_df = spark.sql(
    """
    SELECT
        CAST(pdb.patient_id AS STRING)            AS patient_id,
        DATE(pdb.registration_date)              AS registration_date,
        CAST(pdb.state AS STRING)                AS region_code,
        CAST(pdb.diagnosis AS STRING)            AS disease_code
    FROM patient_data_bronze pdb
    """
)

patient_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_silver.csv"
)

# ============================================================
# Target: lab_test_results_silver
# Mapping: bronze.lab_test_results_bronze ltrb
# ============================================================
lab_test_results_silver_df = spark.sql(
    """
    SELECT
        CAST(ltrb.result_id AS STRING)            AS result_id,
        CAST(ltrb.patient_id AS STRING)           AS patient_id,
        CAST(ltrb.test_name AS STRING)            AS test_name,
        CAST(ltrb.lab_name AS STRING)             AS lab_name,
        DATE(ltrb.collection_date)                AS collection_date,
        DATE(ltrb.result_date)                    AS result_date
    FROM lab_test_results_bronze ltrb
    """
)

lab_test_results_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/lab_test_results_silver.csv"
)

# ============================================================
# Target: sequencing_runs_silver
# Mapping: bronze.genomics_sequencing_runs_bronze gsrb
# ============================================================
sequencing_runs_silver_df = spark.sql(
    """
    SELECT
        CAST(gsrb.run_id AS STRING)               AS run_id,
        DATE(gsrb.run_date)                       AS run_date,
        CAST(gsrb.sequencing_platform AS STRING)  AS instrument_id,
        CAST(gsrb.sequencing_center AS STRING)    AS facility_id,
        CAST(gsrb.coverage_depth AS DOUBLE)       AS coverage_depth,
        CAST(gsrb.quality_score AS DOUBLE)        AS quality_score,
        CAST(gsrb.processing_status AS STRING)    AS processing_status
    FROM genomics_sequencing_runs_bronze gsrb
    """
)

sequencing_runs_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/sequencing_runs_silver.csv"
)

# ============================================================
# Target: genomic_variants_silver
# Mapping: bronze.genomic_variants_bronze gvb
# ============================================================
genomic_variants_silver_df = spark.sql(
    """
    SELECT
        CAST(gvb.variant_id AS STRING)            AS variant_id,
        CAST(gvb.patient_id AS STRING)            AS patient_id,
        CAST(gvb.run_id AS STRING)                AS run_id,
        CAST(gvb.gene_name AS STRING)             AS gene_symbol,
        CAST(gvb.variant_type AS STRING)          AS variant_class,
        DATE(gvb.detected_date)                   AS detected_date
    FROM genomic_variants_bronze gvb
    """
)

genomic_variants_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/genomic_variants_silver.csv"
)

# ============================================================
# Target: patient_lab_events_silver
# Mapping: bronze.lab_test_results_bronze ltrb LEFT JOIN bronze.patient_data_bronze pdb ON ltrb.patient_id = pdb.patient_id
# ============================================================
patient_lab_events_silver_df = spark.sql(
    """
    SELECT
        CAST(ltrb.result_id AS STRING)            AS result_id,
        CAST(ltrb.patient_id AS STRING)           AS patient_id,
        DATE(pdb.registration_date)               AS registration_date,
        CAST(pdb.state AS STRING)                 AS region_code,
        CAST(pdb.diagnosis AS STRING)             AS disease_code,
        CAST(ltrb.lab_name AS STRING)             AS facility_id,
        CAST(ltrb.test_name AS STRING)            AS test_type,
        DATE(ltrb.collection_date)                AS collection_date,
        DATE(ltrb.result_date)                    AS result_date
    FROM lab_test_results_bronze ltrb
    LEFT JOIN patient_data_bronze pdb
        ON ltrb.patient_id = pdb.patient_id
    """
)

patient_lab_events_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_lab_events_silver.csv"
)

# ============================================================
# Target: patient_sequencing_events_silver
# Mapping: bronze.genomics_sequencing_runs_bronze gsrb LEFT JOIN bronze.patient_data_bronze pdb ON gsrb.patient_id = pdb.patient_id
# ============================================================
patient_sequencing_events_silver_df = spark.sql(
    """
    SELECT
        CAST(gsrb.run_id AS STRING)               AS run_id,
        CAST(gsrb.patient_id AS STRING)           AS patient_id,
        DATE(pdb.registration_date)               AS registration_date,
        CAST(pdb.state AS STRING)                 AS region_code,
        CAST(pdb.diagnosis AS STRING)             AS disease_code,
        DATE(gsrb.run_date)                       AS run_date,
        CAST(gsrb.sequencing_center AS STRING)    AS facility_id,
        CAST(gsrb.sequencing_platform AS STRING)  AS instrument_id,
        CAST(gsrb.coverage_depth AS DOUBLE)       AS coverage_depth,
        CAST(gsrb.quality_score AS DOUBLE)        AS quality_score,
        CAST(gsrb.processing_status AS STRING)    AS processing_status
    FROM genomics_sequencing_runs_bronze gsrb
    LEFT JOIN patient_data_bronze pdb
        ON gsrb.patient_id = pdb.patient_id
    """
)

patient_sequencing_events_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_sequencing_events_silver.csv"
)

# ============================================================
# Target: variant_sequencing_events_silver
# Mapping: bronze.genomic_variants_bronze gvb LEFT JOIN bronze.genomics_sequencing_runs_bronze gsrb ON gvb.run_id = gsrb.run_id
# ============================================================
variant_sequencing_events_silver_df = spark.sql(
    """
    SELECT
        CAST(gvb.variant_id AS STRING)            AS variant_id,
        CAST(gvb.run_id AS STRING)                AS run_id,
        DATE(gsrb.run_date)                       AS run_date,
        CAST(gsrb.sequencing_center AS STRING)    AS facility_id,
        CAST(gvb.gene_name AS STRING)             AS gene_symbol,
        CAST(gvb.variant_type AS STRING)          AS variant_class,
        DATE(gvb.detected_date)                   AS detected_date
    FROM genomic_variants_bronze gvb
    LEFT JOIN genomics_sequencing_runs_bronze gsrb
        ON gvb.run_id = gsrb.run_id
    """
)

variant_sequencing_events_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/variant_sequencing_events_silver.csv"
)

# ============================================================
# Target: patient_variant_events_silver
# Mapping: bronze.genomic_variants_bronze gvb LEFT JOIN bronze.patient_data_bronze pdb ON gvb.patient_id = pdb.patient_id
# ============================================================
patient_variant_events_silver_df = spark.sql(
    """
    SELECT
        CAST(gvb.variant_id AS STRING)            AS variant_id,
        CAST(gvb.patient_id AS STRING)            AS patient_id,
        DATE(pdb.registration_date)               AS registration_date,
        CAST(pdb.state AS STRING)                 AS region_code,
        CAST(pdb.diagnosis AS STRING)             AS disease_code,
        CAST(gvb.gene_name AS STRING)             AS gene_symbol,
        CAST(gvb.variant_type AS STRING)          AS variant_class,
        DATE(gvb.detected_date)                   AS detected_date
    FROM genomic_variants_bronze gvb
    LEFT JOIN patient_data_bronze pdb
        ON gvb.patient_id = pdb.patient_id
    """
)

patient_variant_events_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_variant_events_silver.csv"
)

job.commit()
