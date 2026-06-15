```python
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ============================================================
# Source Reads (Bronze)
# ============================================================
patient_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_bronze.{FILE_FORMAT}/")
)
patient_bronze_df.createOrReplaceTempView("patient_bronze")

sequencing_run_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_run_bronze.{FILE_FORMAT}/")
)
sequencing_run_bronze_df.createOrReplaceTempView("sequencing_run_bronze")

variant_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/variant_bronze.{FILE_FORMAT}/")
)
variant_bronze_df.createOrReplaceTempView("variant_bronze")

lab_result_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_result_bronze.{FILE_FORMAT}/")
)
lab_result_bronze_df.createOrReplaceTempView("lab_result_bronze")

# ============================================================
# Target: silver.patient_silver
# ============================================================
patient_silver_df = spark.sql(
    """
    SELECT DISTINCT
        TRIM(pb.patient_id) AS patient_id,
        NULLIF(TRIM(pb.first_name),'') AS first_name,
        NULLIF(TRIM(pb.last_name),'') AS last_name,
        NULLIF(UPPER(TRIM(pb.gender)),'') AS gender,
        CAST(pb.date_of_birth AS DATE) AS date_of_birth,
        NULLIF(UPPER(TRIM(pb.blood_group)),'') AS blood_group,
        NULLIF(TRIM(pb.ethnicity),'') AS ethnicity,
        NULLIF(TRIM(pb.contact_number),'') AS contact_number,
        NULLIF(LOWER(TRIM(pb.email)),'') AS email,
        NULLIF(TRIM(pb.address),'') AS address,
        NULLIF(TRIM(pb.city),'') AS city,
        NULLIF(TRIM(pb.state),'') AS state,
        NULLIF(TRIM(pb.country),'') AS country,
        NULLIF(TRIM(pb.diagnosis),'') AS diagnosis,
        CAST(pb.registration_date AS DATE) AS registration_date
    FROM patient_bronze pb
    WHERE pb.patient_id IS NOT NULL
    """
)

(
    patient_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_silver.csv")
)

patient_silver_df.createOrReplaceTempView(
    "sequencing_run_silver_patient_placeholder_do_not_use"
)

# ============================================================
# Target: silver.sequencing_run_silver
# ============================================================
sequencing_run_silver_df = spark.sql(
    """
    SELECT
        x.run_id,
        x.patient_id,
        x.sample_id,
        x.sequencing_platform,
        x.run_date,
        x.technician_name,
        x.read_length,
        x.coverage_depth,
        x.raw_data_size_gb,
        x.quality_score,
        x.alignment_rate,
        x.reference_genome,
        x.sequencing_center,
        x.processing_status,
        x.upload_timestamp
    FROM (
        SELECT
            TRIM(srb.run_id) AS run_id,
            NULLIF(TRIM(srb.patient_id),'') AS patient_id,
            NULLIF(TRIM(srb.sample_id),'') AS sample_id,
            NULLIF(TRIM(srb.sequencing_platform),'') AS sequencing_platform,
            CAST(srb.run_date AS DATE) AS run_date,
            NULLIF(TRIM(srb.technician_name),'') AS technician_name,
            CAST(srb.read_length AS INT) AS read_length,
            CAST(srb.coverage_depth AS DOUBLE) AS coverage_depth,
            CAST(srb.raw_data_size_gb AS DOUBLE) AS raw_data_size_gb,
            CAST(srb.quality_score AS DOUBLE) AS quality_score,
            CAST(srb.alignment_rate AS DOUBLE) AS alignment_rate,
            NULLIF(TRIM(srb.reference_genome),'') AS reference_genome,
            NULLIF(TRIM(srb.sequencing_center),'') AS sequencing_center,
            NULLIF(UPPER(TRIM(srb.processing_status)),'') AS processing_status,
            CAST(srb.upload_timestamp AS TIMESTAMP) AS upload_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(srb.run_id)
                ORDER BY CAST(srb.upload_timestamp AS TIMESTAMP) DESC, CAST(srb.run_date AS DATE) DESC
            ) AS rn
        FROM sequencing_run_bronze srb
        WHERE srb.run_id IS NOT NULL
    ) x
    WHERE x.rn = 1
    """
)

(
    sequencing_run_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sequencing_run_silver.csv")
)

sequencing_run_silver_df.createOrReplaceTempView("sequencing_run_silver")

# ============================================================
# Target: silver.variant_silver
# ============================================================
variant_silver_df = spark.sql(
    """
    SELECT
        x.variant_id,
        x.patient_id,
        x.run_id,
        x.chromosome,
        x.gene_name,
        x.variant_type,
        x.mutation,
        x.genomic_position,
        x.reference_allele,
        x.alternate_allele,
        x.clinical_significance,
        x.pathogenicity_score,
        x.detected_date,
        x.validation_status,
        x.reporting_lab
    FROM (
        SELECT
            TRIM(vb.variant_id) AS variant_id,
            NULLIF(TRIM(vb.patient_id),'') AS patient_id,
            NULLIF(TRIM(vb.run_id),'') AS run_id,
            NULLIF(TRIM(vb.chromosome),'') AS chromosome,
            NULLIF(TRIM(vb.gene_name),'') AS gene_name,
            NULLIF(TRIM(vb.variant_type),'') AS variant_type,
            NULLIF(TRIM(vb.mutation),'') AS mutation,
            CAST(vb.genomic_position AS INT) AS genomic_position,
            NULLIF(TRIM(vb.reference_allele),'') AS reference_allele,
            NULLIF(TRIM(vb.alternate_allele),'') AS alternate_allele,
            NULLIF(TRIM(vb.clinical_significance),'') AS clinical_significance,
            CAST(vb.pathogenicity_score AS FLOAT) AS pathogenicity_score,
            CAST(vb.detected_date AS DATE) AS detected_date,
            NULLIF(UPPER(TRIM(vb.validation_status)),'') AS validation_status,
            NULLIF(TRIM(vb.reporting_lab),'') AS reporting_lab,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(vb.variant_id)
                ORDER BY CAST(vb.detected_date AS DATE) DESC, CAST(vb.pathogenicity_score AS FLOAT) DESC
            ) AS rn
        FROM variant_bronze vb
        INNER JOIN sequencing_run_silver srs
            ON TRIM(vb.run_id) = srs.run_id
        WHERE vb.variant_id IS NOT NULL
    ) x
    WHERE x.rn = 1
    """
)

(
    variant_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/variant_silver.csv")
)

variant_silver_df.createOrReplaceTempView("variant_silver")

# ============================================================
# Target: silver.lab_result_silver
# ============================================================
lab_result_silver_df = spark.sql(
    """
    SELECT
        x.result_id,
        x.patient_id,
        x.sample_id,
        x.test_name,
        x.biomarker,
        x.test_result,
        x.unit,
        x.reference_range,
        x.interpretation,
        x.performed_by,
        x.lab_name,
        x.collection_date,
        x.result_date,
        x.approval_status,
        x.remarks
    FROM (
        SELECT
            TRIM(lrb.result_id) AS result_id,
            NULLIF(TRIM(lrb.patient_id),'') AS patient_id,
            NULLIF(TRIM(lrb.sample_id),'') AS sample_id,
            NULLIF(TRIM(lrb.test_name),'') AS test_name,
            NULLIF(TRIM(lrb.biomarker),'') AS biomarker,
            NULLIF(TRIM(lrb.test_result),'') AS test_result,
            NULLIF(TRIM(lrb.unit),'') AS unit,
            NULLIF(TRIM(lrb.reference_range),'') AS reference_range,
            NULLIF(TRIM(lrb.interpretation),'') AS interpretation,
            NULLIF(TRIM(lrb.performed_by),'') AS performed_by,
            NULLIF(TRIM(lrb.lab_name),'') AS lab_name,
            CAST(lrb.collection_date AS DATE) AS collection_date,
            CAST(lrb.result_date AS DATE) AS result_date,
            NULLIF(UPPER(TRIM(lrb.approval_status)),'') AS approval_status,
            NULLIF(TRIM(lrb.remarks),'') AS remarks,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(lrb.result_id)
                ORDER BY CAST(lrb.result_date AS DATE) DESC, CAST(lrb.collection_date AS DATE) DESC
            ) AS rn
        FROM lab_result_bronze lrb
        WHERE lrb.result_id IS NOT NULL
    ) x
    WHERE x.rn = 1
    """
)

(
    lab_result_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_result_silver.csv")
)

lab_result_silver_df.createOrReplaceTempView("lab_result_silver")

job.commit()

```