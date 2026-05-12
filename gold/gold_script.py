import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Initialize GlueContext and SparkSession
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# Load data
certification_tracking_silver = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/certification_tracking_silver.{FILE_FORMAT}/")
)

# Create temp view
certification_tracking_silver.createOrReplaceTempView("cts")

# Transform and write gold_certification_tracking
gold_certification_tracking_df = spark.sql("""
SELECT
    cts.department_id,
    cts.department_name,
    cts.coordinator_id,
    cts.coordinator_name,
    cts.certification_id,
    cts.certification_name,
    cts.completion_status,
    cts.completion_date,
    cts.compliance_deadline,
    cts.flagged_for_incomplete
FROM
    cts
""")

gold_certification_tracking_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_certification_tracking.csv", header=True, mode="overwrite")

# Transform and write gold_department_compliance_report
gold_department_compliance_report_df = spark.sql("""
SELECT
    cts.department_id,
    cts.department_name,
    COUNT(cts.certification_id) AS total_certifications,
    SUM(CASE WHEN cts.completion_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_certifications,
    SUM(CASE WHEN cts.flagged_for_incomplete = true THEN 1 ELSE 0 END) AS non_compliant_certifications,
    (SUM(CASE WHEN cts.completion_status = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(cts.certification_id)) AS compliance_rate,
    CURRENT_TIMESTAMP AS last_updated
FROM
    cts
GROUP BY
    cts.department_id,
    cts.department_name
""")

gold_department_compliance_report_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_department_compliance_report.csv", header=True, mode="overwrite")

job.commit()