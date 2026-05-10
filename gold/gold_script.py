```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Initialize Spark session and Glue context
spark = SparkSession.builder \
    .appName("GlueApp") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Load source data
employee_certifications_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/employee_certifications_silver.{FILE_FORMAT}/")
employees_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/employees_silver.{FILE_FORMAT}/")
departments_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/departments_silver.{FILE_FORMAT}/")
certifications_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/certifications_silver.{FILE_FORMAT}/")

department_compliance_statuses_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/department_compliance_statuses_silver.{FILE_FORMAT}/")
compliance_reports_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/compliance_reports_silver.{FILE_FORMAT}/")

audit_trails_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/audit_trails_silver.{FILE_FORMAT}/")

# Create temp views
employee_certifications_df.createOrReplaceTempView("employee_certifications_silver")
employees_df.createOrReplaceTempView("employees_silver")
departments_df.createOrReplaceTempView("departments_silver")
certifications_df.createOrReplaceTempView("certifications_silver")

department_compliance_statuses_df.createOrReplaceTempView("department_compliance_statuses_silver")
compliance_reports_df.createOrReplaceTempView("compliance_reports_silver")

audit_trails_df.createOrReplaceTempView("audit_trails_silver")

# Transformations

# gold_certification_status
gold_certification_status_query = """
    SELECT
        ecs.employee_id,
        es.department_id,
        ds.department_name,
        ecs.certification_id,
        cs.certification_name,
        ecs.certification_target_date,
        ecs.certification_completion_date,
        CASE 
            WHEN ecs.certification_completion_date IS NOT NULL AND ecs.certification_completion_date <= ecs.certification_target_date THEN 'Completed'
            WHEN ecs.certification_completion_date IS NULL AND ecs.certification_target_date < current_date() THEN 'At Risk'
            ELSE 'Pending'
        END AS certification_status,
        COUNT(DISTINCT CASE WHEN ecs.certification_status = 'Completed' THEN ecs.employee_id END) OVER (PARTITION BY es.department_id, ecs.certification_id) AS employees_certified_count
    FROM employee_certifications_silver ecs
    INNER JOIN employees_silver es ON ecs.employee_id = es.employee_id
    INNER JOIN departments_silver ds ON es.department_id = ds.department_id
    INNER JOIN certifications_silver cs ON ecs.certification_id = cs.certification_id
"""
gold_certification_status_df = spark.sql(gold_certification_status_query)

# Write output
gold_certification_status_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_certification_status.csv", header=True)

# department_compliance_report
department_compliance_report_query = """
    SELECT
        dcs.report_id,
        crs.report_generation_date,
        dcs.department_id,
        ds.department_name,
        dcs.non_compliant_status,
        dcs.non_compliance_reason
    FROM department_compliance_statuses_silver dcs
    INNER JOIN compliance_reports_silver crs ON dcs.report_id = crs.report_id
    INNER JOIN departments_silver ds ON dcs.department_id = ds.department_id
"""
department_compliance_report_df = spark.sql(department_compliance_report_query)

# Write output
department_compliance_report_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/department_compliance_report.csv", header=True)

# compliance_audit_trail
compliance_audit_trail_query = """
    SELECT
        ats.audit_id,
        ats.employee_id,
        ats.certification_id,
        ats.verification_status,
        ats.verification_date
    FROM audit_trails_silver ats
"""
compliance_audit_trail_df = spark.sql(compliance_audit_trail_query)

# Write output
compliance_audit_trail_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/compliance_audit_trail.csv", header=True)
```
