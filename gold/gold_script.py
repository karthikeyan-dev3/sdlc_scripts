```python
import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, when, countDistinct, coalesce, upper, trim
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize Spark session and Glue context
spark = SparkSession.builder \
    .appName("Glue Job") \
    .getOrCreate()

glueContext = GlueContext(spark)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables into Spark DataFrames
employee_certifications_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/employee_certifications_silver.{FILE_FORMAT}/")
employees_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/employees_silver.{FILE_FORMAT}/")
departments_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/departments_silver.{FILE_FORMAT}/")
certifications_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/certifications_silver.{FILE_FORMAT}/")
department_compliance_statuses_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/department_compliance_statuses_silver.{FILE_FORMAT}/")
compliance_reports_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/compliance_reports_silver.{FILE_FORMAT}/")
audit_trails_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/audit_trails_silver.{FILE_FORMAT}/")

# Create temp views
employee_certifications_df.createOrReplaceTempView("ecs")
employees_df.createOrReplaceTempView("es")
departments_df.createOrReplaceTempView("ds")
certifications_df.createOrReplaceTempView("cs")
department_compliance_statuses_df.createOrReplaceTempView("dcs")
compliance_reports_df.createOrReplaceTempView("crs")
audit_trails_df.createOrReplaceTempView("ats")

# Gold Certification Status
gold_certification_query = """
SELECT 
    ecs.employee_id,
    es.department_id,
    ds.department_name,
    ecs.certification_id,
    cs.certification_name,
    ecs.certification_status,
    ecs.certification_target_date,
    ecs.certification_completion_date,
    CASE 
        WHEN ecs.certification_completion_date IS NOT NULL THEN 'Completed'
        WHEN ecs.certification_target_date < CURRENT_DATE THEN 'At Risk'
        ELSE 'Pending'
    END AS certification_status,
    COALESCE((
        SELECT count(DISTINCT ecs1.employee_id)
        FROM ecs AS ecs1
        WHERE ecs1.certification_status = 'Completed' 
          AND ecs1.department_id = es.department_id 
          AND ecs1.certification_id = ecs.certification_id
    ), 0) AS employees_certified_count
FROM ecs
INNER JOIN es ON ecs.employee_id = es.employee_id
INNER JOIN ds ON es.department_id = ds.department_id
INNER JOIN cs ON ecs.certification_id = cs.certification_id
"""

gold_certification_status_df = spark.sql(gold_certification_query)
gold_certification_status_df.write.csv(TARGET_PATH + "/gold_certification_status.csv", mode="overwrite", header=True)

# Department Compliance Report
department_compliance_report_query = """
SELECT 
    dcs.report_id,
    crs.report_generation_date,
    dcs.department_id,
    ds.department_name,
    dcs.non_compliant_status,
    dcs.non_compliance_reason
FROM dcs
INNER JOIN crs ON dcs.report_id = crs.report_id
INNER JOIN ds ON dcs.department_id = ds.department_id
"""

department_compliance_report_df = spark.sql(department_compliance_report_query)
department_compliance_report_df.write.csv(TARGET_PATH + "/department_compliance_report.csv", mode="overwrite", header=True)

# Compliance Audit Trail
compliance_audit_trail_query = """
SELECT 
    ats.audit_id,
    ats.employee_id,
    ats.certification_id,
    ats.verification_status,
    ats.verification_date
FROM ats
"""

compliance_audit_trail_df = spark.sql(compliance_audit_trail_query)
compliance_audit_trail_df.write.csv(TARGET_PATH + "/compliance_audit_trail.csv", mode="overwrite", header=True)
```