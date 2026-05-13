import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.transforms import *

# Initialize Glue Context and Spark Session
glueContext = GlueContext(SparkSession.builder.getOrCreate())
spark = glueContext.spark_session

# Define source path and target path
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables from S3 and create temp views
department_employees_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/department_employees_silver.{FILE_FORMAT}/")
department_employees_df.createOrReplaceTempView("department_employees_silver")

certification_records_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/certification_records_silver.{FILE_FORMAT}/")
certification_records_df.createOrReplaceTempView("certification_records_silver")

department_certification_summary_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/department_certification_summary_silver.{FILE_FORMAT}/")
department_certification_summary_df.createOrReplaceTempView("department_certification_summary_silver")

department_compliance_status_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/department_compliance_status_silver.{FILE_FORMAT}/")
department_compliance_status_df.createOrReplaceTempView("department_compliance_status_silver")

# Transformation for gold_certification_data
gold_certification_data_query = """
SELECT 
    des.employee_id,
    des.department_id,
    des.department_name,
    crs.certification_name,
    crs.completion_date,
    dcss.employees_certified_count
FROM 
    department_employees_silver des
LEFT JOIN 
    certification_records_silver crs 
ON 
    des.employee_id = crs.employee_id
LEFT JOIN 
    department_certification_summary_silver dcss 
ON 
    des.department_id = dcss.department_id
"""

gold_certification_data_df = spark.sql(gold_certification_data_query)
gold_certification_data_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_certification_data.csv")

# Transformation for gold_department_compliance_report
gold_department_compliance_report_query = """
SELECT 
    dcs.department_id,
    dcss.department_name,
    dcss.total_employees,
    dcss.employees_certified_count,
    dcs.compliance_status,
    dcs.last_report_update AS last_report_generated
FROM 
    department_compliance_status_silver dcs
INNER JOIN 
    department_certification_summary_silver dcss 
ON 
    dcs.department_id = dcss.department_id
"""

gold_department_compliance_report_df = spark.sql(gold_department_compliance_report_query)
gold_department_compliance_report_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_department_compliance_report.csv")