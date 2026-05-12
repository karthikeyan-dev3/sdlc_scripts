import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark and Glue Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables from S3
departments_silver_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/departments_silver.{FILE_FORMAT}/")
dates_silver_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/dates_silver.{FILE_FORMAT}/")
certifications_silver_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/certifications_silver.{FILE_FORMAT}/")
employees_silver_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/employees_silver.{FILE_FORMAT}/")

# Create temp views
departments_silver_df.createOrReplaceTempView("ds")
dates_silver_df.createOrReplaceTempView("dts")
certifications_silver_df.createOrReplaceTempView("cs")
employees_silver_df.createOrReplaceTempView("es")

# Transform: gold_dim_departments
gold_dim_departments_df = spark.sql("""
    SELECT 
        ds.department_id AS department_id,
        TRIM(ds.department_name) AS department_name,
        UPPER(ds.normalized_department_code) AS normalized_department_code
    FROM ds
    """)
gold_dim_departments_df.write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_dim_departments.csv")

# Transform: gold_dim_dates
gold_dim_dates_df = spark.sql("""
    SELECT 
        dts.date_id AS date_id,
        dts.date AS date,
        dts.fiscal_year AS fiscal_year,
        dts.fiscal_period AS fiscal_period
    FROM dts
    """)
gold_dim_dates_df.write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_dim_dates.csv")

# Transform: gold_fact_certifications
gold_fact_certifications_df = spark.sql("""
    SELECT 
        cs.certification_id AS certification_id,
        cs.employee_id AS employee_id,
        TRIM(cs.certification_name) AS certification_name,
        ds.department_id AS department_id,
        cs.completion_date AS completion_date,
        cs.deadline_date AS deadline_date,
        CASE WHEN cs.completion_date <= cs.deadline_date THEN TRUE ELSE FALSE END AS is_compliant
    FROM cs
    INNER JOIN es ON cs.employee_id = es.employee_id
    INNER JOIN ds ON es.department_id = ds.department_id
    """)
gold_fact_certifications_df.write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_fact_certifications.csv")