from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, row_number
from pyspark.sql.window import Window

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read source tables
hr_employees_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/bronze/hr_employees_raw.csv/")
hr_departments_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/bronze/hr_departments_raw.csv/")
lms_certification_completions_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/bronze/lms_certification_completions_raw.csv/")
lms_certifications_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/bronze/lms_certifications_raw.csv/")

# Create temp views
hr_employees_df.createOrReplaceTempView("hr_employees_raw")
hr_departments_df.createOrReplaceTempView("hr_departments_raw")
lms_certification_completions_df.createOrReplaceTempView("lms_certification_completions_raw")
lms_certifications_df.createOrReplaceTempView("lms_certifications_raw")

# Step 1: Department Employees Silver
department_employees_query = """
SELECT
    er.employee_id,
    er.department_id,
    UPPER(TRIM(dr.department_name)) AS department_name
FROM hr_employees_raw er
LEFT JOIN hr_departments_raw dr ON er.department_id = dr.department_id
"""
department_employees_df = spark.sql(department_employees_query)
department_employees_dedup_df = department_employees_df.withColumn(
    "row_num",
    row_number().over(Window.partitionBy("employee_id").orderBy(col("employee_id")))
).filter("row_num = 1").drop("row_num")

department_employees_dedup_df.write.mode("overwrite").csv(f"s3://sdlc-agent-bucket/engineering-agent/silver/department_employees_silver.csv", header=True)

# Step 2: Certification Records Silver
certification_records_query = """
SELECT
    ccr.employee_id,
    ccr.certification_id,
    UPPER(TRIM(lcr.certification_name)) AS certification_name,
    ccr.completion_date
FROM lms_certification_completions_raw ccr
LEFT JOIN lms_certifications_raw lcr ON ccr.certification_id = lcr.certification_id
"""
certification_records_df = spark.sql(certification_records_query)
certification_records_dedup_df = certification_records_df.withColumn(
    "row_num",
    row_number().over(Window.partitionBy("employee_id", "certification_id").orderBy(col("completion_date").desc()))
).filter("row_num = 1").drop("row_num")

certification_records_dedup_df.write.mode("overwrite").csv(f"s3://sdlc-agent-bucket/engineering-agent/silver/certification_records_silver.csv", header=True)

# Step 3: Department Certification Summary Silver
certification_records_dedup_df.createOrReplaceTempView("certification_records_silver")
department_employees_dedup_df.createOrReplaceTempView("department_employees_silver")

department_certification_summary_query = """
SELECT
    des.department_id,
    des.department_name,
    COUNT(DISTINCT des.employee_id) AS total_employees,
    COUNT(DISTINCT crs.employee_id) AS employees_certified_count
FROM department_employees_silver des
LEFT JOIN certification_records_silver crs ON des.employee_id = crs.employee_id
GROUP BY des.department_id, des.department_name
"""
department_certification_summary_df = spark.sql(department_certification_summary_query)

department_certification_summary_df.write.mode("overwrite").csv(f"s3://sdlc-agent-bucket/engineering-agent/silver/department_certification_summary_silver.csv", header=True)

# Step 4: Department Compliance Status Silver
department_certification_summary_df.createOrReplaceTempView("department_certification_summary_silver")

department_compliance_status_query = """
SELECT
    dcss.department_id,
    CASE
        WHEN dcss.employees_certified_count >= dcss.total_employees THEN 'COMPLIANT'
        ELSE 'NON_COMPLIANT'
    END AS compliance_status,
    CURRENT_TIMESTAMP AS last_report_update
FROM department_certification_summary_silver dcss
"""
department_compliance_status_df = spark.sql(department_compliance_status_query)

department_compliance_status_df.write.mode("overwrite").csv(f"s3://sdlc-agent-bucket/engineering-agent/silver/department_compliance_status_silver.csv", header=True)