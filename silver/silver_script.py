```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F

# Initialize Spark and Glue context
spark = SparkSession.builder \
    .appName("AWS Glue PySpark Job") \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Departments table
departments_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/departments_bronze.{FILE_FORMAT}/")
departments_bronze_df.createOrReplaceTempView("departments_bronze")

departments_silver_query = """
SELECT department_id, 
       TRIM(UPPER(department_name)) AS department_name
FROM (
    SELECT db.department_id, 
           db.department_name,
           ROW_NUMBER() OVER (PARTITION BY db.department_id ORDER BY db._ingest_ts DESC, db._source_file DESC) AS rn
    FROM departments_bronze db
) 
WHERE rn = 1
"""

departments_silver_df = spark.sql(departments_silver_query)
departments_silver_df.write.csv(f"{TARGET_PATH}/departments_silver.csv", mode="overwrite", header=True)

# Employees table
employees_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/employees_bronze.{FILE_FORMAT}/")
employees_bronze_df.createOrReplaceTempView("employees_bronze")

employees_silver_query = """
SELECT eb.employee_id, 
       eb.first_name, 
       eb.last_name, 
       es.department_id
FROM (
    SELECT eb.employee_id, 
           eb.first_name, 
           eb.last_name, 
           eb.department_id, 
           ROW_NUMBER() OVER (PARTITION BY eb.employee_id ORDER BY eb._ingest_ts DESC, eb._source_file DESC) AS rn
    FROM employees_bronze eb
    LEFT JOIN departments_silver ds ON eb.department_id = ds.department_id
) 
WHERE rn = 1
"""

employees_silver_df = spark.sql(employees_silver_query)
employees_silver_df.write.csv(f"{TARGET_PATH}/employees_silver.csv", mode="overwrite", header=True)

# Certifications table
certifications_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/certifications_bronze.{FILE_FORMAT}/")
certifications_bronze_df.createOrReplaceTempView("certifications_bronze")

certifications_silver_query = """
SELECT cb.certification_id, 
       TRIM(UPPER(cb.certification_name)) AS certification_name
FROM (
    SELECT cb.certification_id, 
           cb.certification_name,
           ROW_NUMBER() OVER (PARTITION BY cb.certification_id ORDER BY cb._ingest_ts DESC, cb._source_file DESC) AS rn
    FROM certifications_bronze cb
) 
WHERE rn = 1
"""

certifications_silver_df = spark.sql(certifications_silver_query)
certifications_silver_df.write.csv(f"{TARGET_PATH}/certifications_silver.csv", mode="overwrite", header=True)

# Certification Statuses table
certification_statuses_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/certification_statuses_bronze.{FILE_FORMAT}/")
certification_statuses_bronze_df.createOrReplaceTempView("certification_statuses_bronze")

certification_statuses_silver_query = """
SELECT UPPER(TRIM(csb.certification_status)) AS certification_status
FROM (
    SELECT csb.certification_status,
           ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(csb.certification_status)) ORDER BY csb._ingest_ts DESC, csb._source_file DESC) AS rn
    FROM certification_statuses_bronze csb
) 
WHERE rn = 1
"""

certification_statuses_silver_df = spark.sql(certification_statuses_silver_query)
certification_statuses_silver_df.write.csv(f"{TARGET_PATH}/certification_statuses_silver.csv", mode="overwrite", header=True)

# Employee Certifications table
employee_certifications_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/employee_certifications_bronze.{FILE_FORMAT}/")
employee_certifications_bronze_df.createOrReplaceTempView("employee_certifications_bronze")

employee_certifications_silver_query = """
SELECT ecb.employee_id, 
       ecb.certification_id, 
       ecb.certification_target_date, 
       ecb.certification_completion_date, 
       UPPER(TRIM(css.certification_status)) AS certification_status
FROM (
    SELECT ecb.employee_id, 
           ecb.certification_id, 
           ecb.certification_target_date, 
           ecb.certification_completion_date, 
           ecb.certification_status,
           ROW_NUMBER() OVER (PARTITION BY ecb.employee_id, ecb.certification_id 
                              ORDER BY COALESCE(ecb.certification_completion_date, ecb.certification_target_date) DESC, ecb._ingest_ts DESC, ecb._source_file DESC) AS rn
    FROM employee_certifications_bronze ecb
    INNER JOIN employees_silver es ON ecb.employee_id = es.employee_id
    INNER JOIN certifications_silver cs ON ecb.certification_id = cs.certification_id
    LEFT JOIN certification_statuses_silver css ON UPPER(TRIM(ecb.certification_status)) = UPPER(TRIM(css.certification_status))
) 
WHERE rn = 1
"""

employee_certifications_silver_df = spark.sql(employee_certifications_silver_query)
employee_certifications_silver_df.write.csv(f"{TARGET_PATH}/employee_certifications_silver.csv", mode="overwrite", header=True)

# Compliance Reports table
compliance_reports_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/compliance_reports_bronze.{FILE_FORMAT}/")
compliance_reports_bronze_df.createOrReplaceTempView("compliance_reports_bronze")

compliance_reports_silver_query = """
SELECT crb.report_id, 
       crb.report_generation_date
FROM (
    SELECT crb.report_id, 
           crb.report_generation_date,
           ROW_NUMBER() OVER (PARTITION BY crb.report_id ORDER BY crb.report_generation_date DESC, crb._ingest_ts DESC, crb._source_file DESC) AS rn
    FROM compliance_reports_bronze crb
) 
WHERE rn = 1
"""

compliance_reports_silver_df = spark.sql(compliance_reports_silver_query)
compliance_reports_silver_df.write.csv(f"{TARGET_PATH}/compliance_reports_silver.csv", mode="overwrite", header=True)

# Department Compliance Statuses table
department_compliance_statuses_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/department_compliance_statuses_bronze.{FILE_FORMAT}/")
department_compliance_statuses_bronze_df.createOrReplaceTempView("department_compliance_statuses_bronze")

department_compliance_statuses_silver_query = """
SELECT dcb.report_id, 
       ds.department_id, 
       dcb.non_compliant_status, 
       TRIM(dcb.non_compliance_reason) AS non_compliance_reason
FROM (
    SELECT dcb.report_id, 
           dcb.department_id, 
           dcb.non_compliant_status, 
           dcb.non_compliance_reason,
           ROW_NUMBER() OVER (PARTITION BY dcb.report_id, dcb.department_id ORDER BY crb.report_generation_date DESC, dcb._ingest_ts DESC, dcb._source_file DESC) AS rn
    FROM department_compliance_statuses_bronze dcb
    INNER JOIN compliance_reports_silver crs ON dcb.report_id = crs.report_id
    INNER JOIN departments_silver ds ON dcb.department_id = ds.department_id
) 
WHERE rn = 1
"""

department_compliance_statuses_silver_df = spark.sql(department_compliance_statuses_silver_query)
department_compliance_statuses_silver_df.write.csv(f"{TARGET_PATH}/department_compliance_statuses_silver.csv", mode="overwrite", header=True)

# Audit Trails table
audit_trails_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/audit_trails_bronze.{FILE_FORMAT}/")
audit_trails_bronze_df.createOrReplaceTempView("audit_trails_bronze")

audit_trails_silver_query = """
SELECT atb.audit_id, 
       atb.employee_id, 
       atb.certification_id, 
       UPPER(TRIM(atb.verification_status)) AS verification_status,
       atb.verification_date
FROM (
    SELECT atb.audit_id, 
           atb.employee_id, 
           atb.certification_id, 
           atb.verification_status,
           atb.verification_date,
           ROW_NUMBER() OVER (PARTITION BY atb.audit_id ORDER BY atb.verification_date DESC, atb._ingest_ts DESC, atb._source_file DESC) AS rn
    FROM audit_trails_bronze atb
    INNER JOIN employees_silver es ON atb.employee_id = es.employee_id
    INNER JOIN certifications_silver cs ON atb.certification_id = cs.certification_id
) 
WHERE rn = 1
"""

audit_trails_silver_df = spark.sql(audit_trails_silver_query)
audit_trails_silver_df.write.csv(f"{TARGET_PATH}/audit_trails_silver.csv", mode="overwrite", header=True)
```