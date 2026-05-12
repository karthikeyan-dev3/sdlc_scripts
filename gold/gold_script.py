
import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

# Initialize GlueContext and SparkSession
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Constants
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# Read source tables
department_information_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/department_information_silver.{FILE_FORMAT}/")
certification_records_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/certification_records_silver.{FILE_FORMAT}/")
risk_assessment_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/risk_assessment_silver.{FILE_FORMAT}/")

# Create temp views
department_information_df.createOrReplaceTempView("dis")
certification_records_df.createOrReplaceTempView("crs")
risk_assessment_df.createOrReplaceTempView("ras")

# SQL Transformation for gold_department_dimension
gold_department_dimension_df = spark.sql("""
SELECT
    TRIM(dis.department_id) AS department_id,
    TRIM(COALESCE(dis.normalized_department_code, '')) AS normalized_department_code,
    TRIM(COALESCE(dis.business_unit, '')) AS business_unit
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY department_id) as rn
    FROM dis
) filtered_dis
WHERE rn = 1
""")

# Write the result to S3
gold_department_dimension_df.write.mode('overwrite').option("header", "true").csv(f"{TARGET_PATH}/gold_department_dimension.csv")

# SQL Transformation for gold_certification_compliance
gold_certification_compliance_df = spark.sql("""
SELECT
    TRIM(dis.department_id) AS department_id,
    TRIM(COALESCE(dis.department_name, '')) AS department_name,
    COUNT(DISTINCT crs.employee_id) AS employees_certified_count,
    SUM(CASE WHEN crs.certification_completed IS NULL THEN 1 ELSE 0 END) AS certifications_due,
    CAST(COALESCE(ras.risk_criteria_met, false) AS BOOLEAN) AS at_risk_flag
FROM dis
LEFT JOIN crs ON dis.department_id = crs.department_id
LEFT JOIN ras ON dis.department_id = ras.department_id
GROUP BY dis.department_id, dis.department_name, ras.risk_criteria_met
""")

# Write the result to S3
gold_certification_compliance_df.write.mode('overwrite').option("header", "true").csv(f"{TARGET_PATH}/gold_certification_compliance.csv")
