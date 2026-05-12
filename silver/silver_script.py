from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys

# Create GlueContext and SparkSession
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.appName(args['JOB_NAME']).getOrCreate()
glueContext = GlueContext(spark)

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Process department_information_silver
department_information_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/department_information_bronze.{FILE_FORMAT}/")

department_information_df.createOrReplaceTempView("dib")

department_information_silver_df = spark.sql("""
SELECT
    department_id,
    department_name,
    department_code,
    upper(trim(department_code)) AS normalized_department_code,
    business_unit
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY department_id DESC) as rn
    FROM dib
) tmp
WHERE rn = 1
""")

department_information_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/department_information_silver.csv", header=True)

# Process certification_records_silver
certification_records_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/certification_records_bronze.{FILE_FORMAT}/")

certification_records_df.createOrReplaceTempView("crb")

certification_records_silver_df = spark.sql("""
SELECT
    crb.employee_id,
    crb.certification_id,
    dis.department_id,
    crb.certification_date,
    crb.due_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY employee_id, certification_id, due_date 
                              ORDER BY certification_date DESC) as rn
    FROM crb
    INNER JOIN department_information_silver dis ON crb.department_id = dis.department_id
) tmp
WHERE rn = 1
""")

certification_records_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/certification_records_silver.csv", header=True)

# Process risk_assessment_silver
risk_assessment_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/risk_assessment_bronze.{FILE_FORMAT}/")

risk_assessment_df.createOrReplaceTempView("rab")

risk_assessment_silver_df = spark.sql("""
SELECT
    dis.department_id,
    rab.risk_criteria_met,
    rab.assessment_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY assessment_date DESC) as rn
    FROM rab
    INNER JOIN department_information_silver dis ON rab.department_id = dis.department_id
) tmp
WHERE rn = 1
""")

risk_assessment_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/risk_assessment_silver.csv", header=True)
