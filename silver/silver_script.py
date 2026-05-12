import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import coalesce, trim, upper, lower, col, current_date, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.appName('glue-pyspark-transform').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Departments Table
departments_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/departments_bronze.{FILE_FORMAT}/")
departments_df.createOrReplaceTempView('db')

departments_silver = spark.sql("""
    SELECT
        TRIM(department_id) AS department_id,
        UPPER(TRIM(department_name)) AS department_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY department_id) as rn 
        FROM db
    ) tmp
    WHERE rn = 1
""")

departments_silver.write.csv(TARGET_PATH + "/departments_silver.csv", header=True, mode="overwrite")

# Coordinators Table
coordinators_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/coordinators_bronze.{FILE_FORMAT}/")
coordinators_df.createOrReplaceTempView('cb')

coordinators_silver = spark.sql("""
    SELECT
        TRIM(coordinator_id) AS coordinator_id,
        UPPER(TRIM(coordinator_name)) AS coordinator_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY coordinator_id ORDER BY coordinator_id) as rn 
        FROM cb
    ) tmp
    WHERE rn = 1
""")

coordinators_silver.write.csv(TARGET_PATH + "/coordinators_silver.csv", header=True, mode="overwrite")

# Certifications Table
certifications_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/certifications_bronze.{FILE_FORMAT}/")
certifications_df.createOrReplaceTempView('cfb')

certifications_silver = spark.sql("""
    SELECT
        TRIM(certification_id) AS certification_id,
        UPPER(TRIM(certification_name)) AS certification_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY certification_id ORDER BY certification_id) as rn 
        FROM cfb
    ) tmp
    WHERE rn = 1
""")

certifications_silver.write.csv(TARGET_PATH + "/certifications_silver.csv", header=True, mode="overwrite")

# Department Certification Assignments Table
department_certification_assignments_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/department_certification_assignments_bronze.{FILE_FORMAT}/")
department_certification_assignments_df.createOrReplaceTempView('dcb')

department_certification_assignments_silver = spark.sql("""
    SELECT
        TRIM(department_id) AS department_id,
        TRIM(certification_id) AS certification_id,
        TRIM(coordinator_id) AS coordinator_id,
        CAST(compliance_deadline AS DATE) AS compliance_deadline
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY department_id, certification_id, coordinator_id ORDER BY department_id, certification_id, coordinator_id) as rn
        FROM dcb
    ) tmp
    WHERE rn = 1
""")

department_certification_assignments_silver.write.csv(TARGET_PATH + "/department_certification_assignments_silver.csv", header=True, mode="overwrite")

# Certification Completions Table
certification_completions_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/certification_completions_bronze.{FILE_FORMAT}/")
certification_completions_df.createOrReplaceTempView('ccb')

certification_completions_silver = spark.sql("""
    SELECT
        TRIM(department_id) AS department_id,
        TRIM(coordinator_id) AS coordinator_id,
        TRIM(certification_id) AS certification_id,
        completion_status,
        CAST(completion_date AS DATE) AS completion_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY department_id, coordinator_id, certification_id ORDER BY completion_date DESC) as rn
        FROM ccb
    ) tmp
    WHERE rn = 1
""")

certification_completions_silver.write.csv(TARGET_PATH + "/certification_completions_silver.csv", header=True, mode="overwrite")

# Certification Tracking Table
department_certification_assignments_silver.createOrReplaceTempView('dcas')
certification_completions_silver.createOrReplaceTempView('ccs')
departments_silver.createOrReplaceTempView('ds')
coordinators_silver.createOrReplaceTempView('cos')
certifications_silver.createOrReplaceTempView('cfs')

certification_tracking_silver = spark.sql("""
    SELECT
        dcas.department_id,
        ds.department_name,
        dcas.coordinator_id,
        cos.coordinator_name,
        dcas.certification_id,
        cfs.certification_name,
        ccs.completion_status,
        ccs.completion_date,
        dcas.compliance_deadline,
        CASE WHEN ccs.completion_status <> 'COMPLETED' AND CURRENT_DATE > dcas.compliance_deadline THEN TRUE ELSE FALSE END AS flagged_for_incomplete
    FROM dcas
    LEFT JOIN ccs
    ON dcas.department_id = ccs.department_id 
    AND dcas.coordinator_id = ccs.coordinator_id 
    AND dcas.certification_id = ccs.certification_id
    INNER JOIN ds ON dcas.department_id = ds.department_id
    INNER JOIN cos ON dcas.coordinator_id = cos.coordinator_id
    INNER JOIN cfs ON dcas.certification_id = cfs.certification_id
""")

certification_tracking_silver.write.csv(TARGET_PATH + "/certification_tracking_silver.csv", header=True, mode="overwrite")

job.commit()