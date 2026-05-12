from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Initialize GlueContext and Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Set paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables
departments_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/departments_silver.{FILE_FORMAT}/")

department_coordinator_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/department_coordinator_assignment_silver.{FILE_FORMAT}/")

department_certifications_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/department_required_certifications_silver.{FILE_FORMAT}/")

employees_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/employees_silver.{FILE_FORMAT}/")

employee_certification_status_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/employee_certification_status_silver.{FILE_FORMAT}/")

fiscal_calendar_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/fiscal_calendar_silver.{FILE_FORMAT}/")

certifications_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/certifications_silver.{FILE_FORMAT}/")

# Create temp views
departments_df.createOrReplaceTempView("ds")
department_coordinator_df.createOrReplaceTempView("dca")
department_certifications_df.createOrReplaceTempView("drc")
employees_df.createOrReplaceTempView("es")
employee_certification_status_df.createOrReplaceTempView("ecs")
fiscal_calendar_df.createOrReplaceTempView("fcs")
certifications_df.createOrReplaceTempView("cs")

# Transformations for gold_certification_compliance
gold_certification_compliance_query = """
SELECT
    ds.department_id,
    dca.training_coordinator_id,
    ds.department_name,
    ds.department_code,
    COUNT(DISTINCT drc.certification_id) AS required_certifications,
    COUNT(DISTINCT CASE WHEN ecs.status = 'completed' OR ecs.completion_date IS NOT NULL THEN CONCAT(ecs.employee_id, ecs.certification_id) END) AS completed_certifications,
    CASE WHEN COUNT(DISTINCT CASE WHEN ecs.status = 'completed' OR ecs.completion_date IS NOT NULL THEN CONCAT(ecs.employee_id, ecs.certification_id) END) >= COUNT(DISTINCT drc.certification_id) THEN 'compliant' ELSE 'non-compliant' END AS compliance_status,
    CURRENT_DATE AS report_date
FROM ds
LEFT JOIN dca ON ds.department_id = dca.department_id
LEFT JOIN drc ON ds.department_id = drc.department_id
LEFT JOIN es ON ds.department_id = es.department_id
LEFT JOIN ecs ON es.employee_id = ecs.employee_id AND drc.certification_id = ecs.certification_id
LEFT JOIN fcs ON fcs.calendar_date = CURRENT_DATE
GROUP BY ds.department_id, dca.training_coordinator_id, ds.department_name, ds.department_code
"""

gold_certification_compliance_df = spark.sql(gold_certification_compliance_query)

# Write gold_certification_compliance to target
gold_certification_compliance_df.coalesce(1).write.mode('overwrite').csv(TARGET_PATH + "/gold_certification_compliance.csv", header=True)

# Transformations for gold_training_completion
gold_training_completion_query = """
SELECT
    ecs.employee_id,
    ecs.certification_id,
    ecs.completion_date,
    ecs.status,
    fcs.fiscal_period
FROM ecs
INNER JOIN es ON ecs.employee_id = es.employee_id
INNER JOIN cs ON ecs.certification_id = cs.certification_id
LEFT JOIN fcs ON ecs.completion_date = fcs.calendar_date
"""

gold_training_completion_df = spark.sql(gold_training_completion_query)

# Write gold_training_completion to target
gold_training_completion_df.coalesce(1).write.mode('overwrite').csv(TARGET_PATH + "/gold_training_completion.csv", header=True)