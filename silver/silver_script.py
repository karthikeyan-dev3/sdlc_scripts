
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize GlueContext and SparkSession
glueContext = GlueContext(SparkSession.builder.getOrCreate())
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read and process departments_silver
departments_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/departments_bronze.{FILE_FORMAT}/")
departments_df.createOrReplaceTempView("db")

departments_silver_df = spark.sql("""
    SELECT 
        department_id, 
        TRIM(UPPER(department_name)) AS department_name, 
        TRIM(LOWER(department_code)) AS department_code
    FROM (
        SELECT 
            db.department_id, 
            db.department_name, 
            db.department_code,
            ROW_NUMBER() OVER (PARTITION BY db.department_id ORDER BY db.department_id) as row_num
        FROM db
    ) WHERE row_num = 1
""")

departments_silver_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/departments_silver.csv", header=True)

# Read and process training_coordinators_silver
training_coordinators_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/training_coordinators_bronze.{FILE_FORMAT}/")
training_coordinators_df.createOrReplaceTempView("tcb")

training_coordinators_silver_df = spark.sql("""
    SELECT 
        training_coordinator_id
    FROM (
        SELECT 
            tcb.training_coordinator_id,
            ROW_NUMBER() OVER (PARTITION BY tcb.training_coordinator_id ORDER BY tcb.training_coordinator_id) as row_num
        FROM tcb
    ) WHERE row_num = 1
""")

training_coordinators_silver_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/training_coordinators_silver.csv", header=True)

# Read and process department_coordinator_assignment_silver
department_coordinator_assignment_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/department_coordinator_assignment_bronze.{FILE_FORMAT}/")
department_coordinator_assignment_df.createOrReplaceTempView("dcab")

department_coordinator_assignment_silver_df = spark.sql("""
    SELECT 
        dcab.department_id, 
        dcab.training_coordinator_id
    FROM (
        SELECT 
            dcab.department_id,
            dcab.training_coordinator_id,
            ROW_NUMBER() OVER (PARTITION BY dcab.department_id, dcab.training_coordinator_id ORDER BY dcab.department_id) as row_num
        FROM dcab
        JOIN departments_silver ds ON dcab.department_id = ds.department_id
        JOIN training_coordinators_silver tcs ON dcab.training_coordinator_id = tcs.training_coordinator_id
    ) WHERE row_num = 1
""")

department_coordinator_assignment_silver_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/department_coordinator_assignment_silver.csv", header=True)

# Read and process employees_silver
employees_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/employees_bronze.{FILE_FORMAT}/")
employees_df.createOrReplaceTempView("eb")

employees_silver_df = spark.sql("""
    SELECT 
        eb.employee_id, 
        eb.department_id
    FROM (
        SELECT 
            eb.employee_id, 
            eb.department_id,
            ROW_NUMBER() OVER (PARTITION BY eb.employee_id ORDER BY eb.employee_id) as row_num
        FROM eb
        JOIN departments_silver ds ON eb.department_id = ds.department_id
    ) WHERE row_num = 1
""")

employees_silver_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/employees_silver.csv", header=True)

# Read and process certifications_silver
certifications_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/certifications_bronze.{FILE_FORMAT}/")
certifications_df.createOrReplaceTempView("cb")

certifications_silver_df = spark.sql("""
    SELECT 
        certification_id
    FROM (
        SELECT 
            cb.certification_id,
            ROW_NUMBER() OVER (PARTITION BY cb.certification_id ORDER BY cb.certification_id) as row_num
        FROM cb
    ) WHERE row_num = 1
""")

certifications_silver_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/certifications_silver.csv", header=True)

# Read and process department_required_certifications_silver
department_required_certifications_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/department_required_certifications_bronze.{FILE_FORMAT}/")
department_required_certifications_df.createOrReplaceTempView("drcb")

department_required_certifications_silver_df = spark.sql("""
    SELECT 
        drcb.department_id, 
        drcb.certification_id
    FROM (
        SELECT 
            drcb.department_id, 
            drcb.certification_id,
            ROW_NUMBER() OVER (PARTITION BY drcb.department_id, drcb.certification_id ORDER BY drcb.department_id) as row_num
        FROM drcb
        JOIN departments_silver ds ON drcb.department_id = ds.department_id
        JOIN certifications_silver cs ON drcb.certification_id = cs.certification_id
    ) WHERE row_num = 1
""")

department_required_certifications_silver_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/department_required_certifications_silver.csv", header=True)

# Read and process employee_certification_status_silver
employee_certification_events_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/employee_certification_events_bronze.{FILE_FORMAT}/")
employee_certification_events_df.createOrReplaceTempView("eceb")

employee_certification_status_silver_df = spark.sql("""
    SELECT 
        eceb.employee_id, 
        eceb.certification_id, 
        eceb.status, 
        CAST(eceb.completion_date AS DATE) AS completion_date
    FROM (
        SELECT 
            eceb.employee_id, 
            eceb.certification_id, 
            eceb.status, 
            eceb.completion_date,
            ROW_NUMBER() OVER (PARTITION BY eceb.employee_id, eceb.certification_id ORDER BY eceb.completion_date DESC) as row_num
        FROM eceb
        JOIN employees_silver es ON eceb.employee_id = es.employee_id
        JOIN certifications_silver cs ON eceb.certification_id = cs.certification_id
    ) WHERE row_num = 1
""")

employee_certification_status_silver_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/employee_certification_status_silver.csv", header=True)

# Read and process fiscal_calendar_silver
fiscal_calendar_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/fiscal_calendar_bronze.{FILE_FORMAT}/")
fiscal_calendar_df.createOrReplaceTempView("fcb")

fiscal_calendar_silver_df = spark.sql("""
    SELECT 
        CAST(fcb.calendar_date AS DATE) AS calendar_date, 
        fcb.fiscal_period
    FROM (
        SELECT 
            fcb.calendar_date, 
            fcb.fiscal_period,
            ROW_NUMBER() OVER (PARTITION BY fcb.calendar_date ORDER BY fcb.calendar_date) as row_num
        FROM fcb
    ) WHERE row_num = 1
""")

fiscal_calendar_silver_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/fiscal_calendar_silver.csv", header=True)