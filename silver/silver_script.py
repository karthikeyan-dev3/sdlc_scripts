from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark and Glue context
spark = SparkSession.builder \
    .appName("AWS Glue PySpark Job") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)

# -------------------------------------
# Departments Table
# -------------------------------------

# Read source table
departments_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/departments_bronze.csv/")

# Create a temp view
departments_df.createOrReplaceTempView("departments_bronze")

# Transformations using SQL
departments_transformed_df = spark.sql("""
    SELECT 
        department_id,
        TRIM(UPPER(department_name)) AS department_name,
        ROW_NUMBER() OVER(PARTITION BY department_id ORDER BY department_id) AS row_num
    FROM departments_bronze
""").filter("row_num = 1").drop("row_num")

# Write to target
departments_transformed_df.write \
    .csv("s3://sdlc-agent-bucket/engineering-agent/silver/departments_silver.csv", header=True, mode="overwrite")

# -------------------------------------
# Employees Table
# -------------------------------------

# Read source table
employees_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/employees_bronze.csv/")

# Create temp views
employees_df.createOrReplaceTempView("employees_bronze")
departments_transformed_df.createOrReplaceTempView("departments_silver")

# Transformations using SQL
employees_transformed_df = spark.sql("""
    SELECT 
        eb.employee_id,
        eb.first_name,
        eb.last_name,
        eb.email,
        eb.phone_number,
        eb.hire_date,
        ds.department_name,
        ROW_NUMBER() OVER(PARTITION BY eb.employee_id ORDER BY eb.employee_id) AS row_num
    FROM employees_bronze eb
    INNER JOIN departments_silver ds ON eb.department_id = ds.department_id
""").filter("row_num = 1").drop("row_num")

# Write to target
employees_transformed_df.write \
    .csv("s3://sdlc-agent-bucket/engineering-agent/silver/employees_silver.csv", header=True, mode="overwrite")

# -------------------------------------
# Certifications Table
# -------------------------------------

# Read source table
certifications_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/certifications_bronze.csv/")

# Create temp views
certifications_df.createOrReplaceTempView("certifications_bronze")
employees_transformed_df.createOrReplaceTempView("employees_silver")

# Transformations using SQL
certifications_transformed_df = spark.sql("""
    SELECT 
        cb.certification_id,
        cb.employee_id,
        TRIM(UPPER(cb.certification_name)) AS certification_name,
        DATE(cb.completion_date) AS completion_date,
        es.department_name,
        ROW_NUMBER() OVER(PARTITION BY cb.certification_id ORDER BY cb.certification_id) AS row_num
    FROM certifications_bronze cb
    INNER JOIN employees_silver es ON cb.employee_id = es.employee_id
    INNER JOIN departments_silver ds ON es.department_name = ds.department_name
""").filter("row_num = 1").drop("row_num")

# Write to target
certifications_transformed_df.write \
    .csv("s3://sdlc-agent-bucket/engineering-agent/silver/certifications_silver.csv", header=True, mode="overwrite")

# -------------------------------------
# Dates Table
# -------------------------------------

# Read source table
dates_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/calendar_bronze.csv/")

# Create temp view
dates_df.createOrReplaceTempView("calendar_bronze")

# Transformations using SQL
dates_transformed_df = spark.sql("""
    SELECT 
        date,
        fiscal_year,
        fiscal_period,
        ROW_NUMBER() OVER(PARTITION BY date ORDER BY date) AS row_num
    FROM calendar_bronze
""").filter("row_num = 1").drop("row_num")

# Write to target
dates_transformed_df.write \
    .csv("s3://sdlc-agent-bucket/engineering-agent/silver/dates_silver.csv", header=True, mode="overwrite")