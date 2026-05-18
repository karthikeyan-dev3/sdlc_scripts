import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# Read Source Tables (S3)
# -------------------------------------------------------------------
branch_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/branch_master_bronze.{FILE_FORMAT}/")
)
customer_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/customer_master_bronze.{FILE_FORMAT}/")
)
loan_applications_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_applications_bronze.{FILE_FORMAT}/")
)
repayment_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/repayment_transactions_bronze.{FILE_FORMAT}/")
)
loan_risk_assessment_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_risk_assessment_bronze.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# Create Temp Views
# -------------------------------------------------------------------
branch_master_bronze_df.createOrReplaceTempView("branch_master_bronze")
customer_master_bronze_df.createOrReplaceTempView("customer_master_bronze")
loan_applications_bronze_df.createOrReplaceTempView("loan_applications_bronze")
repayment_transactions_bronze_df.createOrReplaceTempView("repayment_transactions_bronze")
loan_risk_assessment_bronze_df.createOrReplaceTempView("loan_risk_assessment_bronze")

# ===================================================================
# Target: branches_silver
# ===================================================================
branches_silver_sql = """
WITH ranked AS (
  SELECT
    bmb.branch_id AS branch_id,
    TRIM(UPPER(bmb.branch_name)) AS branch_name,
    CONCAT(TRIM(bmb.city), ', ', TRIM(bmb.state)) AS branch_location,
    TRIM(bmb.manager_name) AS branch_manager,
    TO_DATE(CONCAT(CAST(bmb.opening_year AS STRING), '-01-01')) AS branch_opening_date,
    ROW_NUMBER() OVER (
      PARTITION BY bmb.branch_id
      ORDER BY CAST(bmb.opening_year AS INT) DESC, TRIM(UPPER(bmb.branch_name)) DESC
    ) AS rn
  FROM branch_master_bronze bmb
)
SELECT
  branch_id,
  branch_name,
  branch_location,
  branch_manager,
  branch_opening_date
FROM ranked
WHERE rn = 1
"""
branches_silver_df = spark.sql(branches_silver_sql)

(
    branches_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/branches_silver.csv")
)

# ===================================================================
# Target: customers_silver
# ===================================================================
customers_silver_sql = """
WITH base AS (
  SELECT
    cmb.customer_id AS customer_id,
    TRIM(cmb.customer_name) AS customer_name,
    CONCAT(TRIM(cmb.city), ', ', TRIM(cmb.state)) AS address,
    UPPER(TRIM(cmb.income_segment)) AS customer_segment,
    CAST(cmb.credit_score AS INT) AS credit_score_sort,
    CAST(cmb.annual_income AS DOUBLE) AS annual_income_sort
  FROM customer_master_bronze cmb
),
ranked AS (
  SELECT
    customer_id,
    customer_name,
    address,
    customer_segment,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY credit_score_sort DESC, annual_income_sort DESC
    ) AS rn
  FROM base
)
SELECT
  customer_id,
  customer_name,
  address,
  customer_segment
FROM ranked
WHERE rn = 1
"""
customers_silver_df = spark.sql(customers_silver_sql)

(
    customers_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customers_silver.csv")
)

# ===================================================================
# Target: loan_transactions_silver
# ===================================================================
loan_transactions_silver_sql = """
WITH rtb_dedup AS (
  SELECT
    rtb.loan_id,
    rtb.payment_date,
    rtb.payment_status,
    rtb.payment_amount,
    rtb.remaining_balance,
    rtb.transaction_id,
    ROW_NUMBER() OVER (
      PARTITION BY rtb.loan_id, rtb.payment_date
      ORDER BY CAST(rtb.payment_amount AS DOUBLE) DESC, CAST(rtb.transaction_id AS STRING) DESC
    ) AS rn
  FROM repayment_transactions_bronze rtb
),
rtb_latest AS (
  SELECT
    loan_id,
    payment_date,
    payment_status,
    payment_amount,
    remaining_balance
  FROM rtb_dedup
  WHERE rn = 1
)
SELECT
  lab.loan_id AS loan_id,
  lab.customer_id AS customer_id,
  lab.branch_id AS branch_id,
  COALESCE(rtb.payment_date, lab.application_date) AS transaction_date,
  CAST(lab.loan_amount AS DECIMAL(18,2)) AS loan_amount,
  CAST(lab.interest_rate AS DECIMAL(9,4)) AS interest_rate,
  COALESCE(TRIM(UPPER(lab.loan_status)), 'UNKNOWN') AS loan_status,
  CASE
    WHEN rtb.payment_status IS NULL THEN CAST(0 AS DECIMAL(18,2))
    WHEN UPPER(TRIM(rtb.payment_status)) IN ('SUCCESS','COMPLETED','PAID') THEN CAST(rtb.payment_amount AS DECIMAL(18,2))
    ELSE CAST(0 AS DECIMAL(18,2))
  END AS repayment_amount,
  CAST(COALESCE(rtb.remaining_balance, lab.loan_amount) AS DECIMAL(18,2)) AS outstanding_balance
FROM loan_applications_bronze lab
LEFT JOIN rtb_latest rtb
  ON lab.loan_id = rtb.loan_id
WHERE lab.loan_id IS NOT NULL
"""
loan_transactions_silver_df = spark.sql(loan_transactions_silver_sql)

(
    loan_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/loan_transactions_silver.csv")
)

# ===================================================================
# Target: loan_performance_silver
# ===================================================================
loan_performance_silver_sql = """
WITH lab_ranked AS (
  SELECT
    lab.loan_id,
    lab.customer_id,
    lab.branch_id,
    lab.loan_status,
    lab.loan_amount,
    lab.application_date,
    ROW_NUMBER() OVER (
      PARTITION BY lab.loan_id
      ORDER BY lab.application_date DESC
    ) AS rn
  FROM loan_applications_bronze lab
),
lab_latest AS (
  SELECT
    loan_id,
    customer_id,
    branch_id,
    loan_status,
    loan_amount,
    application_date
  FROM lab_ranked
  WHERE rn = 1
),
rtagg AS (
  SELECT
    loan_id,
    MAX(payment_date) AS last_payment_date,
    MAX(remaining_balance) AS latest_remaining_balance
  FROM repayment_transactions_bronze
  GROUP BY loan_id
),
lraagg AS (
  SELECT
    loan_id,
    MAX(risk_score) AS latest_risk_score
  FROM loan_risk_assessment_bronze
  GROUP BY loan_id
),
perf AS (
  SELECT
    lab.loan_id AS loan_id,
    lab.customer_id AS customer_id,
    lab.branch_id AS branch_id,
    COALESCE(TRIM(UPPER(lab.loan_status)), 'UNKNOWN') AS current_status,
    CASE
      WHEN COALESCE(CAST(rtagg.latest_remaining_balance AS DOUBLE), CAST(lab.loan_amount AS DOUBLE)) <= 0 THEN 0
      WHEN rtagg.last_payment_date IS NULL THEN DATEDIFF(CURRENT_DATE, lab.application_date)
      ELSE GREATEST(0, DATEDIFF(CURRENT_DATE, rtagg.last_payment_date))
    END AS days_past_due,
    lraagg.latest_risk_score AS risk_score
  FROM lab_latest lab
  LEFT JOIN rtagg
    ON lab.loan_id = rtagg.loan_id
  LEFT JOIN lraagg
    ON lab.loan_id = lraagg.loan_id
)
SELECT
  loan_id,
  customer_id,
  branch_id,
  current_status,
  days_past_due,
  CAST(
    ROUND(
      (100 - LEAST(100, days_past_due)) * 0.6
      + (COALESCE(CAST(risk_score AS DOUBLE), 50) / 100.0) * 40,
      2
    ) AS DECIMAL(6,2)
  ) AS repayment_behavior_score
FROM perf
"""
loan_performance_silver_df = spark.sql(loan_performance_silver_sql)

(
    loan_performance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/loan_performance_silver.csv")
)

job.commit()