import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Read source tables from S3
# -----------------------------------------------------------------------------------
lts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_transactions_silver.{FILE_FORMAT}/")
)

cs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)

bs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/branches_silver.{FILE_FORMAT}/")
)

lps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_performance_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# Create temp views
# -----------------------------------------------------------------------------------
lts_df.createOrReplaceTempView("loan_transactions_silver")
cs_df.createOrReplaceTempView("customers_silver")
bs_df.createOrReplaceTempView("branches_silver")
lps_df.createOrReplaceTempView("loan_performance_silver")

# -----------------------------------------------------------------------------------
# Target: gold_loan_transactions
# -----------------------------------------------------------------------------------
gold_loan_transactions_df = spark.sql("""
SELECT
  CAST(lts.loan_id AS STRING)                                   AS loan_id,
  CAST(lts.customer_id AS STRING)                               AS customer_id,
  CAST(lts.branch_id AS STRING)                                 AS branch_id,
  CAST(lts.transaction_date AS DATE)                            AS transaction_date,
  CAST(lts.loan_amount AS DECIMAL(18,2))                        AS loan_amount,
  CAST(lts.interest_rate AS DECIMAL(9,4))                       AS interest_rate,
  CAST(lts.loan_status AS STRING)                               AS loan_status,
  CAST(lts.repayment_amount AS DECIMAL(18,2))                   AS repayment_amount,
  CAST(lts.outstanding_balance AS DECIMAL(18,2))                AS outstanding_balance
FROM loan_transactions_silver lts
""")

(
    gold_loan_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_loan_transactions.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_customers
# -----------------------------------------------------------------------------------
gold_customers_df = spark.sql("""
SELECT
  CAST(cs.customer_id AS STRING)        AS customer_id,
  CAST(cs.customer_name AS STRING)      AS customer_name,
  CAST(cs.address AS STRING)            AS address,
  CAST(cs.customer_segment AS STRING)   AS customer_segment
FROM customers_silver cs
""")

(
    gold_customers_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customers.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_branches
# -----------------------------------------------------------------------------------
gold_branches_df = spark.sql("""
SELECT
  CAST(bs.branch_id AS STRING)              AS branch_id,
  CAST(bs.branch_name AS STRING)            AS branch_name,
  CAST(bs.branch_location AS STRING)        AS branch_location,
  CAST(bs.branch_manager AS STRING)         AS branch_manager,
  CAST(bs.branch_opening_date AS DATE)      AS branch_opening_date
FROM branches_silver bs
""")

(
    gold_branches_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_branches.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_loan_analytics
# -----------------------------------------------------------------------------------
gold_loan_analytics_df = spark.sql("""
SELECT
  CAST(lts.branch_id AS STRING)                          AS branch_id,
  CAST(lts.transaction_date AS DATE)                     AS date,
  CAST(COUNT(lts.loan_id) AS BIGINT)                     AS total_loans_issued,
  CAST(SUM(CAST(lts.repayment_amount AS DECIMAL(18,2))) AS DECIMAL(18,2))     AS total_repayments_received,
  CAST(AVG(CAST(lts.loan_amount AS DECIMAL(18,2))) AS DECIMAL(18,2))          AS average_loan_amount,
  CAST(AVG(CAST(lts.interest_rate AS DECIMAL(9,4))) AS DECIMAL(9,4))          AS average_interest_rate,
  CAST(SUM(CAST(lts.outstanding_balance AS DECIMAL(18,2))) AS DECIMAL(18,2))  AS total_outstanding_balance
FROM loan_transactions_silver lts
GROUP BY
  lts.branch_id,
  lts.transaction_date
""")

(
    gold_loan_analytics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_loan_analytics.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_loan_performance
# -----------------------------------------------------------------------------------
gold_loan_performance_df = spark.sql("""
SELECT
  CAST(lps.loan_id AS STRING)                             AS loan_id,
  CAST(lps.customer_id AS STRING)                         AS customer_id,
  CAST(lps.branch_id AS STRING)                           AS branch_id,
  CAST(lps.current_status AS STRING)                      AS current_status,
  CAST(lps.days_past_due AS INT)                          AS days_past_due,
  CAST(lps.repayment_behavior_score AS DECIMAL(6,2))       AS repayment_behavior_score
FROM loan_performance_silver lps
""")

(
    gold_loan_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_loan_performance.csv")
)

job.commit()