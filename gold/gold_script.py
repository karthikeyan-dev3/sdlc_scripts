import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# 1) READ SOURCE TABLES (S3) + CREATE TEMP VIEWS
# -------------------------------------------------------------------
loans_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loans_silver.{FILE_FORMAT}/")
)
loans_silver_df.createOrReplaceTempView("loans_silver")

repayments_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/repayments_silver.{FILE_FORMAT}/")
)
repayments_silver_df.createOrReplaceTempView("repayments_silver")

branches_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/branches_silver.{FILE_FORMAT}/")
)
branches_silver_df.createOrReplaceTempView("branches_silver")

customers_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)
customers_silver_df.createOrReplaceTempView("customers_silver")

loan_risk_assessments_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_risk_assessments_silver.{FILE_FORMAT}/")
)
loan_risk_assessments_silver_df.createOrReplaceTempView("loan_risk_assessments_silver")

# -------------------------------------------------------------------
# 2) TARGET: gold_loan_performance
#    mapping_details: loans_silver ls LEFT JOIN repayments_silver rs ON ls.loan_id = rs.loan_id
# -------------------------------------------------------------------
gold_loan_performance_df = spark.sql(
    """
    SELECT
        CAST(ls.loan_id AS STRING)                         AS loan_id,
        CAST(ls.loan_amount AS INT)                        AS total_loan_amount,
        CAST(rs.payment_amount AS INT)                     AS repayment_amount,
        CAST(ls.loan_status AS STRING)                     AS loan_status,
        CAST(ls.branch_id AS STRING)                       AS branch_id,
        CAST(ls.customer_id AS STRING)                     AS customer_id,
        DATE(rs.payment_date)                              AS transaction_date
    FROM loans_silver ls
    LEFT JOIN repayments_silver rs
        ON ls.loan_id = rs.loan_id
    """
)

(
    gold_loan_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_loan_performance.csv")
)

# -------------------------------------------------------------------
# 3) TARGET: gold_branch_performance
#    mapping_details: branches_silver bs LEFT JOIN loans_silver ls ON bs.branch_id = ls.branch_id
# -------------------------------------------------------------------
gold_branch_performance_df = spark.sql(
    """
    SELECT
        CAST(bs.branch_id AS STRING)   AS branch_id,
        CAST(bs.city AS STRING)        AS city,
        CAST(bs.state AS STRING)       AS state,
        CAST(bs.branch_type AS STRING) AS branch_type,
        CAST(ls.loan_id AS STRING)     AS total_loans,
        CAST(ls.loan_status AS STRING) AS default_rate,
        CAST(ls.loan_id AS STRING)     AS reporting_period
    FROM branches_silver bs
    LEFT JOIN loans_silver ls
        ON bs.branch_id = ls.branch_id
    """
)

(
    gold_branch_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_branch_performance.csv")
)

# -------------------------------------------------------------------
# 4) TARGET: gold_repayment_patterns
#    mapping_details: repayments_silver rs INNER JOIN loans_silver ls ON rs.loan_id = ls.loan_id
#                     LEFT JOIN loan_risk_assessments_silver lras ON ls.loan_id = lras.loan_id
# -------------------------------------------------------------------
gold_repayment_patterns_df = spark.sql(
    """
    SELECT
        CAST(ls.customer_id AS STRING) AS customer_id,
        CAST(rs.loan_id AS STRING)     AS loan_id,
        DATE(rs.payment_date)          AS repayment_date,
        CAST(rs.payment_amount AS INT) AS amount_repaid
    FROM repayments_silver rs
    INNER JOIN loans_silver ls
        ON rs.loan_id = ls.loan_id
    LEFT JOIN loan_risk_assessments_silver lras
        ON ls.loan_id = lras.loan_id
    """
)

(
    gold_repayment_patterns_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_repayment_patterns.csv")
)

# -------------------------------------------------------------------
# 5) TARGET: gold_customer_insights
#    mapping_details: customers_silver cs LEFT JOIN loans_silver ls ON cs.customer_id = ls.customer_id
#                     LEFT JOIN loan_risk_assessments_silver lras ON ls.loan_id = lras.loan_id
# -------------------------------------------------------------------
gold_customer_insights_df = spark.sql(
    """
    SELECT
        CAST(cs.customer_id AS STRING)     AS customer_id,
        CAST(cs.income_segment AS STRING)  AS income_segment,
        CAST(ls.loan_id AS STRING)         AS total_loans,
        CAST(lras.risk_category AS STRING) AS high_risk_category_flag,
        CAST(ls.branch_id AS STRING)       AS branch_id
    FROM customers_silver cs
    LEFT JOIN loans_silver ls
        ON cs.customer_id = ls.customer_id
    LEFT JOIN loan_risk_assessments_silver lras
        ON ls.loan_id = lras.loan_id
    """
)

(
    gold_customer_insights_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_insights.csv")
)

job.commit()