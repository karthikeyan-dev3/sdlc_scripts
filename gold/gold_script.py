import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# ----------------------------
# 1) Read source tables from S3
# ----------------------------
loans_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loans_silver.{FILE_FORMAT}/")
)

customers_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)

branches_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/branches_silver.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
loans_silver_df.createOrReplaceTempView("loans_silver")
customers_silver_df.createOrReplaceTempView("customers_silver")
branches_silver_df.createOrReplaceTempView("branches_silver")

# ============================================================
# Target: gold.gold_loan_performance
# Source: silver.loans_silver ls
#         LEFT JOIN silver.customers_silver cs ON ls.customer_id = cs.customer_id
#         LEFT JOIN silver.branches_silver bs ON ls.branch_id = bs.branch_id
# ============================================================
gold_loan_performance_df = spark.sql(
    """
    SELECT
        CAST(ls.loan_id AS STRING)                 AS loan_id,
        CAST(ls.customer_id AS STRING)             AS customer_id,
        CAST(ls.branch_id AS STRING)               AS branch_id,
        CAST(ls.loan_amount AS INT)                AS loan_amount,
        CAST(ls.interest_rate AS FLOAT)            AS interest_rate,
        CAST(ls.loan_status AS STRING)             AS loan_status,
        DATE(CAST(ls.repayment_date AS STRING))    AS repayment_date,
        CAST(ls.amount_repaid AS INT)              AS amount_repaid,
        CAST(ls.outstanding_balance AS INT)        AS outstanding_balance
    FROM loans_silver ls
    LEFT JOIN customers_silver cs
        ON ls.customer_id = cs.customer_id
    LEFT JOIN branches_silver bs
        ON ls.branch_id = bs.branch_id
    """
)

(
    gold_loan_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_loan_performance.csv")
)

# ============================================================
# Target: gold.gold_customer_data
# Source: silver.customers_silver cs
#         LEFT JOIN silver.loans_silver ls ON cs.customer_id = ls.customer_id
# ============================================================
gold_customer_data_df = spark.sql(
    """
    SELECT
        CAST(cs.customer_id AS STRING)             AS customer_id,
        CAST(cs.customer_name AS STRING)           AS customer_name,
        CAST(cs.customer_segment AS STRING)        AS customer_segment,
        CAST(COUNT(ls.loan_id) AS INT)             AS total_loans,
        CAST(AVG(CAST(ls.loan_amount AS INT)) AS FLOAT) AS average_loan_amount
    FROM customers_silver cs
    LEFT JOIN loans_silver ls
        ON cs.customer_id = ls.customer_id
    GROUP BY
        cs.customer_id,
        cs.customer_name,
        cs.customer_segment
    """
)

(
    gold_customer_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_data.csv")
)

# ============================================================
# Target: gold.gold_branch_financials
# Source: silver.branches_silver bs
#         LEFT JOIN silver.loans_silver ls ON bs.branch_id = ls.branch_id
# ============================================================
gold_branch_financials_df = spark.sql(
    """
    SELECT
        CAST(bs.branch_id AS STRING)               AS branch_id,
        CAST(bs.branch_name AS STRING)             AS branch_name,
        CAST(bs.cost_center AS STRING)             AS cost_center,
        CAST(COUNT(ls.loan_id) AS INT)             AS total_loans_issued,
        CAST(SUM(CAST(ls.amount_repaid AS INT)) AS INT) AS total_repayments_received
    FROM branches_silver bs
    LEFT JOIN loans_silver ls
        ON bs.branch_id = ls.branch_id
    GROUP BY
        bs.branch_id,
        bs.branch_name,
        bs.cost_center
    """
)

(
    gold_branch_financials_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_branch_financials.csv")
)

job.commit()
