from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("silver_gold_job", {})

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"
WRITE_MODE = "overwrite"

# ============================================================
# Source Reads (Silver)
# ============================================================

loan_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_transactions_silver.{FILE_FORMAT}/")
)
loan_transactions_silver_df.createOrReplaceTempView("loan_transactions_silver")

customers_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)
customers_silver_df.createOrReplaceTempView("customers_silver")

branch_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/branch_master_silver.{FILE_FORMAT}/")
)
branch_master_silver_df.createOrReplaceTempView("branch_master_silver")

loan_analytics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_analytics_silver.{FILE_FORMAT}/")
)
loan_analytics_silver_df.createOrReplaceTempView("loan_analytics_silver")

# ============================================================
# Target: gold.gold_loan_transactions
# Mapping: silver.loan_transactions_silver lts
# ============================================================

gold_loan_transactions_sql = """
SELECT
    CAST(lts.transaction_id AS STRING) AS transaction_id,
    CAST(lts.loan_id AS STRING) AS loan_id,
    CAST(lts.customer_id AS STRING) AS customer_id,
    CAST(lts.branch_id AS STRING) AS branch_id,
    CAST(lts.transaction_date AS DATE) AS transaction_date,
    CAST(lts.amount AS INT) AS amount,
    CAST(lts.interest_rate AS FLOAT) AS interest_rate,
    CAST(lts.transaction_type AS STRING) AS transaction_type
FROM loan_transactions_silver lts
"""

gold_loan_transactions_df = spark.sql(gold_loan_transactions_sql)

gold_loan_transactions_output_path = f"{TARGET_PATH}/gold_loan_transactions.csv"
(
    gold_loan_transactions_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(gold_loan_transactions_output_path)
)

# ============================================================
# Target: gold.gold_customers
# Mapping: silver.customers_silver cs
# ============================================================

gold_customers_sql = """
SELECT
    CAST(cs.customer_id AS STRING) AS customer_id,
    CAST(cs.customer_name AS STRING) AS customer_name,
    CAST(cs.customer_income AS INT) AS customer_income,
    CAST(cs.credit_score AS INT) AS credit_score
FROM customers_silver cs
"""

gold_customers_df = spark.sql(gold_customers_sql)

gold_customers_output_path = f"{TARGET_PATH}/gold_customers.csv"
(
    gold_customers_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(gold_customers_output_path)
)

# ============================================================
# Target: gold.gold_branch_master
# Mapping: silver.branch_master_silver bms
# ============================================================

gold_branch_master_sql = """
SELECT
    CAST(bms.branch_id AS STRING) AS branch_id,
    CAST(bms.branch_name AS STRING) AS branch_name,
    CAST(bms.branch_location AS STRING) AS branch_location,
    CAST(bms.branch_manager AS STRING) AS branch_manager
FROM branch_master_silver bms
"""

gold_branch_master_df = spark.sql(gold_branch_master_sql)

gold_branch_master_output_path = f"{TARGET_PATH}/gold_branch_master.csv"
(
    gold_branch_master_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(gold_branch_master_output_path)
)

# ============================================================
# Target: gold.gold_loan_analytics
# Mapping: silver.loan_analytics_silver lasum
# ============================================================

gold_loan_analytics_sql = """
SELECT
    CAST(lasum.branch_id AS STRING) AS branch_id,
    CAST(lasum.customer_id AS STRING) AS customer_id,
    CAST(lasum.total_loans AS INT) AS total_loans,
    CAST(lasum.total_disbursed AS INT) AS total_disbursed,
    CAST(lasum.total_repaid AS INT) AS total_repaid,
    CAST(lasum.current_balance AS INT) AS current_balance,
    CAST(lasum.average_interest_rate AS FLOAT) AS average_interest_rate
FROM loan_analytics_silver lasum
"""

gold_loan_analytics_df = spark.sql(gold_loan_analytics_sql)

gold_loan_analytics_output_path = f"{TARGET_PATH}/gold_loan_analytics.csv"
(
    gold_loan_analytics_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(gold_loan_analytics_output_path)
)

job.commit()
