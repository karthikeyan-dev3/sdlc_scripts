from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("silver_gold_job", {})

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"
WRITE_MODE = "overwrite"

# ============================================================
# Source Reads (Bronze)
# ============================================================

customers_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/customers_bronze.{FILE_FORMAT}/")
)
customers_bronze_df.createOrReplaceTempView("customers_bronze")

branches_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/branches_bronze.{FILE_FORMAT}/")
)
branches_bronze_df.createOrReplaceTempView("branches_bronze")

loan_applications_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_applications_bronze.{FILE_FORMAT}/")
)
loan_applications_bronze_df.createOrReplaceTempView("loan_applications_bronze")

repayment_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/repayment_transactions_bronze.{FILE_FORMAT}/")
)
repayment_transactions_bronze_df.createOrReplaceTempView("repayment_transactions_bronze")

# ============================================================
# Target: silver.customers_silver
# Mapping: bronze.customers_bronze cb
# ============================================================

customers_silver_sql = """
SELECT
    UPPER(TRIM(cb.customer_id))                 AS customer_id,
    TRIM(cb.customer_name)                      AS customer_name,
    CAST(NULL AS int)                           AS customer_age,
    TRIM(cb.income_segment)                     AS customer_segment,
    CAST(cb.annual_income AS int)               AS annual_income,
    CAST(cb.credit_score AS int)                AS credit_score
FROM customers_bronze cb
"""

customers_silver_df = spark.sql(customers_silver_sql)

customers_silver_output_path = TARGET_PATH + "/" + "customers_silver.csv"
(
    customers_silver_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(customers_silver_output_path)
)

# ============================================================
# Target: silver.branches_silver
# Mapping: bronze.branches_bronze bb
# ============================================================

branches_silver_sql = """
SELECT
    UPPER(TRIM(bb.branch_id))                                           AS branch_id,
    TRIM(bb.branch_name)                                                AS branch_name,
    CONCAT(UPPER(TRIM(bb.state)), '-', UPPER(TRIM(bb.city)))             AS region
FROM branches_bronze bb
"""

branches_silver_df = spark.sql(branches_silver_sql)

branches_silver_output_path = TARGET_PATH + "/" + "branches_silver.csv"
(
    branches_silver_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(branches_silver_output_path)
)

# ============================================================
# Target: silver.loan_transactions_silver
# Mapping: bronze.loan_applications_bronze lab
#          LEFT JOIN bronze.repayment_transactions_bronze rtb
#          ON lab.loan_id = rtb.loan_id
# ============================================================

loan_transactions_silver_sql = """
SELECT
    UPPER(TRIM(lab.loan_id))                                                          AS loan_id,
    COALESCE(rtb.payment_date, lab.application_date)                                   AS transaction_date,
    COALESCE(CAST(rtb.payment_amount AS int), CAST(lab.loan_amount AS int))            AS amount,
    COALESCE(TRIM(rtb.payment_status), TRIM(lab.loan_status))                          AS repayment_status,
    UPPER(TRIM(lab.customer_id))                                                      AS customer_id,
    UPPER(TRIM(lab.branch_id))                                                        AS branch_id,
    TRIM(lab.loan_type)                                                               AS loan_type,
    COALESCE(CAST(rtb.remaining_balance AS int), CAST(lab.loan_amount AS int))         AS outstanding_balance,
    TRIM(rtb.transaction_id)                                                          AS transaction_id
FROM loan_applications_bronze lab
LEFT JOIN repayment_transactions_bronze rtb
    ON lab.loan_id = rtb.loan_id
"""

loan_transactions_silver_df = spark.sql(loan_transactions_silver_sql)

loan_transactions_silver_df.createOrReplaceTempView("loan_transactions_silver")

loan_transactions_silver_output_path = TARGET_PATH + "/" + "loan_transactions_silver.csv"
(
    loan_transactions_silver_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(loan_transactions_silver_output_path)
)

# ============================================================
# Target: silver.loan_performance_daily_silver
# Mapping: silver.loan_transactions_silver lts
# ============================================================

loan_performance_daily_silver_sql = """
SELECT
    CAST(lts.transaction_date AS date)                                                                                  AS date,
    lts.branch_id                                                                                                       AS branch_id,
    COUNT(DISTINCT CASE WHEN lts.repayment_status IN ('APPROVED','DISBURSED','ISSUED') THEN lts.loan_id END)             AS total_loans_issued,
    SUM(CASE WHEN lts.repayment_status IN ('PAID','SUCCESS','COMPLETED') THEN lts.amount ELSE 0 END)                    AS total_repayments,
    AVG(CASE WHEN lts.repayment_status IN ('APPROVED','DISBURSED','ISSUED') THEN lts.amount END)                        AS average_loan_amount,
    SUM(CASE WHEN lts.repayment_status IN ('PAID','SUCCESS','COMPLETED') THEN lts.amount ELSE 0 END)
        / NULLIF(
            SUM(CASE WHEN lts.repayment_status IN ('APPROVED','DISBURSED','ISSUED') THEN lts.amount ELSE 0 END),
            0
        )                                                                                                               AS repayment_rate,
    COUNT(DISTINCT CASE WHEN lts.repayment_status IN ('DEFAULT','FAILED','DELINQUENT') THEN lts.loan_id END)
        / NULLIF(COUNT(DISTINCT lts.loan_id), 0)                                                                        AS default_rate
FROM loan_transactions_silver lts
GROUP BY
    CAST(lts.transaction_date AS date),
    lts.branch_id
"""

loan_performance_daily_silver_df = spark.sql(loan_performance_daily_silver_sql)

loan_performance_daily_silver_output_path = TARGET_PATH + "/" + "loan_performance_daily_silver.csv"
(
    loan_performance_daily_silver_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(loan_performance_daily_silver_output_path)
)

job.commit()