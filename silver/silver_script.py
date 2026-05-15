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

# ============================================================
# 1) READ SOURCE TABLES (S3)
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

loans_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loans_bronze.{FILE_FORMAT}/")
)
loans_bronze_df.createOrReplaceTempView("loans_bronze")

repayments_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/repayments_bronze.{FILE_FORMAT}/")
)
repayments_bronze_df.createOrReplaceTempView("repayments_bronze")

loan_risk_assessment_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_risk_assessment_bronze.{FILE_FORMAT}/")
)
loan_risk_assessment_bronze_df.createOrReplaceTempView("loan_risk_assessment_bronze")

# ============================================================
# 2) customers_silver
#    - De-dup: one row per customer_id
# ============================================================

customers_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(cb.customer_id) AS STRING) AS customer_id,
            CAST(TRIM(cb.income_segment) AS STRING) AS income_segment,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(cb.customer_id)
                ORDER BY TRIM(cb.customer_id)
            ) AS rn
        FROM customers_bronze cb
        WHERE TRIM(cb.customer_id) IS NOT NULL
          AND TRIM(cb.customer_id) <> ''
    )
    SELECT
        customer_id,
        income_segment
    FROM ranked
    WHERE rn = 1
    """
)
customers_silver_df.createOrReplaceTempView("customers_silver")

(
    customers_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customers_silver.csv")
)

# ============================================================
# 3) branches_silver
#    - De-dup: one row per branch_id
# ============================================================

branches_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(bb.branch_id) AS STRING) AS branch_id,
            CAST(TRIM(bb.city) AS STRING) AS city,
            CAST(TRIM(bb.state) AS STRING) AS state,
            CAST(TRIM(bb.branch_type) AS STRING) AS branch_type,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(bb.branch_id)
                ORDER BY TRIM(bb.branch_id)
            ) AS rn
        FROM branches_bronze bb
        WHERE TRIM(bb.branch_id) IS NOT NULL
          AND TRIM(bb.branch_id) <> ''
    )
    SELECT
        branch_id,
        city,
        state,
        branch_type
    FROM ranked
    WHERE rn = 1
    """
)
branches_silver_df.createOrReplaceTempView("branches_silver")

(
    branches_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/branches_silver.csv")
)

# ============================================================
# 4) loans_silver
#    - Ensure valid customer_id and branch_id via INNER JOIN
#    - De-dup: one row per loan_id
# ============================================================

loans_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(lb.loan_id) AS STRING) AS loan_id,
            CAST(TRIM(lb.customer_id) AS STRING) AS customer_id,
            CAST(TRIM(lb.branch_id) AS STRING) AS branch_id,
            CAST(TRIM(lb.loan_amount) AS INT) AS loan_amount,
            CAST(TRIM(lb.loan_status) AS STRING) AS loan_status,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(lb.loan_id)
                ORDER BY TRIM(lb.loan_id)
            ) AS rn
        FROM loans_bronze lb
        INNER JOIN customers_silver cs
            ON TRIM(lb.customer_id) = cs.customer_id
        INNER JOIN branches_silver bs
            ON TRIM(lb.branch_id) = bs.branch_id
        WHERE TRIM(lb.loan_id) IS NOT NULL
          AND TRIM(lb.loan_id) <> ''
          AND TRIM(lb.customer_id) IS NOT NULL
          AND TRIM(lb.customer_id) <> ''
          AND TRIM(lb.branch_id) IS NOT NULL
          AND TRIM(lb.branch_id) <> ''
    )
    SELECT
        loan_id,
        customer_id,
        branch_id,
        loan_amount,
        loan_status
    FROM ranked
    WHERE rn = 1
    """
)
loans_silver_df.createOrReplaceTempView("loans_silver")

(
    loans_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/loans_silver.csv")
)

# ============================================================
# 5) repayments_silver
#    - Ensure valid loan_id via INNER JOIN
#    - De-dup: one row per transaction_id
# ============================================================

repayments_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(rb.transaction_id) AS STRING) AS transaction_id,
            CAST(TRIM(rb.loan_id) AS STRING) AS loan_id,
            DATE(TRIM(rb.payment_date)) AS payment_date,
            CAST(TRIM(rb.payment_amount) AS INT) AS payment_amount,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(rb.transaction_id)
                ORDER BY TRIM(rb.transaction_id)
            ) AS rn
        FROM repayments_bronze rb
        INNER JOIN loans_silver ls
            ON TRIM(rb.loan_id) = ls.loan_id
        WHERE TRIM(rb.transaction_id) IS NOT NULL
          AND TRIM(rb.transaction_id) <> ''
          AND TRIM(rb.loan_id) IS NOT NULL
          AND TRIM(rb.loan_id) <> ''
    )
    SELECT
        transaction_id,
        loan_id,
        payment_date,
        payment_amount
    FROM ranked
    WHERE rn = 1
    """
)
repayments_silver_df.createOrReplaceTempView("repayments_silver")

(
    repayments_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/repayments_silver.csv")
)

# ============================================================
# 6) loan_risk_assessments_silver
#    - Ensure valid loan_id via INNER JOIN
#    - De-dup: one row per risk_id
# ============================================================

loan_risk_assessments_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(lrab.risk_id) AS STRING) AS risk_id,
            CAST(TRIM(lrab.loan_id) AS STRING) AS loan_id,
            CAST(TRIM(lrab.risk_category) AS STRING) AS risk_category,
            DATE(TRIM(lrab.evaluation_date)) AS evaluation_date,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(lrab.risk_id)
                ORDER BY TRIM(lrab.risk_id)
            ) AS rn
        FROM loan_risk_assessment_bronze lrab
        INNER JOIN loans_silver ls
            ON TRIM(lrab.loan_id) = ls.loan_id
        WHERE TRIM(lrab.risk_id) IS NOT NULL
          AND TRIM(lrab.risk_id) <> ''
          AND TRIM(lrab.loan_id) IS NOT NULL
          AND TRIM(lrab.loan_id) <> ''
    )
    SELECT
        risk_id,
        loan_id,
        risk_category,
        evaluation_date
    FROM ranked
    WHERE rn = 1
    """
)
loan_risk_assessments_silver_df.createOrReplaceTempView("loan_risk_assessments_silver")

(
    loan_risk_assessments_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/loan_risk_assessments_silver.csv")
)

job.commit()