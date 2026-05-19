from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ============================================================
# Step 1 — Read source table(s)
# ============================================================
df_customer_master_bronze = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{SOURCE_PATH}/customer_master_bronze.{FILE_FORMAT}")

df_branch_master_bronze = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{SOURCE_PATH}/branch_master_bronze.{FILE_FORMAT}")

df_loan_applications_bronze = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{SOURCE_PATH}/loan_applications_bronze.{FILE_FORMAT}")

df_repayment_transactions_bronze = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{SOURCE_PATH}/repayment_transactions_bronze.{FILE_FORMAT}")

# ============================================================
# Step 2 — Register temp view(s)
# ============================================================
df_customer_master_bronze.createOrReplaceTempView("customer_master_bronze")
df_branch_master_bronze.createOrReplaceTempView("branch_master_bronze")
df_loan_applications_bronze.createOrReplaceTempView("loan_applications_bronze")
df_repayment_transactions_bronze.createOrReplaceTempView("repayment_transactions_bronze")

# ============================================================
# customer_silver
# Step 3 — Write SQL transformation query
# Step 4 — Execute query into a result DataFrame
# ============================================================
customer_silver_sql = """
WITH base AS (
    SELECT
        UPPER(TRIM(cmb.customer_id)) AS customer_id,
        TRIM(cmb.customer_name) AS customer_name,
        cmb.income_segment AS customer_segment,
        cmb.credit_score AS customer_credit_score,
        CONCAT_WS(', ', NULLIF(TRIM(cmb.city),''), NULLIF(TRIM(cmb.state),'')) AS customer_address
    FROM customer_master_bronze cmb
),
dedup AS (
    SELECT
        customer_id,
        customer_name,
        customer_segment,
        customer_credit_score,
        customer_address,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY
                customer_credit_score DESC,
                CASE WHEN customer_name IS NOT NULL AND TRIM(customer_name) <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN customer_address IS NOT NULL AND TRIM(customer_address) <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
    FROM base
)
SELECT
    customer_id,
    customer_name,
    customer_segment,
    customer_credit_score,
    customer_address
FROM dedup
WHERE rn = 1
"""
df_customer_silver = spark.sql(customer_silver_sql)

# Step 5 — Write result as single CSV to target path
df_customer_silver.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/customer_silver.csv")

df_customer_silver.createOrReplaceTempView("loan_silver_placeholder_customer_only")

# ============================================================
# branch_silver
# Step 3 — Write SQL transformation query
# Step 4 — Execute query into a result DataFrame
# ============================================================
branch_silver_sql = """
WITH base AS (
    SELECT
        UPPER(TRIM(bmb.branch_id)) AS branch_id,
        TRIM(bmb.branch_name) AS branch_name,
        CONCAT_WS(', ', NULLIF(TRIM(bmb.city),''), NULLIF(TRIM(bmb.state),'')) AS branch_location,
        COALESCE(NULLIF(TRIM(bmb.state),''),'UNKNOWN') AS branch_region
    FROM branch_master_bronze bmb
),
dedup AS (
    SELECT
        branch_id,
        branch_name,
        branch_location,
        branch_region,
        ROW_NUMBER() OVER (
            PARTITION BY branch_id
            ORDER BY
                CASE WHEN branch_name IS NOT NULL AND TRIM(branch_name) <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN branch_location IS NOT NULL AND TRIM(branch_location) <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
    FROM base
)
SELECT
    branch_id,
    branch_name,
    branch_location,
    branch_region
FROM dedup
WHERE rn = 1
"""
df_branch_silver = spark.sql(branch_silver_sql)

# Step 5 — Write result as single CSV to target path
df_branch_silver.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/branch_silver.csv")

# ============================================================
# loan_silver
# Step 3 — Write SQL transformation query
# Step 4 — Execute query into a result DataFrame
# ============================================================
loan_silver_sql = """
WITH base AS (
    SELECT
        UPPER(TRIM(lab.loan_id)) AS loan_id,
        UPPER(TRIM(lab.customer_id)) AS customer_id,
        UPPER(TRIM(lab.branch_id)) AS branch_id,
        lab.loan_amount AS total_loan_amount_source,
        CAST(lab.application_date AS DATE) AS application_date,
        CASE
            WHEN LOWER(TRIM(lab.loan_status)) IN ('active') THEN 'active'
            WHEN LOWER(TRIM(lab.loan_status)) IN ('closed','complete','completed') THEN 'closed'
            WHEN LOWER(TRIM(lab.loan_status)) IN ('delinquent','default','late') THEN 'delinquent'
            ELSE LOWER(TRIM(lab.loan_status))
        END AS loan_status
    FROM loan_applications_bronze lab
),
dedup AS (
    SELECT
        loan_id,
        customer_id,
        branch_id,
        total_loan_amount_source,
        application_date,
        loan_status,
        ROW_NUMBER() OVER (
            PARTITION BY loan_id
            ORDER BY application_date DESC
        ) AS rn
    FROM base
)
SELECT
    loan_id,
    customer_id,
    branch_id,
    total_loan_amount_source,
    application_date,
    loan_status
FROM dedup
WHERE rn = 1
"""
df_loan_silver = spark.sql(loan_silver_sql)

df_loan_silver.createOrReplaceTempView("loan_silver")

# Step 5 — Write result as single CSV to target path
df_loan_silver.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/loan_silver.csv")

# ============================================================
# loan_transactions_silver
# Step 3 — Write SQL transformation query
# Step 4 — Execute query into a result DataFrame
# ============================================================
loan_transactions_silver_sql = """
WITH base AS (
    SELECT
        ls.loan_id AS loan_id,
        CAST(rtb.payment_date AS DATE) AS transaction_date,
        CAST(rtb.payment_amount AS DECIMAL(18,2)) AS transaction_amount,
        CASE
            WHEN UPPER(TRIM(rtb.payment_status)) IN ('SUCCESS','COMPLETED','PAID') THEN 'repayment'
            ELSE 'repayment_failed'
        END AS transaction_type,
        CAST(rtb.remaining_balance AS DECIMAL(18,2)) AS loan_balance,
        rtb.transaction_id AS transaction_id,
        rtb.payment_status AS payment_status
    FROM loan_silver ls
    INNER JOIN repayment_transactions_bronze rtb
        ON ls.loan_id = TRIM(rtb.loan_id)
),
filtered AS (
    SELECT
        loan_id,
        transaction_date,
        transaction_amount,
        transaction_type,
        loan_balance,
        transaction_id,
        payment_status
    FROM base
    WHERE loan_id IS NOT NULL
      AND transaction_date IS NOT NULL
),
dedup AS (
    SELECT
        loan_id,
        transaction_date,
        transaction_amount,
        transaction_type,
        loan_balance,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY
                CASE WHEN payment_status IS NOT NULL AND TRIM(payment_status) <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
    FROM filtered
)
SELECT
    loan_id,
    transaction_date,
    transaction_amount,
    transaction_type,
    loan_balance
FROM dedup
WHERE rn = 1
"""
df_loan_transactions_silver = spark.sql(loan_transactions_silver_sql)

# Step 5 — Write result as single CSV to target path
df_loan_transactions_silver.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/loan_transactions_silver.csv")

# ============================================================
# daily_refresh_log_silver
# Step 3 — Write SQL transformation query
# Step 4 — Execute query into a result DataFrame
# ============================================================
daily_refresh_log_silver_sql = """
WITH agg AS (
    SELECT
        CAST(rtb.payment_date AS DATE) AS refresh_date,
        COUNT(rtb.transaction_id) AS refreshed_records_count
    FROM repayment_transactions_bronze rtb
    GROUP BY CAST(rtb.payment_date AS DATE)
)
SELECT
    refresh_date,
    refreshed_records_count,
    CASE
        WHEN refreshed_records_count > 0 THEN 'success'
        ELSE 'failure'
    END AS status
FROM agg
"""
df_daily_refresh_log_silver = spark.sql(daily_refresh_log_silver_sql)

# Step 5 — Write result as single CSV to target path
df_daily_refresh_log_silver.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/daily_refresh_log_silver.csv")

job.commit()