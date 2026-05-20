import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------
# Glue / Spark setup
# ----------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# 1) sales_transactions_silver
#    Source: bronze.sales_transactions_bronze
# ----------------------------
stb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
stb_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            stb.transaction_id AS transaction_id,
            stb.product_id AS product_id,
            stb.store_id AS store_id,
            CAST(stb.transaction_time AS date) AS sale_date,
            CAST(stb.quantity AS int) AS quantity_sold,
            CASE
                WHEN CAST(stb.sale_amount AS double) < 0 THEN NULL
                ELSE CAST(stb.sale_amount AS double)
            END AS sale_amount,
            stb.transaction_time AS transaction_time
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
          AND stb.store_id IS NOT NULL
          AND stb.product_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            sale_date,
            quantity_sold,
            sale_amount,
            ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
        FROM base
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        sale_date,
        quantity_sold,
        sale_amount
    FROM dedup
    WHERE rn = 1
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ----------------------------
# 2) product_master_silver
#    Source: bronze.product_master_bronze
# ----------------------------
pmb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)
pmb_df.createOrReplaceTempView("product_master_bronze")

product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            pmb.product_id AS product_id,
            TRIM(pmb.product_name) AS product_name,
            TRIM(pmb.category) AS category,
            CAST(pmb.price AS double) AS price
        FROM product_master_bronze pmb
    ),
    dedup AS (
        SELECT
            product_id,
            product_name,
            category,
            price,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM dedup
    WHERE rn = 1
    """
)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ----------------------------
# 3) store_master_silver
#    Source: bronze.store_information_bronze
# ----------------------------
sib_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_information_bronze.{FILE_FORMAT}/")
)
sib_df.createOrReplaceTempView("store_information_bronze")

store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            sib.store_id AS store_id,
            TRIM(sib.store_name) AS store_name,
            CONCAT(TRIM(sib.city), ', ', TRIM(sib.state)) AS location,
            CAST(NULL AS string) AS manager
        FROM store_information_bronze sib
    ),
    dedup AS (
        SELECT
            store_id,
            store_name,
            location,
            manager,
            ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        location,
        manager
    FROM dedup
    WHERE rn = 1
    """
)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ----------------------------
# 4) sales_enrichment_silver
#    Sources: silver.sales_transactions_silver, silver.product_master_silver, silver.store_master_silver
# ----------------------------
sales_enrichment_silver_df = spark.sql(
    """
    SELECT
        sts.transaction_id AS transaction_id,
        sts.product_id AS product_id,
        pms.product_name AS product_name,
        sts.store_id AS store_id,
        sms.store_name AS store_name,
        sts.sale_date AS sale_date,
        sts.quantity_sold AS quantity_sold,
        sts.sale_amount AS sale_amount
    FROM sales_transactions_silver sts
    LEFT JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    LEFT JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    """
)

(
    sales_enrichment_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_enrichment_silver.csv")
)