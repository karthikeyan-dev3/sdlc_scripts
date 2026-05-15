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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# 1) Read source tables (Bronze)
# --------------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# --------------------------------------------------------------------------------------
# 2) products_silver
# --------------------------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(pb.product_id)) AS product_id,
            TRIM(pb.product_name)      AS product_name,
            TRIM(pb.category)          AS category,
            CASE
                WHEN CAST(pb.price AS float) >= 0 THEN CAST(pb.price AS float)
                ELSE NULL
            END                        AS price
        FROM products_bronze pb
    ),
    dedup AS (
        SELECT
            product_id,
            product_name,
            category,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN product_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN category IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM dedup
    WHERE rn = 1
      AND product_id IS NOT NULL
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# --------------------------------------------------------------------------------------
# 3) stores_silver
# --------------------------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(sb.store_id)) AS store_id,
            TRIM(sb.store_name)      AS store_name,
            CASE
                WHEN TRIM(sb.state) IS NOT NULL THEN TRIM(sb.state)
                ELSE NULL
            END                      AS region,
            CAST(NULL AS string)     AS store_manager
        FROM stores_bronze sb
    ),
    dedup AS (
        SELECT
            store_id,
            store_name,
            region,
            store_manager,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN region IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        region,
        store_manager
    FROM dedup
    WHERE rn = 1
      AND store_id IS NOT NULL
    """
)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# --------------------------------------------------------------------------------------
# 4) sales_transactions_silver
# --------------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(tb.transaction_id)) AS transaction_id,
            CAST(tb.transaction_time AS date) AS transaction_date,
            UPPER(TRIM(tb.product_id)) AS product_id,
            UPPER(TRIM(tb.store_id))   AS store_id,
            CASE WHEN CAST(tb.quantity AS int) > 0 THEN CAST(tb.quantity AS int) ELSE NULL END AS quantity_sold,
            CASE WHEN CAST(tb.sale_amount AS double) >= 0 THEN CAST(tb.sale_amount AS double) ELSE NULL END AS sales_amount,
            tb.transaction_time AS transaction_time
        FROM transactions_bronze tb
    ),
    dedup AS (
        SELECT
            transaction_id,
            transaction_date,
            product_id,
            store_id,
            quantity_sold,
            sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM base
    )
    SELECT
        d.transaction_id,
        d.transaction_date,
        d.product_id,
        d.store_id,
        d.quantity_sold,
        d.sales_amount
    FROM dedup d
    LEFT JOIN products_silver ps
        ON d.product_id = ps.product_id
    LEFT JOIN stores_silver ss
        ON d.store_id = ss.store_id
    WHERE d.rn = 1
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# --------------------------------------------------------------------------------------
# 5) sales_aggregated_silver
# --------------------------------------------------------------------------------------
sales_aggregated_silver_df = spark.sql(
    """
    WITH daily AS (
        SELECT
            sts.transaction_date AS aggregation_date,
            SUM(sts.sales_amount)    AS total_sales,
            SUM(sts.quantity_sold)   AS total_quantity
        FROM sales_transactions_silver sts
        GROUP BY sts.transaction_date
    ),
    product_rank AS (
        SELECT
            sts.transaction_date AS aggregation_date,
            sts.product_id       AS top_selling_product_id,
            ROW_NUMBER() OVER (
                PARTITION BY sts.transaction_date
                ORDER BY SUM(sts.sales_amount) DESC
            ) AS rn
        FROM sales_transactions_silver sts
        GROUP BY sts.transaction_date, sts.product_id
    ),
    store_rank AS (
        SELECT
            sts.transaction_date AS aggregation_date,
            sts.store_id         AS top_selling_store_id,
            ROW_NUMBER() OVER (
                PARTITION BY sts.transaction_date
                ORDER BY SUM(sts.sales_amount) DESC
            ) AS rn
        FROM sales_transactions_silver sts
        GROUP BY sts.transaction_date, sts.store_id
    )
    SELECT
        d.aggregation_date,
        d.total_sales,
        d.total_quantity,
        pr.top_selling_product_id,
        sr.top_selling_store_id
    FROM daily d
    LEFT JOIN product_rank pr
        ON d.aggregation_date = pr.aggregation_date AND pr.rn = 1
    LEFT JOIN store_rank sr
        ON d.aggregation_date = sr.aggregation_date AND sr.rn = 1
    """
)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

# --------------------------------------------------------------------------------------
# 6) data_validation_silver
# --------------------------------------------------------------------------------------
data_validation_silver_df = spark.sql(
    """
    SELECT
        sts.transaction_id AS record_id,
        CASE
            WHEN sts.transaction_id IS NOT NULL
             AND sts.transaction_date IS NOT NULL
             AND sts.quantity_sold IS NOT NULL
             AND sts.sales_amount IS NOT NULL
             AND sts.sales_amount >= 0
             AND sts.quantity_sold > 0
             AND ps.product_id IS NOT NULL
             AND ss.store_id IS NOT NULL
            THEN true ELSE false
        END AS is_valid,
        CONCAT_WS(
            '|',
            CASE WHEN sts.transaction_id IS NULL THEN 'MISSING_TRANSACTION_ID' END,
            CASE WHEN sts.transaction_date IS NULL THEN 'MISSING_DATE' END,
            CASE WHEN sts.quantity_sold IS NULL OR sts.quantity_sold <= 0 THEN 'INVALID_QUANTITY' END,
            CASE WHEN sts.sales_amount IS NULL OR sts.sales_amount < 0 THEN 'INVALID_AMOUNT' END,
            CASE WHEN ps.product_id IS NULL THEN 'UNKNOWN_PRODUCT' END,
            CASE WHEN ss.store_id IS NULL THEN 'UNKNOWN_STORE' END
        ) AS validation_errors
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    """
)
data_validation_silver_df.createOrReplaceTempView("data_validation_silver")

(
    data_validation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_validation_silver.csv")
)

job.commit()