```python
import sys
from awsglue.context import GlueConte
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

# ------------------------------------------------------------------
# 1) Read source tables (Bronze)
# ------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------------
# 3) Transform + 4) Save: product_master_silver
# ------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        CAST(pb.product_id AS STRING)    AS product_id,
        CAST(pb.product_name AS STRING)  AS product_name,
        CAST(pb.category AS STRING)      AS category,
        CAST(pb.brand AS STRING)         AS brand,
        CAST(pb.price AS FLOAT)          AS price,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.product_id
        ) AS rn
      FROM products_bronze pb
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      price
    FROM ranked
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

# ------------------------------------------------------------------
# 3) Transform + 4) Save: store_master_silver
# ------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        CAST(sb.store_id AS STRING)     AS store_id,
        CAST(sb.store_name AS STRING)   AS store_name,
        CAST(sb.city AS STRING)         AS region,
        CAST(sb.store_type AS STRING)   AS store_type,
        DATE(CAST(sb.open_date AS DATE)) AS opening_date,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.store_id
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      region,
      store_type,
      opening_date
    FROM ranked
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

# ------------------------------------------------------------------
# 3) Transform + 4) Save: sales_transactions_silver
# ------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        CAST(stb.transaction_id AS STRING) AS transaction_id,
        CAST(pms.product_id AS STRING)     AS product_id,
        CAST(sms.store_id AS STRING)       AS store_id,
        DATE(stb.transaction_time)         AS transaction_date,
        CAST(stb.sale_amount AS DOUBLE)    AS sales_amount,
        CAST(stb.quantity AS INT)          AS quantity_sold,
        CAST(NULL AS STRING)               AS customer_id,
        ROW_NUMBER() OVER (
          PARTITION BY stb.transaction_id
          ORDER BY stb.transaction_id
        ) AS rn
      FROM sales_transactions_bronze stb
      LEFT JOIN product_master_silver pms
        ON stb.product_id = pms.product_id
      LEFT JOIN store_master_silver sms
        ON stb.store_id = sms.store_id
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      transaction_date,
      sales_amount,
      quantity_sold,
      customer_id
    FROM joined
    WHERE rn = 1
      AND product_id IS NOT NULL
      AND store_id IS NOT NULL
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

# ------------------------------------------------------------------
# 3) Transform + 4) Save: aggregated_sales_daily_silver
# ------------------------------------------------------------------
aggregated_sales_daily_silver_df = spark.sql(
    """
    SELECT
      CAST(sts.store_id AS STRING)        AS store_id,
      CAST(sts.product_id AS STRING)      AS product_id,
      DATE(sts.transaction_date)          AS transaction_date,
      SUM(sts.sales_amount)              AS total_sales,
      SUM(sts.quantity_sold)             AS total_quantity
    FROM sales_transactions_silver sts
    GROUP BY
      sts.store_id,
      sts.product_id,
      sts.transaction_date
    """
)

(
    aggregated_sales_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_daily_silver.csv")
)

job.commit()
```