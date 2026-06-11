import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# 1) Read source tables from S3 (CSV) + Create temp views
# ------------------------------------------------------------
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

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# Create bronze schema views to match UDT SQL (bronze.table)
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql(
    "CREATE OR REPLACE TEMP VIEW bronze.products_bronze AS SELECT * FROM products_bronze"
)
spark.sql(
    "CREATE OR REPLACE TEMP VIEW bronze.stores_bronze AS SELECT * FROM stores_bronze"
)
spark.sql(
    "CREATE OR REPLACE TEMP VIEW bronze.sales_transactions_bronze AS SELECT * FROM sales_transactions_bronze"
)

# ------------------------------------------------------------
# Target: silver.products_silver
# ------------------------------------------------------------
products_silver_df = spark.sql(
    """
    SELECT
      product_id,
      product_name,
      category,
      brand,
      CAST(price AS DOUBLE) AS price,
      CAST(is_active AS BOOLEAN) AS is_active
    FROM (
      SELECT
        pb.product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        TRIM(pb.brand) AS brand,
        pb.price,
        pb.is_active,
        ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.product_id) AS rn
      FROM bronze.products_bronze pb
      WHERE pb.product_id IS NOT NULL
        AND pb.product_name IS NOT NULL
        AND pb.category IS NOT NULL
        AND pb.brand IS NOT NULL
        AND pb.price IS NOT NULL
        AND pb.is_active = TRUE
    ) d
    WHERE rn = 1
    """
)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.stores_silver
# ------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    SELECT
      store_id,
      store_name,
      city,
      state,
      store_type,
      CAST(open_date AS DATE) AS open_date
    FROM (
      SELECT
        sb.store_id,
        TRIM(sb.store_name) AS store_name,
        TRIM(sb.city) AS city,
        TRIM(sb.state) AS state,
        TRIM(sb.store_type) AS store_type,
        sb.open_date,
        ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.store_id) AS rn
      FROM bronze.stores_bronze sb
      WHERE sb.store_id IS NOT NULL
        AND sb.store_name IS NOT NULL
        AND sb.city IS NOT NULL
        AND sb.state IS NOT NULL
        AND sb.store_type IS NOT NULL
        AND sb.open_date IS NOT NULL
    ) d
    WHERE rn = 1
    """
)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    SELECT
      transaction_id,
      store_id,
      product_id,
      CAST(quantity AS INT) AS quantity,
      CAST(sale_amount AS DOUBLE) AS sale_amount,
      CAST(transaction_time AS TIMESTAMP) AS transaction_time
    FROM (
      SELECT
        stb.transaction_id,
        stb.store_id,
        stb.product_id,
        stb.quantity,
        stb.sale_amount,
        stb.transaction_time,
        ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
      FROM bronze.sales_transactions_bronze stb
      WHERE stb.transaction_id IS NOT NULL
        AND stb.store_id IS NOT NULL
        AND stb.product_id IS NOT NULL
        AND stb.quantity IS NOT NULL
        AND stb.sale_amount IS NOT NULL
        AND stb.transaction_time IS NOT NULL
    ) d
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

# ------------------------------------------------------------
# Target: silver.orders_silver
# ------------------------------------------------------------
orders_silver_df = spark.sql(
    """
    SELECT
      stb.transaction_id,
      stb.store_id,
      stb.product_id,
      CAST(stb.quantity AS INT) AS quantity,
      CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
      CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
      ss.store_name,
      ss.city,
      ss.state,
      ss.store_type,
      ss.open_date,
      ps.product_name,
      ps.category,
      ps.brand,
      ps.price
    FROM (
      SELECT
        transaction_id,
        store_id,
        product_id,
        quantity,
        sale_amount,
        transaction_time,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
      FROM bronze.sales_transactions_bronze
      WHERE transaction_id IS NOT NULL
        AND store_id IS NOT NULL
        AND product_id IS NOT NULL
        AND quantity IS NOT NULL
        AND sale_amount IS NOT NULL
        AND transaction_time IS NOT NULL
    ) stb
    INNER JOIN (
      SELECT
        store_id,
        store_name,
        city,
        state,
        store_type,
        open_date
      FROM (
        SELECT
          sb.store_id,
          TRIM(sb.store_name) AS store_name,
          TRIM(sb.city) AS city,
          TRIM(sb.state) AS state,
          TRIM(sb.store_type) AS store_type,
          CAST(sb.open_date AS DATE) AS open_date,
          ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.store_id) AS rn
        FROM bronze.stores_bronze sb
        WHERE sb.store_id IS NOT NULL
      ) ssd
      WHERE rn = 1
    ) ss
      ON stb.store_id = ss.store_id
    INNER JOIN (
      SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
        is_active
      FROM (
        SELECT
          pb.product_id,
          TRIM(pb.product_name) AS product_name,
          TRIM(pb.category) AS category,
          TRIM(pb.brand) AS brand,
          CAST(pb.price AS DOUBLE) AS price,
          CAST(pb.is_active AS BOOLEAN) AS is_active,
          ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.product_id) AS rn
        FROM bronze.products_bronze pb
        WHERE pb.product_id IS NOT NULL
          AND pb.is_active = TRUE
      ) psd
      WHERE rn = 1
    ) ps
      ON stb.product_id = ps.product_id
    WHERE stb.rn = 1
    """
)

(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/orders_silver.csv")
)

job.commit()
