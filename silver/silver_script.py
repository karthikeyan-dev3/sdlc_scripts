```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResoltions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# Read source tables from S3 (Bronze) and create temp views
# --------------------------------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# --------------------------------------------------------------------------------------
# Target: stores_silver
# --------------------------------------------------------------------------------------
stores_silver_sql = """
SELECT store_id, store_name, city, state, store_type, open_date, country
FROM (
  SELECT
    TRIM(store_id) AS store_id,
    NULLIF(TRIM(store_name),'') AS store_name,
    NULLIF(TRIM(city),'') AS city,
    NULLIF(TRIM(state),'') AS state,
    UPPER(NULLIF(TRIM(store_type),'')) AS store_type,
    CAST(open_date AS DATE) AS open_date,
    'USA' AS country,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(store_id)
      ORDER BY CAST(open_date AS DATE) DESC NULLS LAST
    ) AS rn
  FROM stores_bronze
  WHERE store_id IS NOT NULL AND TRIM(store_id) <> ''
) d
WHERE rn = 1
"""
stores_silver_df = spark.sql(stores_silver_sql)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: products_silver
# --------------------------------------------------------------------------------------
products_silver_sql = """
SELECT product_id, product_name, category, category_id, category_name, brand, price, is_active
FROM (
  SELECT
    TRIM(product_id) AS product_id,
    NULLIF(TRIM(product_name),'') AS product_name,
    NULLIF(TRIM(category),'') AS category,
    CONCAT('CAT_', UPPER(REGEXP_REPLACE(NULLIF(TRIM(category),''),'[^A-Za-z0-9]+','_'))) AS category_id,
    NULLIF(TRIM(category),'') AS category_name,
    NULLIF(TRIM(brand),'') AS brand,
    CAST(price AS DOUBLE) AS price,
    COALESCE(CAST(is_active AS BOOLEAN), TRUE) AS is_active,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(product_id)
      ORDER BY COALESCE(CAST(is_active AS BOOLEAN), TRUE) DESC, CAST(price AS DOUBLE) DESC NULLS LAST
    ) AS rn
  FROM products_bronze
  WHERE product_id IS NOT NULL AND TRIM(product_id) <> ''
) d
WHERE rn = 1 AND is_active = TRUE
"""
products_silver_df = spark.sql(products_silver_sql)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: transactions_silver
# --------------------------------------------------------------------------------------
transactions_silver_sql = """
SELECT transaction_id, store_id, product_id, quantity, sale_amount, transaction_time, sales_date
FROM (
  SELECT
    TRIM(transaction_id) AS transaction_id,
    TRIM(store_id) AS store_id,
    TRIM(product_id) AS product_id,
    CAST(quantity AS INT) AS quantity,
    CAST(sale_amount AS DOUBLE) AS sale_amount,
    CAST(transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(CAST(transaction_time AS TIMESTAMP) AS DATE) AS sales_date,
    ROW_NUMBER() OVER (
      PARTITION BY
        TRIM(transaction_id),
        TRIM(store_id),
        TRIM(product_id),
        CAST(CAST(transaction_time AS TIMESTAMP) AS DATE)
      ORDER BY
        CAST(transaction_time AS TIMESTAMP) DESC NULLS LAST,
        CAST(sale_amount AS DOUBLE) DESC NULLS LAST
    ) AS rn
  FROM transactions_bronze
  WHERE
    transaction_id IS NOT NULL AND TRIM(transaction_id) <> ''
    AND store_id IS NOT NULL AND TRIM(store_id) <> ''
    AND product_id IS NOT NULL AND TRIM(product_id) <> ''
    AND CAST(quantity AS INT) IS NOT NULL AND CAST(quantity AS INT) > 0
    AND CAST(sale_amount AS DOUBLE) IS NOT NULL AND CAST(sale_amount AS DOUBLE) >= 0
    AND transaction_time IS NOT NULL
) d
WHERE rn = 1
"""
transactions_silver_df = spark.sql(transactions_silver_sql)

(
    transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/transactions_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: daily_data_quality_runs_silver
# --------------------------------------------------------------------------------------
daily_data_quality_runs_silver_sql = """
SELECT run_date, source_name, total_records, duplicate_records_removed, invalid_store_id_count, invalid_product_id_count
FROM (
  SELECT
    CAST(CURRENT_DATE AS DATE) AS run_date,
    src.source_name AS source_name,
    src.total_records AS total_records,
    src.duplicate_records_removed AS duplicate_records_removed,
    src.invalid_store_id_count AS invalid_store_id_count,
    src.invalid_product_id_count AS invalid_product_id_count
  FROM (
    SELECT
      'transactions_bronze' AS source_name,
      (SELECT COUNT(*) FROM transactions_bronze) AS total_records,
      (
        (SELECT COUNT(*) FROM transactions_bronze)
        - (SELECT COUNT(*)
           FROM (
             SELECT
               transaction_id,
               store_id,
               product_id,
               CAST(transaction_time AS DATE) AS sales_date
             FROM transactions_bronze
             WHERE
               transaction_id IS NOT NULL AND TRIM(transaction_id) <> ''
               AND store_id IS NOT NULL AND TRIM(store_id) <> ''
               AND product_id IS NOT NULL AND TRIM(product_id) <> ''
               AND transaction_time IS NOT NULL
             GROUP BY transaction_id, store_id, product_id, CAST(transaction_time AS DATE)
           ) u
        )
      ) AS duplicate_records_removed,
      (
        SELECT COUNT(*)
        FROM (
          SELECT DISTINCT store_id
          FROM transactions_bronze
          WHERE store_id IS NOT NULL AND TRIM(store_id) <> ''
        ) t
        LEFT JOIN (
          SELECT DISTINCT store_id
          FROM stores_bronze
          WHERE store_id IS NOT NULL AND TRIM(store_id) <> ''
        ) s
        ON TRIM(t.store_id) = TRIM(s.store_id)
        WHERE s.store_id IS NULL
      ) AS invalid_store_id_count,
      (
        SELECT COUNT(*)
        FROM (
          SELECT DISTINCT product_id
          FROM transactions_bronze
          WHERE product_id IS NOT NULL AND TRIM(product_id) <> ''
        ) t
        LEFT JOIN (
          SELECT DISTINCT product_id
          FROM products_bronze
          WHERE
            product_id IS NOT NULL AND TRIM(product_id) <> ''
            AND COALESCE(CAST(is_active AS BOOLEAN), TRUE) = TRUE
        ) p
        ON TRIM(t.product_id) = TRIM(p.product_id)
        WHERE p.product_id IS NULL
      ) AS invalid_product_id_count
  ) src
) q
"""
daily_data_quality_runs_silver_df = spark.sql(daily_data_quality_runs_silver_sql)

(
    daily_data_quality_runs_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_data_quality_runs_silver.csv")
)

job.commit()

```