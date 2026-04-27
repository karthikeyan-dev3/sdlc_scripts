```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# AWS Glue bootstrap
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
FILE_FORMAT = "csv"

# CSV handling (typical bronze exports). Adjust options only if needed.
read_options = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3
#    (STRICT SOURCE READING RULE: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/"))
# --------------------------------------------------------------------------------------
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------
orders_bronze_df.createOrReplaceTempView("orders_bronze")
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

# ======================================================================================
# TABLE: orders_silver
# ======================================================================================
# 3) SQL transform (dedup + mappings + validations)
orders_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            ob.transaction_id,
            ob.transaction_time,
            ob.store_id,
            ob.sale_amount,

            oib.product_id,

            -- pass-through metadata if present in bronze; will be NULL if not present
            ob.source_s3_file_path AS source_s3_file_path,
            CAST(ob.ingestion_date AS DATE) AS ingestion_date
        FROM orders_bronze ob
        LEFT JOIN order_items_bronze oib
            ON ob.transaction_id = oib.transaction_id
    ),
    aggregated AS (
        SELECT
            -- keys & core fields
            CAST(transaction_id AS STRING) AS order_id,
            CAST(transaction_time AS DATE) AS order_date,
            CAST(store_id AS STRING) AS customer_id,
            CAST(sale_amount AS DECIMAL(38, 10)) AS order_total_amount,

            -- defaults (not present in bronze)
            CAST('COMPLETED' AS STRING) AS order_status,
            CAST('USD' AS STRING) AS currency_code,

            -- UDT required: COUNT(DISTINCT oib.product_id) OVER (PARTITION BY ob.transaction_id)
            -- implemented as window over joined rows
            CAST(
                COUNT(DISTINCT product_id) OVER (PARTITION BY transaction_id)
                AS INT
            ) AS items_count,

            source_s3_file_path,
            ingestion_date,

            current_timestamp() AS load_timestamp,

            -- Deduplicate on (transaction_id) keep latest by transaction_time
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC, ingestion_date DESC, source_s3_file_path DESC
            ) AS rn
        FROM joined
    )
    SELECT
        order_id,
        order_date,
        customer_id,
        order_status,
        order_total_amount,
        currency_code,
        items_count,
        source_s3_file_path,
        ingestion_date,
        load_timestamp
    FROM aggregated
    WHERE rn = 1
      AND order_id IS NOT NULL
      AND order_date IS NOT NULL
      AND customer_id IS NOT NULL
      AND order_total_amount IS NOT NULL
      AND order_total_amount >= 0
    """
)

# 4) Save output (STRICT OUTPUT RULE: single CSV file directly under TARGET_PATH)
(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/orders_silver.csv")
)

# ======================================================================================
# TABLE: order_items_silver
# ======================================================================================
# 3) SQL transform (dedup + mappings + validations)
order_items_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            oib.transaction_id,
            oib.product_id,
            oib.quantity,

            ob.transaction_time,
            CAST(ob.transaction_time AS DATE) AS order_date,

            -- pass-through metadata if present in bronze; will be NULL if not present
            oib.source_s3_file_path AS source_s3_file_path,
            CAST(oib.ingestion_date AS DATE) AS ingestion_date
        FROM order_items_bronze oib
        INNER JOIN orders_bronze ob
            ON oib.transaction_id = ob.transaction_id
    ),
    numbered AS (
        SELECT
            CAST(transaction_id AS STRING) AS order_id,

            -- UDT required unique id:
            -- CONCAT(transaction_id,'-',product_id,'-',ROW_NUMBER() OVER (PARTITION BY transaction_id,product_id ORDER BY transaction_time))
            CONCAT(
                CAST(transaction_id AS STRING), '-',
                CAST(product_id AS STRING), '-',
                CAST(
                    ROW_NUMBER() OVER (
                        PARTITION BY transaction_id, product_id
                        ORDER BY transaction_time
                    ) AS STRING
                )
            ) AS order_item_id,

            CAST(product_id AS STRING) AS product_id,
            CAST(quantity AS INT) AS quantity,

            -- not available / not computable in bronze
            CAST(NULL AS DECIMAL(38, 10)) AS unit_price,
            CAST(NULL AS DECIMAL(38, 10)) AS line_amount,

            CAST('USD' AS STRING) AS currency_code,

            CAST(order_date AS DATE) AS order_date,

            -- not available
            CAST(NULL AS STRING) AS customer_id,

            source_s3_file_path,
            ingestion_date,

            current_timestamp() AS load_timestamp,

            -- Deduplicate on (transaction_id, product_id, quantity) keep latest by transaction_time
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id, product_id, quantity
                ORDER BY transaction_time DESC, ingestion_date DESC, source_s3_file_path DESC
            ) AS rn
        FROM joined
    )
    SELECT
        order_id,
        order_item_id,
        product_id,
        quantity,
        unit_price,
        line_amount,
        currency_code,
        order_date,
        customer_id,
        source_s3_file_path,
        ingestion_date,
        load_timestamp
    FROM numbered
    WHERE rn = 1
      AND order_id IS NOT NULL
      AND order_item_id IS NOT NULL
      AND product_id IS NOT NULL
      AND quantity IS NOT NULL
      AND quantity > 0
      AND order_date IS NOT NULL
    """
)

# 4) Save output (STRICT OUTPUT RULE: single CSV file directly under TARGET_PATH)
(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_items_silver.csv")
)

# ======================================================================================
# TABLE: customers_silver
# ======================================================================================
# 3) SQL transform (dedup + mappings + validations)
customers_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(ob.store_id AS STRING) AS customer_id,

            CAST(NULL AS STRING) AS customer_name,
            CAST(NULL AS STRING) AS customer_email,

            -- UDT required: MIN(CAST(ob.transaction_time AS DATE)) OVER (PARTITION BY ob.store_id)
            CAST(
                MIN(CAST(ob.transaction_time AS DATE)) OVER (PARTITION BY ob.store_id)
                AS DATE
            ) AS customer_created_date,

            CAST('ACTIVE' AS STRING) AS customer_status,

            current_timestamp() AS load_timestamp,

            -- For dedup: keep earliest created date and latest load timestamp.
            -- Since load_timestamp is generated, use a stable rule: pick one row per store_id.
            ROW_NUMBER() OVER (
                PARTITION BY ob.store_id
                ORDER BY CAST(ob.transaction_time AS DATE) ASC, ob.transaction_time ASC
            ) AS rn
        FROM orders_bronze ob
        WHERE ob.store_id IS NOT NULL
          AND ob.transaction_time IS NOT NULL
    )
    SELECT
        customer_id,
        customer_name,
        customer_email,
        customer_created_date,
        customer_status,
        load_timestamp
    FROM base
    WHERE rn = 1
      AND customer_id IS NOT NULL
      AND customer_created_date IS NOT NULL
    """
)

# 4) Save output (STRICT OUTPUT RULE: single CSV file directly under TARGET_PATH)
(
    customers_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customers_silver.csv")
)

job.commit()
```