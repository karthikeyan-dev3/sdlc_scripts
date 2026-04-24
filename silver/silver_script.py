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
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended CSV options (adjust if your bronze has different settings)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "multiLine": "false",
    "quote": "\"",
    "escape": "\"",
}

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
#    NOTE: Path MUST follow: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# --------------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

customers_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customers_bronze.{FILE_FORMAT}/")
)

customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views (Bronze)
# --------------------------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
customers_bronze_df.createOrReplaceTempView("customers_bronze")
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

# ======================================================================================
# TABLE: silver_products
# ======================================================================================

# 3) SQL transformation (dedup + trim) using ROW_NUMBER
silver_products_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        pb.product_id AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS product_category,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.product_id DESC
        ) AS rn
      FROM products_bronze pb
      WHERE pb.product_id IS NOT NULL
    )
    SELECT
      product_id,
      product_name,
      product_category
    FROM ranked
    WHERE rn = 1
    """
)

# Create temp view for downstream joins
silver_products_df.createOrReplaceTempView("silver_products")

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    silver_products_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_products.csv")
)

# ======================================================================================
# TABLE: silver_customers
# ======================================================================================

# 3) SQL transformation (dedup + standardization) using ROW_NUMBER
silver_customers_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        cb.customer_id AS customer_id,
        TRIM(cb.customer_name) AS customer_name,
        LOWER(TRIM(cb.customer_email)) AS customer_email,
        CAST(cb.customer_created_date AS DATE) AS customer_created_date,
        ROW_NUMBER() OVER (
          PARTITION BY cb.customer_id
          ORDER BY cb.customer_id DESC
        ) AS rn
      FROM customers_bronze cb
      WHERE cb.customer_id IS NOT NULL
    )
    SELECT
      customer_id,
      customer_name,
      customer_email,
      customer_created_date
    FROM ranked
    WHERE rn = 1
    """
)

# Create temp view for downstream joins
silver_customers_df.createOrReplaceTempView("silver_customers")

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    silver_customers_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_customers.csv")
)

# ======================================================================================
# TABLE: silver_customer_orders
# ======================================================================================

# 3) SQL transformation (join to silver_customers, dedup, casts, normalization)
silver_customer_orders_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        cob.order_id AS order_id,
        CAST(cob.order_date AS DATE) AS order_date,
        sc.customer_id AS customer_id,
        UPPER(TRIM(cob.order_status)) AS order_status,
        UPPER(TRIM(cob.currency_code)) AS currency_code,
        COALESCE(CAST(cob.order_subtotal_amount AS DECIMAL(18,2)), 0) AS order_subtotal_amount,
        COALESCE(CAST(cob.order_tax_amount AS DECIMAL(18,2)), 0) AS order_tax_amount,
        COALESCE(CAST(cob.order_shipping_amount AS DECIMAL(18,2)), 0) AS order_shipping_amount,
        COALESCE(CAST(cob.order_discount_amount AS DECIMAL(18,2)), 0) AS order_discount_amount,
        COALESCE(CAST(cob.order_total_amount AS DECIMAL(18,2)), 0) AS order_total_amount,
        ROW_NUMBER() OVER (
          PARTITION BY cob.order_id
          ORDER BY cob.order_id DESC
        ) AS rn
      FROM customer_orders_bronze cob
      LEFT JOIN silver_customers sc
        ON cob.customer_id = sc.customer_id
      WHERE cob.order_id IS NOT NULL
    )
    SELECT
      order_id,
      order_date,
      customer_id,
      order_status,
      currency_code,
      order_subtotal_amount,
      order_tax_amount,
      order_shipping_amount,
      order_discount_amount,
      order_total_amount
    FROM joined
    WHERE rn = 1
    """
)

# Create temp view for downstream aggregation
silver_customer_orders_df.createOrReplaceTempView("silver_customer_orders")

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    silver_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_customer_orders.csv")
)

# ======================================================================================
# TABLE: silver_order_items
# ======================================================================================

# 3) SQL transformation (join to silver_products, dedup, casts, normalization)
silver_order_items_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        oib.order_id AS order_id,
        oib.order_item_id AS order_item_id,
        sp.product_id AS product_id,
        CAST(oib.quantity AS INTEGER) AS quantity,
        CAST(oib.unit_price_amount AS DECIMAL(18,2)) AS unit_price_amount,
        COALESCE(CAST(oib.line_discount_amount AS DECIMAL(18,2)), 0) AS line_discount_amount,
        CAST(oib.line_total_amount AS DECIMAL(18,2)) AS line_total_amount,
        ROW_NUMBER() OVER (
          PARTITION BY oib.order_id, oib.order_item_id
          ORDER BY oib.order_id DESC, oib.order_item_id DESC
        ) AS rn
      FROM order_items_bronze oib
      LEFT JOIN silver_products sp
        ON oib.product_id = sp.product_id
      WHERE oib.order_id IS NOT NULL
        AND oib.order_item_id IS NOT NULL
    )
    SELECT
      order_id,
      order_item_id,
      product_id,
      quantity,
      unit_price_amount,
      line_discount_amount,
      line_total_amount
    FROM joined
    WHERE rn = 1
    """
)

# Create temp view (optional)
silver_order_items_df.createOrReplaceTempView("silver_order_items")

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    silver_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_order_items.csv")
)

# ======================================================================================
# TABLE: silver_daily_order_kpis
# ======================================================================================

# 3) SQL transformation (daily aggregation from silver_customer_orders)
silver_daily_order_kpis_df = spark.sql(
    """
    SELECT
      sco.order_date AS order_date,
      COUNT(DISTINCT sco.order_id) AS orders_count,
      COUNT(DISTINCT sco.customer_id) AS unique_customers_count,
      SUM(sco.order_subtotal_amount) AS gross_sales_amount,
      SUM(sco.order_discount_amount) AS discount_amount,
      SUM(sco.order_tax_amount) AS tax_amount,
      SUM(sco.order_shipping_amount) AS shipping_amount,
      SUM(sco.order_total_amount) AS net_sales_amount
    FROM silver_customer_orders sco
    GROUP BY sco.order_date
    """
)

# Create temp view (optional)
silver_daily_order_kpis_df.createOrReplaceTempView("silver_daily_order_kpis")

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    silver_daily_order_kpis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_daily_order_kpis.csv")
)

job.commit()
```