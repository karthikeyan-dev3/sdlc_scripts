import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, [])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ============================================================
# 1) Read source tables from S3
# ============================================================

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)

# ============================================================
# 2) Create temp views
# ============================================================

stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")
sales_silver_df.createOrReplaceTempView("sales_silver")

# ============================================================
# 3) gold.gold_dim_store
# ============================================================

gold_dim_store_df = spark.sql(
    """
    SELECT
        ss.store_id AS store_id,
        ss.store_name AS store_name,
        ss.store_type AS store_type,
        ss.city AS city,
        ss.state AS state,
        ss.open_date AS open_date,
        CAST(NULL AS string) AS region,
        CAST(NULL AS string) AS district,
        CAST(NULL AS string) AS country,
        CAST(NULL AS date) AS close_date,
        CASE WHEN ss.store_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_active
    FROM stores_silver ss
    """
)

gold_dim_store_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_dim_store.csv"
)

# ============================================================
# 4) gold.gold_dim_product
# ============================================================

gold_dim_product_df = spark.sql(
    """
    SELECT
        ps.product_id AS product_id,
        ps.product_name AS product_name,
        ps.brand AS brand,
        ps.category AS category,
        ps.is_active AS is_active,
        CAST(NULL AS string) AS sku,
        CAST(NULL AS string) AS subcategory,
        CAST(NULL AS string) AS department,
        CAST(NULL AS string) AS unit_of_measure
    FROM products_silver ps
    """
)

gold_dim_product_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_dim_product.csv"
)

# ============================================================
# 5) gold.gold_fact_sales
# ============================================================

gold_fact_sales_df = spark.sql(
    """
    SELECT
        ssal.transaction_id AS sale_id,
        ssal.transaction_time AS sale_ts,
        CAST(ssal.transaction_time AS date) AS sale_date,
        ssal.store_id AS store_id,
        ssal.product_id AS product_id,
        ssal.quantity AS quantity_sold,
        (ssal.sale_amount / ssal.quantity) AS unit_price,
        ssal.sale_amount AS gross_sales_amount,
        ssal.sale_amount AS net_sales_amount,
        CAST(NULL AS string) AS currency_code
    FROM sales_silver ssal
    LEFT JOIN stores_silver ss
        ON ssal.store_id = ss.store_id
    LEFT JOIN products_silver ps
        ON ssal.product_id = ps.product_id
    """
)

gold_fact_sales_df.createOrReplaceTempView("gold_fact_sales")

gold_fact_sales_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_fact_sales.csv"
)

# ============================================================
# 6) gold.gold_agg_store_daily
# ============================================================

gold_agg_store_daily_df = spark.sql(
    """
    SELECT
        gfs.sale_date AS sale_date,
        gfs.store_id AS store_id,
        COUNT(DISTINCT gfs.sale_id) AS total_transactions,
        SUM(gfs.quantity_sold) AS total_quantity_sold,
        SUM(gfs.gross_sales_amount) AS total_gross_sales_amount,
        SUM(gfs.net_sales_amount) AS total_net_sales_amount
    FROM gold_fact_sales gfs
    GROUP BY
        gfs.sale_date,
        gfs.store_id
    """
)

gold_agg_store_daily_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_agg_store_daily.csv"
)

# ============================================================
# 7) gold.gold_agg_product_daily
# ============================================================

gold_agg_product_daily_df = spark.sql(
    """
    SELECT
        gfs.sale_date AS sale_date,
        gfs.product_id AS product_id,
        COUNT(DISTINCT gfs.sale_id) AS total_transactions,
        SUM(gfs.quantity_sold) AS total_quantity_sold,
        SUM(gfs.gross_sales_amount) AS total_gross_sales_amount,
        SUM(gfs.net_sales_amount) AS total_net_sales_amount
    FROM gold_fact_sales gfs
    GROUP BY
        gfs.sale_date,
        gfs.product_id
    """
)

gold_agg_product_daily_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_agg_product_daily.csv"
)
