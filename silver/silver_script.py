import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# ------------------------------------------------------------------------------------
# 1) Read Source Tables (Bronze) + Create Temp Views
# ------------------------------------------------------------------------------------
stb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
stb_df.createOrReplaceTempView("sales_transactions_bronze")

pdb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_bronze.{FILE_FORMAT}/")
)
pdb_df.createOrReplaceTempView("product_details_bronze")

sdb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_details_bronze.{FILE_FORMAT}/")
)
sdb_df.createOrReplaceTempView("store_details_bronze")

# ------------------------------------------------------------------------------------
# 2) sales_transactions_silver
#    - transaction_date = CAST(stb.transaction_time AS DATE)
#    - quantity_sold = stb.quantity
#    - total_sales_amount = stb.sale_amount
#    - Deduplicate on transaction_id keeping latest transaction_time
#    - Filter out null/blank transaction_id, product_id, store_id
#    - Enforce non-negative quantity and sales amounts
# ------------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        stb.transaction_id,
        stb.product_id,
        stb.store_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
        ROW_NUMBER() OVER (
          PARTITION BY stb.transaction_id
          ORDER BY stb.transaction_time DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      WHERE
        stb.transaction_id IS NOT NULL AND TRIM(stb.transaction_id) <> ''
        AND stb.product_id IS NOT NULL AND TRIM(stb.product_id) <> ''
        AND stb.store_id IS NOT NULL AND TRIM(stb.store_id) <> ''
        AND CAST(stb.quantity AS INT) >= 0
        AND CAST(stb.sale_amount AS DOUBLE) >= 0
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      transaction_date,
      quantity_sold,
      total_sales_amount
    FROM base
    WHERE rn = 1
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/sales_transactions_silver.csv")
)

# ------------------------------------------------------------------------------------
# 3) product_master_silver
#    - Keeps only active products (is_active = true)
#    - Deduplicate on product_id (latest record if available)
#    - trims/standardizes text fields; validates price >= 0
#    - outputs product_id, product_name, category, brand, price
# ------------------------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        pdb.product_id,
        TRIM(pdb.product_name) AS product_name,
        TRIM(pdb.category) AS category,
        TRIM(pdb.brand) AS brand,
        CAST(pdb.price AS FLOAT) AS price,
        ROW_NUMBER() OVER (
          PARTITION BY pdb.product_id
          ORDER BY pdb.product_id
        ) AS rn
      FROM product_details_bronze pdb
      WHERE
        pdb.is_active = true
        AND pdb.product_id IS NOT NULL AND TRIM(pdb.product_id) <> ''
        AND CAST(pdb.price AS FLOAT) >= 0
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      price
    FROM base
    WHERE rn = 1
    """
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/product_master_silver.csv")
)

# ------------------------------------------------------------------------------------
# 4) store_master_silver
#    - location = CONCAT(TRIM(city), ', ', TRIM(state))
#    - region = state_to_region_map(state) implemented via SQL CASE
#    - Deduplicate on store_id
#    - standardizes store_name and store_type
#    - outputs store_id, store_name, location, region, store_type
# ------------------------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        sdb.store_id,
        TRIM(sdb.store_name) AS store_name,
        CONCAT(TRIM(sdb.city), ', ', TRIM(sdb.state)) AS location,
        CASE
          WHEN UPPER(TRIM(sdb.state)) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
          WHEN UPPER(TRIM(sdb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
          WHEN UPPER(TRIM(sdb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
          WHEN UPPER(TRIM(sdb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
          ELSE NULL
        END AS region,
        TRIM(sdb.store_type) AS store_type,
        ROW_NUMBER() OVER (
          PARTITION BY sdb.store_id
          ORDER BY sdb.store_id
        ) AS rn
      FROM store_details_bronze sdb
      WHERE
        sdb.store_id IS NOT NULL AND TRIM(sdb.store_id) <> ''
    )
    SELECT
      store_id,
      store_name,
      location,
      region,
      store_type
    FROM base
    WHERE rn = 1
    """
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/store_master_silver.csv")
)

# ------------------------------------------------------------------------------------
# 5) daily_sales_aggregate_silver
#    - reporting_date = sts.transaction_date
#    - total_sales_amount = SUM(sts.total_sales_amount)
#    - total_quantity_sold = SUM(sts.quantity_sold)
#    - average_sales_per_transaction = SUM(sts.total_sales_amount)/COUNT(DISTINCT sts.transaction_id)
#    - sales_by_category = OBJECT_AGG(pms.category, SUM(sts.total_sales_amount)) (emulated using JSON map)
#    - sales_by_region   = OBJECT_AGG(sms.region, SUM(sts.total_sales_amount)) (emulated using JSON map)
# ------------------------------------------------------------------------------------
daily_sales_aggregate_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        sts.transaction_date AS reporting_date,
        sts.transaction_id,
        sts.total_sales_amount,
        sts.quantity_sold,
        pms.category,
        sms.region
      FROM sales_transactions_silver sts
      LEFT JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
      LEFT JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    ),
    base_agg AS (
      SELECT
        reporting_date,
        SUM(total_sales_amount) AS total_sales_amount,
        SUM(quantity_sold) AS total_quantity_sold,
        SUM(total_sales_amount) / COUNT(DISTINCT transaction_id) AS average_sales_per_transaction
      FROM joined
      GROUP BY reporting_date
    ),
    cat_kv AS (
      SELECT
        reporting_date,
        to_json(
          map_from_entries(
            collect_list(
              named_struct('key', category, 'value', category_sales)
            )
          )
        ) AS sales_by_category
      FROM (
        SELECT
          reporting_date,
          category,
          SUM(total_sales_amount) AS category_sales
        FROM joined
        GROUP BY reporting_date, category
      ) x
      GROUP BY reporting_date
    ),
    region_kv AS (
      SELECT
        reporting_date,
        to_json(
          map_from_entries(
            collect_list(
              named_struct('key', region, 'value', region_sales)
            )
          )
        ) AS sales_by_region
      FROM (
        SELECT
          reporting_date,
          region,
          SUM(total_sales_amount) AS region_sales
        FROM joined
        GROUP BY reporting_date, region
      ) y
      GROUP BY reporting_date
    )
    SELECT
      b.reporting_date,
      b.total_sales_amount,
      b.total_quantity_sold,
      b.average_sales_per_transaction,
      c.sales_by_category,
      r.sales_by_region
    FROM base_agg b
    LEFT JOIN cat_kv c
      ON b.reporting_date = c.reporting_date
    LEFT JOIN region_kv r
      ON b.reporting_date = r.reporting_date
    """
)

(
    daily_sales_aggregate_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/daily_sales_aggregate_silver.csv")
)

job.commit()