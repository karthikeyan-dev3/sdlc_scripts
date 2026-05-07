```python
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = SparkSession.builder.appName("AWS Glue PySpark Job").getOrCreate()

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read inventory_levels_silver
inventory_levels_df = spark.read.format(FILE_FORMAT) \
    .load(f"{SOURCE_PATH}/inventory_levels_silver.{FILE_FORMAT}/")
inventory_levels_df.createOrReplaceTempView("ils")

# Read daily_sales_silver
daily_sales_df = spark.read.format(FILE_FORMAT) \
    .load(f"{SOURCE_PATH}/daily_sales_silver.{FILE_FORMAT}/")
daily_sales_df.createOrReplaceTempView("dss")

# Read stockout_status_silver
stockout_status_df = spark.read.format(FILE_FORMAT) \
    .load(f"{SOURCE_PATH}/stockout_status_silver.{FILE_FORMAT}/")
stockout_status_df.createOrReplaceTempView("sos")

# Transformations for gold_inventory_sales
gold_inventory_sales_query = """
SELECT
    ils.store_id AS store_id,
    ils.product_id AS product_id,
    ils.date AS date,
    ils.inventory_quantity AS inventory_quantity,
    COALESCE(dss.sales_quantity, 0) AS sales_quantity,
    COALESCE(dss.sales_revenue, 0) AS sales_revenue
FROM
    ils
LEFT JOIN
    dss ON ils.store_id = dss.store_id
       AND ils.product_id = dss.product_id
       AND ils.date = dss.date
"""
gold_inventory_sales_df = spark.sql(gold_inventory_sales_query)
gold_inventory_sales_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_inventory_sales.csv", header=True, mode='overwrite')

# Transformations for gold_kpi_metrics
gold_kpi_metrics_query = """
SELECT
    ils.store_id AS store_id,
    ils.date AS date,
    SUM(ils.inventory_quantity) AS total_inventory,
    SUM(COALESCE(dss.sales_revenue, 0)) AS total_sales,
    SUM(COALESCE(dss.sales_quantity, 0)) AS sales_velocity,
    SUM(COALESCE(sos.stockout_flag, 0)) AS stockout_events
FROM
    ils
LEFT JOIN
    dss ON ils.store_id = dss.store_id
       AND ils.product_id = dss.product_id
       AND ils.date = dss.date
LEFT JOIN
    sos ON ils.store_id = sos.store_id
       AND ils.product_id = sos.product_id
       AND ils.date = sos.date
GROUP BY
    ils.store_id, ils.date
"""
gold_kpi_metrics_df = spark.sql(gold_kpi_metrics_query)
gold_kpi_metrics_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_kpi_metrics.csv", header=True, mode='overwrite')

# Transformations for gold_sales_trends
gold_sales_trends_query = """
SELECT
    dss.product_id AS product_id,
    dss.date AS date,
    SUM(dss.sales_quantity) AS sales_quantity,
    SUM(dss.sales_revenue) AS sales_revenue,
    AVG(SUM(dss.sales_quantity)) OVER (
        PARTITION BY dss.product_id
        ORDER BY dss.date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_average_sales
FROM
    dss
GROUP BY
    dss.product_id, dss.date
"""
gold_sales_trends_df = spark.sql(gold_sales_trends_query)
gold_sales_trends_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_sales_trends.csv", header=True, mode='overwrite')

# Transformations for gold_stockout_alerts
gold_stockout_alerts_query = """
SELECT
    sos.store_id AS store_id,
    sos.product_id AS product_id,
    sos.date AS date,
    sos.stockout_flag AS stockout_flag,
    sos.replenishment_status AS replenishment_status
FROM
    sos
"""
gold_stockout_alerts_df = spark.sql(gold_stockout_alerts_query)
gold_stockout_alerts_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_stockout_alerts.csv", header=True, mode='overwrite')
```