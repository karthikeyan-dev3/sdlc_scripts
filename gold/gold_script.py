```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder \
    .appName("AWS Glue Job") \
    .getOrCreate()
glueContext = GlueContext(spark)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read and process gold_sales_transactions
sales_transactions_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv/")
sales_transactions_df.createOrReplaceTempView("sts")

gold_sales_transactions_df = spark.sql("""
    SELECT 
        sts.transaction_id AS transaction_id,
        CAST(sts.sale_date AS DATE) AS sale_date,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_sales_amount AS DECIMAL(18,2)) AS total_sales_amount
    FROM sts
""")

gold_sales_transactions_df.repartition(1).write.mode('overwrite').csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_sales_transactions.csv", header=True)

# Read and process gold_product_master
product_master_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/product_master_silver.csv/")
product_master_df.createOrReplaceTempView("pms")

gold_product_master_df = spark.sql("""
    SELECT 
        pms.product_id AS product_id,
        pms.product_name AS product_name,
        pms.category AS category,
        pms.brand AS brand
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) as row_num
        FROM pms
    ) tmp
    WHERE tmp.row_num = 1
""")

gold_product_master_df.repartition(1).write.mode('overwrite').csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_product_master.csv", header=True)

# Read and process gold_store_master
store_master_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/store_master_silver.csv/")
store_master_df.createOrReplaceTempView("sms")

gold_store_master_df = spark.sql("""
    SELECT 
        sms.store_id AS store_id,
        sms.store_name AS store_name,
        sms.location AS location
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) as row_num
        FROM sms
    ) tmp
    WHERE tmp.row_num = 1
""")

gold_store_master_df.repartition(1).write.mode('overwrite').csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_store_master.csv", header=True)

# Read and process gold_aggregated_sales
aggregated_sales_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/daily_store_product_sales_agg_silver.csv/")
aggregated_sales_df.createOrReplaceTempView("dspsa")

gold_aggregated_sales_df = spark.sql("""
    SELECT 
        CAST(dspsa.aggregation_date AS DATE) AS aggregation_date,
        dspsa.store_id AS store_id,
        dspsa.product_id AS product_id,
        CAST(dspsa.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(dspsa.total_sales_amount AS DECIMAL(18,2)) AS total_sales_amount
    FROM dspsa
""")

gold_aggregated_sales_df.repartition(1).write.mode('overwrite').csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_aggregated_sales.csv", header=True)

job.commit()
```