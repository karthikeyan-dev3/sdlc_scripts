```python
import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.appName(args['JOB_NAME']).getOrCreate()
glueContext = GlueContext(spark)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read sales_transaction_silver
sales_transaction_df = spark.read.option("header", "true").csv(f"{SOURCE_PATH}/sales_transaction_silver.{FILE_FORMAT}/")
sales_transaction_df.createOrReplaceTempView("sales_transaction_silver")

# Read store_silver
store_df = spark.read.option("header", "true").csv(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
store_df.createOrReplaceTempView("store_silver")

# Read product_silver
product_df = spark.read.option("header", "true").csv(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
product_df.createOrReplaceTempView("product_silver")

# Transformation for gold_sales
gold_sales = spark.sql("""
    SELECT 
        sts.sales_id, 
        sts.product_id, 
        sts.store_id, 
        CAST(sts.sale_date AS DATE) as sale_date, 
        sts.quantity_sold, 
        sts.sale_amount, 
        COALESCE(ss.region, 'UNKNOWN') as region
    FROM 
        sales_transaction_silver sts 
    LEFT JOIN 
        store_silver ss 
    ON 
        sts.store_id = ss.store_id
""")
gold_sales.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales.csv", header=True)

# Transformation for gold_product
gold_product = spark.sql("""
    SELECT 
        ps.product_id, 
        ps.product_name, 
        ps.category, 
        ps.price
    FROM 
        product_silver ps
""")
gold_product.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_product.csv", header=True)

# Transformation for gold_store
gold_store = spark.sql("""
    SELECT 
        ss.store_id, 
        ss.store_name, 
        ss.location, 
        ss.region, 
        ss.store_type
    FROM 
        store_silver ss
""")
gold_store.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_store.csv", header=True)

# Transformation for gold_sales_aggregated
gold_sales_aggregated = spark.sql("""
    SELECT 
        CAST(sts.sale_date AS DATE) as date,
        COALESCE(ss.region, 'UNKNOWN') as region,
        ps.category as product_category,
        SUM(sts.sale_amount) as total_sales_amount,
        SUM(sts.quantity_sold) as total_quantity_sold,
        AVG(sts.sale_amount) as average_sale_amount
    FROM 
        sales_transaction_silver sts
    LEFT JOIN 
        store_silver ss
    ON 
        sts.store_id = ss.store_id
    LEFT JOIN 
        product_silver ps
    ON 
        sts.product_id = ps.product_id
    GROUP BY 
        CAST(sts.sale_date AS DATE), 
        COALESCE(ss.region, 'UNKNOWN'), 
        ps.category
""")
gold_sales_aggregated.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales_aggregated.csv", header=True)
```