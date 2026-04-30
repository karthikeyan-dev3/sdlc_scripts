```python
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Read sales_transactions_silver
sales_transactions_df = spark.read.format('csv').option('header', 'true').load(f"s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv/")
sales_transactions_df.createOrReplaceTempView("st")

# Read store_master_silver
store_master_df = spark.read.format('csv').option('header', 'true').load(f"s3://sdlc-agent-bucket/engineering-agent/silver/store_master_silver.csv/")
store_master_df.createOrReplaceTempView("sm")

# Create gold_sales_performance
gold_sales_performance_df = spark.sql("""
    SELECT
        st.sale_id,
        st.product_id,
        st.store_id,
        st.transaction_date,
        st.quantity_sold,
        st.revenue,
        sm.store_type,
        sm.city
    FROM
        st
    LEFT JOIN
        sm
    ON
        st.store_id = sm.store_id
""")
gold_sales_performance_df.coalesce(1).write.csv(f"s3://sdlc-agent-bucket/engineering-agent/gold/gold_sales_performance.csv", header=True)

# Read product_master_silver
product_master_df = spark.read.format('csv').option('header', 'true').load(f"s3://sdlc-agent-bucket/engineering-agent/silver/product_master_silver.csv/")
product_master_df.createOrReplaceTempView("pm")

# Create gold_product_master
gold_product_master_df = spark.sql("""
    SELECT
        pm.product_id,
        pm.product_name,
        pm.category,
        COALESCE(pm.brand, '') AS brand
    FROM
        pm
""")
gold_product_master_df.coalesce(1).write.csv(f"s3://sdlc-agent-bucket/engineering-agent/gold/gold_product_master.csv", header=True)

# Create gold_store_master
gold_store_master_df = spark.sql("""
    SELECT
        sm.store_id,
        sm.store_name,
        sm.store_type,
        sm.city,
        sm.region
    FROM
        sm
""")
gold_store_master_df.coalesce(1).write.csv(f"s3://sdlc-agent-bucket/engineering-agent/gold/gold_store_master.csv", header=True)

# Read sales_daily_aggregation_silver
sales_daily_aggregation_df = spark.read.format('csv').option('header', 'true').load(f"s3://sdlc-agent-bucket/engineering-agent/silver/sales_daily_aggregation_silver.csv/")
sales_daily_aggregation_df.createOrReplaceTempView("sda")

# Create gold_sales_summary
gold_sales_summary_df = spark.sql("""
    SELECT
        sda.reporting_date,
        sda.total_revenue,
        sda.total_quantity_sold,
        sda.revenue_by_city,
        sda.revenue_by_store_type
    FROM
        sda
""")
gold_sales_summary_df.coalesce(1).write.csv(f"s3://sdlc-agent-bucket/engineering-agent/gold/gold_sales_summary.csv", header=True)

job.commit()
```