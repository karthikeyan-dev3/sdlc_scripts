from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

# Runtime configuration
base_path = 's3://sdlc-agent-bucket/engineering-agent/src/'
target_path = 's3://sdlc-agent-bucket/engineering-agent/bronze/'
read_format = 'csv'
write_format = 'csv'
write_mode = 'overwrite'

# Table metadata
tables = [
    {'target_table': 'sales_transactions_bronze', 'source_table': 'sales_transactions_raw', 'source_alias': 'str', 'target_alias': 'stb'},
    {'target_table': 'stores_bronze', 'source_table': 'stores_raw', 'source_alias': 'sr', 'target_alias': 'sb'},
    {'target_table': 'products_bronze', 'source_table': 'products_raw', 'source_alias': 'pr', 'target_alias': 'pb'},
]

# Column transformations per table
column_transformations = {
    'stb': [
        'str.transaction_id as transaction_id',
        'str.store_id as store_id',
        'str.product_id as product_id',
        'str.quantity as quantity',
        'str.sale_amount as sale_amount',
        'str.transaction_time as transaction_time'
    ],
    'sb': [
        'sr.store_id as store_id',
        'sr.store_name as store_name',
        'sr.city as city',
        'sr.state as state',
        'sr.store_type as store_type',
        'sr.open_date as open_date'
    ],
    'pb': [
        'pr.product_id as product_id',
        'pr.product_name as product_name',
        'pr.category as category',
        'pr.brand as brand',
        'pr.price as price',
        'pr.is_active as is_active'
    ]
}

for table in tables:
    # Read the data
    source_table = table['source_table']
    df = spark.read.format(read_format)\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(base_path + source_table + '.' + read_format)

    # Apply alias
    df = df.alias(table['source_alias'])

    # Apply column transformations
    transformations = column_transformations[table['target_alias']]
    df = df.selectExpr(*transformations)

    # Write the data
    target_table = table['target_table']
    df.write.mode(write_mode).format(write_format)\
        .option("header", "true")\
        .save(target_path + target_table + '.' + write_format)

job.commit()