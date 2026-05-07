from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {
    'tables': [
        {'target_schema': 'bronze', 'target_table': 'products_bronze', 'target_alias': 'pb', 'mapping_details': 'products_raw pr', 'description': 'Bronze raw ingestion of product master data.'},
        {'target_schema': 'bronze', 'target_table': 'stores_bronze', 'target_alias': 'sb', 'mapping_details': 'stores_raw sr', 'description': 'Bronze raw ingestion of store master data.'},
        {'target_schema': 'bronze', 'target_table': 'sales_transactions_bronze', 'target_alias': 'stb', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze raw ingestion of sales transaction fact data.'}
    ],
    'columns': [
        {'transformation': 'pb.product_id = pr.product_id', 'target_table': 'pb'},
        {'transformation': 'pb.product_name = pr.product_name', 'target_table': 'pb'},
        {'transformation': 'pb.category = pr.category', 'target_table': 'pb'},
        {'transformation': 'pb.brand = pr.brand', 'target_table': 'pb'},
        {'transformation': 'pb.price = pr.price', 'target_table': 'pb'},
        {'transformation': 'pb.is_active = pr.is_active', 'target_table': 'pb'},
        {'transformation': 'sb.store_id = sr.store_id', 'target_table': 'sb'},
        {'transformation': 'sb.store_name = sr.store_name', 'target_table': 'sb'},
        {'transformation': 'sb.city = sr.city', 'target_table': 'sb'},
        {'transformation': 'sb.state = sr.state', 'target_table': 'sb'},
        {'transformation': 'sb.store_type = sr.store_type', 'target_table': 'sb'},
        {'transformation': 'sb.open_date = sr.open_date', 'target_table': 'sb'},
        {'transformation': 'stb.transaction_id = str.transaction_id', 'target_table': 'stb'},
        {'transformation': 'stb.store_id = str.store_id', 'target_table': 'stb'},
        {'transformation': 'stb.product_id = str.product_id', 'target_table': 'stb'},
        {'transformation': 'stb.quantity = str.quantity', 'target_table': 'stb'},
        {'transformation': 'stb.sale_amount = str.sale_amount', 'target_table': 'stb'},
        {'transformation': 'stb.transaction_time = str.transaction_time', 'target_table': 'stb'}
    ],
    'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}
}

# Extract runtime configuration
base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

# Process each table in metadata
def process_table(table_metadata):
    source_table, source_alias = table_metadata['mapping_details'].split()
    target_table = table_metadata['target_table']
    target_alias = table_metadata['target_alias']

    # Read data
df = spark.read.format(read_format) \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{base_path}{source_table}.{read_format}")

    # Apply alias
df = df.alias(source_alias)

    # Extract transformations for the current table
    transformations = [
        column['transformation'].split('=')[1].strip() + ' as ' + column['transformation'].split('=')[0].strip().split('.')[-1]
        for column in metadata['columns']
        if column['target_table'] == target_alias
    ]

    # Select transformed columns
df = df.selectExpr(*transformations)

    # Write data
df.write.mode(write_mode) \
        .format(write_format) \
        .option("header", "true") \
        .save(f"{target_path}{target_table}.{write_format}")

for table in metadata['tables']:
    process_table(table)

job.commit()