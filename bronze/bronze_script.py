from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

# Configuration
base_path = "s3://sdlc-agent-bucket/engineering-agent/src/"
target_path = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
read_format = "csv"
write_format = "csv"
write_mode = "overwrite"

# Table processing metadata
metadata = {
    'tables': [
        {'target_table': 'products_bronze', 'target_alias': 'pb', 'mapping_details': 'products_raw pr'},
        {'target_table': 'stores_bronze', 'target_alias': 'sb', 'mapping_details': 'stores_raw sr'},
        {'target_table': 'sales_transactions_bronze', 'target_alias': 'stb', 'mapping_details': 'sales_transactions_raw str'}
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
    'runtime_config': {
        'base_path': base_path,
        'target_path': target_path,
        'read_format': read_format,
        'write_format': write_format,
        'write_mode': write_mode
    }
}

# Process each table
for table in metadata['tables']:
    source_table, source_alias = table['mapping_details'].split()
    target_table = table['target_table']

    # Read data
    df = spark.read.format(read_format)
    if read_format == 'csv':
        df = df.option("header", "true").option("inferSchema", "true")
    df = df.load(f"{base_path}{source_table}.{read_format}")

    # Apply alias
    df = df.alias(source_alias)

    # Column transformations
    transformations = [col['transformation'].split('=')[1].strip() + f" as {col['transformation'].split('=')[0].strip().split('.')[1]}"
                       for col in metadata['columns'] if col['target_table'] == table['target_alias']]
    
    # Select the transformed columns
    df = df.selectExpr(*transformations)

    # Write data
    if write_format == 'csv':
        df.write.mode(write_mode).option("header", "true").format(write_format)
    else:
        df.write.mode(write_mode).format(write_format)
    df.save(f"{target_path}{target_table}.{write_format}")

job.commit()