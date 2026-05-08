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
        {'target_schema': 'bronze', 'target_table': 'sales_bronze', 'target_alias': 'sb', 'mapping_details': 'sales_transactions_raw str'},
        {'target_schema': 'bronze', 'target_table': 'products_bronze', 'target_alias': 'pb', 'mapping_details': 'products_raw pr'},
        {'target_schema': 'bronze', 'target_table': 'stores_bronze', 'target_alias': 'stb', 'mapping_details': 'stores_raw sr'}
    ],
    'columns': [
        {'transformation': 'str.transaction_id = str.transaction_id', 'target_column': 'transaction_id', 'target_table': 'str'},
        {'transformation': 'str.store_id = str.store_id', 'target_column': 'store_id', 'target_table': 'str'},
        {'transformation': 'str.product_id = str.product_id', 'target_column': 'product_id', 'target_table': 'str'},
        {'transformation': 'str.quantity = str.quantity', 'target_column': 'quantity', 'target_table': 'str'},
        {'transformation': 'str.sale_amount = str.sale_amount', 'target_column': 'sale_amount', 'target_table': 'str'},
        {'transformation': 'str.transaction_time = str.transaction_time', 'target_column': 'transaction_time', 'target_table': 'str'},
        {'transformation': 'pr.product_id = pr.product_id', 'target_column': 'product_id', 'target_table': 'pr'},
        {'transformation': 'pr.product_name = pr.product_name', 'target_column': 'product_name', 'target_table': 'pr'},
        {'transformation': 'pr.category = pr.category', 'target_column': 'category', 'target_table': 'pr'},
        {'transformation': 'pr.brand = pr.brand', 'target_column': 'brand', 'target_table': 'pr'},
        {'transformation': 'pr.price = pr.price', 'target_column': 'price', 'target_table': 'pr'},
        {'transformation': 'pr.is_active = pr.is_active', 'target_column': 'is_active', 'target_table': 'pr'},
        {'transformation': 'sr.store_id = sr.store_id', 'target_column': 'store_id', 'target_table': 'sr'},
        {'transformation': 'sr.store_name = sr.store_name', 'target_column': 'store_name', 'target_table': 'sr'},
        {'transformation': 'sr.city = sr.city', 'target_column': 'city', 'target_table': 'sr'},
        {'transformation': 'sr.state = sr.state', 'target_column': 'state', 'target_table': 'sr'},
        {'transformation': 'sr.store_type = sr.store_type', 'target_column': 'store_type', 'target_table': 'sr'},
        {'transformation': 'sr.open_date = sr.open_date', 'target_column': 'open_date', 'target_table': 'sr'}
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

runtime_config = metadata['runtime_config']
base_path = runtime_config['base_path']
target_path = runtime_config['target_path']
read_format = runtime_config['read_format']
write_format = runtime_config['write_format']
write_mode = runtime_config['write_mode']

for table in metadata['tables']:
    source_table, source_alias = table['mapping_details'].split()
    target_table = table['target_table']
    
    df = spark.read.format(read_format)\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(base_path + source_table + '.' + read_format)
    
    df = df.alias(source_alias)
    
    transformations = [col['transformation'].split('=', 1)[1].strip() + ' as ' + col['target_column']
                       for col in metadata['columns'] if col['target_table'] == source_alias]

    df = df.selectExpr(*transformations)

    df.write.mode(write_mode).format(write_format)\
        .option("header", "true")\
        .save(target_path + target_table + '.' + write_format)

job.commit()