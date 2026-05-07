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
        {'target_schema': 'bronze', 'target_table': 'sales_transactions_bronze', 'target_alias': 'stb', 'mapping_details': 'sales_transactions_raw str'},
        {'target_schema': 'bronze', 'target_table': 'product_master_bronze', 'target_alias': 'pmb', 'mapping_details': 'products_raw pr'},
        {'target_schema': 'bronze', 'target_table': 'store_master_bronze', 'target_alias': 'smb', 'mapping_details': 'stores_raw sr'},
    ],
    'columns': [
        {'transformation': 'stb.transaction_id = str.transaction_id', 'target_table': 'stb', 'target_column': 'transaction_id'},
        {'transformation': 'stb.store_id = str.store_id', 'target_table': 'stb', 'target_column': 'store_id'},
        {'transformation': 'stb.product_id = str.product_id', 'target_table': 'stb', 'target_column': 'product_id'},
        {'transformation': 'stb.quantity = str.quantity', 'target_table': 'stb', 'target_column': 'quantity'},
        {'transformation': 'stb.sale_amount = str.sale_amount', 'target_table': 'stb', 'target_column': 'sale_amount'},
        {'transformation': 'stb.transaction_time = str.transaction_time', 'target_table': 'stb', 'target_column': 'transaction_time'},
        {'transformation': 'pmb.product_id = pr.product_id', 'target_table': 'pmb', 'target_column': 'product_id'},
        {'transformation': 'pmb.product_name = pr.product_name', 'target_table': 'pmb', 'target_column': 'product_name'},
        {'transformation': 'pmb.category = pr.category', 'target_table': 'pmb', 'target_column': 'category'},
        {'transformation': 'pmb.brand = pr.brand', 'target_table': 'pmb', 'target_column': 'brand'},
        {'transformation': 'pmb.price = pr.price', 'target_table': 'pmb', 'target_column': 'price'},
        {'transformation': 'pmb.is_active = pr.is_active', 'target_table': 'pmb', 'target_column': 'is_active'},
        {'transformation': 'smb.store_id = sr.store_id', 'target_table': 'smb', 'target_column': 'store_id'},
        {'transformation': 'smb.store_name = sr.store_name', 'target_table': 'smb', 'target_column': 'store_name'},
        {'transformation': 'smb.city = sr.city', 'target_table': 'smb', 'target_column': 'city'},
        {'transformation': 'smb.state = sr.state', 'target_table': 'smb', 'target_column': 'state'},
        {'transformation': 'smb.store_type = sr.store_type', 'target_table': 'smb', 'target_column': 'store_type'},
        {'transformation': 'smb.open_date = sr.open_date', 'target_table': 'smb', 'target_column': 'open_date'},
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    mapping_details = table['mapping_details'].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table['target_table']
    target_alias = table['target_alias']
    
    df = spark.read.format(read_format)
    if read_format == 'csv':
        df = df.option("header", "true").option("inferSchema", "true")
    df = df.load(base_path + source_table + '.' + read_format)
    
    df = df.alias(source_alias)
    
    transformations = [col['transformation'].split(' = ')[1] + ' as ' + col['target_column']
                       for col in metadata['columns'] if col['target_table'] == target_alias]
    
    df = df.selectExpr(*transformations)
    
    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")
    writer.save(target_path + target_table + '.' + write_format)

job.commit()