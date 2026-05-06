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
        {'target_schema': 'bronze', 'target_table': 'products_bronze', 'target_alias': 'pb', 'mapping_details': 'products_raw pr'},
        {'target_schema': 'bronze', 'target_table': 'stores_bronze', 'target_alias': 'sb', 'mapping_details': 'stores_raw sr'},
        {'target_schema': 'bronze', 'target_table': 'sales_transactions_bronze', 'target_alias': 'stb', 'mapping_details': 'sales_transactions_raw str'}
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
        {'transformation': 'stb.transaction_time = str.transaction_time', 'target_table': 'stb'},
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

for table in metadata['tables']:
    source_table, source_alias = table['mapping_details'].split()
    target_table = table['target_table']
    
    df = spark.read.format(metadata['runtime_config']['read_format']) \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(metadata['runtime_config']['base_path'] + source_table + '.' + metadata['runtime_config']['read_format'])
        
    df = df.alias(source_alias)
    
    transformations = [col['transformation'].split('=')[1].strip() + ' as ' + col['transformation'].split('.')[1].split('=')[0].strip() 
                       for col in metadata['columns'] if col['target_table'] == table['target_alias']]
    
    df = df.selectExpr(*transformations)
    
    df.write.mode(metadata['runtime_config']['write_mode']) \
        .format(metadata['runtime_config']['write_format']) \
        .option("header", "true") \
        .save(metadata['runtime_config']['target_path'] + target_table + '.' + metadata['runtime_config']['write_format'])

job.commit()