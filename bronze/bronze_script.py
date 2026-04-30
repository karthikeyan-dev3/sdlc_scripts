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
        {'target_schema': 'bronze', 'target_table': 'pos_sales_event_bronze', 'target_alias': 'pseb', 'mapping_details': 'sales_event se'},
        {'target_schema': 'bronze', 'target_table': 'payment_gateway_payment_event_bronze', 'target_alias': 'pgpeb', 'mapping_details': 'payment_event pe'}
    ],
    'columns': [
        {'target_column': 'transaction_id', 'transformation': 'pseb.transaction_id = pseb.transaction_id', 'target_table': 'pseb'},
        {'target_column': 'order_id', 'transformation': 'pseb.order_id = pseb.order_id', 'target_table': 'pseb'},
        {'target_column': 'store_id', 'transformation': 'pseb.store_id = pseb.store_id', 'target_table': 'pseb'},
        {'target_column': 'product_id', 'transformation': 'pseb.product_id = pseb.product_id', 'target_table': 'pseb'},
        {'target_column': 'product_name', 'transformation': 'pseb.product_name = pseb.product_name', 'target_table': 'pseb'},
        {'target_column': 'category', 'transformation': 'pseb.category = pseb.category', 'target_table': 'pseb'},
        {'target_column': 'sub_category', 'transformation': 'pseb.sub_category = pseb.sub_category', 'target_table': 'pseb'},
        {'target_column': 'quantity', 'transformation': 'pseb.quantity = pseb.quantity', 'target_table': 'pseb'},
        {'target_column': 'unit_price', 'transformation': 'pseb.unit_price = pseb.unit_price', 'target_table': 'pseb'},
        {'target_column': 'discount', 'transformation': 'pseb.discount = pseb.discount', 'target_table': 'pseb'},
        {'target_column': 'total_amount', 'transformation': 'pseb.total_amount = pseb.total_amount', 'target_table': 'pseb'},
        {'target_column': 'payment_id', 'transformation': 'pseb.payment_id = pseb.payment_id', 'target_table': 'pseb'},
        {'target_column': 'event_action', 'transformation': 'pseb.event_action = pseb.event_action', 'target_table': 'pseb'},
        {'target_column': 'payment_id', 'transformation': 'pgpeb.payment_id = pgpeb.payment_id', 'target_table': 'pgpeb'},
        {'target_column': 'transaction_id', 'transformation': 'pgpeb.transaction_id = pgpeb.transaction_id', 'target_table': 'pgpeb'},
        {'target_column': 'payment_mode', 'transformation': 'pgpeb.payment_mode = pgpeb.payment_mode', 'target_table': 'pgpeb'},
        {'target_column': 'provider', 'transformation': 'pgpeb.provider = pgpeb.provider', 'target_table': 'pgpeb'},
        {'target_column': 'amount', 'transformation': 'pgpeb.amount = pgpeb.amount', 'target_table': 'pgpeb'},
        {'target_column': 'currency', 'transformation': 'pgpeb.currency = pgpeb.currency', 'target_table': 'pgpeb'},
        {'target_column': 'payment_status', 'transformation': 'pgpeb.payment_status = pgpeb.payment_status', 'target_table': 'pgpeb'}
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

for table_meta in metadata['tables']:
    source_table, source_alias = table_meta['mapping_details'].split()
    target_table = table_meta['target_table']
    
    df = spark.read.format(metadata['runtime_config']['read_format']) \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(metadata['runtime_config']['base_path'] + source_table + '.' + metadata['runtime_config']['read_format'])

    df = df.alias(source_alias)
    
    transformations = [col['transformation'].split('=')[1].strip() + ' as ' + col['target_column']
                       for col in metadata['columns'] if col['target_table'] == table_meta['target_alias']]

    df = df.selectExpr(*transformations)
    
    df.write.mode(metadata['runtime_config']['write_mode']).format(metadata['runtime_config']['write_format']) \
        .option("header", "true") \
        .save(metadata['runtime_config']['target_path'] + target_table + '.' + metadata['runtime_config']['write_format'])

job.commit()