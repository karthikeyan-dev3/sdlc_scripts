from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

# Metadata provided
metadata = {
    'tables': [
        {
            'target_schema': 'bronze',
            'target_table': 'customer_orders',
            'target_alias': 'co',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze customer_orders is sourced directly from sales_transactions_raw. Map: order_id = transaction_id, store_id = store_id, order_time = transaction_time, order_total_amount = sale_amount.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_order_items',
            'target_alias': 'coi',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze customer_order_items is sourced directly from sales_transactions_raw (one row per transaction line as provided). Map: order_id = transaction_id, product_id = product_id, quantity = quantity, line_amount = sale_amount.'
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'co.order_id = str.transaction_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'co.store_id = str.store_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'accepted',
            'target_column': 'order_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'accepted',
            'transformation': 'co.order_time = str.transaction_time',
            'target_table': 'co'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'order_total_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'co.order_total_amount = str.sale_amount',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'coi.order_id = str.transaction_id',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'coi.product_id = str.product_id',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'accepted',
            'transformation': 'coi.quantity = str.quantity',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'line_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'coi.line_amount = str.sale_amount',
            'target_table': 'coi'
        }
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']
base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']

for table in metadata['tables']:
    mapping_details = table['mapping_details'].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            transformation = col_meta['transformation']
            rhs = transformation.split('=', 1)[1].strip()
            target_col = col_meta['target_column']
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()