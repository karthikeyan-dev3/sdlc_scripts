from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

# Metadata provided at runtime
metadata = {
    'tables': [
        {
            'target_schema': 'bronze',
            'target_table': 'customer_orders',
            'target_alias': 'co',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze table for customer orders created by directly ingesting each record from sales_transactions_raw. Map: transaction_id->order_id, store_id->store_id, sale_amount->order_total_amount, transaction_time->order_timestamp.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_order_items',
            'target_alias': 'coi',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze table for customer order items created by directly ingesting each record from sales_transactions_raw. Map: transaction_id->order_id, product_id->product_id, quantity->quantity, sale_amount->line_sale_amount, transaction_time->transaction_time, store_id->store_id.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'daily_order_data_quality',
            'target_alias': 'dodq',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze table for daily order data quality created by directly ingesting each record from sales_transactions_raw to support downstream data quality checks. Map: transaction_id->transaction_id, store_id->store_id, product_id->product_id, quantity->quantity, sale_amount->sale_amount, transaction_time->transaction_time.'
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
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'co.store_id = str.store_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'order_total_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'co.order_total_amount = str.sale_amount',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'order_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'co.order_timestamp = str.transaction_time',
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
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'coi.product_id = str.product_id',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_accepted',
            'transformation': 'coi.quantity = str.quantity',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'line_sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'coi.line_sale_amount = str.sale_amount',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'coi.transaction_time = str.transaction_time',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'coi.store_id = str.store_id',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'dodq.transaction_id = str.transaction_id',
            'target_table': 'dodq'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'dodq.store_id = str.store_id',
            'target_table': 'dodq'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'dodq.product_id = str.product_id',
            'target_table': 'dodq'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_accepted',
            'transformation': 'dodq.quantity = str.quantity',
            'target_table': 'dodq'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'dodq.sale_amount = str.sale_amount',
            'target_table': 'dodq'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'dodq.transaction_time = str.transaction_time',
            'target_table': 'dodq'
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

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table in metadata.get('tables', []):
    mapping_details = table.get('mapping_details', '')
    mapping_parts = mapping_details.split()
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else None
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else None
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            target_column = col_meta.get('target_column')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
