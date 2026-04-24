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
        {
            'target_schema': 'bronze',
            'target_table': 'customer_orders_bronze',
            'target_alias': 'cob',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze table for customer_orders sourced directly from sales_transactions_raw. Columns mapped 1:1: transaction_id, store_id, sale_amount, transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'order_items_bronze',
            'target_alias': 'oib',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze table for order_items sourced directly from sales_transactions_raw. Columns mapped 1:1: transaction_id (as order/transaction reference), product_id, quantity.'
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'cob.transaction_id = str.transaction_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'cob.store_id = str.store_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_specified',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_specified',
            'transformation': 'cob.sale_amount = str.sale_amount',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_specified',
            'transformation': 'cob.transaction_time = str.transaction_time',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'oib.transaction_id = str.transaction_id',
            'target_table': 'oib'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'oib.product_id = str.product_id',
            'target_table': 'oib'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_specified',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_specified',
            'transformation': 'oib.quantity = str.quantity',
            'target_table': 'oib'
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

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table_meta in metadata.get('tables', []):
    mapping_details = table_meta.get('mapping_details', '').split()
    source_table = mapping_details[0] if len(mapping_details) > 0 else None
    source_alias = mapping_details[1] if len(mapping_details) > 1 else None

    target_table = table_meta.get('target_table')
    target_alias = table_meta.get('target_alias')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
