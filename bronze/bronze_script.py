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
            'target_table': 'customer_orders',
            'target_alias': 'co',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze raw landing for customer_orders sourced directly from sales_transactions_raw with 1:1 column capture: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_orders_daily',
            'target_alias': 'cod',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze raw landing for customer_orders_daily sourced directly from sales_transactions_raw with 1:1 column capture (no aggregations): transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_provided',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_provided',
            'transformation': 'co.transaction_id = str.transaction_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_provided',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_provided',
            'transformation': 'co.store_id = str.store_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_provided',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_provided',
            'transformation': 'co.product_id = str.product_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_provided',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_provided',
            'transformation': 'co.quantity = str.quantity',
            'target_table': 'co'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_provided',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_provided',
            'transformation': 'co.sale_amount = str.sale_amount',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_provided',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_provided',
            'transformation': 'co.transaction_time = str.transaction_time',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_provided',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_provided',
            'transformation': 'cod.transaction_id = str.transaction_id',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_provided',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_provided',
            'transformation': 'cod.store_id = str.store_id',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_provided',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_provided',
            'transformation': 'cod.product_id = str.product_id',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_provided',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_provided',
            'transformation': 'cod.quantity = str.quantity',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_provided',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_provided',
            'transformation': 'cod.sale_amount = str.sale_amount',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_provided',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_provided',
            'transformation': 'cod.transaction_time = str.transaction_time',
            'target_table': 'cod'
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

for table in metadata.get('tables', []):
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    mapping_details = table.get('mapping_details')
    source_table = None
    source_alias = None
    if mapping_details:
        parts = mapping_details.split()
        if len(parts) >= 2:
            source_table = parts[0]
            source_alias = parts[1]

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(f"{base_path}{source_table}.{read_format}")

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            if '=' in transformation:
                rhs = transformation.split('=', 1)[1].strip()
                target_column = col_meta.get('target_column')
                transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()
