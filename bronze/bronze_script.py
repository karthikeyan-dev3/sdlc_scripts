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
            'target_table': 'orders_bronze',
            'target_alias': 'ob',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Create one order record per transaction from sales_transactions_raw. Map: order_id=transaction_id, store_id=store_id, order_time=transaction_time, order_total_amount=sale_amount.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'order_lines_bronze',
            'target_alias': 'olb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Create one order line per transaction from sales_transactions_raw. Map: order_id=transaction_id, product_id=product_id, quantity=quantity, line_amount=sale_amount.'
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ob.order_id = str.transaction_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ob.store_id = str.store_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_null',
            'target_column': 'order_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_null',
            'transformation': 'ob.order_time = str.transaction_time',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_null',
            'target_column': 'order_total_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_null',
            'transformation': 'ob.order_total_amount = str.sale_amount',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'olb.order_id = str.transaction_id',
            'target_table': 'olb'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'olb.product_id = str.product_id',
            'target_table': 'olb'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_null',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_null',
            'transformation': 'olb.quantity = str.quantity',
            'target_table': 'olb'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_null',
            'target_column': 'line_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_null',
            'transformation': 'olb.line_amount = str.sale_amount',
            'target_table': 'olb'
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
    mapping_details = table.get('mapping_details', '')
    parts = mapping_details.split()
    source_table = parts[0] if len(parts) > 0 else None
    source_alias = parts[1] if len(parts) > 1 else None

    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(f"{base_path}{source_table}.{read_format}")
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

    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()