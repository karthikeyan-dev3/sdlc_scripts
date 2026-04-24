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
            'target_table': 'product_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze products table created directly from products_raw. Columns: product_id, product_name, category, brand, price, is_active.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'order_bronze',
            'target_alias': 'ob',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze orders table created directly from sales_transactions_raw (treating each transaction as an order record). Columns: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        }
    ],
    'columns': [
        {
            'source_column': "['pr.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.product_id = pr.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.product_name = pr.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.category']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.category = pr.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.brand']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.brand = pr.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_specified',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_specified',
            'transformation': 'pb.price = pr.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'not_specified',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'not_specified',
            'transformation': 'pb.is_active = pr.is_active',
            'target_table': 'pb'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'ob.transaction_id = str.transaction_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'ob.store_id = str.store_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'ob.product_id = str.product_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_specified',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_specified',
            'transformation': 'ob.quantity = str.quantity',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_specified',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_specified',
            'transformation': 'ob.sale_amount = str.sale_amount',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_specified',
            'transformation': 'ob.transaction_time = str.transaction_time',
            'target_table': 'ob'
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
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
