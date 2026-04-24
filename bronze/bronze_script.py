from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

# Metadata-driven configuration
metadata = {
    'tables': [
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw p',
            'description': 'Bronze products table sourced directly from products_raw. Columns: product_id, product_name, category, brand, price, is_active.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores_raw s',
            'description': 'Bronze stores table sourced directly from stores_raw. Columns: store_id, store_name, city, state, store_type, open_date.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'orders_bronze',
            'target_alias': 'ob',
            'mapping_details': 'sales_transactions_raw t',
            'description': 'Bronze orders table sourced directly from sales_transactions_raw at transaction grain. Columns: transaction_id (as order_id), store_id, transaction_time, sale_amount.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'order_items_bronze',
            'target_alias': 'oib',
            'mapping_details': 'sales_transactions_raw t',
            'description': 'Bronze order items table sourced directly from sales_transactions_raw at transaction-product line grain. Columns: transaction_id (as order_id), product_id, quantity, sale_amount.'
        }
    ],
    'columns': [
        {
            'source_column': "['p.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'pb.product_id = p.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'pb.product_name = p.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.category']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'pb.category = p.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.brand']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'pb.brand = p.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'pb.price = p.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'accepted',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'accepted',
            'transformation': 'pb.is_active = p.is_active',
            'target_table': 'pb'
        },
        {
            'source_column': "['s.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'sb.store_id = s.store_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['s.store_name']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_name',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.store_name = s.store_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['s.city']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'city',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.city = s.city',
            'target_table': 'sb'
        },
        {
            'source_column': "['s.state']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'state',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.state = s.state',
            'target_table': 'sb'
        },
        {
            'source_column': "['s.store_type']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_type',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.store_type = s.store_type',
            'target_table': 'sb'
        },
        {
            'source_column': "['s.open_date']",
            'source_type': 'DATE',
            'source_nullable': 'accepted',
            'target_column': 'open_date',
            'target_type': 'DATE',
            'target_nullable': 'accepted',
            'transformation': 'sb.open_date = s.open_date',
            'target_table': 'sb'
        },
        {
            'source_column': "['t.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'ob.order_id = t.transaction_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['t.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'ob.store_id = t.store_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['t.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'ob.transaction_time = t.transaction_time',
            'target_table': 'ob'
        },
        {
            'source_column': "['t.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'ob.sale_amount = t.sale_amount',
            'target_table': 'ob'
        },
        {
            'source_column': "['t.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'oib.order_id = t.transaction_id',
            'target_table': 'oib'
        },
        {
            'source_column': "['t.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'oib.product_id = t.product_id',
            'target_table': 'oib'
        },
        {
            'source_column': "['t.quantity']",
            'source_type': 'INT',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'accepted',
            'transformation': 'oib.quantity = t.quantity',
            'target_table': 'oib'
        },
        {
            'source_column': "['t.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'oib.sale_amount = t.sale_amount',
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
    parts = mapping_details.split()
    source_table = parts[0] if len(parts) > 0 else None
    source_alias = parts[1] if len(parts) > 1 else None
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            if '=' in transformation:
                rhs = transformation.split('=', 1)[1].strip()
            else:
                rhs = transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
