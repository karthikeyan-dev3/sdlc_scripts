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
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores_raw s',
            'description': 'Bronze ingestion of store master data. Maps: store_id, store_name, city, state, store_type, open_date from stores_raw.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw p',
            'description': 'Bronze ingestion of product master data. Maps: product_id, product_name, category, brand, price, is_active from products_raw.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sales_transactions_bronze',
            'target_alias': 'stb',
            'mapping_details': 'sales_transactions_raw t',
            'description': 'Bronze ingestion of sales transaction facts. Maps: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time from sales_transactions_raw.'
        }
    ],
    'columns': [
        {
            'source_column': "['sb.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'sb.store_id = s.store_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_name']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_name',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.store_name = s.store_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.city']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'city',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.city = s.city',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.state']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'state',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.state = s.state',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_type']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_type',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.store_type = s.store_type',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.open_date']",
            'source_type': 'DATE',
            'source_nullable': 'accepted',
            'target_column': 'open_date',
            'target_type': 'DATE',
            'target_nullable': 'accepted',
            'transformation': 'sb.open_date = s.open_date',
            'target_table': 'sb'
        },
        {
            'source_column': "['pb.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'pb.product_id = p.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'pb.product_name = p.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.category']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'pb.category = p.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.brand']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'pb.brand = p.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'pb.price = p.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'accepted',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'accepted',
            'transformation': 'pb.is_active = p.is_active',
            'target_table': 'pb'
        },
        {
            'source_column': "['stb.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'stb.transaction_id = t.transaction_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'stb.store_id = t.store_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'stb.product_id = t.product_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.quantity']",
            'source_type': 'INT',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'accepted',
            'transformation': 'stb.quantity = t.quantity',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'stb.sale_amount = t.sale_amount',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'accepted',
            'transformation': 'stb.transaction_time = t.transaction_time',
            'target_table': 'stb'
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
base_path = runtime_config.get('base_path', '')
target_path = runtime_config.get('target_path', '')
read_format = runtime_config.get('read_format', '')
write_format = runtime_config.get('write_format', '')
write_mode = runtime_config.get('write_mode', '')

for table in metadata.get('tables', []):
    mapping_details = table.get('mapping_details', '')
    mapping_parts = mapping_details.split()
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else ''
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else ''

    target_table = table.get('target_table', '')
    target_alias = table.get('target_alias', '')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + source_table + '.' + read_format)

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation
            target_column = col_meta.get('target_column', '')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + '.' + write_format)

job.commit()