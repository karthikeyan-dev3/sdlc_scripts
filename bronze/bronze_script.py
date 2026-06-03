from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {
    'tables': [
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze ingestion of products from products_raw. Columns mapped 1:1: product_id, product_name, category, brand, price, is_active.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores_raw sr',
            'description': 'Bronze ingestion of stores from stores_raw. Columns mapped 1:1: store_id, store_name, city, state, store_type, open_date.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sales_transactions_bronze',
            'target_alias': 'stb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze ingestion of sales transactions from sales_transactions_raw. Columns mapped 1:1: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        }
    ],
    'columns': [
        {
            'source_column': "['pb.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not accepted',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not accepted',
            'transformation': 'pb.product_id = pr.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.product_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'product_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'pb.product_name = pr.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.category']",
            'source_type': 'varchar(100)',
            'source_nullable': 'accepted',
            'target_column': 'category',
            'target_type': 'varchar(100)',
            'target_nullable': 'accepted',
            'transformation': 'pb.category = pr.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.brand']",
            'source_type': 'varchar(100)',
            'source_nullable': 'accepted',
            'target_column': 'brand',
            'target_type': 'varchar(100)',
            'target_nullable': 'accepted',
            'transformation': 'pb.brand = pr.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.price']",
            'source_type': 'float',
            'source_nullable': 'accepted',
            'target_column': 'price',
            'target_type': 'float',
            'target_nullable': 'accepted',
            'transformation': 'pb.price = pr.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.is_active']",
            'source_type': 'boolean',
            'source_nullable': 'accepted',
            'target_column': 'is_active',
            'target_type': 'boolean',
            'target_nullable': 'accepted',
            'transformation': 'pb.is_active = pr.is_active',
            'target_table': 'pb'
        },
        {
            'source_column': "['sb.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not accepted',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not accepted',
            'transformation': 'sb.store_id = sr.store_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'store_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'sb.store_name = sr.store_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.city']",
            'source_type': 'varchar(100)',
            'source_nullable': 'accepted',
            'target_column': 'city',
            'target_type': 'varchar(100)',
            'target_nullable': 'accepted',
            'transformation': 'sb.city = sr.city',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.state']",
            'source_type': 'varchar(100)',
            'source_nullable': 'accepted',
            'target_column': 'state',
            'target_type': 'varchar(100)',
            'target_nullable': 'accepted',
            'transformation': 'sb.state = sr.state',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_type']",
            'source_type': 'varchar(50)',
            'source_nullable': 'accepted',
            'target_column': 'store_type',
            'target_type': 'varchar(50)',
            'target_nullable': 'accepted',
            'transformation': 'sb.store_type = sr.store_type',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.open_date']",
            'source_type': 'date',
            'source_nullable': 'accepted',
            'target_column': 'open_date',
            'target_type': 'date',
            'target_nullable': 'accepted',
            'transformation': 'sb.open_date = sr.open_date',
            'target_table': 'sb'
        },
        {
            'source_column': "['stb.transaction_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not accepted',
            'target_column': 'transaction_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not accepted',
            'transformation': 'stb.transaction_id = str.transaction_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not accepted',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not accepted',
            'transformation': 'stb.store_id = str.store_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not accepted',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not accepted',
            'transformation': 'stb.product_id = str.product_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.quantity']",
            'source_type': 'int',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'int',
            'target_nullable': 'accepted',
            'transformation': 'stb.quantity = str.quantity',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.sale_amount']",
            'source_type': 'double',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'double',
            'target_nullable': 'accepted',
            'transformation': 'stb.sale_amount = str.sale_amount',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.transaction_time']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'stb.transaction_time = str.transaction_time',
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
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

def _join_path(base, name, fmt):
    if base is None:
        base = ''
    if not base.endswith('/'):
        base = base + '/'
    return base + f"{name}.{fmt}"

for table_meta in metadata.get('tables', []):
    mapping_details = table_meta.get('mapping_details', '')
    parts = mapping_details.split()
    source_table = parts[0] if len(parts) > 0 else None
    source_alias = parts[1] if len(parts) > 1 else None

    target_table = table_meta.get('target_table')
    target_alias = table_meta.get('target_alias')

    read_path = _join_path(base_path, source_table, read_format)

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')
    df = reader.load(read_path)

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') != target_alias:
            continue
        transformation = col_meta.get('transformation', '')
        rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
        target_column = col_meta.get('target_column')
        transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    write_path = _join_path(target_path, target_table, write_format)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')
    writer.save(write_path)

job.commit()
