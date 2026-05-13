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
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze ingestion of product master data at raw grain (one row per product_id). Direct mapping from products_raw without transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores_raw sr',
            'description': 'Bronze ingestion of store master data at raw grain (one row per store_id). Direct mapping from stores_raw without transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sales_transactions_bronze',
            'target_alias': 'stb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze ingestion of transaction fact data at raw grain (one row per transaction_id). Direct mapping from sales_transactions_raw without transformations, joins, or aggregations.'
        }
    ],
    'columns': [
        {
            'source_column': "['pb.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_provided',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_provided',
            'transformation': 'pr.product_id = pb.product_id',
            'target_table': 'pr'
        },
        {
            'source_column': "['pb.product_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'product_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'pr.product_name = pb.product_name',
            'target_table': 'pr'
        },
        {
            'source_column': "['pb.category']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_provided',
            'target_column': 'category',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_provided',
            'transformation': 'pr.category = pb.category',
            'target_table': 'pr'
        },
        {
            'source_column': "['pb.brand']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_provided',
            'target_column': 'brand',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_provided',
            'transformation': 'pr.brand = pb.brand',
            'target_table': 'pr'
        },
        {
            'source_column': "['pb.price']",
            'source_type': 'float',
            'source_nullable': 'not_provided',
            'target_column': 'price',
            'target_type': 'float',
            'target_nullable': 'not_provided',
            'transformation': 'pr.price = pb.price',
            'target_table': 'pr'
        },
        {
            'source_column': "['pb.is_active']",
            'source_type': 'boolean',
            'source_nullable': 'not_provided',
            'target_column': 'is_active',
            'target_type': 'boolean',
            'target_nullable': 'not_provided',
            'transformation': 'pr.is_active = pb.is_active',
            'target_table': 'pr'
        },
        {
            'source_column': "['sb.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_provided',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_provided',
            'transformation': 'sr.store_id = sb.store_id',
            'target_table': 'sr'
        },
        {
            'source_column': "['sb.store_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'store_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'sr.store_name = sb.store_name',
            'target_table': 'sr'
        },
        {
            'source_column': "['sb.city']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_provided',
            'target_column': 'city',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_provided',
            'transformation': 'sr.city = sb.city',
            'target_table': 'sr'
        },
        {
            'source_column': "['sb.state']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_provided',
            'target_column': 'state',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_provided',
            'transformation': 'sr.state = sb.state',
            'target_table': 'sr'
        },
        {
            'source_column': "['sb.store_type']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not_provided',
            'target_column': 'store_type',
            'target_type': 'varchar(50)',
            'target_nullable': 'not_provided',
            'transformation': 'sr.store_type = sb.store_type',
            'target_table': 'sr'
        },
        {
            'source_column': "['sb.open_date']",
            'source_type': 'date',
            'source_nullable': 'not_provided',
            'target_column': 'open_date',
            'target_type': 'date',
            'target_nullable': 'not_provided',
            'transformation': 'sr.open_date = sb.open_date',
            'target_table': 'sr'
        },
        {
            'source_column': "['stb.transaction_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_provided',
            'target_column': 'transaction_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_provided',
            'transformation': 'str.transaction_id = stb.transaction_id',
            'target_table': 'str'
        },
        {
            'source_column': "['stb.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_provided',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_provided',
            'transformation': 'str.store_id = stb.store_id',
            'target_table': 'str'
        },
        {
            'source_column': "['stb.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_provided',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_provided',
            'transformation': 'str.product_id = stb.product_id',
            'target_table': 'str'
        },
        {
            'source_column': "['stb.quantity']",
            'source_type': 'int',
            'source_nullable': 'not_provided',
            'target_column': 'quantity',
            'target_type': 'int',
            'target_nullable': 'not_provided',
            'transformation': 'str.quantity = stb.quantity',
            'target_table': 'str'
        },
        {
            'source_column': "['stb.sale_amount']",
            'source_type': 'double',
            'source_nullable': 'not_provided',
            'target_column': 'sale_amount',
            'target_type': 'double',
            'target_nullable': 'not_provided',
            'transformation': 'str.sale_amount = stb.sale_amount',
            'target_table': 'str'
        },
        {
            'source_column': "['stb.transaction_time']",
            'source_type': 'timestamp',
            'source_nullable': 'not_provided',
            'target_column': 'transaction_time',
            'target_type': 'timestamp',
            'target_nullable': 'not_provided',
            'transformation': 'str.transaction_time = stb.transaction_time',
            'target_table': 'str'
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

for table_meta in metadata.get('tables', []):
    mapping_details = table_meta.get('mapping_details', '').split()
    source_table = mapping_details[0] if len(mapping_details) > 0 else ''
    source_alias = mapping_details[1] if len(mapping_details) > 1 else ''

    target_table = table_meta.get('target_table', '')
    target_alias = table_meta.get('target_alias', '')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == source_alias:
            transformation = col_meta.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            target_column = col_meta.get('target_column', '')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()