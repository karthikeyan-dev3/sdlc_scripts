
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
            'description': 'Bronze ingestion of raw product master data with no transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sales_transactions_bronze',
            'target_alias': 'stb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze ingestion of raw sales transaction events with no transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores_raw sr',
            'description': 'Bronze ingestion of raw store master data with no transformations, joins, or aggregations.'
        }
    ],
    'columns': [
        {
            'source_column': "['pb.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'pb.product_id = pr.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.product_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'product_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'pb.product_name = pr.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.category']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'category',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'pb.category = pr.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.brand']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'brand',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'pb.brand = pr.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.price']",
            'source_type': 'float',
            'source_nullable': 'not_specified',
            'target_column': 'price',
            'target_type': 'float',
            'target_nullable': 'not_specified',
            'transformation': 'pb.price = pr.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.is_active']",
            'source_type': 'boolean',
            'source_nullable': 'not_specified',
            'target_column': 'is_active',
            'target_type': 'boolean',
            'target_nullable': 'not_specified',
            'transformation': 'pb.is_active = pr.is_active',
            'target_table': 'pb'
        },
        {
            'source_column': "['stb.transaction_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'stb.transaction_id = str.transaction_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.store_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'store_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'stb.store_id = str.store_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.product_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'stb.product_id = str.product_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.quantity']",
            'source_type': 'int',
            'source_nullable': 'not_specified',
            'target_column': 'quantity',
            'target_type': 'int',
            'target_nullable': 'not_specified',
            'transformation': 'stb.quantity = str.quantity',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.sale_amount']",
            'source_type': 'double',
            'source_nullable': 'not_specified',
            'target_column': 'sale_amount',
            'target_type': 'double',
            'target_nullable': 'not_specified',
            'transformation': 'stb.sale_amount = str.sale_amount',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.transaction_time']",
            'source_type': 'timestamp',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_time',
            'target_type': 'timestamp',
            'target_nullable': 'not_specified',
            'transformation': 'stb.transaction_time = str.transaction_time',
            'target_table': 'stb'
        },
        {
            'source_column': "['sb.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'sb.store_id = sr.store_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'store_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'sb.store_name = sr.store_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.city']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'city',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'sb.city = sr.city',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.state']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'state',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'sb.state = sr.state',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_type']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not_specified',
            'target_column': 'store_type',
            'target_type': 'varchar(50)',
            'target_nullable': 'not_specified',
            'transformation': 'sb.store_type = sr.store_type',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.open_date']",
            'source_type': 'date',
            'source_nullable': 'not_specified',
            'target_column': 'open_date',
            'target_type': 'date',
            'target_nullable': 'not_specified',
            'transformation': 'sb.open_date = sr.open_date',
            'target_table': 'sb'
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

def _build_source_path(_base_path, _source_table, _read_format):
    if _base_path is None or _source_table is None or _read_format is None:
        return None
    if not str(_base_path).endswith('/'):
        _base_path = str(_base_path) + '/'
    return str(_base_path) + str(_source_table) + '.' + str(_read_format)

def _build_target_path(_target_path, _target_table, _write_format):
    if _target_path is None or _target_table is None or _write_format is None:
        return None
    if not str(_target_path).endswith('/'):
        _target_path = str(_target_path) + '/'
    return str(_target_path) + str(_target_table) + '.' + str(_write_format)

for table in metadata.get('tables', []):
    mapping_details = table.get('mapping_details')
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    source_table = None
    source_alias = None
    if mapping_details is not None:
        parts = str(mapping_details).split()
        if len(parts) >= 1:
            source_table = parts[0]
        if len(parts) >= 2:
            source_alias = parts[1]

    source_path = _build_source_path(base_path, source_table, read_format)

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(source_path)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation')
            target_column = col_meta.get('target_column')
            if transformation is not None and target_column is not None:
                rhs = str(transformation).split('=', 1)[1].strip() if '=' in str(transformation) else str(transformation).strip()
                transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    target_full_path = _build_target_path(target_path, target_table, write_format)
    writer.save(target_full_path)

job.commit()