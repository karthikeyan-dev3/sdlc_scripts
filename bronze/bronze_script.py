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
            'target_table': 'sales_transactions_bronze',
            'target_alias': 'stb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze ingestion of raw sales transactions from sales_transactions_raw. Includes transaction_id, store_id, product_id, quantity, sale_amount, transaction_time with no transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze ingestion of raw product master data from products_raw. Includes product_id, product_name, category, brand, price, is_active with no transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores_raw sr',
            'description': 'Bronze ingestion of raw store master data from stores_raw. Includes store_id, store_name, city, state, store_type, open_date with no transformations, joins, or aggregations.'
        }
    ],
    'columns': [
        {
            'source_column': "['stb.transaction_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'stb.transaction_id = stb.transaction_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.store_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'store_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'stb.store_id = stb.store_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.product_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'stb.product_id = stb.product_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.quantity']",
            'source_type': 'int',
            'source_nullable': 'not_specified',
            'target_column': 'quantity',
            'target_type': 'int',
            'target_nullable': 'not_specified',
            'transformation': 'stb.quantity = stb.quantity',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.sale_amount']",
            'source_type': 'double',
            'source_nullable': 'not_specified',
            'target_column': 'sale_amount',
            'target_type': 'double',
            'target_nullable': 'not_specified',
            'transformation': 'stb.sale_amount = stb.sale_amount',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.transaction_time']",
            'source_type': 'timestamp',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_time',
            'target_type': 'timestamp',
            'target_nullable': 'not_specified',
            'transformation': 'stb.transaction_time = stb.transaction_time',
            'target_table': 'stb'
        },
        {
            'source_column': "['pb.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'pb.product_id = pb.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.product_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'product_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'pb.product_name = pb.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.category']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'category',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'pb.category = pb.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.brand']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'brand',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'pb.brand = pb.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.price']",
            'source_type': 'float',
            'source_nullable': 'not_specified',
            'target_column': 'price',
            'target_type': 'float',
            'target_nullable': 'not_specified',
            'transformation': 'pb.price = pb.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.is_active']",
            'source_type': 'boolean',
            'source_nullable': 'not_specified',
            'target_column': 'is_active',
            'target_type': 'boolean',
            'target_nullable': 'not_specified',
            'transformation': 'pb.is_active = pb.is_active',
            'target_table': 'pb'
        },
        {
            'source_column': "['sb.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'sb.store_id = sb.store_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'store_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'sb.store_name = sb.store_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.city']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'city',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'sb.city = sb.city',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.state']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'state',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'sb.state = sb.state',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_type']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not_specified',
            'target_column': 'store_type',
            'target_type': 'varchar(50)',
            'target_nullable': 'not_specified',
            'transformation': 'sb.store_type = sb.store_type',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.open_date']",
            'source_type': 'date',
            'source_nullable': 'not_specified',
            'target_column': 'open_date',
            'target_type': 'date',
            'target_nullable': 'not_specified',
            'transformation': 'sb.open_date = sb.open_date',
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

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    mapping_details = table['mapping_details']
    source_table = mapping_details.split()[0]
    source_alias = mapping_details.split()[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            rhs = col_meta['transformation'].split('=', 1)[1].strip()
            target_col = col_meta['target_column']
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()