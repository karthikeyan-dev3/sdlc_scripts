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
            'mapping_details': 'products_raw p',
            'description': 'Direct ingestion of product master data from products_raw into bronze.products_bronze (no joins/aggregations). Columns: product_id, product_name, category, brand, price, is_active.'
        }
    ],
    'columns': [
        {
            'source_column': "['p.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.product_id = p.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.product_name = p.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.category']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.category = p.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.brand']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.brand = p.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_specified',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_specified',
            'transformation': 'pb.price = p.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'not_specified',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'not_specified',
            'transformation': 'pb.is_active = p.is_active',
            'target_table': 'pb'
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
    mapping_details = table['mapping_details'].strip().split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1] if len(mapping_details) > 1 else source_table

    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta['transformation']
            rhs = transformation.split('=', 1)[1].strip()
            target_column = col_meta['target_column']
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
