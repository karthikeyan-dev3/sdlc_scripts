from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'stores_bronze', 'target_alias': 'sb', 'mapping_details': 'stores_raw sr', 'description': 'Bronze table for store master data. Direct copy of stores_raw (store_id, store_name, city, state, store_type, open_date).'}, {'target_schema': 'bronze', 'target_table': 'products_bronze', 'target_alias': 'pb', 'mapping_details': 'products_raw pr', 'description': 'Bronze table for product master data. Direct copy of products_raw (product_id, product_name, category, brand, price, is_active).'}], 'columns': [{'source_column': "['sr.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'sr.store_id', 'target_table': 'sr'}, {'source_column': "['sr.store_name']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'store_name', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'sr.store_name', 'target_table': 'sr'}, {'source_column': "['sr.city']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'city', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'sr.city', 'target_table': 'sr'}, {'source_column': "['sr.state']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'state', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'sr.state', 'target_table': 'sr'}, {'source_column': "['sr.store_type']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'store_type', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'sr.store_type', 'target_table': 'sr'}, {'source_column': "['sr.open_date']", 'source_type': 'DATE', 'source_nullable': 'null_accepted', 'target_column': 'open_date', 'target_type': 'DATE', 'target_nullable': 'null_accepted', 'transformation': 'sr.open_date', 'target_table': 'sr'}, {'source_column': "['pr.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'pr.product_id', 'target_table': 'pr'}, {'source_column': "['pr.product_name']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'product_name', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'pr.product_name', 'target_table': 'pr'}, {'source_column': "['pr.category']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'category', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'pr.category', 'target_table': 'pr'}, {'source_column': "['pr.brand']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'brand', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'pr.brand', 'target_table': 'pr'}, {'source_column': "['pr.price']", 'source_type': 'DECIMAL', 'source_nullable': 'null_accepted', 'target_column': 'price', 'target_type': 'DECIMAL', 'target_nullable': 'null_accepted', 'transformation': 'pr.price', 'target_table': 'pr'}, {'source_column': "['pr.is_active']", 'source_type': 'BOOLEAN', 'source_nullable': 'null_accepted', 'target_column': 'is_active', 'target_type': 'BOOLEAN', 'target_nullable': 'null_accepted', 'transformation': 'pr.is_active', 'target_table': 'pr'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table in metadata.get('tables', []):
    mapping_details = table.get('mapping_details')
    target_table = table.get('target_table')

    source_table, source_alias = mapping_details.split()

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == source_alias:
            transformation = col_meta.get('transformation', '')
            rhs = transformation.split('=', 1)[-1].strip() if '=' in transformation else transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()