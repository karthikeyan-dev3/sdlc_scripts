from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'sales_transactions_bronze', 'target_alias': 'stb', 'mapping_details': 'sales_transactions_raw', 'description': 'Bronze table for raw sales transactions; direct 1:1 ingestion of transaction_id, store_id, product_id, quantity, sale_amount, transaction_time from sales_transactions_raw with no transformations.'}, {'target_schema': 'bronze', 'target_table': 'products_bronze', 'target_alias': 'pb', 'mapping_details': 'products_raw', 'description': 'Bronze table for raw products master data; direct 1:1 ingestion of product_id, product_name, category, brand, price, is_active from products_raw with no transformations.'}, {'target_schema': 'bronze', 'target_table': 'stores_bronze', 'target_alias': 'sb', 'mapping_details': 'stores_raw', 'description': 'Bronze table for raw stores master data; direct 1:1 ingestion of store_id, store_name, city, state, store_type, open_date from stores_raw with no transformations.'}], 'columns': [{'source_column': "['stb.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'stb.transaction_id = sales_transactions_raw.transaction_id', 'target_table': 'stb'}, {'source_column': "['stb.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'stb.store_id = sales_transactions_raw.store_id', 'target_table': 'stb'}, {'source_column': "['stb.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'stb.product_id = sales_transactions_raw.product_id', 'target_table': 'stb'}, {'source_column': "['stb.quantity']", 'source_type': 'INT', 'source_nullable': 'not_provided', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'not_provided', 'transformation': 'stb.quantity = sales_transactions_raw.quantity', 'target_table': 'stb'}, {'source_column': "['stb.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'not_provided', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'not_provided', 'transformation': 'stb.sale_amount = sales_transactions_raw.sale_amount', 'target_table': 'stb'}, {'source_column': "['stb.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_provided', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_provided', 'transformation': 'stb.transaction_time = sales_transactions_raw.transaction_time', 'target_table': 'stb'}, {'source_column': "['pb.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'pb.product_id = products_raw.product_id', 'target_table': 'pb'}, {'source_column': "['pb.product_name']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'product_name', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'pb.product_name = products_raw.product_name', 'target_table': 'pb'}, {'source_column': "['pb.category']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'category', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'pb.category = products_raw.category', 'target_table': 'pb'}, {'source_column': "['pb.brand']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'brand', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'pb.brand = products_raw.brand', 'target_table': 'pb'}, {'source_column': "['pb.price']", 'source_type': 'DECIMAL', 'source_nullable': 'not_provided', 'target_column': 'price', 'target_type': 'DECIMAL', 'target_nullable': 'not_provided', 'transformation': 'pb.price = products_raw.price', 'target_table': 'pb'}, {'source_column': "['pb.is_active']", 'source_type': 'BOOLEAN', 'source_nullable': 'not_provided', 'target_column': 'is_active', 'target_type': 'BOOLEAN', 'target_nullable': 'not_provided', 'transformation': 'pb.is_active = products_raw.is_active', 'target_table': 'pb'}, {'source_column': "['sb.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'sb.store_id = stores_raw.store_id', 'target_table': 'sb'}, {'source_column': "['sb.store_name']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'store_name', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'sb.store_name = stores_raw.store_name', 'target_table': 'sb'}, {'source_column': "['sb.city']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'city', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'sb.city = stores_raw.city', 'target_table': 'sb'}, {'source_column': "['sb.state']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'state', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'sb.state = stores_raw.state', 'target_table': 'sb'}, {'source_column': "['sb.store_type']", 'source_type': 'STRING', 'source_nullable': 'not_provided', 'target_column': 'store_type', 'target_type': 'STRING', 'target_nullable': 'not_provided', 'transformation': 'sb.store_type = stores_raw.store_type', 'target_table': 'sb'}, {'source_column': "['sb.open_date']", 'source_type': 'DATE', 'source_nullable': 'not_provided', 'target_column': 'open_date', 'target_type': 'DATE', 'target_nullable': 'not_provided', 'transformation': 'sb.open_date = stores_raw.open_date', 'target_table': 'sb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table in metadata.get('tables', []):
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    mapping_details = table.get('mapping_details')
    source_table = mapping_details
    source_alias = target_alias

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            target_column = col_meta.get('target_column')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
