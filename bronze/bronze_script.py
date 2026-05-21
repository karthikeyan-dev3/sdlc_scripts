from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'products_bronze', 'target_alias': 'pb', 'mapping_details': 'SELECT product_id, product_name, category, brand, price, is_active FROM products_raw', 'description': 'Bronze ingestion of raw product records from products_raw with columns: product_id, product_name, category, brand, price, is_active.'}, {'target_schema': 'bronze', 'target_table': 'stores_bronze', 'target_alias': 'sb', 'mapping_details': 'SELECT store_id, store_name, city, state, store_type, open_date FROM stores_raw', 'description': 'Bronze ingestion of raw store records from stores_raw with columns: store_id, store_name, city, state, store_type, open_date.'}, {'target_schema': 'bronze', 'target_table': 'sales_bronze', 'target_alias': 'slb', 'mapping_details': 'SELECT transaction_id, store_id, product_id, quantity, sale_amount, transaction_time FROM sales_transactions_raw', 'description': 'Bronze ingestion of raw sales transactions from sales_transactions_raw with columns: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'}], 'columns': [{'source_column': "['pb.product_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'product_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'pb.product_id = pb.product_id', 'target_table': 'pb'}, {'source_column': "['pb.product_name']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'product_name', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'pb.product_name = pb.product_name', 'target_table': 'pb'}, {'source_column': "['pb.category']", 'source_type': 'varchar(100)', 'source_nullable': 'null_accepted', 'target_column': 'category', 'target_type': 'varchar(100)', 'target_nullable': 'null_accepted', 'transformation': 'pb.category = pb.category', 'target_table': 'pb'}, {'source_column': "['pb.brand']", 'source_type': 'varchar(100)', 'source_nullable': 'null_accepted', 'target_column': 'brand', 'target_type': 'varchar(100)', 'target_nullable': 'null_accepted', 'transformation': 'pb.brand = pb.brand', 'target_table': 'pb'}, {'source_column': "['pb.price']", 'source_type': 'float', 'source_nullable': 'null_accepted', 'target_column': 'price', 'target_type': 'float', 'target_nullable': 'null_accepted', 'transformation': 'pb.price = pb.price', 'target_table': 'pb'}, {'source_column': "['pb.is_active']", 'source_type': 'boolean', 'source_nullable': 'null_accepted', 'target_column': 'is_active', 'target_type': 'boolean', 'target_nullable': 'null_accepted', 'transformation': 'pb.is_active = pb.is_active', 'target_table': 'pb'}, {'source_column': "['sb.store_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'store_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'sb.store_id = sb.store_id', 'target_table': 'sb'}, {'source_column': "['sb.store_name']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'store_name', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'sb.store_name = sb.store_name', 'target_table': 'sb'}, {'source_column': "['sb.city']", 'source_type': 'varchar(100)', 'source_nullable': 'null_accepted', 'target_column': 'city', 'target_type': 'varchar(100)', 'target_nullable': 'null_accepted', 'transformation': 'sb.city = sb.city', 'target_table': 'sb'}, {'source_column': "['sb.state']", 'source_type': 'varchar(100)', 'source_nullable': 'null_accepted', 'target_column': 'state', 'target_type': 'varchar(100)', 'target_nullable': 'null_accepted', 'transformation': 'sb.state = sb.state', 'target_table': 'sb'}, {'source_column': "['sb.store_type']", 'source_type': 'varchar(50)', 'source_nullable': 'null_accepted', 'target_column': 'store_type', 'target_type': 'varchar(50)', 'target_nullable': 'null_accepted', 'transformation': 'sb.store_type = sb.store_type', 'target_table': 'sb'}, {'source_column': "['sb.open_date']", 'source_type': 'date', 'source_nullable': 'null_accepted', 'target_column': 'open_date', 'target_type': 'date', 'target_nullable': 'null_accepted', 'transformation': 'sb.open_date = sb.open_date', 'target_table': 'sb'}, {'source_column': "['slb.transaction_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_null', 'transformation': 'slb.transaction_id = slb.transaction_id', 'target_table': 'slb'}, {'source_column': "['slb.store_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_null', 'target_column': 'store_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_null', 'transformation': 'slb.store_id = slb.store_id', 'target_table': 'slb'}, {'source_column': "['slb.product_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_null', 'target_column': 'product_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_null', 'transformation': 'slb.product_id = slb.product_id', 'target_table': 'slb'}, {'source_column': "['slb.quantity']", 'source_type': 'int', 'source_nullable': 'null_accepted', 'target_column': 'quantity', 'target_type': 'int', 'target_nullable': 'null_accepted', 'transformation': 'slb.quantity = slb.quantity', 'target_table': 'slb'}, {'source_column': "['slb.sale_amount']", 'source_type': 'double', 'source_nullable': 'null_accepted', 'target_column': 'sale_amount', 'target_type': 'double', 'target_nullable': 'null_accepted', 'transformation': 'slb.sale_amount = slb.sale_amount', 'target_table': 'slb'}, {'source_column': "['slb.transaction_time']", 'source_type': 'timestamp', 'source_nullable': 'null_accepted', 'target_column': 'transaction_time', 'target_type': 'timestamp', 'target_nullable': 'null_accepted', 'transformation': 'slb.transaction_time = slb.transaction_time', 'target_table': 'slb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table_meta in metadata.get('tables', []):
    target_table = table_meta.get('target_table')
    target_alias = table_meta.get('target_alias')
    mapping_details = table_meta.get('mapping_details', '')

    mapping_upper = mapping_details.upper()
    source_table = mapping_details
    if ' FROM ' in mapping_upper:
        source_table = mapping_details[mapping_upper.rfind(' FROM ') + len(' FROM '):].strip()

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + source_table + '.' + read_format)
    df = df.alias(target_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            target_column = col_meta.get('target_column')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + '.' + write_format)

job.commit()
