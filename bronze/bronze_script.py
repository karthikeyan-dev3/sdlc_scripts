from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'products_bronze', 'target_alias': 'pb', 'mapping_details': 'products_raw pr', 'description': 'Bronze table for Product entity sourced directly from products_raw with columns: product_id, product_name, category, brand, price, is_active.'}, {'target_schema': 'bronze', 'target_table': 'stores_bronze', 'target_alias': 'sb', 'mapping_details': 'stores_raw sr', 'description': 'Bronze table for Store entity sourced directly from stores_raw with columns: store_id, store_name, city, state, store_type, open_date.'}, {'target_schema': 'bronze', 'target_table': 'sales_transactions_bronze', 'target_alias': 'stb', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze table for Sales Transactions entity sourced directly from sales_transactions_raw with columns: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'}], 'columns': [{'source_column': "['pb.product_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not specified', 'target_column': 'product_id', 'target_type': 'varchar(10)', 'target_nullable': 'not specified', 'transformation': 'pb.product_id = pr.product_id', 'target_table': 'pb'}, {'source_column': "['pb.product_name']", 'source_type': 'varchar(255)', 'source_nullable': 'not specified', 'target_column': 'product_name', 'target_type': 'varchar(255)', 'target_nullable': 'not specified', 'transformation': 'pb.product_name = pr.product_name', 'target_table': 'pb'}, {'source_column': "['pb.category']", 'source_type': 'varchar(100)', 'source_nullable': 'not specified', 'target_column': 'category', 'target_type': 'varchar(100)', 'target_nullable': 'not specified', 'transformation': 'pb.category = pr.category', 'target_table': 'pb'}, {'source_column': "['pb.price']", 'source_type': 'float', 'source_nullable': 'not specified', 'target_column': 'price', 'target_type': 'float', 'target_nullable': 'not specified', 'transformation': 'pb.price = pr.price', 'target_table': 'pb'}, {'source_column': "['sb.store_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not specified', 'target_column': 'store_id', 'target_type': 'varchar(10)', 'target_nullable': 'not specified', 'transformation': 'sb.store_id = sr.store_id', 'target_table': 'sb'}, {'source_column': "['sb.city', 'sb.state']", 'source_type': 'varchar(100)', 'source_nullable': 'not specified', 'target_column': 'store_location', 'target_type': 'varchar(201)', 'target_nullable': 'not specified', 'transformation': "sb.city || ', ' || sb.state = sr.city || ', ' || sr.state", 'target_table': 'sb'}, {'source_column': "['sb.store_type']", 'source_type': 'varchar(50)', 'source_nullable': 'not specified', 'target_column': 'store_type', 'target_type': 'varchar(50)', 'target_nullable': 'not specified', 'transformation': 'sb.store_type = sr.store_type', 'target_table': 'sb'}, {'source_column': "['stb.transaction_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not specified', 'target_column': 'transaction_id', 'target_type': 'varchar(10)', 'target_nullable': 'not specified', 'transformation': 'stb.transaction_id = str.transaction_id', 'target_table': 'stb'}, {'source_column': "['stb.product_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not specified', 'target_column': 'product_id', 'target_type': 'varchar(10)', 'target_nullable': 'not specified', 'transformation': 'stb.product_id = str.product_id', 'target_table': 'stb'}, {'source_column': "['stb.store_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not specified', 'target_column': 'store_id', 'target_type': 'varchar(10)', 'target_nullable': 'not specified', 'transformation': 'stb.store_id = str.store_id', 'target_table': 'stb'}, {'source_column': "['stb.sale_amount']", 'source_type': 'double', 'source_nullable': 'not specified', 'target_column': 'revenue', 'target_type': 'double', 'target_nullable': 'not specified', 'transformation': 'stb.sale_amount = str.sale_amount', 'target_table': 'stb'}, {'source_column': "['stb.transaction_time']", 'source_type': 'timestamp', 'source_nullable': 'not specified', 'target_column': 'transaction_date', 'target_type': 'date', 'target_nullable': 'not specified', 'transformation': 'cast(stb.transaction_time as date) = cast(str.transaction_time as date)', 'target_table': 'stb'}, {'source_column': "['stb.quantity']", 'source_type': 'int', 'source_nullable': 'not specified', 'target_column': 'quantity_sold', 'target_type': 'int', 'target_nullable': 'not specified', 'transformation': 'stb.quantity = str.quantity', 'target_table': 'stb'}, {'source_column': "['pb.category']", 'source_type': 'varchar(100)', 'source_nullable': 'not specified', 'target_column': 'category', 'target_type': 'varchar(100)', 'target_nullable': 'not specified', 'transformation': 'stb.product_id = pb.product_id', 'target_table': 'stb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata['runtime_config']
base_path = runtime_config['base_path']
target_path = runtime_config['target_path']
read_format = runtime_config['read_format']
write_format = runtime_config['write_format']
write_mode = runtime_config['write_mode']

for table in metadata['tables']:
    mapping_details = table['mapping_details']
    source_table, source_alias = mapping_details.split(" ", 1)
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}").alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            transformation = col_meta['transformation']
            rhs = transformation.split("=", 1)[1].strip()
            target_column = col_meta['target_column']
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()