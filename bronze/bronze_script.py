from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'sales_transactions_bronze', 'target_alias': 'stb', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze table for sales transaction entity (sales_transactions). Direct ingestion of raw sales transactions with columns: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'}, {'target_schema': 'bronze', 'target_table': 'product_details_bronze', 'target_alias': 'pdb', 'mapping_details': 'products_raw pr', 'description': 'Bronze table for product entity (product_details). Direct ingestion of raw product attributes with columns: product_id, product_name, category, brand, price, is_active.'}, {'target_schema': 'bronze', 'target_table': 'store_details_bronze', 'target_alias': 'sdb', 'mapping_details': 'stores_raw sr', 'description': 'Bronze table for store entity (store_details). Direct ingestion of raw store attributes with columns: store_id, store_name, city, state, store_type, open_date.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'stb.transaction_id = str.transaction_id', 'target_table': 'stb'}, {'source_column': "['str.store_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'store_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'stb.store_id = str.store_id', 'target_table': 'stb'}, {'source_column': "['str.product_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'product_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'stb.product_id = str.product_id', 'target_table': 'stb'}, {'source_column': "['str.quantity']", 'source_type': 'int', 'source_nullable': 'not_null', 'target_column': 'quantity', 'target_type': 'int', 'target_nullable': 'not_null', 'transformation': 'stb.quantity = str.quantity', 'target_table': 'stb'}, {'source_column': "['str.sale_amount']", 'source_type': 'double', 'source_nullable': 'not_null', 'target_column': 'sale_amount', 'target_type': 'double', 'target_nullable': 'not_null', 'transformation': 'stb.sale_amount = str.sale_amount', 'target_table': 'stb'}, {'source_column': "['str.transaction_time']", 'source_type': 'timestamp', 'source_nullable': 'not_null', 'target_column': 'transaction_time', 'target_type': 'timestamp', 'target_nullable': 'not_null', 'transformation': 'stb.transaction_time = str.transaction_time', 'target_table': 'stb'}, {'source_column': "['pr.product_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'product_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'pdb.product_id = pr.product_id', 'target_table': 'pdb'}, {'source_column': "['pr.product_name']", 'source_type': 'varchar(255)', 'source_nullable': 'not_null', 'target_column': 'product_name', 'target_type': 'varchar(255)', 'target_nullable': 'not_null', 'transformation': 'pdb.product_name = pr.product_name', 'target_table': 'pdb'}, {'source_column': "['pr.category']", 'source_type': 'varchar(100)', 'source_nullable': 'not_null', 'target_column': 'category', 'target_type': 'varchar(100)', 'target_nullable': 'not_null', 'transformation': 'pdb.category = pr.category', 'target_table': 'pdb'}, {'source_column': "['pr.brand']", 'source_type': 'varchar(100)', 'source_nullable': 'not_null', 'target_column': 'brand', 'target_type': 'varchar(100)', 'target_nullable': 'not_null', 'transformation': 'pdb.brand = pr.brand', 'target_table': 'pdb'}, {'source_column': "['pr.price']", 'source_type': 'float', 'source_nullable': 'not_null', 'target_column': 'price', 'target_type': 'float', 'target_nullable': 'not_null', 'transformation': 'pdb.price = pr.price', 'target_table': 'pdb'}, {'source_column': "['pr.is_active']", 'source_type': 'boolean', 'source_nullable': 'not_null', 'target_column': 'is_active', 'target_type': 'boolean', 'target_nullable': 'not_null', 'transformation': 'pdb.is_active = pr.is_active', 'target_table': 'pdb'}, {'source_column': "['sr.store_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'store_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'sdb.store_id = sr.store_id', 'target_table': 'sdb'}, {'source_column': "['sr.store_name']", 'source_type': 'varchar(255)', 'source_nullable': 'not_null', 'target_column': 'store_name', 'target_type': 'varchar(255)', 'target_nullable': 'not_null', 'transformation': 'sdb.store_name = sr.store_name', 'target_table': 'sdb'}, {'source_column': "['sr.city']", 'source_type': 'varchar(100)', 'source_nullable': 'not_null', 'target_column': 'city', 'target_type': 'varchar(100)', 'target_nullable': 'not_null', 'transformation': 'sdb.city = sr.city', 'target_table': 'sdb'}, {'source_column': "['sr.state']", 'source_type': 'varchar(100)', 'source_nullable': 'not_null', 'target_column': 'state', 'target_type': 'varchar(100)', 'target_nullable': 'not_null', 'transformation': 'sdb.state = sr.state', 'target_table': 'sdb'}, {'source_column': "['sr.store_type']", 'source_type': 'varchar(50)', 'source_nullable': 'not_null', 'target_column': 'store_type', 'target_type': 'varchar(50)', 'target_nullable': 'not_null', 'transformation': 'sdb.store_type = sr.store_type', 'target_table': 'sdb'}, {'source_column': "['sr.open_date']", 'source_type': 'date', 'source_nullable': 'not_null', 'target_column': 'open_date', 'target_type': 'date', 'target_nullable': 'not_null', 'transformation': 'sdb.open_date = sr.open_date', 'target_table': 'sdb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata['runtime_config']
base_path = runtime_config['base_path']
target_path = runtime_config['target_path']
read_format = runtime_config['read_format']
write_format = runtime_config['write_format']
write_mode = runtime_config['write_mode']

for table in metadata['tables']:
    mapping_details = table['mapping_details'].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col in metadata['columns']:
        if col['target_table'] == target_alias:
            rhs = col['transformation'].split('=', 1)[1].strip()
            target_column = col['target_column']
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()