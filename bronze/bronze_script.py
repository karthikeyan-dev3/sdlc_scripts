from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders', 'target_alias': 'co', 'mapping_details': 'sales_transactions_raw str', 'description': 'Create bronze customer_orders from sales_transactions_raw at transaction grain. Map: order_id=transaction_id, store_id=store_id, order_total_amount=sale_amount, order_time=transaction_time.'}, {'target_schema': 'bronze', 'target_table': 'customer_order_items', 'target_alias': 'coi', 'mapping_details': 'sales_transactions_raw str', 'description': 'Create bronze customer_order_items from sales_transactions_raw at transaction-product line grain as provided by source. Map: order_id=transaction_id, product_id=product_id, quantity=quantity, line_amount=sale_amount.'}, {'target_schema': 'bronze', 'target_table': 'ingestion_audit_daily', 'target_alias': 'iad', 'mapping_details': 'sales_transactions_raw str; products_raw pr; stores_raw sr', 'description': 'Create bronze ingestion_audit_daily capturing daily ingestion metadata per source table. Fields to capture include: source_table_name, ingestion_date, record_count, load_timestamp.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'co.order_id = str.transaction_id', 'target_table': 'co'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'co.store_id = str.store_id', 'target_table': 'co'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'not_null', 'target_column': 'order_total_amount', 'target_type': 'DECIMAL', 'target_nullable': 'not_null', 'transformation': 'co.order_total_amount = str.sale_amount', 'target_table': 'co'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_null', 'target_column': 'order_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_null', 'transformation': 'co.order_time = str.transaction_time', 'target_table': 'co'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'coi.order_id = str.transaction_id', 'target_table': 'coi'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'coi.product_id = str.product_id', 'target_table': 'coi'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'not_null', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'not_null', 'transformation': 'coi.quantity = str.quantity', 'target_table': 'coi'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'not_null', 'target_column': 'line_amount', 'target_type': 'DECIMAL', 'target_nullable': 'not_null', 'transformation': 'coi.line_amount = str.sale_amount', 'target_table': 'coi'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_null', 'target_column': 'ingestion_date', 'target_type': 'DATE', 'target_nullable': 'not_null', 'transformation': 'iad.ingestion_date = CAST(str.transaction_time AS DATE)', 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'source_table_name', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': "iad.source_table_name = 'sales_transactions_raw'", 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'BIGINT', 'source_nullable': 'not_null', 'target_column': 'record_count', 'target_type': 'BIGINT', 'target_nullable': 'not_null', 'transformation': 'iad.record_count = COUNT(1) FROM sales_transactions_raw str GROUP BY CAST(str.transaction_time AS DATE)', 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'TIMESTAMP', 'source_nullable': 'not_null', 'target_column': 'load_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_null', 'transformation': 'iad.load_timestamp = CURRENT_TIMESTAMP', 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'DATE', 'source_nullable': 'not_null', 'target_column': 'ingestion_date', 'target_type': 'DATE', 'target_nullable': 'not_null', 'transformation': 'iad.ingestion_date = CURRENT_DATE', 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'source_table_name', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': "iad.source_table_name = 'products_raw'", 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'BIGINT', 'source_nullable': 'not_null', 'target_column': 'record_count', 'target_type': 'BIGINT', 'target_nullable': 'not_null', 'transformation': 'iad.record_count = COUNT(1) FROM products_raw pr', 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'TIMESTAMP', 'source_nullable': 'not_null', 'target_column': 'load_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_null', 'transformation': 'iad.load_timestamp = CURRENT_TIMESTAMP', 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'DATE', 'source_nullable': 'not_null', 'target_column': 'ingestion_date', 'target_type': 'DATE', 'target_nullable': 'not_null', 'transformation': 'iad.ingestion_date = CURRENT_DATE', 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'source_table_name', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': "iad.source_table_name = 'stores_raw'", 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'BIGINT', 'source_nullable': 'not_null', 'target_column': 'record_count', 'target_type': 'BIGINT', 'target_nullable': 'not_null', 'transformation': 'iad.record_count = COUNT(1) FROM stores_raw sr', 'target_table': 'iad'}, {'source_column': '[]', 'source_type': 'TIMESTAMP', 'source_nullable': 'not_null', 'target_column': 'load_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_null', 'transformation': 'iad.load_timestamp = CURRENT_TIMESTAMP', 'target_table': 'iad'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata['runtime_config']
base_path = runtime_config['base_path']
target_path = runtime_config['target_path']
read_format = runtime_config['read_format']
write_format = runtime_config['write_format']
write_mode = runtime_config['write_mode']

for table in metadata['tables']:
    target_table = table['target_table']

    mapping_details = table['mapping_details']
    first_mapping = mapping_details.split(';')[0].strip()
    source_table = first_mapping.split()[0].strip()
    source_alias = first_mapping.split()[1].strip()

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(f"{base_path}{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col in metadata['columns']:
        if col['target_table'] == table['target_alias']:
            rhs = col['transformation'].split('=', 1)[1].strip()
            target_col = col['target_column']
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()
