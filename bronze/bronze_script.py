from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders', 'target_alias': 'co', 'mapping_details': 'sales_transactions_raw str', 'description': 'Raw customer order/transaction records ingested from sales_transactions_raw with 1:1 column mapping: transaction_id, store_id, transaction_time, sale_amount, quantity.'}, {'target_schema': 'bronze', 'target_table': 'order_items', 'target_alias': 'oi', 'mapping_details': 'sales_transactions_raw str', 'description': 'Raw order line items ingested from sales_transactions_raw with 1:1 column mapping: transaction_id, product_id, quantity, sale_amount, transaction_time, store_id (as received).'}, {'target_schema': 'bronze', 'target_table': 'etl_run_log', 'target_alias': 'erl', 'mapping_details': 'nan', 'description': 'ETL operational run logging table created in bronze; populated by the ingestion framework (not sourced from provided raw source tables).'}, {'target_schema': 'bronze', 'target_table': 'data_quality_results', 'target_alias': 'dqr', 'mapping_details': 'nan', 'description': 'Data quality checks results table created in bronze; populated by DQ framework validations executed on bronze ingested data (not sourced from provided raw source tables).'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'co.transaction_id = str.transaction_id', 'target_table': 'co'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'co.store_id = str.store_id', 'target_table': 'co'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'co.transaction_time = str.transaction_time', 'target_table': 'co'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'accepted', 'transformation': 'co.sale_amount = str.sale_amount', 'target_table': 'co'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'accepted', 'transformation': 'co.quantity = str.quantity', 'target_table': 'co'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'oi.transaction_id = str.transaction_id', 'target_table': 'oi'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'oi.product_id = str.product_id', 'target_table': 'oi'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'accepted', 'transformation': 'oi.quantity = str.quantity', 'target_table': 'oi'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'accepted', 'transformation': 'oi.sale_amount = str.sale_amount', 'target_table': 'oi'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'oi.transaction_time = str.transaction_time', 'target_table': 'oi'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'oi.store_id = str.store_id', 'target_table': 'oi'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    mapping_details = table.get('mapping_details')
    if mapping_details is None or str(mapping_details).lower() == 'nan':
        continue

    parts = str(mapping_details).split()
    if len(parts) < 2:
        continue

    source_table = parts[0]
    source_alias = parts[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta.get('target_table') != target_alias:
            continue
        transformation = col_meta.get('transformation', '')
        if '=' not in transformation:
            continue
        rhs = transformation.split('=', 1)[1].strip()
        target_column = col_meta['target_column']
        transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
