from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

# Metadata provided at runtime
metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'orders_bronze', 'target_alias': 'ob', 'mapping_details': 'sales_transactions_raw st', 'description': 'Bronze table for orders at transaction grain. Map transaction_id, store_id, sale_amount, transaction_time from sales_transactions_raw.'}, {'target_schema': 'bronze', 'target_table': 'order_items_bronze', 'target_alias': 'oib', 'mapping_details': 'sales_transactions_raw st', 'description': 'Bronze table for order items at transaction-product grain as provided by source. Map transaction_id (as order reference), product_id, quantity from sales_transactions_raw.'}, {'target_schema': 'bronze', 'target_table': 'orders_data_quality_daily_bronze', 'target_alias': 'odqdb', 'mapping_details': 'sales_transactions_raw st', 'description': 'Bronze data quality daily raw capture based on sales_transactions_raw. Store raw transaction fields needed for downstream DQ checks (transaction_id, store_id, product_id, quantity, sale_amount, transaction_time) without aggregation.'}], 'columns': [{'source_column': "['st.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'ob.transaction_id = st.transaction_id', 'target_table': 'ob'}, {'source_column': "['st.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'ob.store_id = st.store_id', 'target_table': 'ob'}, {'source_column': "['st.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'not_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'not_accepted', 'transformation': 'ob.sale_amount = st.sale_amount', 'target_table': 'ob'}, {'source_column': "['st.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'ob.transaction_time = st.transaction_time', 'target_table': 'ob'}, {'source_column': "['st.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'oib.transaction_id = st.transaction_id', 'target_table': 'oib'}, {'source_column': "['st.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'oib.product_id = st.product_id', 'target_table': 'oib'}, {'source_column': "['st.quantity']", 'source_type': 'INT', 'source_nullable': 'not_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'not_accepted', 'transformation': 'oib.quantity = st.quantity', 'target_table': 'oib'}, {'source_column': "['st.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'odqdb.transaction_id = st.transaction_id', 'target_table': 'odqdb'}, {'source_column': "['st.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'odqdb.store_id = st.store_id', 'target_table': 'odqdb'}, {'source_column': "['st.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'odqdb.product_id = st.product_id', 'target_table': 'odqdb'}, {'source_column': "['st.quantity']", 'source_type': 'INT', 'source_nullable': 'not_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'not_accepted', 'transformation': 'odqdb.quantity = st.quantity', 'target_table': 'odqdb'}, {'source_column': "['st.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'not_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'not_accepted', 'transformation': 'odqdb.sale_amount = st.sale_amount', 'target_table': 'odqdb'}, {'source_column': "['st.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'odqdb.transaction_time = st.transaction_time', 'target_table': 'odqdb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table in metadata.get('tables', []):
    mapping_details = table.get('mapping_details', '')
    mapping_parts = mapping_details.split()
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else None
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else None

    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
