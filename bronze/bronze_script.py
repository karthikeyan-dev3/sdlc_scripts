from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders_bronze', 'target_alias': 'cob', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze table for customer_orders mapped directly from sales_transactions_raw at transaction grain (one row per transaction). Columns: transaction_id, store_id, transaction_time, sale_amount.'}, {'target_schema': 'bronze', 'target_table': 'customer_order_items_bronze', 'target_alias': 'coib', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze table for customer_order_items mapped directly from sales_transactions_raw at transaction line-item grain using product_id and quantity. Columns: transaction_id, product_id, quantity.'}, {'target_schema': 'bronze', 'target_table': 'etl_data_quality_results_bronze', 'target_alias': 'dqrb', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze table for storing ETL data quality results/flags derived at ingestion time for sales_transactions_raw (no joins/aggregations). Intended columns include: source_table, record_id (transaction_id), check_name, check_status, check_message, detected_at.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'cob.transaction_id = str.transaction_id', 'target_table': 'cob'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'cob.store_id = str.store_id', 'target_table': 'cob'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'cob.transaction_time = str.transaction_time', 'target_table': 'cob'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'accepted', 'transformation': 'cob.sale_amount = str.sale_amount', 'target_table': 'cob'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coib.transaction_id = str.transaction_id', 'target_table': 'coib'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'coib.product_id = str.product_id', 'target_table': 'coib'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'accepted', 'transformation': 'coib.quantity = str.quantity', 'target_table': 'coib'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'not_accepted', 'target_column': 'source_table', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': "dqrb.source_table = 'sales_transactions_raw'", 'target_table': 'dqrb'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'record_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'dqrb.record_id = str.transaction_id', 'target_table': 'dqrb'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'not_accepted', 'target_column': 'check_name', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'dqrb.check_name = <ingestion_rule_name>', 'target_table': 'dqrb'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'not_accepted', 'target_column': 'check_status', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'dqrb.check_status = <PASS|FAIL>', 'target_table': 'dqrb'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'accepted', 'target_column': 'check_message', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'dqrb.check_message = <failure_or_info_message>', 'target_table': 'dqrb'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'not_accepted', 'target_column': 'detected_at', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'dqrb.detected_at = <ingestion_timestamp>', 'target_table': 'dqrb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

base_path = metadata["runtime_config"]["base_path"]
target_path = metadata["runtime_config"]["target_path"]
read_format = metadata["runtime_config"]["read_format"]
write_format = metadata["runtime_config"]["write_format"]
write_mode = metadata["runtime_config"]["write_mode"]

for table in metadata["tables"]:
    mapping_details = table["mapping_details"].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table["target_table"]
    target_alias = table["target_alias"]

    reader = spark.read.format(read_format)
    if read_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata["columns"]:
        if col_meta["target_table"] == target_alias:
            rhs = col_meta["transformation"].split("=", 1)[1].strip()
            target_col = col_meta["target_column"]
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == "csv":
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()