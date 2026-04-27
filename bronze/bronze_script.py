from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders_bronze', 'target_alias': 'cob', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze ingestion of customer order transactions from sales_transactions_raw. Maps: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time as-is (no joins/aggregations).'}, {'target_schema': 'bronze', 'target_table': 'customer_orders_dq_summary_bronze', 'target_alias': 'codq', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze landing table for data quality summary records for customer_orders. As no DQ metrics are available in the provided sources and joins/aggregations are not allowed, this table is sourced directly from sales_transactions_raw as a raw pass-through placeholder to support downstream DQ computation.'}, {'target_schema': 'bronze', 'target_table': 'customer_orders_sla_bronze', 'target_alias': 'cosla', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze landing table for SLA tracking related to customer_orders. As no SLA attributes are available in the provided sources and joins/aggregations are not allowed, this table is sourced directly from sales_transactions_raw as a raw pass-through placeholder to support downstream SLA derivation.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'cob.transaction_id = str.transaction_id', 'target_table': 'cob'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'cob.store_id = str.store_id', 'target_table': 'cob'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'cob.product_id = str.product_id', 'target_table': 'cob'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'null_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'null_accepted', 'transformation': 'cob.quantity = str.quantity', 'target_table': 'cob'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'null_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'null_accepted', 'transformation': 'cob.sale_amount = str.sale_amount', 'target_table': 'cob'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'null_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'null_accepted', 'transformation': 'cob.transaction_time = str.transaction_time', 'target_table': 'cob'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'codq.transaction_id = str.transaction_id', 'target_table': 'codq'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'codq.store_id = str.store_id', 'target_table': 'codq'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'codq.product_id = str.product_id', 'target_table': 'codq'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'null_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'null_accepted', 'transformation': 'codq.quantity = str.quantity', 'target_table': 'codq'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'null_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'null_accepted', 'transformation': 'codq.sale_amount = str.sale_amount', 'target_table': 'codq'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'null_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'null_accepted', 'transformation': 'codq.transaction_time = str.transaction_time', 'target_table': 'codq'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'cosla.transaction_id = str.transaction_id', 'target_table': 'cosla'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'cosla.store_id = str.store_id', 'target_table': 'cosla'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'cosla.product_id = str.product_id', 'target_table': 'cosla'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'null_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'null_accepted', 'transformation': 'cosla.quantity = str.quantity', 'target_table': 'cosla'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'null_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'null_accepted', 'transformation': 'cosla.sale_amount = str.sale_amount', 'target_table': 'cosla'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'null_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'null_accepted', 'transformation': 'cosla.transaction_time = str.transaction_time', 'target_table': 'cosla'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata["runtime_config"]
base_path = runtime_config["base_path"]
target_path = runtime_config["target_path"]
read_format = runtime_config["read_format"]
write_format = runtime_config["write_format"]
write_mode = runtime_config["write_mode"]

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
            transformation = col_meta["transformation"]
            rhs = transformation.split("=", 1)[1].strip()
            target_column = col_meta["target_column"]
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == "csv":
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
