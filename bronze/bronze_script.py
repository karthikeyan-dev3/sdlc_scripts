from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders', 'target_alias': 'co', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze table for customer orders created by directly ingesting sales transaction records. Maps 1:1 from sales_transactions_raw: transaction_id, store_id, sale_amount, transaction_time.'}, {'target_schema': 'bronze', 'target_table': 'customer_order_items', 'target_alias': 'coi', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze table for customer order line items created by directly ingesting sales transaction records at transaction-product granularity. Maps 1:1 from sales_transactions_raw: transaction_id, product_id, quantity, sale_amount, transaction_time.'}, {'target_schema': 'bronze', 'target_table': 'data_quality_daily_orders', 'target_alias': 'dqdo', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze table for daily order data quality inputs created by directly ingesting sales transaction records. Stores raw fields needed for downstream daily DQ checks without aggregation: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'co.transaction_id = str.transaction_id', 'target_table': 'co'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'co.store_id = str.store_id', 'target_table': 'co'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'null_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'null_accepted', 'transformation': 'co.sale_amount = str.sale_amount', 'target_table': 'co'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'null_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'null_accepted', 'transformation': 'co.transaction_time = str.transaction_time', 'target_table': 'co'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coi.transaction_id = str.transaction_id', 'target_table': 'coi'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'coi.product_id = str.product_id', 'target_table': 'coi'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'null_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'null_accepted', 'transformation': 'coi.quantity = str.quantity', 'target_table': 'coi'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'null_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'null_accepted', 'transformation': 'coi.sale_amount = str.sale_amount', 'target_table': 'coi'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'null_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'null_accepted', 'transformation': 'coi.transaction_time = str.transaction_time', 'target_table': 'coi'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'dqdo.transaction_id = str.transaction_id', 'target_table': 'dqdo'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'dqdo.store_id = str.store_id', 'target_table': 'dqdo'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'dqdo.product_id = str.product_id', 'target_table': 'dqdo'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'null_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'null_accepted', 'transformation': 'dqdo.quantity = str.quantity', 'target_table': 'dqdo'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'null_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'null_accepted', 'transformation': 'dqdo.sale_amount = str.sale_amount', 'target_table': 'dqdo'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'null_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'null_accepted', 'transformation': 'dqdo.transaction_time = str.transaction_time', 'target_table': 'dqdo'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

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

    df = reader.load(base_path + f"{source_table}.{read_format}")
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

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
