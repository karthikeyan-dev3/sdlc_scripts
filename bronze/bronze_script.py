from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders_bronze', 'target_alias': 'cob', 'mapping_details': 'sales_transactions_raw str', 'description': 'Create one row per transaction as a customer order. Map: cob.order_id = str.transaction_id; cob.store_id = str.store_id; cob.order_timestamp = str.transaction_time; cob.order_total_amount = str.sale_amount.'}, {'target_schema': 'bronze', 'target_table': 'customer_order_items_bronze', 'target_alias': 'coib', 'mapping_details': 'sales_transactions_raw str', 'description': 'Create one row per transaction line item (assumes one product per transaction in source). Map: coib.order_item_id = str.transaction_id; coib.order_id = str.transaction_id; coib.product_id = str.product_id; coib.quantity = str.quantity; coib.line_amount = str.sale_amount; coib.transaction_timestamp = str.transaction_time; coib.store_id = str.store_id.'}, {'target_schema': 'bronze', 'target_table': 'products_bronze', 'target_alias': 'pb', 'mapping_details': 'products_raw pr', 'description': 'Ingest product master data. Map: pb.product_id = pr.product_id; pb.product_name = pr.product_name; pb.category = pr.category; pb.brand = pr.brand; pb.price = pr.price; pb.is_active = pr.is_active.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'cob.order_id = str.transaction_id', 'target_table': 'cob'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'cob.store_id = str.store_id', 'target_table': 'cob'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'order_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'cob.order_timestamp = str.transaction_time', 'target_table': 'cob'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'not_accepted', 'target_column': 'order_total_amount', 'target_type': 'DECIMAL', 'target_nullable': 'not_accepted', 'transformation': 'cob.order_total_amount = str.sale_amount', 'target_table': 'cob'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'order_item_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coib.order_item_id = str.transaction_id', 'target_table': 'coib'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coib.order_id = str.transaction_id', 'target_table': 'coib'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coib.product_id = str.product_id', 'target_table': 'coib'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'not_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'not_accepted', 'transformation': 'coib.quantity = str.quantity', 'target_table': 'coib'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'not_accepted', 'target_column': 'line_amount', 'target_type': 'DECIMAL', 'target_nullable': 'not_accepted', 'transformation': 'coib.line_amount = str.sale_amount', 'target_table': 'coib'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'transaction_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'coib.transaction_timestamp = str.transaction_time', 'target_table': 'coib'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coib.store_id = str.store_id', 'target_table': 'coib'}, {'source_column': "['pr.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pb.product_id = pr.product_id', 'target_table': 'pb'}, {'source_column': "['pr.product_name']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_name', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pb.product_name = pr.product_name', 'target_table': 'pb'}, {'source_column': "['pr.category']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'category', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pb.category = pr.category', 'target_table': 'pb'}, {'source_column': "['pr.brand']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'brand', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pb.brand = pr.brand', 'target_table': 'pb'}, {'source_column': "['pr.price']", 'source_type': 'DECIMAL', 'source_nullable': 'not_accepted', 'target_column': 'price', 'target_type': 'DECIMAL', 'target_nullable': 'not_accepted', 'transformation': 'pb.price = pr.price', 'target_table': 'pb'}, {'source_column': "['pr.is_active']", 'source_type': 'BOOLEAN', 'source_nullable': 'not_accepted', 'target_column': 'is_active', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'pb.is_active = pr.is_active', 'target_table': 'pb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

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

    df = reader.load(f"{base_path}{source_table}.{read_format}")
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

    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()
