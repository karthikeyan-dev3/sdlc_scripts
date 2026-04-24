from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders', 'target_alias': 'co', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze-level customer orders sourced directly from sales_transactions_raw without transformations, joins, or aggregations. Columns: transaction_id, store_id, transaction_time, sale_amount.'}, {'target_schema': 'bronze', 'target_table': 'customer_order_items', 'target_alias': 'coi', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze-level order line items sourced directly from sales_transactions_raw without transformations, joins, or aggregations. Columns: transaction_id, product_id, quantity.'}, {'target_schema': 'bronze', 'target_table': 'etl_run_audit', 'target_alias': 'era', 'mapping_details': 'system_generated', 'description': 'Bronze-level ETL run audit table populated by the ingestion framework (no direct source mapping). Typical fields include: etl_run_id, pipeline_name, source_name, target_table, run_start_ts, run_end_ts, status, rows_read, rows_written, error_message.'}, {'target_schema': 'bronze', 'target_table': 'data_quality_results', 'target_alias': 'dqr', 'mapping_details': 'system_generated', 'description': 'Bronze-level data quality results table populated by the data quality framework (no direct source mapping). Typical fields include: dq_run_id, etl_run_id, target_table, rule_name, rule_type, rule_definition, records_checked, records_failed, status, created_ts.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'co.transaction_id = str.transaction_id', 'target_table': 'co'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'co.store_id = str.store_id', 'target_table': 'co'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'co.transaction_time = str.transaction_time', 'target_table': 'co'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'accepted', 'transformation': 'co.sale_amount = str.sale_amount', 'target_table': 'co'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coi.transaction_id = str.transaction_id', 'target_table': 'coi'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'coi.product_id = str.product_id', 'target_table': 'coi'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'accepted', 'transformation': 'coi.quantity = str.quantity', 'target_table': 'coi'}, {'source_column': "['system_generated.etl_run_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'etl_run_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'era.etl_run_id = system_generated.etl_run_id', 'target_table': 'era'}, {'source_column': "['system_generated.pipeline_name']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'pipeline_name', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'era.pipeline_name = system_generated.pipeline_name', 'target_table': 'era'}, {'source_column': "['system_generated.source_name']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'source_name', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'era.source_name = system_generated.source_name', 'target_table': 'era'}, {'source_column': "['system_generated.target_table']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'target_table', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'era.target_table = system_generated.target_table', 'target_table': 'era'}, {'source_column': "['system_generated.run_start_ts']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'run_start_ts', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'era.run_start_ts = system_generated.run_start_ts', 'target_table': 'era'}, {'source_column': "['system_generated.run_end_ts']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'run_end_ts', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'era.run_end_ts = system_generated.run_end_ts', 'target_table': 'era'}, {'source_column': "['system_generated.status']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'status', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'era.status = system_generated.status', 'target_table': 'era'}, {'source_column': "['system_generated.rows_read']", 'source_type': 'BIGINT', 'source_nullable': 'accepted', 'target_column': 'rows_read', 'target_type': 'BIGINT', 'target_nullable': 'accepted', 'transformation': 'era.rows_read = system_generated.rows_read', 'target_table': 'era'}, {'source_column': "['system_generated.rows_written']", 'source_type': 'BIGINT', 'source_nullable': 'accepted', 'target_column': 'rows_written', 'target_type': 'BIGINT', 'target_nullable': 'accepted', 'transformation': 'era.rows_written = system_generated.rows_written', 'target_table': 'era'}, {'source_column': "['system_generated.error_message']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'error_message', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'era.error_message = system_generated.error_message', 'target_table': 'era'}, {'source_column': "['system_generated.dq_run_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'dq_run_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'dqr.dq_run_id = system_generated.dq_run_id', 'target_table': 'dqr'}, {'source_column': "['system_generated.etl_run_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'etl_run_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'dqr.etl_run_id = system_generated.etl_run_id', 'target_table': 'dqr'}, {'source_column': "['system_generated.target_table']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'target_table', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'dqr.target_table = system_generated.target_table', 'target_table': 'dqr'}, {'source_column': "['system_generated.rule_name']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'rule_name', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'dqr.rule_name = system_generated.rule_name', 'target_table': 'dqr'}, {'source_column': "['system_generated.rule_type']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'rule_type', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'dqr.rule_type = system_generated.rule_type', 'target_table': 'dqr'}, {'source_column': "['system_generated.rule_definition']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'rule_definition', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'dqr.rule_definition = system_generated.rule_definition', 'target_table': 'dqr'}, {'source_column': "['system_generated.records_checked']", 'source_type': 'BIGINT', 'source_nullable': 'accepted', 'target_column': 'records_checked', 'target_type': 'BIGINT', 'target_nullable': 'accepted', 'transformation': 'dqr.records_checked = system_generated.records_checked', 'target_table': 'dqr'}, {'source_column': "['system_generated.records_failed']", 'source_type': 'BIGINT', 'source_nullable': 'accepted', 'target_column': 'records_failed', 'target_type': 'BIGINT', 'target_nullable': 'accepted', 'transformation': 'dqr.records_failed = system_generated.records_failed', 'target_table': 'dqr'}, {'source_column': "['system_generated.status']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'status', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'dqr.status = system_generated.status', 'target_table': 'dqr'}, {'source_column': "['system_generated.created_ts']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'created_ts', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'dqr.created_ts = system_generated.created_ts', 'target_table': 'dqr'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    target_table = table['target_table']
    target_alias = table['target_alias']

    mapping_details = table.get('mapping_details')
    if mapping_details is None:
        continue

    parts = str(mapping_details).split()
    if len(parts) < 2:
        continue

    source_table = parts[0]
    source_alias = parts[1]

    df = spark.read.format(read_format)
    if read_format == 'csv':
        df = df.option('header', 'true').option('inferSchema', 'true')
    df = df.load(base_path + source_table + '.' + read_format)

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            if '=' in transformation:
                rhs = transformation.split('=', 1)[1].strip()
                target_column = col_meta.get('target_column')
                if target_column is not None:
                    transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')
    writer.save(target_path + target_table + '.' + write_format)

job.commit()
