from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {
    'tables': [
        {
            'target_schema': 'bronze',
            'target_table': 'sales_transactions_bronze',
            'target_alias': 'stb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze raw sales transactions ingested 1:1 from sales_transactions_raw. Columns: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze raw products ingested 1:1 from products_raw. Columns: product_id, product_name, category, brand, price, is_active.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores_raw sr',
            'description': 'Bronze raw stores ingested 1:1 from stores_raw. Columns: store_id, store_name, city, state, store_type, open_date.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'ingestion_runs_bronze',
            'target_alias': 'irb',
            'mapping_details': 'system_metadata.ingestion_runs ir',
            'description': 'Bronze ingestion run log ingested 1:1 from system metadata source system_metadata.ingestion_runs capturing ingestion execution metadata (e.g., run identifiers, source/target, status, timestamps).'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'pipeline_runs_bronze',
            'target_alias': 'prb',
            'mapping_details': 'system_metadata.pipeline_runs prun',
            'description': 'Bronze pipeline run log ingested 1:1 from system metadata source system_metadata.pipeline_runs capturing pipeline execution metadata (e.g., pipeline name/id, status, start/end timestamps, parameters).'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'data_quality_checks_bronze',
            'target_alias': 'dqcb',
            'mapping_details': 'system_metadata.data_quality_checks dqc',
            'description': 'Bronze data quality checks log ingested 1:1 from system metadata source system_metadata.data_quality_checks capturing check definitions/executions and outcomes (e.g., rule name, dataset, status, observed value, threshold, timestamps).'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'dataset_catalog_bronze',
            'target_alias': 'dscb',
            'mapping_details': 'system_metadata.dataset_catalog dsc',
            'description': 'Bronze dataset catalog ingested 1:1 from system metadata source system_metadata.dataset_catalog capturing dataset inventory and metadata (e.g., dataset name, schema/table, owner, tags, description, update timestamps).'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'data_access_audit_bronze',
            'target_alias': 'daab',
            'mapping_details': 'system_metadata.data_access_audit daa',
            'description': 'Bronze data access audit log ingested 1:1 from system metadata source system_metadata.data_access_audit capturing access events (e.g., user/principal, action, dataset/object, timestamp, source IP/application).'
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'stb.transaction_id = str.transaction_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'stb.store_id = str.store_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'stb.product_id = str.product_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_specified',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_specified',
            'transformation': 'stb.quantity = str.quantity',
            'target_table': 'stb'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_specified',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_specified',
            'transformation': 'stb.sale_amount = str.sale_amount',
            'target_table': 'stb'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_specified',
            'transformation': 'stb.transaction_time = str.transaction_time',
            'target_table': 'stb'
        },
        {
            'source_column': "['pr.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.product_id = pr.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.product_name = pr.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.category']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.category = pr.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.brand']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'pb.brand = pr.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_specified',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_specified',
            'transformation': 'pb.price = pr.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'not_specified',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'not_specified',
            'transformation': 'pb.is_active = pr.is_active',
            'target_table': 'pb'
        },
        {
            'source_column': "['sr.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'sb.store_id = sr.store_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['sr.store_name']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'store_name',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'sb.store_name = sr.store_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['sr.city']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'city',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'sb.city = sr.city',
            'target_table': 'sb'
        },
        {
            'source_column': "['sr.state']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'state',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'sb.state = sr.state',
            'target_table': 'sb'
        },
        {
            'source_column': "['sr.store_type']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'store_type',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'sb.store_type = sr.store_type',
            'target_table': 'sb'
        },
        {
            'source_column': "['sr.open_date']",
            'source_type': 'DATE',
            'source_nullable': 'not_specified',
            'target_column': 'open_date',
            'target_type': 'DATE',
            'target_nullable': 'not_specified',
            'transformation': 'sb.open_date = sr.open_date',
            'target_table': 'sb'
        }
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table in metadata.get('tables', []):
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    mapping_details = table.get('mapping_details', '')
    mapping_parts = mapping_details.split()
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else None
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else None

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            target_column = col_meta.get('target_column')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation
            transformations.append(f"{rhs} as {target_column}")

    if transformations:
        df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
