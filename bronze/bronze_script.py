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
            'target_table': 'pipeline_run_metrics_bronze',
            'target_alias': 'prm_b',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze ingestion of raw sales transaction records as pipeline run metric events; direct copy of source records with no joins or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'data_quality_results_bronze',
            'target_alias': 'dqr_b',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze ingestion of raw sales transaction records to support downstream data quality result generation; direct copy with no joins or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'data_catalog_assets_bronze',
            'target_alias': 'dca_b',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze ingestion of raw product master data as catalog assets; direct copy of source records with no joins or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'data_access_audit_bronze',
            'target_alias': 'daa_b',
            'mapping_details': 'stores_raw sr',
            'description': 'Bronze ingestion of raw store master data used for downstream access/audit context; direct copy of source records with no joins or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sla_availability_daily_bronze',
            'target_alias': 'sad_b',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze ingestion of raw sales transaction records to support downstream SLA availability calculations; direct copy of source records with no joins or aggregations.'
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'prm_b.transaction_id = str.transaction_id',
            'target_table': 'prm_b'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'prm_b.store_id = str.store_id',
            'target_table': 'prm_b'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'prm_b.product_id = str.product_id',
            'target_table': 'prm_b'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'accepted',
            'transformation': 'prm_b.quantity = str.quantity',
            'target_table': 'prm_b'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'prm_b.sale_amount = str.sale_amount',
            'target_table': 'prm_b'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'accepted',
            'transformation': 'prm_b.transaction_time = str.transaction_time',
            'target_table': 'prm_b'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'dqr_b.transaction_id = str.transaction_id',
            'target_table': 'dqr_b'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'dqr_b.store_id = str.store_id',
            'target_table': 'dqr_b'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'dqr_b.product_id = str.product_id',
            'target_table': 'dqr_b'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'accepted',
            'transformation': 'dqr_b.quantity = str.quantity',
            'target_table': 'dqr_b'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'dqr_b.sale_amount = str.sale_amount',
            'target_table': 'dqr_b'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'accepted',
            'transformation': 'dqr_b.transaction_time = str.transaction_time',
            'target_table': 'dqr_b'
        },
        {
            'source_column': "['pr.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'dca_b.product_id = pr.product_id',
            'target_table': 'dca_b'
        },
        {
            'source_column': "['pr.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'dca_b.product_name = pr.product_name',
            'target_table': 'dca_b'
        },
        {
            'source_column': "['pr.category']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'dca_b.category = pr.category',
            'target_table': 'dca_b'
        },
        {
            'source_column': "['pr.brand']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'dca_b.brand = pr.brand',
            'target_table': 'dca_b'
        },
        {
            'source_column': "['pr.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'dca_b.price = pr.price',
            'target_table': 'dca_b'
        },
        {
            'source_column': "['pr.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'accepted',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'accepted',
            'transformation': 'dca_b.is_active = pr.is_active',
            'target_table': 'dca_b'
        },
        {
            'source_column': "['sr.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'daa_b.store_id = sr.store_id',
            'target_table': 'daa_b'
        },
        {
            'source_column': "['sr.store_name']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_name',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'daa_b.store_name = sr.store_name',
            'target_table': 'daa_b'
        },
        {
            'source_column': "['sr.city']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'city',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'daa_b.city = sr.city',
            'target_table': 'daa_b'
        },
        {
            'source_column': "['sr.state']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'state',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'daa_b.state = sr.state',
            'target_table': 'daa_b'
        },
        {
            'source_column': "['sr.store_type']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_type',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'daa_b.store_type = sr.store_type',
            'target_table': 'daa_b'
        },
        {
            'source_column': "['sr.open_date']",
            'source_type': 'DATE',
            'source_nullable': 'accepted',
            'target_column': 'open_date',
            'target_type': 'DATE',
            'target_nullable': 'accepted',
            'transformation': 'daa_b.open_date = sr.open_date',
            'target_table': 'daa_b'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'sad_b.transaction_id = str.transaction_id',
            'target_table': 'sad_b'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sad_b.store_id = str.store_id',
            'target_table': 'sad_b'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sad_b.product_id = str.product_id',
            'target_table': 'sad_b'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'accepted',
            'transformation': 'sad_b.quantity = str.quantity',
            'target_table': 'sad_b'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'sad_b.sale_amount = str.sale_amount',
            'target_table': 'sad_b'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'accepted',
            'transformation': 'sad_b.transaction_time = str.transaction_time',
            'target_table': 'sad_b'
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
    for col in metadata["columns"]:
        if col["target_table"] == target_alias:
            rhs = col["transformation"].split("=", 1)[1].strip()
            target_col = col["target_column"]
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == "csv":
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
