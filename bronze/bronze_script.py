from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

base_path = "s3://sdlc-agent-bucket/engineering-agent/src/"
target_path = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
read_format = "csv"
write_format = "csv"
write_mode = "overwrite"

metadata_tables = [
    {"source_table": "POS_sales_events", "source_alias": "seb", "target_table": "sales_event_bronze"},
    {"source_table": "PAYMENT_GATEWAY", "source_alias": "peb", "target_table": "payment_event_bronze"},
    {"source_table": "INVENTORY_SYSTEM", "source_alias": "ieb", "target_table": "inventory_event_bronze"},
    {"source_table": "SENSOR", "source_alias": "feb", "target_table": "footfall_event_bronze"}
]

columns_metadata = {
    "seb": [
        "seb.event_id as event_id",
        "seb.event_type as event_type",
        "seb.source_system as source_system",
        "seb.event_timestamp as event_timestamp",
        "seb.ingestion_timestamp as ingestion_timestamp",
        "seb.batch_id as batch_id",
        "seb.is_deleted as is_deleted",
        "seb.transaction_id as transaction_id",
        "seb.order_id as order_id",
        "seb.store_id as store_id",
        "seb.terminal_id as terminal_id",
        "seb.cashier_id as cashier_id",
        "seb.product_id as product_id",
        "seb.product_name as product_name",
        "seb.category as category",
        "seb.sub_category as sub_category",
        "seb.quantity as quantity",
        "seb.unit_price as unit_price",
        "seb.discount as discount",
        "seb.total_amount as total_amount",
        "seb.payment_id as payment_id",
        "seb.event_action as event_action"
    ],
    "peb": [
        "peb.event_id as event_id",
        "peb.event_type as event_type",
        "peb.source_system as source_system",
        "peb.event_timestamp as event_timestamp",
        "peb.ingestion_timestamp as ingestion_timestamp",
        "peb.batch_id as batch_id",
        "peb.is_deleted as is_deleted",
        "peb.payment_id as payment_id",
        "peb.transaction_id as transaction_id",
        "peb.payment_mode as payment_mode",
        "peb.provider as provider",
        "peb.amount as amount",
        "peb.currency as currency",
        "peb.payment_status as payment_status"
    ],
    "ieb": [
        "ieb.event_id as event_id",
        "ieb.event_type as event_type",
        "ieb.source_system as source_system",
        "ieb.event_timestamp as event_timestamp",
        "ieb.ingestion_timestamp as ingestion_timestamp",
        "ieb.batch_id as batch_id",
        "ieb.is_deleted as is_deleted",
        "ieb.inventory_event_id as inventory_event_id",
        "ieb.product_id as product_id",
        "ieb.store_id as store_id",
        "ieb.warehouse_id as warehouse_id",
        "ieb.change_type as change_type",
        "ieb.quantity_changed as quantity_changed",
        "ieb.current_stock as current_stock"
    ],
    "feb": [
        "feb.event_id as event_id",
        "feb.event_type as event_type",
        "feb.source_system as source_system",
        "feb.event_timestamp as event_timestamp",
        "feb.ingestion_timestamp as ingestion_timestamp",
        "feb.batch_id as batch_id",
        "feb.is_deleted as is_deleted",
        "feb.footfall_event_id as footfall_event_id",
        "feb.store_id as store_id",
        "feb.entry_count as entry_count",
        "feb.exit_count as exit_count",
        "feb.sensor_id as sensor_id"
    ]
}

for table in metadata_tables:
    source_table = table["source_table"]
    source_alias = table["source_alias"]
    target_table = table["target_table"]

    df = spark.read.format(read_format) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(base_path + source_table + "." + read_format)
    
    df = df.alias(source_alias)

    transformations = columns_metadata[source_alias]
    df = df.selectExpr(*transformations)
    
    df.write.mode(write_mode) \
           .format(write_format) \
           .option("header", "true") \
           .save(target_path + target_table + "." + write_format)

job.commit()