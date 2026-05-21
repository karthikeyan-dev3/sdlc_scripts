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
            'target_table': 'patient_bronze',
            'target_alias': 'pb',
            'mapping_details': 'sales_event se',
            'description': 'No patient-related source data provided. Placeholder mapping only; requires patient source (e.g., patient master/enrollment) under the configured base_path to create this bronze table.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'patient_encounter_bronze',
            'target_alias': 'peb',
            'mapping_details': 'sales_event se',
            'description': 'No patient encounter source data provided. Placeholder mapping only; requires encounter/visit source under the configured base_path to create this bronze table.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'lab_result_bronze',
            'target_alias': 'lrb',
            'mapping_details': 'inventory_event ie',
            'description': 'No lab result source data provided. Placeholder mapping only; requires lab results source under the configured base_path to create this bronze table.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'drug_administration_bronze',
            'target_alias': 'dab',
            'mapping_details': 'payment_event pe',
            'description': 'No drug administration source data provided. Placeholder mapping only; requires medication administration source under the configured base_path to create this bronze table.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'adverse_event_bronze',
            'target_alias': 'aeb',
            'mapping_details': 'sales_event se',
            'description': 'No adverse event source data provided. Placeholder mapping only; requires adverse event source under the configured base_path to create this bronze table.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'wearable_observation_bronze',
            'target_alias': 'wob',
            'mapping_details': 'footfall_event fe',
            'description': 'No wearable observation source data provided. Placeholder mapping only; requires wearable/telemetry observation source under the configured base_path to create this bronze table.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'patient_360_timeline_bronze',
            'target_alias': 'p360b',
            'mapping_details': 'sales_event se',
            'description': 'Patient 360 timeline cannot be built from provided sources without joins/aggregation and without healthcare-domain source tables. Placeholder mapping only; requires timeline-ready patient domain events under the configured base_path.'
        }
    ],
    'columns': [
        {
            'source_column': "['se.transaction_id']",
            'source_type': 'string',
            'source_nullable': 'not_null',
            'target_column': 'transaction_id',
            'target_type': 'string',
            'target_nullable': 'not_null',
            'transformation': 'pb.transaction_id = se.transaction_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['se.order_id']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'order_id',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'pb.order_id = se.order_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['se.store_id']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'store_id',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'pb.store_id = se.store_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['se.transaction_id']",
            'source_type': 'string',
            'source_nullable': 'not_null',
            'target_column': 'transaction_id',
            'target_type': 'string',
            'target_nullable': 'not_null',
            'transformation': 'peb.transaction_id = se.transaction_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['se.store_id']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'store_id',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'peb.store_id = se.store_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['se.terminal_id']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'terminal_id',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'peb.terminal_id = se.terminal_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['ie.inventory_event_id']",
            'source_type': 'string',
            'source_nullable': 'not_null',
            'target_column': 'inventory_event_id',
            'target_type': 'string',
            'target_nullable': 'not_null',
            'transformation': 'lrb.inventory_event_id = ie.inventory_event_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['ie.product_id']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'product_id',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'lrb.product_id = ie.product_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['ie.store_id']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'store_id',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'lrb.store_id = ie.store_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['pe.payment_id']",
            'source_type': 'string',
            'source_nullable': 'not_null',
            'target_column': 'payment_id',
            'target_type': 'string',
            'target_nullable': 'not_null',
            'transformation': 'dab.payment_id = pe.payment_id',
            'target_table': 'dab'
        },
        {
            'source_column': "['pe.transaction_id']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'transaction_id',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'dab.transaction_id = pe.transaction_id',
            'target_table': 'dab'
        },
        {
            'source_column': "['pe.amount']",
            'source_type': 'decimal',
            'source_nullable': 'null_accepted',
            'target_column': 'amount',
            'target_type': 'decimal',
            'target_nullable': 'null_accepted',
            'transformation': 'dab.amount = pe.amount',
            'target_table': 'dab'
        },
        {
            'source_column': "['se.transaction_id']",
            'source_type': 'string',
            'source_nullable': 'not_null',
            'target_column': 'transaction_id',
            'target_type': 'string',
            'target_nullable': 'not_null',
            'transformation': 'aeb.transaction_id = se.transaction_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['se.event_action']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'event_action',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'aeb.event_action = se.event_action',
            'target_table': 'aeb'
        },
        {
            'source_column': "['se.product_id']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'product_id',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'aeb.product_id = se.product_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['fe.footfall_event_id']",
            'source_type': 'string',
            'source_nullable': 'not_null',
            'target_column': 'footfall_event_id',
            'target_type': 'string',
            'target_nullable': 'not_null',
            'transformation': 'wob.footfall_event_id = fe.footfall_event_id',
            'target_table': 'wob'
        },
        {
            'source_column': "['fe.store_id']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'store_id',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'wob.store_id = fe.store_id',
            'target_table': 'wob'
        },
        {
            'source_column': "['fe.entry_count']",
            'source_type': 'int',
            'source_nullable': 'null_accepted',
            'target_column': 'entry_count',
            'target_type': 'int',
            'target_nullable': 'null_accepted',
            'transformation': 'wob.entry_count = fe.entry_count',
            'target_table': 'wob'
        },
        {
            'source_column': "['se.transaction_id']",
            'source_type': 'string',
            'source_nullable': 'not_null',
            'target_column': 'transaction_id',
            'target_type': 'string',
            'target_nullable': 'not_null',
            'transformation': 'p360b.transaction_id = se.transaction_id',
            'target_table': 'p360b'
        },
        {
            'source_column': "['se.event_action']",
            'source_type': 'string',
            'source_nullable': 'null_accepted',
            'target_column': 'event_action',
            'target_type': 'string',
            'target_nullable': 'null_accepted',
            'transformation': 'p360b.event_action = se.event_action',
            'target_table': 'p360b'
        },
        {
            'source_column': "['se.total_amount']",
            'source_type': 'decimal',
            'source_nullable': 'null_accepted',
            'target_column': 'total_amount',
            'target_type': 'decimal',
            'target_nullable': 'null_accepted',
            'transformation': 'p360b.total_amount = se.total_amount',
            'target_table': 'p360b'
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

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    mapping_details = table['mapping_details']
    source_table = mapping_details.split()[0]
    source_alias = mapping_details.split()[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + source_table + '.' + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            rhs = col_meta['transformation'].split('=', 1)[1].strip()
            target_col = col_meta['target_column']
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + '.' + write_format)

job.commit()
