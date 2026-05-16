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
            'target_table': 'adverse_events_bronze',
            'target_alias': 'aeb',
            'mapping_details': 'adverse_events_raw_2000 ae',
            'description': 'Bronze ingestion of adverse event records from adverse_events_raw_2000. Columns mapped: event_id, patient_id, event_type, severity, event_start_date, event_end_date, outcome, related_to_drug, hospitalization_required, reported_by.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'drug_administration_bronze',
            'target_alias': 'dab',
            'mapping_details': 'drug_administration_raw_2000 da',
            'description': 'Bronze ingestion of drug administration events from drug_administration_raw_2000. Columns mapped: administration_id, patient_id, drug_code, dosage_mg, administration_date, administration_route, administered_by, batch_number.'
        }
    ],
    'columns': [
        {
            'source_column': "['ae.event_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_accepted',
            'target_column': 'event_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_accepted',
            'transformation': 'aeb.event_id = ae.event_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'aeb.patient_id = ae.patient_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.event_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'event_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'aeb.event_type = ae.event_type',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.severity']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'severity',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'aeb.severity = ae.severity',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.event_start_date']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'event_start_date',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'aeb.event_start_date = ae.event_start_date',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.event_end_date']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'event_end_date',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'aeb.event_end_date = ae.event_end_date',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.outcome']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'outcome',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'aeb.outcome = ae.outcome',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.related_to_drug']",
            'source_type': 'boolean',
            'source_nullable': 'accepted',
            'target_column': 'related_to_drug',
            'target_type': 'boolean',
            'target_nullable': 'accepted',
            'transformation': 'aeb.related_to_drug = ae.related_to_drug',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.hospitalization_required']",
            'source_type': 'boolean',
            'source_nullable': 'accepted',
            'target_column': 'hospitalization_required',
            'target_type': 'boolean',
            'target_nullable': 'accepted',
            'transformation': 'aeb.hospitalization_required = ae.hospitalization_required',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.reported_by']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'reported_by',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'aeb.reported_by = ae.reported_by',
            'target_table': 'aeb'
        },
        {
            'source_column': "['da.administration_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_accepted',
            'target_column': 'administration_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_accepted',
            'transformation': 'dab.administration_id = da.administration_id',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'dab.patient_id = da.patient_id',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.drug_code']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'drug_code',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'dab.drug_code = da.drug_code',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.dosage_mg']",
            'source_type': 'double',
            'source_nullable': 'accepted',
            'target_column': 'dosage_mg',
            'target_type': 'double',
            'target_nullable': 'accepted',
            'transformation': 'dab.dosage_mg = da.dosage_mg',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.administration_date']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'administration_date',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'dab.administration_date = da.administration_date',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.administration_route']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'administration_route',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'dab.administration_route = da.administration_route',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.administered_by']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'administered_by',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'dab.administered_by = da.administered_by',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.batch_number']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'batch_number',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'dab.batch_number = da.batch_number',
            'target_table': 'dab'
        }
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/clinical_trail/',
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

def _join_path(path, name):
    if path is None:
        return None
    if path.endswith('/'):
        return path + name
    return path + '/' + name

for table in metadata.get('tables', []):
    mapping_details = table.get('mapping_details', '')
    parts = mapping_details.split()
    source_table = parts[0] if len(parts) > 0 else None
    source_alias = parts[1] if len(parts) > 1 else None

    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    input_path = _join_path(base_path, f"{source_table}.{read_format}")

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(input_path)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') != target_alias:
            continue
        transformation = col_meta.get('transformation', '')
        if '=' not in transformation:
            continue
        rhs = transformation.split('=', 1)[1].strip()
        target_column = col_meta.get('target_column')
        transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    output_path = _join_path(target_path, f"{target_table}.{write_format}")

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(output_path)

job.commit()