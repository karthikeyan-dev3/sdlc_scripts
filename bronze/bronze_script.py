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
            'target_table': 'patient_enrollment_bronze',
            'target_alias': 'peb',
            'mapping_details': 'patient_enrollment_raw_2000 per',
            'description': 'Bronze ingestion of patient enrollment data for trial/site/country enrollment tracking and refresh timing. Columns: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'clinical_visit_bronze',
            'target_alias': 'cvb',
            'mapping_details': 'clinical_visit_raw_2000 cvr',
            'description': 'Bronze ingestion of clinical visit records to support visit adherence and completion measures. Columns: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'adverse_events_bronze',
            'target_alias': 'aeb',
            'mapping_details': 'adverse_events_raw_2000 aer',
            'description': 'Bronze ingestion of adverse event records for dropout/active-patient context and safety tracking at patient level. Columns: event_id, patient_id, event_type, severity, event_start_date, event_end_date, outcome, related_to_drug, hospitalization_required, reported_by.'
        }
    ],
    'columns': [
        {
            'source_column': "['per.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.patient_id = per.patient_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.trial_id = per.trial_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.site_id = per.site_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'peb.patient_name = per.patient_name',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 'peb.gender = per.gender',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'not specified',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'not specified',
            'transformation': 'peb.date_of_birth = per.date_of_birth',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 'peb.country = per.country',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'peb.enrollment_date = per.enrollment_date',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.consent_status = per.consent_status',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'not specified',
            'transformation': 'peb.source_system = per.source_system',
            'target_table': 'peb'
        },
        {
            'source_column': "['cvr.visit_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'visit_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cvb.visit_id = cvr.visit_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvr.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cvb.patient_id = cvr.patient_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvr.trial_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cvb.trial_id = cvr.trial_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvr.visit_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'visit_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'cvb.visit_date = cvr.visit_date',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvr.visit_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'visit_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cvb.visit_type = cvr.visit_type',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvr.blood_pressure']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'blood_pressure',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'cvb.blood_pressure = cvr.blood_pressure',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvr.heart_rate']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'heart_rate',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'cvb.heart_rate = cvr.heart_rate',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvr.weight_kg']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'weight_kg',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'cvb.weight_kg = cvr.weight_kg',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvr.physician_notes']",
            'source_type': 'text',
            'source_nullable': 'not specified',
            'target_column': 'physician_notes',
            'target_type': 'text',
            'target_nullable': 'not specified',
            'transformation': 'cvb.physician_notes = cvr.physician_notes',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvr.source_system']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cvb.source_system = cvr.source_system',
            'target_table': 'cvb'
        },
        {
            'source_column': "['aer.event_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'event_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_id = aer.event_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.patient_id = aer.patient_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.event_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'event_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_type = aer.event_type',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.severity']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'severity',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.severity = aer.severity',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.event_start_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'event_start_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_start_date = aer.event_start_date',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.event_end_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'event_end_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_end_date = aer.event_end_date',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.outcome']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'outcome',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.outcome = aer.outcome',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.related_to_drug']",
            'source_type': 'boolean',
            'source_nullable': 'not specified',
            'target_column': 'related_to_drug',
            'target_type': 'boolean',
            'target_nullable': 'not specified',
            'transformation': 'aeb.related_to_drug = aer.related_to_drug',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.hospitalization_required']",
            'source_type': 'boolean',
            'source_nullable': 'not specified',
            'target_column': 'hospitalization_required',
            'target_type': 'boolean',
            'target_nullable': 'not specified',
            'transformation': 'aeb.hospitalization_required = aer.hospitalization_required',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.reported_by']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'reported_by',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.reported_by = aer.reported_by',
            'target_table': 'aeb'
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

for table in metadata.get('tables', []):
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    mapping_details = table.get('mapping_details', '')
    mapping_parts = mapping_details.split()
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else None
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else None

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + source_table + '.' + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            parts = transformation.split('=', 1)
            rhs = parts[1].strip() if len(parts) > 1 else ''
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + '.' + write_format)

job.commit()