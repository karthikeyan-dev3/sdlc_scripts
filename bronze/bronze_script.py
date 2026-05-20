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
            'mapping_details': 'patient_enrollment_raw_2000 pe',
            'description': 'Bronze ingestion of patient enrollment records from patient_enrollment_raw_2000. Columns mapped: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'clinical_visit_bronze',
            'target_alias': 'cvb',
            'mapping_details': 'clinical_visit_raw_2000 cv',
            'description': 'Bronze ingestion of clinical visit records from clinical_visit_raw_2000. Columns mapped: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'adverse_events_bronze',
            'target_alias': 'aeb',
            'mapping_details': 'adverse_events_raw_2000 ae',
            'description': 'Bronze ingestion of adverse event records from adverse_events_raw_2000. Columns mapped: event_id, patient_id, event_type, severity, event_start_date, event_end_date, outcome, related_to_drug, hospitalization_required, reported_by.'
        }
    ],
    'columns': [
        {
            'source_column': "['pe.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not accepted',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not accepted',
            'transformation': 'pe.patient_id = peb.patient_id',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'accepted',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'accepted',
            'transformation': 'pe.trial_id = peb.trial_id',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'accepted',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'accepted',
            'transformation': 'pe.site_id = peb.site_id',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'pe.patient_name = peb.patient_name',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'accepted',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'accepted',
            'transformation': 'pe.gender = peb.gender',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'accepted',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'accepted',
            'transformation': 'pe.date_of_birth = peb.date_of_birth',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'accepted',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'accepted',
            'transformation': 'pe.country = peb.country',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'pe.enrollment_date = peb.enrollment_date',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'accepted',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'accepted',
            'transformation': 'pe.consent_status = peb.consent_status',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'accepted',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'accepted',
            'transformation': 'pe.source_system = peb.source_system',
            'target_table': 'pe'
        },
        {
            'source_column': "['cv.visit_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not accepted',
            'target_column': 'visit_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not accepted',
            'transformation': 'cv.visit_id = cvb.visit_id',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'cv.patient_id = cvb.patient_id',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.trial_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'trial_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'cv.trial_id = cvb.trial_id',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.visit_date']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'visit_date',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'cv.visit_date = cvb.visit_date',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.visit_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'visit_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'cv.visit_type = cvb.visit_type',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.blood_pressure']",
            'source_type': 'float64',
            'source_nullable': 'accepted',
            'target_column': 'blood_pressure',
            'target_type': 'float64',
            'target_nullable': 'accepted',
            'transformation': 'cv.blood_pressure = cvb.blood_pressure',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.heart_rate']",
            'source_type': 'float64',
            'source_nullable': 'accepted',
            'target_column': 'heart_rate',
            'target_type': 'float64',
            'target_nullable': 'accepted',
            'transformation': 'cv.heart_rate = cvb.heart_rate',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.weight_kg']",
            'source_type': 'float64',
            'source_nullable': 'accepted',
            'target_column': 'weight_kg',
            'target_type': 'float64',
            'target_nullable': 'accepted',
            'transformation': 'cv.weight_kg = cvb.weight_kg',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.physician_notes']",
            'source_type': 'text',
            'source_nullable': 'accepted',
            'target_column': 'physician_notes',
            'target_type': 'text',
            'target_nullable': 'accepted',
            'transformation': 'cv.physician_notes = cvb.physician_notes',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.source_system']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'source_system',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'cv.source_system = cvb.source_system',
            'target_table': 'cv'
        },
        {
            'source_column': "['ae.event_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not accepted',
            'target_column': 'event_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not accepted',
            'transformation': 'ae.event_id = aeb.event_id',
            'target_table': 'ae'
        },
        {
            'source_column': "['ae.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'ae.patient_id = aeb.patient_id',
            'target_table': 'ae'
        },
        {
            'source_column': "['ae.event_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'event_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'ae.event_type = aeb.event_type',
            'target_table': 'ae'
        },
        {
            'source_column': "['ae.severity']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'severity',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'ae.severity = aeb.severity',
            'target_table': 'ae'
        },
        {
            'source_column': "['ae.event_start_date']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'event_start_date',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'ae.event_start_date = aeb.event_start_date',
            'target_table': 'ae'
        },
        {
            'source_column': "['ae.event_end_date']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'event_end_date',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'ae.event_end_date = aeb.event_end_date',
            'target_table': 'ae'
        },
        {
            'source_column': "['ae.outcome']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'outcome',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'ae.outcome = aeb.outcome',
            'target_table': 'ae'
        },
        {
            'source_column': "['ae.related_to_drug']",
            'source_type': 'boolean',
            'source_nullable': 'accepted',
            'target_column': 'related_to_drug',
            'target_type': 'boolean',
            'target_nullable': 'accepted',
            'transformation': 'ae.related_to_drug = aeb.related_to_drug',
            'target_table': 'ae'
        },
        {
            'source_column': "['ae.hospitalization_required']",
            'source_type': 'boolean',
            'source_nullable': 'accepted',
            'target_column': 'hospitalization_required',
            'target_type': 'boolean',
            'target_nullable': 'accepted',
            'transformation': 'ae.hospitalization_required = aeb.hospitalization_required',
            'target_table': 'ae'
        },
        {
            'source_column': "['ae.reported_by']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'reported_by',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'ae.reported_by = aeb.reported_by',
            'target_table': 'ae'
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
    mapping_details = table.get('mapping_details', '')
    parts = mapping_details.split()
    source_table = parts[0] if len(parts) > 0 else None
    source_alias = parts[1] if len(parts) > 1 else None

    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(f"{base_path}{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col in metadata.get('columns', []):
        if col.get('target_table') == source_alias:
            transformation = col.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            target_column = col.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()
