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
            'target_table': 'patient_enrollments_bronze',
            'target_alias': 'peb',
            'mapping_details': 'patient_enrollment_raw_2000 per',
            'description': 'Bronze ingestion of patient enrollment records at patient-trial-site grain. Columns mapped from patient_enrollment_raw_2000: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system. Excludes Unnamed: 10.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'visits_bronze',
            'target_alias': 'vb',
            'mapping_details': 'clinical_visit_raw_2000 cv',
            'description': 'Bronze ingestion of clinical visit events at visit grain. Columns mapped from clinical_visit_raw_2000: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system. Excludes Unnamed: 10.'
        }
    ],
    'columns': [
        {
            'source_column': "['per.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_accepted',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_accepted',
            'transformation': 'peb.patient_id = per.patient_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_accepted',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_accepted',
            'transformation': 'peb.trial_id = per.trial_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_accepted',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_accepted',
            'transformation': 'peb.site_id = per.site_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'peb.patient_name = per.patient_name',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'accepted',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'accepted',
            'transformation': 'peb.gender = per.gender',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'accepted',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'accepted',
            'transformation': 'peb.date_of_birth = per.date_of_birth',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'accepted',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'accepted',
            'transformation': 'peb.country = per.country',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'peb.enrollment_date = per.enrollment_date',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'accepted',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'accepted',
            'transformation': 'peb.consent_status = per.consent_status',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'accepted',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'accepted',
            'transformation': 'peb.source_system = per.source_system',
            'target_table': 'peb'
        },
        {
            'source_column': "['cv.visit_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_accepted',
            'target_column': 'visit_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_accepted',
            'transformation': 'vb.visit_id = cv.visit_id',
            'target_table': 'vb'
        },
        {
            'source_column': "['cv.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_accepted',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_accepted',
            'transformation': 'vb.patient_id = cv.patient_id',
            'target_table': 'vb'
        },
        {
            'source_column': "['cv.trial_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_accepted',
            'target_column': 'trial_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_accepted',
            'transformation': 'vb.trial_id = cv.trial_id',
            'target_table': 'vb'
        },
        {
            'source_column': "['cv.visit_date']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'visit_date',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'vb.visit_date = cv.visit_date',
            'target_table': 'vb'
        },
        {
            'source_column': "['cv.visit_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'visit_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'vb.visit_type = cv.visit_type',
            'target_table': 'vb'
        },
        {
            'source_column': "['cv.blood_pressure']",
            'source_type': 'float64',
            'source_nullable': 'accepted',
            'target_column': 'blood_pressure',
            'target_type': 'float64',
            'target_nullable': 'accepted',
            'transformation': 'vb.blood_pressure = cv.blood_pressure',
            'target_table': 'vb'
        },
        {
            'source_column': "['cv.heart_rate']",
            'source_type': 'float64',
            'source_nullable': 'accepted',
            'target_column': 'heart_rate',
            'target_type': 'float64',
            'target_nullable': 'accepted',
            'transformation': 'vb.heart_rate = cv.heart_rate',
            'target_table': 'vb'
        },
        {
            'source_column': "['cv.weight_kg']",
            'source_type': 'float64',
            'source_nullable': 'accepted',
            'target_column': 'weight_kg',
            'target_type': 'float64',
            'target_nullable': 'accepted',
            'transformation': 'vb.weight_kg = cv.weight_kg',
            'target_table': 'vb'
        },
        {
            'source_column': "['cv.physician_notes']",
            'source_type': 'text',
            'source_nullable': 'accepted',
            'target_column': 'physician_notes',
            'target_type': 'text',
            'target_nullable': 'accepted',
            'transformation': 'vb.physician_notes = cv.physician_notes',
            'target_table': 'vb'
        },
        {
            'source_column': "['cv.source_system']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'source_system',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'vb.source_system = cv.source_system',
            'target_table': 'vb'
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
    mapping_parts = mapping_details.split()
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else None
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else None

    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            if '=' in transformation:
                rhs = transformation.split('=', 1)[1].strip()
            else:
                rhs = transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
