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
            'description': 'Bronze table for patient enrollment sourced from patient_enrollment_raw_2000. Columns: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'clinical_visit_bronze',
            'target_alias': 'cvb',
            'mapping_details': 'clinical_visit_raw_2000 cv',
            'description': 'Bronze table for clinical visits sourced from clinical_visit_raw_2000. Columns: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system.'
        }
    ],
    'columns': [
        {
            'source_column': "['pe.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_null',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_null',
            'transformation': 'peb.patient_id = pe.patient_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_null',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_null',
            'transformation': 'peb.trial_id = pe.trial_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_null',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_null',
            'transformation': 'peb.site_id = pe.site_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'nan',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'nan',
            'transformation': 'peb.patient_name = pe.patient_name',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'nan',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'nan',
            'transformation': 'peb.gender = pe.gender',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'nan',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'nan',
            'transformation': 'peb.date_of_birth = pe.date_of_birth',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'nan',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'nan',
            'transformation': 'peb.country = pe.country',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'nan',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'nan',
            'transformation': 'peb.enrollment_date = pe.enrollment_date',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'nan',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'nan',
            'transformation': 'peb.consent_status = pe.consent_status',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'nan',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'nan',
            'transformation': 'peb.source_system = pe.source_system',
            'target_table': 'peb'
        },
        {
            'source_column': "['cv.visit_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_null',
            'target_column': 'visit_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_null',
            'transformation': 'cvb.visit_id = cv.visit_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_null',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_null',
            'transformation': 'cvb.patient_id = cv.patient_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.trial_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_null',
            'target_column': 'trial_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_null',
            'transformation': 'cvb.trial_id = cv.trial_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.visit_date']",
            'source_type': 'timestamp',
            'source_nullable': 'nan',
            'target_column': 'visit_date',
            'target_type': 'timestamp',
            'target_nullable': 'nan',
            'transformation': 'cvb.visit_date = cv.visit_date',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.visit_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'nan',
            'target_column': 'visit_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'nan',
            'transformation': 'cvb.visit_type = cv.visit_type',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.blood_pressure']",
            'source_type': 'double',
            'source_nullable': 'nan',
            'target_column': 'blood_pressure',
            'target_type': 'double',
            'target_nullable': 'nan',
            'transformation': 'cvb.blood_pressure = cv.blood_pressure',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.heart_rate']",
            'source_type': 'double',
            'source_nullable': 'nan',
            'target_column': 'heart_rate',
            'target_type': 'double',
            'target_nullable': 'nan',
            'transformation': 'cvb.heart_rate = cv.heart_rate',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.weight_kg']",
            'source_type': 'double',
            'source_nullable': 'nan',
            'target_column': 'weight_kg',
            'target_type': 'double',
            'target_nullable': 'nan',
            'transformation': 'cvb.weight_kg = cv.weight_kg',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.physician_notes']",
            'source_type': 'text',
            'source_nullable': 'nan',
            'target_column': 'physician_notes',
            'target_type': 'text',
            'target_nullable': 'nan',
            'transformation': 'cvb.physician_notes = cv.physician_notes',
            'target_table': 'cvb'
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

def _join_path(base, leaf):
    if base is None:
        base = ''
    if leaf is None:
        leaf = ''
    if base.endswith('/'):
        return base + leaf
    return base + '/' + leaf

for table in metadata.get('tables', []):
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    mapping_details = table.get('mapping_details')
    mapping_parts = mapping_details.split() if mapping_details else []
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else None
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else None

    source_location = _join_path(base_path, f"{source_table}.{read_format}")

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(source_location)

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation')
            target_column = col_meta.get('target_column')
            rhs = transformation.split('=', 1)[1].strip() if transformation and '=' in transformation else ''
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    target_location = _join_path(target_path, f"{target_table}.{write_format}")

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_location)

job.commit()