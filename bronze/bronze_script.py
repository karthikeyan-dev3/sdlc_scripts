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
            'description': 'Bronze ingestion of raw patient enrollment records at patient/trial/site granularity. Includes all source fields: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system, and Unnamed: 10.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'clinical_visit_bronze',
            'target_alias': 'cvb',
            'mapping_details': 'clinical_visit_raw_2000 cvr',
            'description': 'Bronze ingestion of raw clinical visit records at visit granularity. Includes all source fields: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system, and Unnamed: 10.'
        }
    ],
    'columns': [
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
    mapping_details = table['mapping_details'].strip().split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format).alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
