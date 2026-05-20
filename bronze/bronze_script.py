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
            'description': 'Bronze table for patient_enrollment sourced from patient_enrollment_raw_2000. Columns: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'clinical_visits_bronze',
            'target_alias': 'cvb',
            'mapping_details': 'clinical_visit_raw_2000 cv',
            'description': 'Bronze table for clinical_visits sourced from clinical_visit_raw_2000. Columns: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'lab_results_bronze',
            'target_alias': 'lrb',
            'mapping_details': 'lab_results_raw_2000 lr',
            'description': 'Bronze table for lab_results sourced from lab_results_raw_2000. Columns: lab_result_id, patient_id, sample_id, test_name, test_result, test_unit, reference_range, abnormal_flag, test_date, lab_name.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'wearable_data_bronze',
            'target_alias': 'wdb',
            'mapping_details': 'wearable_monitoring_raw_2000 wm',
            'description': 'Bronze table for wearable_data sourced from wearable_monitoring_raw_2000. Columns: device_record_id, patient_id, device_type, recorded_timestamp, glucose_level, step_count, sleep_hours, heart_rate, battery_status.'
        }
    ],
    'columns': [
        {
            'source_column': "['pe.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'pe.patient_id = peb.patient_id',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'pe.trial_id = peb.trial_id',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'pe.site_id = peb.site_id',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'pe.patient_name = peb.patient_name',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 'pe.gender = peb.gender',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'not specified',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'not specified',
            'transformation': 'pe.date_of_birth = peb.date_of_birth',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 'pe.country = peb.country',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'pe.enrollment_date = peb.enrollment_date',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'pe.consent_status = peb.consent_status',
            'target_table': 'pe'
        },
        {
            'source_column': "['pe.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'not specified',
            'transformation': 'pe.source_system = peb.source_system',
            'target_table': 'pe'
        },
        {
            'source_column': "['cv.visit_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'visit_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cv.visit_id = cvb.visit_id',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cv.patient_id = cvb.patient_id',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.trial_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cv.trial_id = cvb.trial_id',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.visit_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'visit_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'cv.visit_date = cvb.visit_date',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.visit_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'visit_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cv.visit_type = cvb.visit_type',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.blood_pressure']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'blood_pressure',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'cv.blood_pressure = cvb.blood_pressure',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.heart_rate']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'heart_rate',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'cv.heart_rate = cvb.heart_rate',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.weight_kg']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'weight_kg',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'cv.weight_kg = cvb.weight_kg',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.physician_notes']",
            'source_type': 'text',
            'source_nullable': 'not specified',
            'target_column': 'physician_notes',
            'target_type': 'text',
            'target_nullable': 'not specified',
            'transformation': 'cv.physician_notes = cvb.physician_notes',
            'target_table': 'cv'
        },
        {
            'source_column': "['cv.source_system']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cv.source_system = cvb.source_system',
            'target_table': 'cv'
        },
        {
            'source_column': "['lr.lab_result_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'lab_result_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lr.lab_result_id = lrb.lab_result_id',
            'target_table': 'lr'
        },
        {
            'source_column': "['lr.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lr.patient_id = lrb.patient_id',
            'target_table': 'lr'
        },
        {
            'source_column': "['lr.sample_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'sample_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lr.sample_id = lrb.sample_id',
            'target_table': 'lr'
        },
        {
            'source_column': "['lr.test_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'test_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lr.test_name = lrb.test_name',
            'target_table': 'lr'
        },
        {
            'source_column': "['lr.test_result']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'test_result',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'lr.test_result = lrb.test_result',
            'target_table': 'lr'
        },
        {
            'source_column': "['lr.test_unit']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'test_unit',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lr.test_unit = lrb.test_unit',
            'target_table': 'lr'
        },
        {
            'source_column': "['lr.reference_range']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'reference_range',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lr.reference_range = lrb.reference_range',
            'target_table': 'lr'
        },
        {
            'source_column': "['lr.abnormal_flag']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'abnormal_flag',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lr.abnormal_flag = lrb.abnormal_flag',
            'target_table': 'lr'
        },
        {
            'source_column': "['lr.test_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'test_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'lr.test_date = lrb.test_date',
            'target_table': 'lr'
        },
        {
            'source_column': "['lr.lab_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'lab_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lr.lab_name = lrb.lab_name',
            'target_table': 'lr'
        },
        {
            'source_column': "['wm.device_record_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'device_record_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wm.device_record_id = wdb.device_record_id',
            'target_table': 'wm'
        },
        {
            'source_column': "['wm.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wm.patient_id = wdb.patient_id',
            'target_table': 'wm'
        },
        {
            'source_column': "['wm.device_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'device_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wm.device_type = wdb.device_type',
            'target_table': 'wm'
        },
        {
            'source_column': "['wm.recorded_timestamp']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'recorded_timestamp',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'wm.recorded_timestamp = wdb.recorded_timestamp',
            'target_table': 'wm'
        },
        {
            'source_column': "['wm.glucose_level']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'glucose_level',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'wm.glucose_level = wdb.glucose_level',
            'target_table': 'wm'
        },
        {
            'source_column': "['wm.step_count']",
            'source_type': 'int',
            'source_nullable': 'not specified',
            'target_column': 'step_count',
            'target_type': 'int',
            'target_nullable': 'not specified',
            'transformation': 'wm.step_count = wdb.step_count',
            'target_table': 'wm'
        },
        {
            'source_column': "['wm.sleep_hours']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'sleep_hours',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'wm.sleep_hours = wdb.sleep_hours',
            'target_table': 'wm'
        },
        {
            'source_column': "['wm.heart_rate']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'heart_rate',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'wm.heart_rate = wdb.heart_rate',
            'target_table': 'wm'
        },
        {
            'source_column': "['wm.battery_status']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'battery_status',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wm.battery_status = wdb.battery_status',
            'target_table': 'wm'
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
    mapping_details = table.get('mapping_details')
    if mapping_details is None:
        continue

    mapping_parts = mapping_details.split()
    if len(mapping_parts) < 2:
        continue

    source_table = mapping_parts[0]
    source_alias = mapping_parts[1]
    target_table = table.get('target_table')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') != source_alias:
            continue
        transformation = col_meta.get('transformation')
        target_column = col_meta.get('target_column')
        if transformation is None or target_column is None:
            continue
        rhs = transformation.split('=', 1)[1].strip()
        transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
