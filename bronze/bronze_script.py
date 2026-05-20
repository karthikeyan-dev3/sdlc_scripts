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
            'description': 'Bronze ingestion of patient enrollment records at row-level from patient_enrollment_raw_2000. Columns: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'clinical_visit_bronze',
            'target_alias': 'cvb',
            'mapping_details': 'clinical_visit_raw_2000 cv',
            'description': 'Bronze ingestion of clinical visit records at row-level from clinical_visit_raw_2000. Columns: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'lab_results_bronze',
            'target_alias': 'lrb',
            'mapping_details': 'lab_results_raw_2000 lr',
            'description': 'Bronze ingestion of laboratory test results at row-level from lab_results_raw_2000. Columns: lab_result_id, patient_id, sample_id, test_name, test_result, test_unit, reference_range, abnormal_flag, test_date, lab_name.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'drug_administration_bronze',
            'target_alias': 'dab',
            'mapping_details': 'drug_administration_raw_2000 da',
            'description': 'Bronze ingestion of drug administration events at row-level from drug_administration_raw_2000. Columns: administration_id, patient_id, drug_code, dosage_mg, administration_date, administration_route, administered_by, batch_number.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'adverse_events_bronze',
            'target_alias': 'aeb',
            'mapping_details': 'adverse_events_raw_2000 ae',
            'description': 'Bronze ingestion of adverse event records at row-level from adverse_events_raw_2000. Columns: event_id, patient_id, event_type, severity, event_start_date, event_end_date, outcome, related_to_drug, hospitalization_required, reported_by.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'wearable_monitoring_bronze',
            'target_alias': 'wmb',
            'mapping_details': 'wearable_monitoring_raw_2000 wm',
            'description': 'Bronze ingestion of wearable monitoring records at row-level from wearable_monitoring_raw_2000. Columns: device_record_id, patient_id, device_type, recorded_timestamp, glucose_level, step_count, sleep_hours, heart_rate, battery_status.'
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
            'transformation': 'peb.patient_id = pe.patient_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.trial_id = pe.trial_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.site_id = pe.site_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'peb.patient_name = pe.patient_name',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 'peb.gender = pe.gender',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'not specified',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'not specified',
            'transformation': 'peb.date_of_birth = pe.date_of_birth',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 'peb.country = pe.country',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'peb.enrollment_date = pe.enrollment_date',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.consent_status = pe.consent_status',
            'target_table': 'peb'
        },
        {
            'source_column': "['pe.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'not specified',
            'transformation': 'peb.source_system = pe.source_system',
            'target_table': 'peb'
        },
        {
            'source_column': "['cv.visit_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'visit_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cvb.visit_id = cv.visit_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cvb.patient_id = cv.patient_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.trial_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cvb.trial_id = cv.trial_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.visit_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'visit_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'cvb.visit_date = cv.visit_date',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.visit_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'visit_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cvb.visit_type = cv.visit_type',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.blood_pressure']",
            'source_type': 'float64',
            'source_nullable': 'not specified',
            'target_column': 'blood_pressure',
            'target_type': 'float64',
            'target_nullable': 'not specified',
            'transformation': 'cvb.blood_pressure = cv.blood_pressure',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.heart_rate']",
            'source_type': 'float64',
            'source_nullable': 'not specified',
            'target_column': 'heart_rate',
            'target_type': 'float64',
            'target_nullable': 'not specified',
            'transformation': 'cvb.heart_rate = cv.heart_rate',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.weight_kg']",
            'source_type': 'float64',
            'source_nullable': 'not specified',
            'target_column': 'weight_kg',
            'target_type': 'float64',
            'target_nullable': 'not specified',
            'transformation': 'cvb.weight_kg = cv.weight_kg',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.physician_notes']",
            'source_type': 'text',
            'source_nullable': 'not specified',
            'target_column': 'physician_notes',
            'target_type': 'text',
            'target_nullable': 'not specified',
            'transformation': 'cvb.physician_notes = cv.physician_notes',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cv.source_system']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cvb.source_system = cv.source_system',
            'target_table': 'cvb'
        },
        {
            'source_column': "['lr.lab_result_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'lab_result_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.lab_result_id = lr.lab_result_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lr.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.patient_id = lr.patient_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lr.sample_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'sample_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.sample_id = lr.sample_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lr.test_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'test_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.test_name = lr.test_name',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lr.test_result']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'test_result',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'lrb.test_result = lr.test_result',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lr.test_unit']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'test_unit',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.test_unit = lr.test_unit',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lr.reference_range']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'reference_range',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.reference_range = lr.reference_range',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lr.abnormal_flag']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'abnormal_flag',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.abnormal_flag = lr.abnormal_flag',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lr.test_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'test_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'lrb.test_date = lr.test_date',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lr.lab_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'lab_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.lab_name = lr.lab_name',
            'target_table': 'lrb'
        },
        {
            'source_column': "['da.administration_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'administration_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dab.administration_id = da.administration_id',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dab.patient_id = da.patient_id',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.drug_code']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'drug_code',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dab.drug_code = da.drug_code',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.dosage_mg']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'dosage_mg',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'dab.dosage_mg = da.dosage_mg',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.administration_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'administration_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'dab.administration_date = da.administration_date',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.administration_route']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'administration_route',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dab.administration_route = da.administration_route',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.administered_by']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'administered_by',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dab.administered_by = da.administered_by',
            'target_table': 'dab'
        },
        {
            'source_column': "['da.batch_number']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'batch_number',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dab.batch_number = da.batch_number',
            'target_table': 'dab'
        },
        {
            'source_column': "['ae.event_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'event_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_id = ae.event_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.patient_id = ae.patient_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.event_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'event_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_type = ae.event_type',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.severity']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'severity',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.severity = ae.severity',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.event_start_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'event_start_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_start_date = ae.event_start_date',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.event_end_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'event_end_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_end_date = ae.event_end_date',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.outcome']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'outcome',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.outcome = ae.outcome',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.related_to_drug']",
            'source_type': 'boolean',
            'source_nullable': 'not specified',
            'target_column': 'related_to_drug',
            'target_type': 'boolean',
            'target_nullable': 'not specified',
            'transformation': 'aeb.related_to_drug = ae.related_to_drug',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.hospitalization_required']",
            'source_type': 'boolean',
            'source_nullable': 'not specified',
            'target_column': 'hospitalization_required',
            'target_type': 'boolean',
            'target_nullable': 'not specified',
            'transformation': 'aeb.hospitalization_required = ae.hospitalization_required',
            'target_table': 'aeb'
        },
        {
            'source_column': "['ae.reported_by']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'reported_by',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.reported_by = ae.reported_by',
            'target_table': 'aeb'
        },
        {
            'source_column': "['wm.device_record_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'device_record_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wmb.device_record_id = wm.device_record_id',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wm.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wmb.patient_id = wm.patient_id',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wm.device_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'device_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wmb.device_type = wm.device_type',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wm.recorded_timestamp']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'recorded_timestamp',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'wmb.recorded_timestamp = wm.recorded_timestamp',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wm.glucose_level']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'glucose_level',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'wmb.glucose_level = wm.glucose_level',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wm.step_count']",
            'source_type': 'int',
            'source_nullable': 'not specified',
            'target_column': 'step_count',
            'target_type': 'int',
            'target_nullable': 'not specified',
            'transformation': 'wmb.step_count = wm.step_count',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wm.sleep_hours']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'sleep_hours',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'wmb.sleep_hours = wm.sleep_hours',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wm.heart_rate']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'heart_rate',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'wmb.heart_rate = wm.heart_rate',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wm.battery_status']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'battery_status',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wmb.battery_status = wm.battery_status',
            'target_table': 'wmb'
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

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    mapping_details = table['mapping_details'].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            transformation = col_meta['transformation']
            rhs = transformation.split('=', 1)[1].strip()
            target_column = col_meta['target_column']
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + "." + write_format)

job.commit()