from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'patient_enrollment_bronze', 'target_alias': 'peb', 'mapping_details': 'patient_enrollment_raw_2000 pe', 'description': 'Bronze ingestion of patient enrollment records from patient_enrollment_raw_2000. Columns mapped: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'}, {'target_schema': 'bronze', 'target_table': 'clinical_visits_bronze', 'target_alias': 'cvb', 'mapping_details': 'clinical_visit_raw_2000 cv', 'description': 'Bronze ingestion of clinical visit records from clinical_visit_raw_2000. Columns mapped: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system.'}, {'target_schema': 'bronze', 'target_table': 'laboratory_tests_bronze', 'target_alias': 'ltb', 'mapping_details': 'lab_results_raw_2000 lr', 'description': 'Bronze ingestion of laboratory test results from lab_results_raw_2000. Columns mapped: lab_result_id, patient_id, sample_id, test_name, test_result, test_unit, reference_range, abnormal_flag, test_date, lab_name.'}, {'target_schema': 'bronze', 'target_table': 'drug_administrations_bronze', 'target_alias': 'dab', 'mapping_details': 'drug_administration_raw_2000 da', 'description': 'Bronze ingestion of drug administration events from drug_administration_raw_2000. Columns mapped: administration_id, patient_id, drug_code, dosage_mg, administration_date, administration_route, administered_by, batch_number.'}, {'target_schema': 'bronze', 'target_table': 'adverse_events_bronze', 'target_alias': 'aeb', 'mapping_details': 'adverse_events_raw_2000 ae', 'description': 'Bronze ingestion of adverse event records from adverse_events_raw_2000. Columns mapped: event_id, patient_id, event_type, severity, event_start_date, event_end_date, outcome, related_to_drug, hospitalization_required, reported_by.'}, {'target_schema': 'bronze', 'target_table': 'wearable_device_data_bronze', 'target_alias': 'wdb', 'mapping_details': 'wearable_monitoring_raw_2000 wm', 'description': 'Bronze ingestion of wearable monitoring records from wearable_monitoring_raw_2000. Columns mapped: device_record_id, patient_id, device_type, recorded_timestamp, glucose_level, step_count, sleep_hours, heart_rate, battery_status.'}], 'columns': [{'source_column': "['pe.patient_id']", 'source_type': 'varchar(20)', 'source_nullable': 'not_specified', 'target_column': 'patient_id', 'target_type': 'varchar(20)', 'target_nullable': 'not_specified', 'transformation': 'peb.patient_id = pe.patient_id', 'target_table': 'peb'}, {'source_column': "['pe.enrollment_date']", 'source_type': 'timestamp', 'source_nullable': 'not_specified', 'target_column': 'enrollment_date', 'target_type': 'timestamp', 'target_nullable': 'not_specified', 'transformation': 'peb.enrollment_date = pe.enrollment_date', 'target_table': 'peb'}, {'source_column': "['cv.patient_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_specified', 'target_column': 'patient_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_specified', 'transformation': 'cvb.patient_id = cv.patient_id', 'target_table': 'cvb'}, {'source_column': "['cv.visit_date']", 'source_type': 'timestamp', 'source_nullable': 'not_specified', 'target_column': 'visit_date', 'target_type': 'timestamp', 'target_nullable': 'not_specified', 'transformation': 'cvb.visit_date = cv.visit_date', 'target_table': 'cvb'}, {'source_column': "['lr.lab_result_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_specified', 'target_column': 'lab_result_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_specified', 'transformation': 'ltb.lab_result_id = lr.lab_result_id', 'target_table': 'ltb'}, {'source_column': "['lr.patient_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_specified', 'target_column': 'patient_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_specified', 'transformation': 'ltb.patient_id = lr.patient_id', 'target_table': 'ltb'}, {'source_column': "['da.administration_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_specified', 'target_column': 'administration_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_specified', 'transformation': 'dab.administration_id = da.administration_id', 'target_table': 'dab'}, {'source_column': "['da.patient_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_specified', 'target_column': 'patient_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_specified', 'transformation': 'dab.patient_id = da.patient_id', 'target_table': 'dab'}, {'source_column': "['ae.event_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_specified', 'target_column': 'event_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_specified', 'transformation': 'aeb.event_id = ae.event_id', 'target_table': 'aeb'}, {'source_column': "['ae.patient_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_specified', 'target_column': 'patient_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_specified', 'transformation': 'aeb.patient_id = ae.patient_id', 'target_table': 'aeb'}, {'source_column': "['wm.patient_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_specified', 'target_column': 'patient_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_specified', 'transformation': 'wdb.patient_id = wm.patient_id', 'target_table': 'wdb'}, {'source_column': "['wm.device_record_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not_specified', 'target_column': 'device_record_id', 'target_type': 'varchar(255)', 'target_nullable': 'not_specified', 'transformation': 'wdb.device_record_id = wm.device_record_id', 'target_table': 'wdb'}, {'source_column': "['wm.recorded_timestamp']", 'source_type': 'timestamp', 'source_nullable': 'not_specified', 'target_column': 'recorded_timestamp', 'target_type': 'timestamp', 'target_nullable': 'not_specified', 'transformation': 'wdb.recorded_timestamp = wm.recorded_timestamp', 'target_table': 'wdb'}, {'source_column': "['wm.heart_rate']", 'source_type': 'float64', 'source_nullable': 'not_specified', 'target_column': 'heart_rate', 'target_type': 'float64', 'target_nullable': 'not_specified', 'transformation': 'wdb.heart_rate = wm.heart_rate', 'target_table': 'wdb'}, {'source_column': "['wm.step_count']", 'source_type': 'int', 'source_nullable': 'not_specified', 'target_column': 'step_count', 'target_type': 'int', 'target_nullable': 'not_specified', 'transformation': 'wdb.step_count = wm.step_count', 'target_table': 'wdb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/clinical_trail/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata["runtime_config"]
base_path = runtime_config["base_path"]
target_path = runtime_config["target_path"]
read_format = runtime_config["read_format"]
write_format = runtime_config["write_format"]
write_mode = runtime_config["write_mode"]

for table in metadata["tables"]:
    mapping_details = table["mapping_details"].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table["target_table"]
    target_alias = table["target_alias"]

    reader = spark.read.format(read_format)
    if read_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata["columns"]:
        if col_meta.get("target_table") == target_alias:
            transformation = col_meta["transformation"]
            rhs = transformation.split("=", 1)[1].strip()
            target_column = col_meta["target_column"]
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == "csv":
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()