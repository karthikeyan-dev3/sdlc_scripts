from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

import json
import sys
import uuid
import traceback
from datetime import datetime

from pyspark.sql import functions as F


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})


metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'branches_bronze', 'target_alias': 'bb', 'mapping_details': 'branch_master bm', 'description': 'Bronze raw ingestion of branch_master with columns: branch_id, branch_name, city, state, branch_type, manager_name, opening_year, contact_number.'}, {'target_schema': 'bronze', 'target_table': 'customers_bronze', 'target_alias': 'cb', 'mapping_details': 'customer_master cm', 'description': 'Bronze raw ingestion of customer_master with columns: customer_id, customer_name, gender, city, state, income_segment, annual_income, credit_score.'}, {'target_schema': 'bronze', 'target_table': 'loan_applications_bronze', 'target_alias': 'lab', 'mapping_details': 'loan_applications la', 'description': 'Bronze raw ingestion of loan_applications with columns: loan_id, customer_id, branch_id, loan_type, loan_amount, interest_rate, loan_status, application_date.'}, {'target_schema': 'bronze', 'target_table': 'loan_risk_assessment_bronze', 'target_alias': 'lrab', 'mapping_details': 'loan_risk_assessment lra', 'description': 'Bronze raw ingestion of loan_risk_assessment with columns: risk_id, loan_id, risk_score, default_probability, risk_category, evaluation_date, analyst_name, review_status.'}, {'target_schema': 'bronze', 'target_table': 'repayment_transactions_bronze', 'target_alias': 'rtb', 'mapping_details': 'repayment_transactions rt', 'description': 'Bronze raw ingestion of repayment_transactions with columns: transaction_id, loan_id, payment_date, payment_amount, payment_method, payment_status, remaining_balance, processed_by.'}], 'columns': [{'source_column': "['bm.branch_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'branch_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'bb.branch_id = bm.branch_id', 'target_table': 'bb'}, {'source_column': "['bm.branch_name']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'branch_name', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'bb.branch_name = bm.branch_name', 'target_table': 'bb'}, {'source_column': "['bm.city']", 'source_type': 'varchar(100)', 'source_nullable': 'null_accepted', 'target_column': 'city', 'target_type': 'varchar(100)', 'target_nullable': 'null_accepted', 'transformation': 'bb.city = bm.city', 'target_table': 'bb'}, {'source_column': "['bm.state']", 'source_type': 'varchar(100)', 'source_nullable': 'null_accepted', 'target_column': 'state', 'target_type': 'varchar(100)', 'target_nullable': 'null_accepted', 'transformation': 'bb.state = bm.state', 'target_table': 'bb'}, {'source_column': "['bm.branch_type']", 'source_type': 'varchar(50)', 'source_nullable': 'null_accepted', 'target_column': 'branch_type', 'target_type': 'varchar(50)', 'target_nullable': 'null_accepted', 'transformation': 'bb.branch_type = bm.branch_type', 'target_table': 'bb'}, {'source_column': "['bm.manager_name']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'manager_name', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'bb.manager_name = bm.manager_name', 'target_table': 'bb'}, {'source_column': "['bm.opening_year']", 'source_type': 'int', 'source_nullable': 'null_accepted', 'target_column': 'opening_year', 'target_type': 'int', 'target_nullable': 'null_accepted', 'transformation': 'bb.opening_year = bm.opening_year', 'target_table': 'bb'}, {'source_column': "['bm.contact_number']", 'source_type': 'varchar(15)', 'source_nullable': 'null_accepted', 'target_column': 'contact_number', 'target_type': 'varchar(15)', 'target_nullable': 'null_accepted', 'transformation': 'bb.contact_number = bm.contact_number', 'target_table': 'bb'}, {'source_column': "['cm.customer_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'customer_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'cb.customer_id = cm.customer_id', 'target_table': 'cb'}, {'source_column': "['cm.customer_name']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'customer_name', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'cb.customer_name = cm.customer_name', 'target_table': 'cb'}, {'source_column': "['cm.gender']", 'source_type': 'varchar(10)', 'source_nullable': 'null_accepted', 'target_column': 'gender', 'target_type': 'varchar(10)', 'target_nullable': 'null_accepted', 'transformation': 'cb.gender = cm.gender', 'target_table': 'cb'}, {'source_column': "['cm.city']", 'source_type': 'varchar(100)', 'source_nullable': 'null_accepted', 'target_column': 'city', 'target_type': 'varchar(100)', 'target_nullable': 'null_accepted', 'transformation': 'cb.city = cm.city', 'target_table': 'cb'}, {'source_column': "['cm.state']", 'source_type': 'varchar(100)', 'source_nullable': 'null_accepted', 'target_column': 'state', 'target_type': 'varchar(100)', 'target_nullable': 'null_accepted', 'transformation': 'cb.state = cm.state', 'target_table': 'cb'}, {'source_column': "['cm.income_segment']", 'source_type': 'varchar(10)', 'source_nullable': 'null_accepted', 'target_column': 'income_segment', 'target_type': 'varchar(10)', 'target_nullable': 'null_accepted', 'transformation': 'cb.income_segment = cm.income_segment', 'target_table': 'cb'}, {'source_column': "['cm.annual_income']", 'source_type': 'int', 'source_nullable': 'null_accepted', 'target_column': 'annual_income', 'target_type': 'int', 'target_nullable': 'null_accepted', 'transformation': 'cb.annual_income = cm.annual_income', 'target_table': 'cb'}, {'source_column': "['cm.credit_score']", 'source_type': 'int', 'source_nullable': 'null_accepted', 'target_column': 'credit_score', 'target_type': 'int', 'target_nullable': 'null_accepted', 'transformation': 'cb.credit_score = cm.credit_score', 'target_table': 'cb'}, {'source_column': "['la.loan_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'loan_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'lab.loan_id = la.loan_id', 'target_table': 'lab'}, {'source_column': "['la.customer_id']", 'source_type': 'varchar(10)', 'source_nullable': 'null_accepted', 'target_column': 'customer_id', 'target_type': 'varchar(10)', 'target_nullable': 'null_accepted', 'transformation': 'lab.customer_id = la.customer_id', 'target_table': 'lab'}, {'source_column': "['la.branch_id']", 'source_type': 'varchar(10)', 'source_nullable': 'null_accepted', 'target_column': 'branch_id', 'target_type': 'varchar(10)', 'target_nullable': 'null_accepted', 'transformation': 'lab.branch_id = la.branch_id', 'target_table': 'lab'}, {'source_column': "['la.loan_type']", 'source_type': 'varchar(50)', 'source_nullable': 'null_accepted', 'target_column': 'loan_type', 'target_type': 'varchar(50)', 'target_nullable': 'null_accepted', 'transformation': 'lab.loan_type = la.loan_type', 'target_table': 'lab'}, {'source_column': "['la.loan_amount']", 'source_type': 'int', 'source_nullable': 'null_accepted', 'target_column': 'loan_amount', 'target_type': 'int', 'target_nullable': 'null_accepted', 'transformation': 'lab.loan_amount = la.loan_amount', 'target_table': 'lab'}, {'source_column': "['la.interest_rate']", 'source_type': 'float', 'source_nullable': 'null_accepted', 'target_column': 'interest_rate', 'target_type': 'float', 'target_nullable': 'null_accepted', 'transformation': 'lab.interest_rate = la.interest_rate', 'target_table': 'lab'}, {'source_column': "['la.loan_status']", 'source_type': 'varchar(20)', 'source_nullable': 'null_accepted', 'target_column': 'loan_status', 'target_type': 'varchar(20)', 'target_nullable': 'null_accepted', 'transformation': 'lab.loan_status = la.loan_status', 'target_table': 'lab'}, {'source_column': "['la.application_date']", 'source_type': 'date', 'source_nullable': 'null_accepted', 'target_column': 'application_date', 'target_type': 'date', 'target_nullable': 'null_accepted', 'transformation': 'lab.application_date = la.application_date', 'target_table': 'lab'}, {'source_column': "['lra.risk_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'risk_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'lrab.risk_id = lra.risk_id', 'target_table': 'lrab'}, {'source_column': "['lra.loan_id']", 'source_type': 'varchar(10)', 'source_nullable': 'null_accepted', 'target_column': 'loan_id', 'target_type': 'varchar(10)', 'target_nullable': 'null_accepted', 'transformation': 'lrab.loan_id = lra.loan_id', 'target_table': 'lrab'}, {'source_column': "['lra.risk_score']", 'source_type': 'int', 'source_nullable': 'null_accepted', 'target_column': 'risk_score', 'target_type': 'int', 'target_nullable': 'null_accepted', 'transformation': 'lrab.risk_score = lra.risk_score', 'target_table': 'lrab'}, {'source_column': "['lra.default_probability']", 'source_type': 'double', 'source_nullable': 'null_accepted', 'target_column': 'default_probability', 'target_type': 'double', 'target_nullable': 'null_accepted', 'transformation': 'lrab.default_probability = lra.default_probability', 'target_table': 'lrab'}, {'source_column': "['lra.risk_category']", 'source_type': 'varchar(10)', 'source_nullable': 'null_accepted', 'target_column': 'risk_category', 'target_type': 'varchar(10)', 'target_nullable': 'null_accepted', 'transformation': 'lrab.risk_category = lra.risk_category', 'target_table': 'lrab'}, {'source_column': "['lra.evaluation_date']", 'source_type': 'date', 'source_nullable': 'null_accepted', 'target_column': 'evaluation_date', 'target_type': 'date', 'target_nullable': 'null_accepted', 'transformation': 'lrab.evaluation_date = lra.evaluation_date', 'target_table': 'lrab'}, {'source_column': "['lra.analyst_name']", 'source_type': 'varchar(50)', 'source_nullable': 'null_accepted', 'target_column': 'analyst_name', 'target_type': 'varchar(50)', 'target_nullable': 'null_accepted', 'transformation': 'lrab.analyst_name = lra.analyst_name', 'target_table': 'lrab'}, {'source_column': "['lra.review_status']", 'source_type': 'varchar(10)', 'source_nullable': 'null_accepted', 'target_column': 'review_status', 'target_type': 'varchar(10)', 'target_nullable': 'null_accepted', 'transformation': 'lrab.review_status = lra.review_status', 'target_table': 'lrab'}, {'source_column': "['rt.transaction_id']", 'source_type': 'varchar(20)', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'varchar(20)', 'target_nullable': 'not_null', 'transformation': 'rtb.transaction_id = rt.transaction_id', 'target_table': 'rtb'}, {'source_column': "['rt.loan_id']", 'source_type': 'varchar(20)', 'source_nullable': 'null_accepted', 'target_column': 'loan_id', 'target_type': 'varchar(20)', 'target_nullable': 'null_accepted', 'transformation': 'rtb.loan_id = rt.loan_id', 'target_table': 'rtb'}, {'source_column': "['rt.payment_date']", 'source_type': 'date', 'source_nullable': 'null_accepted', 'target_column': 'payment_date', 'target_type': 'date', 'target_nullable': 'null_accepted', 'transformation': 'rtb.payment_date = rt.payment_date', 'target_table': 'rtb'}, {'source_column': "['rt.payment_amount']", 'source_type': 'int', 'source_nullable': 'null_accepted', 'target_column': 'payment_amount', 'target_type': 'int', 'target_nullable': 'null_accepted', 'transformation': 'rtb.payment_amount = rt.payment_amount', 'target_table': 'rtb'}, {'source_column': "['rt.payment_method']", 'source_type': 'varchar(20)', 'source_nullable': 'null_accepted', 'target_column': 'payment_method', 'target_type': 'varchar(20)', 'target_nullable': 'null_accepted', 'transformation': 'rtb.payment_method = rt.payment_method', 'target_table': 'rtb'}, {'source_column': "['rt.payment_status']", 'source_type': 'varchar(20)', 'source_nullable': 'null_accepted', 'target_column': 'payment_status', 'target_type': 'varchar(20)', 'target_nullable': 'null_accepted', 'transformation': 'rtb.payment_status = rt.payment_status', 'target_table': 'rtb'}, {'source_column': "['rt.remaining_balance']", 'source_type': 'int', 'source_nullable': 'null_accepted', 'target_column': 'remaining_balance', 'target_type': 'int', 'target_nullable': 'null_accepted', 'transformation': 'rtb.remaining_balance = rt.remaining_balance', 'target_table': 'rtb'}, {'source_column': "['rt.processed_by']", 'source_type': 'varchar(50)', 'source_nullable': 'null_accepted', 'target_column': 'processed_by', 'target_type': 'varchar(50)', 'target_nullable': 'null_accepted', 'transformation': 'rtb.processed_by = rt.processed_by', 'target_table': 'rtb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}


def _log(level, message, **kwargs):
    payload = {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "level": level,
        "message": message,
    }
    if kwargs:
        payload.update(kwargs)
    print(json.dumps(payload, default=str))


def _safe_get(dct, *keys, default=None):
    cur = dct
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _parse_mapping_details(mapping_details):
    if not mapping_details or not isinstance(mapping_details, str):
        return None, None
    parts = [p for p in mapping_details.strip().split(" ") if p]
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[-1]


def _normalize_source_column_entry(source_column):
    if source_column is None:
        return []
    if isinstance(source_column, list):
        return [str(x) for x in source_column]
    if isinstance(source_column, str):
        s = source_column.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s.replace("'", '"'))
                if isinstance(arr, list):
                    return [str(x) for x in arr]
            except Exception:
                pass
        return [s]
    return [str(source_column)]


def _extract_eq_transformation_rhs(transformation):
    if not transformation or not isinstance(transformation, str):
        return None
    if "=" not in transformation:
        return None
    rhs = transformation.split("=", 1)[1].strip()
    return rhs if rhs else None


def _strip_qualifier(col_expr):
    if col_expr is None:
        return None
    s = str(col_expr).strip()
    if s.startswith("`") and s.endswith("`"):
        s = s[1:-1]
    if "." in s:
        return s.split(".")[-1]
    return s


def _build_projection_for_alias(columns_meta, target_alias):
    rules = [c for c in (columns_meta or []) if c.get("target_table") == target_alias]
    if not rules:
        return None

    exprs = []
    for r in rules:
        target_col = r.get("target_column")
        rhs = _extract_eq_transformation_rhs(r.get("transformation"))
        src_candidates = _normalize_source_column_entry(r.get("source_column"))

        chosen = None
        if rhs:
            chosen = rhs
        elif src_candidates:
            chosen = src_candidates[0]

        if chosen is None:
            continue

        src_col = _strip_qualifier(chosen)
        if not target_col:
            continue
        exprs.append(F.col(src_col).alias(str(target_col)))

    return exprs if exprs else None


def _apply_read_options(reader, read_options):
    if isinstance(read_options, dict):
        for k, v in read_options.items():
            reader = reader.option(str(k), str(v))
    return reader


def _apply_write_options(writer, write_options):
    if isinstance(write_options, dict):
        for k, v in write_options.items():
            writer = writer.option(str(k), str(v))
    return writer


def _join_paths(base, leaf):
    if base is None:
        base = ""
    if leaf is None:
        leaf = ""
    b = str(base)
    l = str(leaf)
    if not b.endswith("/"):
        b += "/"
    if l.startswith("/"):
        l = l[1:]
    return b + l


def _process_table(table_meta, metadata_all, run_id, runtime_conf):
    table_name = table_meta.get("target_table")
    target_schema = table_meta.get("target_schema")
    target_alias = table_meta.get("target_alias")
    mapping_details = table_meta.get("mapping_details")

    source_table, source_alias = _parse_mapping_details(mapping_details)

    base_path = runtime_conf.get("base_path")
    target_base_path = runtime_conf.get("target_path")

    read_format = runtime_conf.get("read_format")
    write_format = runtime_conf.get("write_format")
    write_mode = runtime_conf.get("write_mode")

    read_options = runtime_conf.get("read_options")
    write_options = runtime_conf.get("write_options")

    if not base_path or not target_base_path or not read_format or not write_format or not write_mode:
        raise ValueError("Missing required runtime_config keys")

    if not source_table:
        raise ValueError("Missing source table in mapping_details")

    source_path = table_meta.get("source_path")
    if not source_path:
        source_path = _join_paths(base_path, source_table)

    target_path = table_meta.get("target_path")
    if not target_path:
        leaf = table_name
        if target_schema:
            leaf = _join_paths(str(target_schema), str(table_name))
        target_path = _join_paths(target_base_path, leaf)

    _log(
        "INFO",
        "table_start",
        run_id=run_id,
        target_schema=target_schema,
        target_table=table_name,
        target_alias=target_alias,
        source_table=source_table,
        source_alias=source_alias,
        source_path=source_path,
        target_path=target_path,
        read_format=read_format,
        write_format=write_format,
        write_mode=write_mode,
    )

    reader = spark.read.format(str(read_format))
    reader = _apply_read_options(reader, read_options)

    df_raw = reader.load(str(source_path))

    projection = _build_projection_for_alias(metadata_all.get("columns"), target_alias)
    if projection:
        df_out = df_raw.select(*projection)
    else:
        df_out = df_raw

    writer = df_out.write.format(str(write_format)).mode(str(write_mode))
    writer = _apply_write_options(writer, write_options)

    partition_columns = None
    if isinstance(table_meta.get("partition_columns"), list):
        partition_columns = table_meta.get("partition_columns")
    elif isinstance(runtime_conf.get("partition_columns"), list):
        partition_columns = runtime_conf.get("partition_columns")

    if partition_columns:
        writer = writer.partitionBy(*[str(c) for c in partition_columns])

    writer.save(str(target_path))

    _log(
        "INFO",
        "table_success",
        run_id=run_id,
        target_schema=target_schema,
        target_table=table_name,
        target_path=target_path,
        record_count=df_out.count(),
    )


runtime_config = metadata.get("runtime_config", {})
fail_fast = bool(runtime_config.get("fail_fast", False))
run_id = str(runtime_config.get("run_id") or uuid.uuid4())

_log("INFO", "job_start", run_id=run_id)

failures = []
for t in metadata.get("tables", []):
    try:
        _process_table(t, metadata, run_id, runtime_config)
    except Exception as e:
        err = {
            "run_id": run_id,
            "target_table": t.get("target_table"),
            "target_alias": t.get("target_alias"),
            "error": str(e),
            "traceback": traceback.format_exc(),
        }
        failures.append(err)
        _log("ERROR", "table_failed", **err)
        if fail_fast:
            raise

if failures:
    _log("WARN", "job_completed_with_failures", run_id=run_id, failures=failures)
else:
    _log("INFO", "job_success", run_id=run_id)

job.commit()
