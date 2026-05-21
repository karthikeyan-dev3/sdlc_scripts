from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

import sys
import json
import uuid
import traceback
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})


metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'branch_master_bronze', 'target_alias': 'bmb', 'mapping_details': 'branch_master bm', 'description': 'Bronze ingestion of branch_master with columns: branch_id, branch_name, city, state, branch_type, manager_name, opening_year, contact_number.'}, {'target_schema': 'bronze', 'target_table': 'customer_master_bronze', 'target_alias': 'cmb', 'mapping_details': 'customer_master cm', 'description': 'Bronze ingestion of customer_master with columns: customer_id, customer_name, gender, city, state, income_segment, annual_income, credit_score.'}, {'target_schema': 'bronze', 'target_table': 'loan_applications_bronze', 'target_alias': 'lab', 'mapping_details': 'loan_applications la', 'description': 'Bronze ingestion of loan_applications with columns: loan_id, customer_id, branch_id, loan_type, loan_amount, interest_rate, loan_status, application_date.'}, {'target_schema': 'bronze', 'target_table': 'loan_risk_assessment_bronze', 'target_alias': 'lrab', 'mapping_details': 'loan_risk_assessment lra', 'description': 'Bronze ingestion of loan_risk_assessment with columns: risk_id, loan_id, risk_score, default_probability, risk_category, evaluation_date, analyst_name, review_status.'}, {'target_schema': 'bronze', 'target_table': 'repayment_transactions_bronze', 'target_alias': 'rtb', 'mapping_details': 'repayment_transactions rt', 'description': 'Bronze ingestion of repayment_transactions with columns: transaction_id, loan_id, payment_date, payment_amount, payment_method, payment_status, remaining_balance, processed_by.'}], 'columns': [{'source_column': "['bmb.branch_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_accepted', 'target_column': 'branch_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_accepted', 'transformation': 'bm.branch_id = bmb.branch_id', 'target_table': 'bm'}, {'source_column': "['bmb.branch_name']", 'source_type': 'varchar(255)', 'source_nullable': 'accepted', 'target_column': 'branch_name', 'target_type': 'varchar(255)', 'target_nullable': 'accepted', 'transformation': 'bm.branch_name = bmb.branch_name', 'target_table': 'bm'}, {'source_column': "['bmb.city']", 'source_type': 'varchar(100)', 'source_nullable': 'accepted', 'target_column': 'city', 'target_type': 'varchar(100)', 'target_nullable': 'accepted', 'transformation': 'bm.city = bmb.city', 'target_table': 'bm'}, {'source_column': "['bmb.state']", 'source_type': 'varchar(100)', 'source_nullable': 'accepted', 'target_column': 'state', 'target_type': 'varchar(100)', 'target_nullable': 'accepted', 'transformation': 'bm.state = bmb.state', 'target_table': 'bm'}, {'source_column': "['bmb.branch_type']", 'source_type': 'varchar(50)', 'source_nullable': 'accepted', 'target_column': 'branch_type', 'target_type': 'varchar(50)', 'target_nullable': 'accepted', 'transformation': 'bm.branch_type = bmb.branch_type', 'target_table': 'bm'}, {'source_column': "['bmb.manager_name']", 'source_type': 'varchar(255)', 'source_nullable': 'accepted', 'target_column': 'manager_name', 'target_type': 'varchar(255)', 'target_nullable': 'accepted', 'transformation': 'bm.manager_name = bmb.manager_name', 'target_table': 'bm'}, {'source_column': "['bmb.opening_year']", 'source_type': 'int', 'source_nullable': 'accepted', 'target_column': 'opening_year', 'target_type': 'int', 'target_nullable': 'accepted', 'transformation': 'bm.opening_year = bmb.opening_year', 'target_table': 'bm'}, {'source_column': "['bmb.contact_number']", 'source_type': 'varchar(15)', 'source_nullable': 'accepted', 'target_column': 'contact_number', 'target_type': 'varchar(15)', 'target_nullable': 'accepted', 'transformation': 'bm.contact_number = bmb.contact_number', 'target_table': 'bm'}, {'source_column': "['cmb.customer_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_accepted', 'target_column': 'customer_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_accepted', 'transformation': 'cm.customer_id = cmb.customer_id', 'target_table': 'cm'}, {'source_column': "['cmb.customer_name']", 'source_type': 'varchar(255)', 'source_nullable': 'accepted', 'target_column': 'customer_name', 'target_type': 'varchar(255)', 'target_nullable': 'accepted', 'transformation': 'cm.customer_name = cmb.customer_name', 'target_table': 'cm'}, {'source_column': "['cmb.gender']", 'source_type': 'varchar(10)', 'source_nullable': 'accepted', 'target_column': 'gender', 'target_type': 'varchar(10)', 'target_nullable': 'accepted', 'transformation': 'cm.gender = cmb.gender', 'target_table': 'cm'}, {'source_column': "['cmb.city']", 'source_type': 'varchar(100)', 'source_nullable': 'accepted', 'target_column': 'city', 'target_type': 'varchar(100)', 'target_nullable': 'accepted', 'transformation': 'cm.city = cmb.city', 'target_table': 'cm'}, {'source_column': "['cmb.state']", 'source_type': 'varchar(100)', 'source_nullable': 'accepted', 'target_column': 'state', 'target_type': 'varchar(100)', 'target_nullable': 'accepted', 'transformation': 'cm.state = cmb.state', 'target_table': 'cm'}, {'source_column': "['cmb.income_segment']", 'source_type': 'varchar(10)', 'source_nullable': 'accepted', 'target_column': 'income_segment', 'target_type': 'varchar(10)', 'target_nullable': 'accepted', 'transformation': 'cm.income_segment = cmb.income_segment', 'target_table': 'cm'}, {'source_column': "['cmb.annual_income']", 'source_type': 'int', 'source_nullable': 'accepted', 'target_column': 'annual_income', 'target_type': 'int', 'target_nullable': 'accepted', 'transformation': 'cm.annual_income = cmb.annual_income', 'target_table': 'cm'}, {'source_column': "['cmb.credit_score']", 'source_type': 'int', 'source_nullable': 'accepted', 'target_column': 'credit_score', 'target_type': 'int', 'target_nullable': 'accepted', 'transformation': 'cm.credit_score = cmb.credit_score', 'target_table': 'cm'}, {'source_column': "['lab.loan_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_accepted', 'target_column': 'loan_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_accepted', 'transformation': 'la.loan_id = lab.loan_id', 'target_table': 'la'}, {'source_column': "['lab.customer_id']", 'source_type': 'varchar(10)', 'source_nullable': 'accepted', 'target_column': 'customer_id', 'target_type': 'varchar(10)', 'target_nullable': 'accepted', 'transformation': 'la.customer_id = lab.customer_id', 'target_table': 'la'}, {'source_column': "['lab.branch_id']", 'source_type': 'varchar(10)', 'source_nullable': 'accepted', 'target_column': 'branch_id', 'target_type': 'varchar(10)', 'target_nullable': 'accepted', 'transformation': 'la.branch_id = lab.branch_id', 'target_table': 'la'}, {'source_column': "['lab.loan_type']", 'source_type': 'varchar(50)', 'source_nullable': 'accepted', 'target_column': 'loan_type', 'target_type': 'varchar(50)', 'target_nullable': 'accepted', 'transformation': 'la.loan_type = lab.loan_type', 'target_table': 'la'}, {'source_column': "['lab.loan_amount']", 'source_type': 'int', 'source_nullable': 'accepted', 'target_column': 'loan_amount', 'target_type': 'int', 'target_nullable': 'accepted', 'transformation': 'la.loan_amount = lab.loan_amount', 'target_table': 'la'}, {'source_column': "['lab.interest_rate']", 'source_type': 'float', 'source_nullable': 'accepted', 'target_column': 'interest_rate', 'target_type': 'float', 'target_nullable': 'accepted', 'transformation': 'la.interest_rate = lab.interest_rate', 'target_table': 'la'}, {'source_column': "['lab.loan_status']", 'source_type': 'varchar(20)', 'source_nullable': 'accepted', 'target_column': 'loan_status', 'target_type': 'varchar(20)', 'target_nullable': 'accepted', 'transformation': 'la.loan_status = lab.loan_status', 'target_table': 'la'}, {'source_column': "['lab.application_date']", 'source_type': 'date', 'source_nullable': 'accepted', 'target_column': 'application_date', 'target_type': 'date', 'target_nullable': 'accepted', 'transformation': 'la.application_date = lab.application_date', 'target_table': 'la'}, {'source_column': "['lrab.risk_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_accepted', 'target_column': 'risk_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_accepted', 'transformation': 'lra.risk_id = lrab.risk_id', 'target_table': 'lra'}, {'source_column': "['lrab.loan_id']", 'source_type': 'varchar(10)', 'source_nullable': 'accepted', 'target_column': 'loan_id', 'target_type': 'varchar(10)', 'target_nullable': 'accepted', 'transformation': 'lra.loan_id = lrab.loan_id', 'target_table': 'lra'}, {'source_column': "['lrab.risk_score']", 'source_type': 'int', 'source_nullable': 'accepted', 'target_column': 'risk_score', 'target_type': 'int', 'target_nullable': 'accepted', 'transformation': 'lra.risk_score = lrab.risk_score', 'target_table': 'lra'}, {'source_column': "['lrab.default_probability']", 'source_type': 'double', 'source_nullable': 'accepted', 'target_column': 'default_probability', 'target_type': 'double', 'target_nullable': 'accepted', 'transformation': 'lra.default_probability = lrab.default_probability', 'target_table': 'lra'}, {'source_column': "['lrab.risk_category']", 'source_type': 'varchar(10)', 'source_nullable': 'accepted', 'target_column': 'risk_category', 'target_type': 'varchar(10)', 'target_nullable': 'accepted', 'transformation': 'lra.risk_category = lrab.risk_category', 'target_table': 'lra'}, {'source_column': "['lrab.evaluation_date']", 'source_type': 'date', 'source_nullable': 'accepted', 'target_column': 'evaluation_date', 'target_type': 'date', 'target_nullable': 'accepted', 'transformation': 'lra.evaluation_date = lrab.evaluation_date', 'target_table': 'lra'}, {'source_column': "['lrab.analyst_name']", 'source_type': 'varchar(50)', 'source_nullable': 'accepted', 'target_column': 'analyst_name', 'target_type': 'varchar(50)', 'target_nullable': 'accepted', 'transformation': 'lra.analyst_name = lrab.analyst_name', 'target_table': 'lra'}, {'source_column': "['lrab.review_status']", 'source_type': 'varchar(10)', 'source_nullable': 'accepted', 'target_column': 'review_status', 'target_type': 'varchar(10)', 'target_nullable': 'accepted', 'transformation': 'lra.review_status = lrab.review_status', 'target_table': 'lra'}, {'source_column': "['rtb.transaction_id']", 'source_type': 'varchar(20)', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'varchar(20)', 'target_nullable': 'not_accepted', 'transformation': 'rt.transaction_id = rtb.transaction_id', 'target_table': 'rt'}, {'source_column': "['rtb.loan_id']", 'source_type': 'varchar(20)', 'source_nullable': 'accepted', 'target_column': 'loan_id', 'target_type': 'varchar(20)', 'target_nullable': 'accepted', 'transformation': 'rt.loan_id = rtb.loan_id', 'target_table': 'rt'}, {'source_column': "['rtb.payment_date']", 'source_type': 'date', 'source_nullable': 'accepted', 'target_column': 'payment_date', 'target_type': 'date', 'target_nullable': 'accepted', 'transformation': 'rt.payment_date = rtb.payment_date', 'target_table': 'rt'}, {'source_column': "['rtb.payment_amount']", 'source_type': 'int', 'source_nullable': 'accepted', 'target_column': 'payment_amount', 'target_type': 'int', 'target_nullable': 'accepted', 'transformation': 'rt.payment_amount = rtb.payment_amount', 'target_table': 'rt'}, {'source_column': "['rtb.payment_method']", 'source_type': 'varchar(50)', 'source_nullable': 'accepted', 'target_column': 'payment_method', 'target_type': 'varchar(50)', 'target_nullable': 'accepted', 'transformation': 'rt.payment_method = rtb.payment_method', 'target_table': 'rt'}, {'source_column': "['rtb.payment_status']", 'source_type': 'varchar(20)', 'source_nullable': 'accepted', 'target_column': 'payment_status', 'target_type': 'varchar(20)', 'target_nullable': 'accepted', 'transformation': 'rt.payment_status = rtb.payment_status', 'target_table': 'rt'}, {'source_column': "['rtb.remaining_balance']", 'source_type': 'int', 'source_nullable': 'accepted', 'target_column': 'remaining_balance', 'target_type': 'int', 'target_nullable': 'accepted', 'transformation': 'rt.remaining_balance = rtb.remaining_balance', 'target_table': 'rt'}, {'source_column': "['rtb.processed_by']", 'source_type': 'varchar(50)', 'source_nullable': 'accepted', 'target_column': 'processed_by', 'target_type': 'varchar(50)', 'target_nullable': 'accepted', 'transformation': 'rt.processed_by = rtb.processed_by', 'target_table': 'rt'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(level: str, message: str, **kwargs):
    payload = {
        "ts": _utc_now_iso(),
        "level": level,
        "msg": message,
    }
    if kwargs:
        payload.update(kwargs)
    print(json.dumps(payload, default=str))


def _safe_get(dct, keys, default=None):
    cur = dct
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def _is_empty(x) -> bool:
    if x is None:
        return True
    if isinstance(x, (list, dict, str)) and len(x) == 0:
        return True
    return False


def _apply_options(reader_or_writer, options_dict: dict):
    obj = reader_or_writer
    if isinstance(options_dict, dict):
        for k, v in options_dict.items():
            if v is None:
                obj = obj.option(str(k), "")
            else:
                obj = obj.option(str(k), str(v))
    return obj


def _parse_mapping_details(mapping_details: str):
    src_table = None
    src_alias = None
    if isinstance(mapping_details, str):
        tokens = [t for t in mapping_details.strip().split(" ") if t]
        if len(tokens) >= 1:
            src_table = tokens[0]
        if len(tokens) >= 2:
            src_alias = tokens[1]
    return src_table, src_alias


def _build_source_path(runtime_cfg: dict, table_cfg: dict):
    base_path = _safe_get(runtime_cfg, ["base_path"], None)
    src_path = table_cfg.get("source_path")
    if not _is_empty(src_path):
        return src_path
    src_table = table_cfg.get("source_table")
    if _is_empty(src_table):
        md = table_cfg.get("mapping_details")
        src_table, _ = _parse_mapping_details(md)
    if _is_empty(base_path) or _is_empty(src_table):
        return None
    if base_path.endswith("/"):
        return f"{base_path}{src_table}/"
    return f"{base_path}/{src_table}/"


def _build_target_path(runtime_cfg: dict, table_cfg: dict):
    target_base = _safe_get(runtime_cfg, ["target_path"], None)
    tgt_table = table_cfg.get("target_table")
    if _is_empty(target_base) or _is_empty(tgt_table):
        return None
    if target_base.endswith("/"):
        return f"{target_base}{tgt_table}/"
    return f"{target_base}/{tgt_table}/"


def _resolve_read_format(runtime_cfg: dict, table_cfg: dict):
    return table_cfg.get("read_format") or runtime_cfg.get("read_format")


def _resolve_write_format(runtime_cfg: dict, table_cfg: dict):
    return table_cfg.get("write_format") or runtime_cfg.get("write_format")


def _resolve_write_mode(runtime_cfg: dict, table_cfg: dict):
    return table_cfg.get("write_mode") or runtime_cfg.get("write_mode")


def _read_source_df(runtime_cfg: dict, table_cfg: dict) -> DataFrame:
    read_format = _resolve_read_format(runtime_cfg, table_cfg)
    if _is_empty(read_format):
        raise ValueError("read_format is required in metadata")

    source_path = _build_source_path(runtime_cfg, table_cfg)
    if _is_empty(source_path):
        raise ValueError("source_path/base_path+source_table is required in metadata")

    read_options = table_cfg.get("read_options")
    if _is_empty(read_options):
        read_options = runtime_cfg.get("read_options")

    reader = spark.read.format(str(read_format))

    if str(read_format).lower() == "csv" and _is_empty(read_options):
        reader = reader.option("header", "true").option("inferSchema", "true")
    else:
        reader = _apply_options(reader, read_options if isinstance(read_options, dict) else {})

    df = reader.load(source_path)
    return df


def _apply_transformations_if_any(df: DataFrame, table_cfg: dict, all_columns_meta: list) -> DataFrame:
    transformations = table_cfg.get("transformations")
    if _is_empty(transformations):
        return df

    if not isinstance(transformations, list):
        raise ValueError("transformations must be a list when provided")

    out_df = df
    for t in transformations:
        if not isinstance(t, dict):
            continue
        ttype = t.get("type")
        if _is_empty(ttype):
            continue
        ttype_l = str(ttype).lower()

        if ttype_l in ("select_expr", "selectexpr"):
            exprs = t.get("expressions") or t.get("exprs")
            if not _is_empty(exprs) and isinstance(exprs, list):
                out_df = out_df.selectExpr(*[str(e) for e in exprs])

        elif ttype_l in ("with_column", "withcolumn"):
            col_name = t.get("name") or t.get("column")
            expr = t.get("expr") or t.get("expression")
            if not _is_empty(col_name) and not _is_empty(expr):
                out_df = out_df.withColumn(str(col_name), F.expr(str(expr)))

        elif ttype_l in ("drop", "drop_columns", "dropcolumns"):
            cols = t.get("columns") or t.get("cols")
            if not _is_empty(cols) and isinstance(cols, list):
                out_df = out_df.drop(*[str(c) for c in cols])

        elif ttype_l in ("rename", "rename_columns", "renamecolumns"):
            mappings = t.get("mappings")
            if isinstance(mappings, dict):
                for src, dst in mappings.items():
                    out_df = out_df.withColumnRenamed(str(src), str(dst))
            elif isinstance(mappings, list):
                for m in mappings:
                    if isinstance(m, dict) and m.get("from") is not None and m.get("to") is not None:
                        out_df = out_df.withColumnRenamed(str(m.get("from")), str(m.get("to")))

        else:
            continue

    return out_df


def _add_operational_metadata_if_any(df: DataFrame, table_cfg: dict, runtime_cfg: dict, batch_id: str) -> DataFrame:
    op_meta = table_cfg.get("operational_metadata")
    if _is_empty(op_meta):
        op_meta = runtime_cfg.get("operational_metadata")
    if _is_empty(op_meta) or not isinstance(op_meta, dict):
        return df

    out_df = df
    for col_name, spec in op_meta.items():
        if _is_empty(col_name):
            continue
        if spec is None:
            out_df = out_df.withColumn(str(col_name), F.lit(None))
            continue
        if isinstance(spec, str):
            s = spec.lower().strip()
            if s in ("ingestion_timestamp", "ingest_timestamp", "load_timestamp", "ingestion_ts"):
                out_df = out_df.withColumn(str(col_name), F.current_timestamp())
            elif s in ("batch_id", "batchid"):
                out_df = out_df.withColumn(str(col_name), F.lit(batch_id))
            elif s in ("source_file", "sourcefile", "input_file", "inputfile"):
                out_df = out_df.withColumn(str(col_name), F.input_file_name())
            elif s in ("load_date", "ingestion_date", "load_dt"):
                out_df = out_df.withColumn(str(col_name), F.current_date())
            else:
                out_df = out_df.withColumn(str(col_name), F.expr(spec))
        elif isinstance(spec, dict):
            expr = spec.get("expr")
            literal = spec.get("literal")
            if expr is not None:
                out_df = out_df.withColumn(str(col_name), F.expr(str(expr)))
            elif literal is not None:
                out_df = out_df.withColumn(str(col_name), F.lit(literal))
            else:
                out_df = out_df.withColumn(str(col_name), F.lit(None))
        else:
            out_df = out_df.withColumn(str(col_name), F.lit(spec))

    return out_df


def _apply_data_quality_if_any(df: DataFrame, table_cfg: dict, runtime_cfg: dict):
    dq = table_cfg.get("data_quality")
    if _is_empty(dq):
        dq = runtime_cfg.get("data_quality")

    if _is_empty(dq):
        return df, None

    if not isinstance(dq, dict):
        raise ValueError("data_quality must be a dict when provided")

    rules = dq.get("rules")
    reject_path = dq.get("reject_path")
    reject_format = dq.get("reject_format")
    reject_options = dq.get("reject_options")
    reject_mode = dq.get("reject_mode")
    if _is_empty(reject_mode):
        reject_mode = "append"

    if _is_empty(rules) or not isinstance(rules, list):
        return df, None

    valid_cond = None
    for r in rules:
        if isinstance(r, dict):
            expr = r.get("expr") or r.get("expression")
        else:
            expr = r
        if _is_empty(expr):
            continue
        c = F.expr(str(expr))
        valid_cond = c if valid_cond is None else (valid_cond & c)

    if valid_cond is None:
        return df, None

    valid_df = df.filter(valid_cond)
    invalid_df = df.filter(~valid_cond)

    reject_cfg = {
        "path": reject_path,
        "format": reject_format,
        "options": reject_options,
        "mode": reject_mode,
    }
    return valid_df, (invalid_df, reject_cfg)


def _write_df(df: DataFrame, table_cfg: dict, runtime_cfg: dict, out_path: str):
    write_format = _resolve_write_format(runtime_cfg, table_cfg)
    if _is_empty(write_format):
        raise ValueError("write_format is required in metadata")

    write_mode = _resolve_write_mode(runtime_cfg, table_cfg)
    if _is_empty(write_mode):
        raise ValueError("write_mode is required in metadata")

    write_options = table_cfg.get("write_options")
    if _is_empty(write_options):
        write_options = runtime_cfg.get("write_options")

    writer = df.write.format(str(write_format)).mode(str(write_mode))

    if str(write_format).lower() == "csv" and _is_empty(write_options):
        writer = writer.option("header", "true")
    else:
        writer = _apply_options(writer, write_options if isinstance(write_options, dict) else {})

    partition_columns = table_cfg.get("partition_columns")
    if _is_empty(partition_columns):
        partition_columns = runtime_cfg.get("partition_columns")
    if not _is_empty(partition_columns):
        if isinstance(partition_columns, list):
            writer = writer.partitionBy(*[str(c) for c in partition_columns])
        else:
            raise ValueError("partition_columns must be a list when provided")

    writer.save(out_path)


def _write_rejects_if_any(reject_tuple):
    if reject_tuple is None:
        return
    invalid_df, cfg = reject_tuple
    if invalid_df is None or cfg is None:
        return
    path = cfg.get("path")
    fmt = cfg.get("format")
    mode = cfg.get("mode")
    options = cfg.get("options")

    if _is_empty(path) or _is_empty(fmt):
        return

    w = invalid_df.write.format(str(fmt)).mode(str(mode) if not _is_empty(mode) else "append")
    w = _apply_options(w, options if isinstance(options, dict) else {})
    if str(fmt).lower() == "csv" and _is_empty(options):
        w = w.option("header", "true")
    w.save(str(path))


def _process_table(table_cfg: dict, all_columns_meta: list, runtime_cfg: dict, batch_id: str):
    target_table = table_cfg.get("target_table")
    src_table = table_cfg.get("source_table")
    if _is_empty(src_table):
        src_table, _ = _parse_mapping_details(table_cfg.get("mapping_details"))

    out_path = _build_target_path(runtime_cfg, table_cfg)
    if _is_empty(out_path):
        raise ValueError("target_path and target_table are required to build output path")

    _log("INFO", "table_start", target_table=target_table, source_table=src_table, output_path=out_path)

    df = _read_source_df(runtime_cfg, table_cfg)

    df = _apply_transformations_if_any(df, table_cfg, all_columns_meta)

    df = _add_operational_metadata_if_any(df, table_cfg, runtime_cfg, batch_id)

    valid_df, reject_tuple = _apply_data_quality_if_any(df, table_cfg, runtime_cfg)

    _write_df(valid_df, table_cfg, runtime_cfg, out_path)

    _write_rejects_if_any(reject_tuple)

    _log("INFO", "table_success", target_table=target_table, source_table=src_table, output_path=out_path)


runtime_cfg = metadata.get("runtime_config", {})
all_tables = metadata.get("tables", [])
all_columns_meta = metadata.get("columns", [])

fail_fast = runtime_cfg.get("fail_fast")
if fail_fast is None:
    fail_fast = False

batch_id = str(runtime_cfg.get("batch_id") or uuid.uuid4())

summary = {
    "batch_id": batch_id,
    "total_tables": len(all_tables) if isinstance(all_tables, list) else 0,
    "succeeded": [],
    "failed": [],
}

if not isinstance(all_tables, list) or len(all_tables) == 0:
    _log("WARN", "no_tables_to_process")
else:
    for t in all_tables:
        if not isinstance(t, dict):
            continue
        tgt = t.get("target_table")
        try:
            _process_table(t, all_columns_meta, runtime_cfg, batch_id)
            summary["succeeded"].append(tgt)
        except Exception as e:
            err = {
                "target_table": tgt,
                "error": str(e),
                "trace": traceback.format_exc(),
            }
            summary["failed"].append(err)
            _log("ERROR", "table_failed", **err)
            if bool(fail_fast):
                raise

_log("INFO", "job_summary", **summary)

job.commit()
