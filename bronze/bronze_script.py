from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import json
import traceback
import datetime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("gold_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'branch_master_bronze', 'target_alias': 'bmb', 'mapping_details': 'branch_master bm', 'description': 'Bronze table sourced from branch_master containing: branch_id, branch_name, city, state, branch_type, manager_name, opening_year, contact_number.'}, {'target_schema': 'bronze', 'target_table': 'customer_master_bronze', 'target_alias': 'cmb', 'mapping_details': 'customer_master cm', 'description': 'Bronze table sourced from customer_master containing: customer_id, customer_name, gender, city, state, income_segment, annual_income, credit_score.'}, {'target_schema': 'bronze', 'target_table': 'loan_applications_bronze', 'target_alias': 'lab', 'mapping_details': 'loan_applications la', 'description': 'Bronze table sourced from loan_applications containing: loan_id, customer_id, branch_id, loan_type, loan_amount, interest_rate, loan_status, application_date.'}, {'target_schema': 'bronze', 'target_table': 'loan_risk_assessment_bronze', 'target_alias': 'lrab', 'mapping_details': 'loan_risk_assessment lra', 'description': 'Bronze table sourced from loan_risk_assessment containing: risk_id, loan_id, risk_score, default_probability, risk_category, evaluation_date, analyst_name, review_status.'}, {'target_schema': 'bronze', 'target_table': 'repayment_transactions_bronze', 'target_alias': 'rtb', 'mapping_details': 'repayment_transactions rt', 'description': 'Bronze table sourced from repayment_transactions containing: transaction_id, loan_id, payment_date, payment_amount, payment_method, payment_status, remaining_balance, processed_by.'}], 'columns': [{'source_column': "['bmb.branch_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'branch_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'bmb.branch_id = bm.branch_id', 'target_table': 'bmb'}, {'source_column': "['bmb.branch_name']", 'source_type': 'varchar(255)', 'source_nullable': 'nan', 'target_column': 'branch_name', 'target_type': 'varchar(255)', 'target_nullable': 'nan', 'transformation': 'bmb.branch_name = bm.branch_name', 'target_table': 'bmb'}, {'source_column': "['bmb.city']", 'source_type': 'varchar(100)', 'source_nullable': 'nan', 'target_column': 'city', 'target_type': 'varchar(100)', 'target_nullable': 'nan', 'transformation': 'bmb.city = bm.city', 'target_table': 'bmb'}, {'source_column': "['bmb.state']", 'source_type': 'varchar(100)', 'source_nullable': 'nan', 'target_column': 'state', 'target_type': 'varchar(100)', 'target_nullable': 'nan', 'transformation': 'bmb.state = bm.state', 'target_table': 'bmb'}, {'source_column': "['bmb.branch_type']", 'source_type': 'varchar(50)', 'source_nullable': 'nan', 'target_column': 'branch_type', 'target_type': 'varchar(50)', 'target_nullable': 'nan', 'transformation': 'bmb.branch_type = bm.branch_type', 'target_table': 'bmb'}, {'source_column': "['bmb.manager_name']", 'source_type': 'varchar(255)', 'source_nullable': 'nan', 'target_column': 'manager_name', 'target_type': 'varchar(255)', 'target_nullable': 'nan', 'transformation': 'bmb.manager_name = bm.manager_name', 'target_table': 'bmb'}, {'source_column': "['bmb.opening_year']", 'source_type': 'int', 'source_nullable': 'nan', 'target_column': 'opening_year', 'target_type': 'int', 'target_nullable': 'nan', 'transformation': 'bmb.opening_year = bm.opening_year', 'target_table': 'bmb'}, {'source_column': "['bmb.contact_number']", 'source_type': 'varchar(15)', 'source_nullable': 'nan', 'target_column': 'contact_number', 'target_type': 'varchar(15)', 'target_nullable': 'nan', 'transformation': 'bmb.contact_number = bm.contact_number', 'target_table': 'bmb'}, {'source_column': "['cmb.customer_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'customer_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'cmb.customer_id = cm.customer_id', 'target_table': 'cmb'}, {'source_column': "['cmb.customer_name']", 'source_type': 'varchar(255)', 'source_nullable': 'nan', 'target_column': 'customer_name', 'target_type': 'varchar(255)', 'target_nullable': 'nan', 'transformation': 'cmb.customer_name = cm.customer_name', 'target_table': 'cmb'}, {'source_column': "['cmb.gender']", 'source_type': 'varchar(10)', 'source_nullable': 'nan', 'target_column': 'gender', 'target_type': 'varchar(10)', 'target_nullable': 'nan', 'transformation': 'cmb.gender = cm.gender', 'target_table': 'cmb'}, {'source_column': "['cmb.city']", 'source_type': 'varchar(100)', 'source_nullable': 'nan', 'target_column': 'city', 'target_type': 'varchar(100)', 'target_nullable': 'nan', 'transformation': 'cmb.city = cm.city', 'target_table': 'cmb'}, {'source_column': "['cmb.state']", 'source_type': 'varchar(100)', 'source_nullable': 'nan', 'target_column': 'state', 'target_type': 'varchar(100)', 'target_nullable': 'nan', 'transformation': 'cmb.state = cm.state', 'target_table': 'cmb'}, {'source_column': "['cmb.income_segment']", 'source_type': 'varchar(10)', 'source_nullable': 'nan', 'target_column': 'income_segment', 'target_type': 'varchar(10)', 'target_nullable': 'nan', 'transformation': 'cmb.income_segment = cm.income_segment', 'target_table': 'cmb'}, {'source_column': "['cmb.annual_income']", 'source_type': 'int', 'source_nullable': 'nan', 'target_column': 'annual_income', 'target_type': 'int', 'target_nullable': 'nan', 'transformation': 'cmb.annual_income = cm.annual_income', 'target_table': 'cmb'}, {'source_column': "['cmb.credit_score']", 'source_type': 'int', 'source_nullable': 'nan', 'target_column': 'credit_score', 'target_type': 'int', 'target_nullable': 'nan', 'transformation': 'cmb.credit_score = cm.credit_score', 'target_table': 'cmb'}, {'source_column': "['lab.loan_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'loan_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'lab.loan_id = la.loan_id', 'target_table': 'lab'}, {'source_column': "['lab.customer_id']", 'source_type': 'varchar(10)', 'source_nullable': 'nan', 'target_column': 'customer_id', 'target_type': 'varchar(10)', 'target_nullable': 'nan', 'transformation': 'lab.customer_id = la.customer_id', 'target_table': 'lab'}, {'source_column': "['lab.branch_id']", 'source_type': 'varchar(10)', 'source_nullable': 'nan', 'target_column': 'branch_id', 'target_type': 'varchar(10)', 'target_nullable': 'nan', 'transformation': 'lab.branch_id = la.branch_id', 'target_table': 'lab'}, {'source_column': "['lab.loan_type']", 'source_type': 'varchar(50)', 'source_nullable': 'nan', 'target_column': 'loan_type', 'target_type': 'varchar(50)', 'target_nullable': 'nan', 'transformation': 'lab.loan_type = la.loan_type', 'target_table': 'lab'}, {'source_column': "['lab.loan_amount']", 'source_type': 'int', 'source_nullable': 'nan', 'target_column': 'loan_amount', 'target_type': 'int', 'target_nullable': 'nan', 'transformation': 'lab.loan_amount = la.loan_amount', 'target_table': 'lab'}, {'source_column': "['lab.interest_rate']", 'source_type': 'float', 'source_nullable': 'nan', 'target_column': 'interest_rate', 'target_type': 'float', 'target_nullable': 'nan', 'transformation': 'lab.interest_rate = la.interest_rate', 'target_table': 'lab'}, {'source_column': "['lab.loan_status']", 'source_type': 'varchar(20)', 'source_nullable': 'nan', 'target_column': 'loan_status', 'target_type': 'varchar(20)', 'target_nullable': 'nan', 'transformation': 'lab.loan_status = la.loan_status', 'target_table': 'lab'}, {'source_column': "['lab.application_date']", 'source_type': 'date', 'source_nullable': 'nan', 'target_column': 'application_date', 'target_type': 'date', 'target_nullable': 'nan', 'transformation': 'lab.application_date = la.application_date', 'target_table': 'lab'}, {'source_column': "['lrab.risk_id']", 'source_type': 'varchar(10)', 'source_nullable': 'not_null', 'target_column': 'risk_id', 'target_type': 'varchar(10)', 'target_nullable': 'not_null', 'transformation': 'lrab.risk_id = lra.risk_id', 'target_table': 'lrab'}, {'source_column': "['lrab.loan_id']", 'source_type': 'varchar(10)', 'source_nullable': 'nan', 'target_column': 'loan_id', 'target_type': 'varchar(10)', 'target_nullable': 'nan', 'transformation': 'lrab.loan_id = lra.loan_id', 'target_table': 'lrab'}, {'source_column': "['lrab.risk_score']", 'source_type': 'int', 'source_nullable': 'nan', 'target_column': 'risk_score', 'target_type': 'int', 'target_nullable': 'nan', 'transformation': 'lrab.risk_score = lra.risk_score', 'target_table': 'lrab'}, {'source_column': "['lrab.default_probability']", 'source_type': 'float', 'source_nullable': 'nan', 'target_column': 'default_probability', 'target_type': 'float', 'target_nullable': 'nan', 'transformation': 'lrab.default_probability = lra.default_probability', 'target_table': 'lrab'}, {'source_column': "['lrab.risk_category']", 'source_type': 'varchar(10)', 'source_nullable': 'nan', 'target_column': 'risk_category', 'target_type': 'varchar(10)', 'target_nullable': 'nan', 'transformation': 'lrab.risk_category = lra.risk_category', 'target_table': 'lrab'}, {'source_column': "['lrab.evaluation_date']", 'source_type': 'date', 'source_nullable': 'nan', 'target_column': 'evaluation_date', 'target_type': 'date', 'target_nullable': 'nan', 'transformation': 'lrab.evaluation_date = lra.evaluation_date', 'target_table': 'lrab'}, {'source_column': "['lrab.analyst_name']", 'source_type': 'varchar(50)', 'source_nullable': 'nan', 'target_column': 'analyst_name', 'target_type': 'varchar(50)', 'target_nullable': 'nan', 'transformation': 'lrab.analyst_name = lra.analyst_name', 'target_table': 'lrab'}, {'source_column': "['lrab.review_status']", 'source_type': 'varchar(10)', 'source_nullable': 'nan', 'target_column': 'review_status', 'target_type': 'varchar(10)', 'target_nullable': 'nan', 'transformation': 'lrab.review_status = lra.review_status', 'target_table': 'lrab'}, {'source_column': "['rtb.transaction_id']", 'source_type': 'varchar(20)', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'varchar(20)', 'target_nullable': 'not_null', 'transformation': 'rtb.transaction_id = rt.transaction_id', 'target_table': 'rtb'}, {'source_column': "['rtb.loan_id']", 'source_type': 'varchar(20)', 'source_nullable': 'nan', 'target_column': 'loan_id', 'target_type': 'varchar(20)', 'target_nullable': 'nan', 'transformation': 'rtb.loan_id = rt.loan_id', 'target_table': 'rtb'}, {'source_column': "['rtb.payment_date']", 'source_type': 'date', 'source_nullable': 'nan', 'target_column': 'payment_date', 'target_type': 'date', 'target_nullable': 'nan', 'transformation': 'rtb.payment_date = rt.payment_date', 'target_table': 'rtb'}, {'source_column': "['rtb.payment_amount']", 'source_type': 'int', 'source_nullable': 'nan', 'target_column': 'payment_amount', 'target_type': 'int', 'target_nullable': 'nan', 'transformation': 'rtb.payment_amount = rt.payment_amount', 'target_table': 'rtb'}, {'source_column': "['rtb.payment_method']", 'source_type': 'varchar(20)', 'source_nullable': 'nan', 'target_column': 'payment_method', 'target_type': 'varchar(20)', 'target_nullable': 'nan', 'transformation': 'rtb.payment_method = rt.payment_method', 'target_table': 'rtb'}, {'source_column': "['rtb.payment_status']", 'source_type': 'varchar(20)', 'source_nullable': 'nan', 'target_column': 'payment_status', 'target_type': 'varchar(20)', 'target_nullable': 'nan', 'transformation': 'rtb.payment_status = rt.payment_status', 'target_table': 'rtb'}, {'source_column': "['rtb.remaining_balance']", 'source_type': 'int', 'source_nullable': 'nan', 'target_column': 'remaining_balance', 'target_type': 'int', 'target_nullable': 'nan', 'transformation': 'rtb.remaining_balance = rt.remaining_balance', 'target_table': 'rtb'}, {'source_column': "['rtb.processed_by']", 'source_type': 'varchar(50)', 'source_nullable': 'nan', 'target_column': 'processed_by', 'target_type': 'varchar(50)', 'target_nullable': 'nan', 'transformation': 'rtb.processed_by = rt.processed_by', 'target_table': 'rtb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}


def _log(level, message, **kwargs):
    payload = {
        "ts": datetime.datetime.utcnow().isoformat() + "Z",
        "level": level,
        "message": message,
    }
    if kwargs:
        payload.update(kwargs)
    print(json.dumps(payload, default=str))


def _safe_get(dct, key, default=None):
    return dct[key] if isinstance(dct, dict) and key in dct else default


def _normalize_table_alias(table_entry):
    return _safe_get(table_entry, "target_alias") or _safe_get(table_entry, "alias") or _safe_get(table_entry, "target_table")


def _parse_mapping_details(mapping_details):
    if not mapping_details or not isinstance(mapping_details, str):
        return None, None
    parts = mapping_details.strip().split()
    if len(parts) >= 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return parts[0], parts[0]
    return None, None


def _apply_read_options(reader, options_dict):
    if isinstance(options_dict, dict):
        for k, v in options_dict.items():
            reader = reader.option(str(k), str(v))
    return reader


def _strip_brackets_quotes(s):
    if s is None:
        return None
    if not isinstance(s, str):
        s = str(s)
    ss = s.strip()
    if (ss.startswith("[") and ss.endswith("]")):
        ss = ss[1:-1].strip()
    if (ss.startswith("(") and ss.endswith(")")):
        ss = ss[1:-1].strip()
    if (ss.startswith("{") and ss.endswith("}")):
        ss = ss[1:-1].strip()
    if (ss.startswith("'") and ss.endswith("'")) or (ss.startswith('"') and ss.endswith('"')):
        ss = ss[1:-1]
    return ss.strip()


def _extract_source_expr(col_meta):
    raw = _safe_get(col_meta, "source_column")
    raw = _strip_brackets_quotes(raw)
    if not raw:
        return None
    return raw


def _build_select_exprs_for_table(table_alias, cols_meta):
    exprs = []
    for c in cols_meta:
        tgt = _safe_get(c, "target_column")
        src_expr = _extract_source_expr(c)
        if not tgt or not src_expr:
            continue
        exprs.append(F.col(src_expr).alias(tgt))
    return exprs


def _write_df(df, table_entry, runtime_config):
    write_format = _safe_get(table_entry, "write_format", _safe_get(runtime_config, "write_format"))
    write_mode = _safe_get(table_entry, "write_mode", _safe_get(runtime_config, "write_mode"))
    target_path = _safe_get(table_entry, "target_path")
    if not target_path:
        base_target = _safe_get(runtime_config, "target_path")
        if base_target:
            target_path = base_target.rstrip("/") + "/" + str(_safe_get(table_entry, "target_table")).strip("/") + "/"

    if not (write_format and target_path):
        raise ValueError("Missing write configuration (write_format/target_path) in metadata")

    writer = df.write.format(write_format)

    write_options = _safe_get(table_entry, "write_options", _safe_get(runtime_config, "write_options"))
    if isinstance(write_options, dict):
        for k, v in write_options.items():
            writer = writer.option(str(k), str(v))

    partition_columns = _safe_get(table_entry, "partition_columns", _safe_get(runtime_config, "partition_columns"))
    if isinstance(partition_columns, list) and partition_columns:
        writer = writer.partitionBy(*[str(x) for x in partition_columns])

    if write_mode:
        writer = writer.mode(write_mode)

    writer.save(target_path)
    return target_path


def _read_source_df(table_entry, runtime_config):
    read_format = _safe_get(table_entry, "read_format", _safe_get(runtime_config, "read_format"))
    read_options = _safe_get(table_entry, "read_options", _safe_get(runtime_config, "read_options"))

    src_path = _safe_get(table_entry, "source_path")
    if not src_path:
        base_path = _safe_get(runtime_config, "base_path")
        src_name, _ = _parse_mapping_details(_safe_get(table_entry, "mapping_details"))
        if base_path and src_name:
            src_path = base_path.rstrip("/") + "/" + src_name.strip("/")

    if not (read_format and src_path):
        raise ValueError("Missing read configuration (read_format/source_path) in metadata")

    reader = spark.read.format(read_format)
    reader = _apply_read_options(reader, read_options)
    return reader.load(src_path)


def process_table(table_entry, metadata_all):
    runtime_config = _safe_get(metadata_all, "runtime_config", {})

    target_table = _safe_get(table_entry, "target_table")
    table_alias = _normalize_table_alias(table_entry)

    _log("INFO", "table_start", target_table=target_table, target_alias=table_alias)

    df = _read_source_df(table_entry, runtime_config)

    if table_alias:
        df = df.alias(str(table_alias))

    cols_meta_all = _safe_get(metadata_all, "columns", [])
    cols_meta = [c for c in cols_meta_all if _safe_get(c, "target_table") == table_alias]

    select_exprs = _build_select_exprs_for_table(table_alias, cols_meta) if cols_meta else []
    if select_exprs:
        df = df.select(*select_exprs)

    filters = _safe_get(table_entry, "filters", _safe_get(runtime_config, "filters"))
    if isinstance(filters, list):
        for fexpr in filters:
            if isinstance(fexpr, str) and fexpr.strip():
                df = df.filter(F.expr(fexpr))

    window_cfgs = _safe_get(table_entry, "window_functions", _safe_get(runtime_config, "window_functions"))
    if isinstance(window_cfgs, list):
        for w in window_cfgs:
            out_col = _safe_get(w, "output_column")
            func_expr = _safe_get(w, "expression")
            if not out_col or not func_expr:
                continue
            partition_by = _safe_get(w, "partition_by")
            order_by = _safe_get(w, "order_by")
            frame = _safe_get(w, "frame")

            win = Window
            if isinstance(partition_by, list) and partition_by:
                win = win.partitionBy(*[F.col(c) for c in partition_by])
            if isinstance(order_by, list) and order_by:
                win = win.orderBy(*[F.col(c) for c in order_by])
            if isinstance(frame, dict):
                start = frame.get("start")
                end = frame.get("end")
                frame_type = frame.get("type", "rows")
                if start is not None and end is not None:
                    if str(frame_type).lower().startswith("range"):
                        win = win.rangeBetween(int(start), int(end))
                    else:
                        win = win.rowsBetween(int(start), int(end))

            df = df.withColumn(str(out_col), F.expr(str(func_expr)).over(win))

    agg_cfg = _safe_get(table_entry, "aggregation")
    if isinstance(agg_cfg, dict):
        group_by = _safe_get(agg_cfg, "group_by")
        metrics = _safe_get(agg_cfg, "metrics")
        if isinstance(group_by, list) and group_by and isinstance(metrics, list) and metrics:
            agg_exprs = []
            for m in metrics:
                alias = _safe_get(m, "alias")
                expr = _safe_get(m, "expression")
                if alias and expr:
                    agg_exprs.append(F.expr(str(expr)).alias(str(alias)))
            if agg_exprs:
                df = df.groupBy(*[F.col(c) for c in group_by]).agg(*agg_exprs)

    joins = _safe_get(table_entry, "joins")
    if isinstance(joins, list) and joins:
        table_map = {}
        for t in _safe_get(metadata_all, "tables", []):
            table_map[_normalize_table_alias(t)] = t

        for j in joins:
            right_alias = _safe_get(j, "right_table")
            join_type = _safe_get(j, "join_type", "inner")
            join_condition = _safe_get(j, "condition")
            if not right_alias or not join_condition:
                continue
            right_entry = table_map.get(right_alias)
            if not right_entry:
                raise ValueError(f"Join table metadata not found for alias: {right_alias}")
            right_df = _read_source_df(right_entry, runtime_config).alias(str(right_alias))

            right_cols_meta = [c for c in _safe_get(metadata_all, "columns", []) if _safe_get(c, "target_table") == right_alias]
            right_select_exprs = _build_select_exprs_for_table(right_alias, right_cols_meta) if right_cols_meta else []
            if right_select_exprs:
                right_df = right_df.select(*right_select_exprs)

            df = df.join(right_df, on=F.expr(str(join_condition)), how=str(join_type))

    dq_rules = _safe_get(table_entry, "validation_rules")
    quarantine_cfg = _safe_get(table_entry, "quarantine")
    if isinstance(dq_rules, list) and dq_rules:
        rule_exprs = []
        for r in dq_rules:
            rexpr = _safe_get(r, "expression")
            if isinstance(rexpr, str) and rexpr.strip():
                rule_exprs.append(f"({rexpr})")
        if rule_exprs:
            valid_expr = " AND ".join(rule_exprs)
            valid_df = df.filter(F.expr(valid_expr))
            invalid_df = df.filter(F.expr(f"NOT ({valid_expr})"))

            if isinstance(quarantine_cfg, dict):
                reject_path = _safe_get(quarantine_cfg, "reject_path")
                reject_format = _safe_get(quarantine_cfg, "write_format", _safe_get(runtime_config, "write_format"))
                reject_mode = _safe_get(quarantine_cfg, "write_mode", "append")
                reject_options = _safe_get(quarantine_cfg, "write_options")
                if reject_path and reject_format:
                    w = invalid_df.write.format(reject_format).mode(reject_mode)
                    if isinstance(reject_options, dict):
                        for k, v in reject_options.items():
                            w = w.option(str(k), str(v))
                    w.save(reject_path)
            df = valid_df

    op_cols = _safe_get(table_entry, "operational_columns", _safe_get(runtime_config, "operational_columns"))
    if isinstance(op_cols, list):
        for oc in op_cols:
            name = _safe_get(oc, "name")
            expr = _safe_get(oc, "expression")
            if name and expr:
                df = df.withColumn(str(name), F.expr(str(expr)))

    out_path = _write_df(df, table_entry, runtime_config)

    _log("INFO", "table_success", target_table=target_table, output_path=out_path, record_count=df.count())


def main():
    runtime_config = _safe_get(metadata, "runtime_config", {})
    fail_fast = bool(_safe_get(runtime_config, "fail_fast", _safe_get(metadata, "fail_fast", False)))

    errors = []
    for table_entry in _safe_get(metadata, "tables", []):
        try:
            process_table(table_entry, metadata)
        except Exception as e:
            err = {
                "target_table": _safe_get(table_entry, "target_table"),
                "target_alias": _normalize_table_alias(table_entry),
                "error": str(e),
                "traceback": traceback.format_exc(),
            }
            _log("ERROR", "table_failed", **err)
            errors.append(err)
            if fail_fast:
                raise

    if errors:
        _log("ERROR", "job_completed_with_errors", error_count=len(errors))
    else:
        _log("INFO", "job_completed_successfully")


try:
    main()
finally:
    job.commit()
