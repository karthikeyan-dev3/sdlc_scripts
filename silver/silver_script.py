from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
import json
import sys
import traceback
import datetime
import re

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("gold_job", {})

logger = glueContext.get_logger()

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

metadata = {
  "tables": [
    {
      "target_schema": "silver",
      "target_table": "customer_profiles_silver",
      "target_alias": "cps",
      "mapping_details": "bronze.customer_master_bronze cmb LEFT JOIN (SELECT loan_id, risk_score, evaluation_date, ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY evaluation_date DESC, risk_id DESC) AS rn FROM bronze.loan_risk_assessment_bronze) lra_latest ON cmb.customer_id = (SELECT customer_id FROM bronze.loan_applications_bronze lab WHERE lab.loan_id = lra_latest.loan_id FETCH FIRST 1 ROW ONLY) AND lra_latest.rn = 1",
      "description": "Conformed, de-duplicated customer master with a standardized contact_information field and customer_risk_score derived from latest available loan risk assessment for any loan associated to the customer (fallback to cmb.credit_score when no risk assessment exists). Cleaning: trim/upper IDs, remove duplicate customer_id keeping latest/highest credit_score; contact_information formed from city/state (and optionally other available fields). Outputs required for gold_customers: customer_id, customer_name, contact_information, customer_risk_score."
    },
    {
      "target_schema": "silver",
      "target_table": "branch_profiles_silver",
      "target_alias": "bps",
      "mapping_details": "bronze.branch_master_bronze bmb",
      "description": "Conformed, de-duplicated branch master. Cleaning: trim/upper branch_id, remove duplicate branch_id keeping latest record; standardize location as 'city, state'; manager sourced from manager_name. Outputs required for gold_branches: branch_id, branch_name, location, manager."
    },
    {
      "target_schema": "silver",
      "target_table": "loan_transactions_summary_silver",
      "target_alias": "lts",
      "mapping_details": "bronze.repayment_transactions_bronze rtb INNER JOIN bronze.loan_applications_bronze lab ON rtb.loan_id = lab.loan_id LEFT JOIN silver.customer_profiles_silver cps ON lab.customer_id = cps.customer_id LEFT JOIN silver.branch_profiles_silver bps ON lab.branch_id = bps.branch_id",
      "description": "Loan transaction-level fact derived from repayment transactions and conformed to gold_loan_transactions requirements. Cleaning: deduplicate on transaction_id; trim/upper loan_id/customer_id/branch_id; enforce valid dates; amount = payment_amount cast to numeric; transaction_date = payment_date. Outputs required for gold_loan_transactions: loan_id, transaction_date, amount, customer_id, branch_id."
    },
    {
      "target_schema": "silver",
      "target_table": "branch_performance_silver",
      "target_alias": "bpsf",
      "mapping_details": "bronze.loan_applications_bronze lab LEFT JOIN bronze.repayment_transactions_bronze rtb ON lab.loan_id = rtb.loan_id LEFT JOIN (SELECT loan_id, risk_score, default_probability, evaluation_date, ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY evaluation_date DESC, risk_id DESC) AS rn FROM bronze.loan_risk_assessment_bronze) lra_latest ON lab.loan_id = lra_latest.loan_id AND lra_latest.rn = 1",
      "description": "Pre-aggregated loan performance inputs at loan + branch grain to support gold_loan_performance. Computes per (branch_id, loan_id, customer_id): total_amount = SUM(CASE WHEN rtb.payment_status IN ('SUCCESS','COMPLETED','PAID') THEN rtb.payment_amount ELSE 0 END); average_repayment_time = AVG(DATEDIFF(day, lab.application_date, rtb.payment_date)) over successful payments; overdue_loans_count = CASE WHEN lab.loan_status IN ('OVERDUE','DELINQUENT') OR (lra_latest.default_probability >= 0.5) THEN 1 ELSE 0 END (can be summed later at branch level). Cleaning: deduplicate lab on loan_id (keep latest application_date), deduplicate rtb on transaction_id, trim/upper IDs, filter null critical keys where needed."
    }
  ],
  "columns": [
    {
      "source_columns": [
        "cmb.customer_id"
      ],
      "source_type": "varchar(10)",
      "source_nullable": "not_accepted",
      "target_column": "customer_id",
      "target_type": "varchar(10)",
      "target_nullable": "not_accepted",
      "transformation": "cps.customer_id = UPPER(TRIM(cmb.customer_id))"
    },
    {
      "source_columns": [
        "cmb.customer_name"
      ],
      "source_type": "varchar(255)",
      "source_nullable": "accepted",
      "target_column": "customer_name",
      "target_type": "varchar(255)",
      "target_nullable": "accepted",
      "transformation": "cps.customer_name = cmb.customer_name"
    },
    {
      "source_columns": [
        "cmb.city",
        "cmb.state"
      ],
      "source_type": "varchar(100)",
      "source_nullable": "accepted",
      "target_column": "contact_information",
      "target_type": "varchar(255)",
      "target_nullable": "accepted",
      "transformation": "cps.contact_information = CONCAT(cmb.city, ', ', cmb.state)"
    },
    {
      "source_columns": [
        "lra_latest.risk_score",
        "cmb.credit_score"
      ],
      "source_type": "int",
      "source_nullable": "accepted",
      "target_column": "customer_risk_score",
      "target_type": "int",
      "target_nullable": "accepted",
      "transformation": "cps.customer_risk_score = COALESCE(lra_latest.risk_score, cmb.credit_score)"
    },
    {
      "source_columns": [
        "bmb.branch_id"
      ],
      "source_type": "varchar(10)",
      "source_nullable": "not_accepted",
      "target_column": "branch_id",
      "target_type": "varchar(10)",
      "target_nullable": "not_accepted",
      "transformation": "bps.branch_id = UPPER(TRIM(bmb.branch_id))"
    },
    {
      "source_columns": [
        "bmb.branch_name"
      ],
      "source_type": "varchar(255)",
      "source_nullable": "accepted",
      "target_column": "branch_name",
      "target_type": "varchar(255)",
      "target_nullable": "accepted",
      "transformation": "bps.branch_name = bmb.branch_name"
    },
    {
      "source_columns": [
        "bmb.city",
        "bmb.state"
      ],
      "source_type": "varchar(100)",
      "source_nullable": "accepted",
      "target_column": "location",
      "target_type": "varchar(255)",
      "target_nullable": "accepted",
      "transformation": "bps.location = CONCAT(bmb.city, ', ', bmb.state)"
    },
    {
      "source_columns": [
        "bmb.manager_name"
      ],
      "source_type": "varchar(255)",
      "source_nullable": "accepted",
      "target_column": "manager",
      "target_type": "varchar(255)",
      "target_nullable": "accepted",
      "transformation": "bps.manager = bmb.manager_name"
    },
    {
      "source_columns": [
        "rtb.loan_id"
      ],
      "source_type": "varchar(20)",
      "source_nullable": "accepted",
      "target_column": "loan_id",
      "target_type": "varchar(10)",
      "target_nullable": "not_accepted",
      "transformation": "lts.loan_id = UPPER(TRIM(rtb.loan_id))"
    },
    {
      "source_columns": [
        "rtb.payment_date"
      ],
      "source_type": "date",
      "source_nullable": "accepted",
      "target_column": "transaction_date",
      "target_type": "date",
      "target_nullable": "accepted",
      "transformation": "lts.transaction_date = rtb.payment_date"
    },
    {
      "source_columns": [
        "rtb.payment_amount"
      ],
      "source_type": "int",
      "source_nullable": "accepted",
      "target_column": "amount",
      "target_type": "int",
      "target_nullable": "accepted",
      "transformation": "lts.amount = rtb.payment_amount"
    },
    {
      "source_columns": [
        "lab.customer_id"
      ],
      "source_type": "varchar(10)",
      "source_nullable": "accepted",
      "target_column": "customer_id",
      "target_type": "varchar(10)",
      "target_nullable": "accepted",
      "transformation": "lts.customer_id = UPPER(TRIM(lab.customer_id))"
    },
    {
      "source_columns": [
        "lab.branch_id"
      ],
      "source_type": "varchar(10)",
      "source_nullable": "accepted",
      "target_column": "branch_id",
      "target_type": "varchar(10)",
      "target_nullable": "accepted",
      "transformation": "lts.branch_id = UPPER(TRIM(lab.branch_id))"
    },
    {
      "source_columns": [
        "lab.branch_id"
      ],
      "source_type": "varchar(10)",
      "source_nullable": "accepted",
      "target_column": "branch_id",
      "target_type": "varchar(10)",
      "target_nullable": "accepted",
      "transformation": "bpsf.branch_id = UPPER(TRIM(lab.branch_id))"
    },
    {
      "source_columns": [
        "lab.loan_id"
      ],
      "source_type": "varchar(10)",
      "source_nullable": "not_accepted",
      "target_column": "loan_id",
      "target_type": "varchar(10)",
      "target_nullable": "not_accepted",
      "transformation": "bpsf.loan_id = UPPER(TRIM(lab.loan_id))"
    },
    {
      "source_columns": [
        "lab.customer_id"
      ],
      "source_type": "varchar(10)",
      "source_nullable": "accepted",
      "target_column": "customer_id",
      "target_type": "varchar(10)",
      "target_nullable": "accepted",
      "transformation": "bpsf.customer_id = UPPER(TRIM(lab.customer_id))"
    },
    {
      "source_columns": [
        "rtb.payment_status",
        "rtb.payment_amount"
      ],
      "source_type": "varchar(20)",
      "source_nullable": "accepted",
      "target_column": "total_amount",
      "target_type": "int",
      "target_nullable": "accepted",
      "transformation": "bpsf.total_amount = SUM(CASE WHEN rtb.payment_status IN ('SUCCESS','COMPLETED','PAID') THEN rtb.payment_amount ELSE 0 END)"
    },
    {
      "source_columns": [
        "lab.application_date",
        "rtb.payment_date",
        "rtb.payment_status"
      ],
      "source_type": "date",
      "source_nullable": "accepted",
      "target_column": "average_repayment_time",
      "target_type": "double",
      "target_nullable": "accepted",
      "transformation": "bpsf.average_repayment_time = AVG(CASE WHEN rtb.payment_status IN ('SUCCESS','COMPLETED','PAID') THEN DATEDIFF(day, lab.application_date, rtb.payment_date) END)"
    },
    {
      "source_columns": [
        "lab.loan_status",
        "lra_latest.default_probability"
      ],
      "source_type": "varchar(20)",
      "source_nullable": "accepted",
      "target_column": "overdue_loans_count",
      "target_type": "int",
      "target_nullable": "accepted",
      "transformation": "bpsf.overdue_loans_count = CASE WHEN lab.loan_status IN ('OVERDUE','DELINQUENT') OR (lra_latest.default_probability >= 0.5) THEN 1 ELSE 0 END"
    }
  ]
}

def _now_utc_iso():
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

def _safe_log_json(level, payload):
    try:
        msg = json.dumps(payload, default=str)
    except Exception:
        msg = str(payload)
    if level == "error":
        logger.error(msg)
    elif level == "warn":
        logger.warn(msg)
    else:
        logger.info(msg)

def _csv_read_defaults_if_needed(fmt, opts):
    if (fmt or "").lower() == "csv" and (not opts):
        return {"header": "true", "inferSchema": "true"}
    return opts or {}

def _csv_write_defaults_if_needed(fmt, opts):
    if (fmt or "").lower() == "csv" and (not opts):
        return {"header": "true"}
    return opts or {}

def _read_dataset(path, fmt, options):
    reader = spark.read.format(fmt)
    for k, v in (options or {}).items():
        reader = reader.option(k, v)
    return reader.load(path)

def _write_dataset(df, path, fmt, mode, options, partitions):
    writer = df.write.format(fmt).mode(mode)
    for k, v in (options or {}).items():
        writer = writer.option(k, v)
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(path)

def _extract_tables_from_mapping_details(mapping_details):
    if not mapping_details:
        return []
    tokens = re.findall(r'(?i)\b(?:from|join)\s+([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\b', mapping_details)
    tables = []
    seen = set()
    for schema, table in tokens:
        key = f"{schema}.{table}"
        if key.lower() not in seen:
            seen.add(key.lower())
            tables.append({"schema": schema, "table": table})
    return tables

def _extract_aliases_from_mapping_details(mapping_details):
    if not mapping_details:
        return set()
    aliases = set()
    pairs = re.findall(r'([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\s+([a-zA-Z_][\w]*)', mapping_details)
    for _, _, alias in pairs:
        aliases.add(alias)
    return aliases

def _columns_for_target_alias(columns_metadata, target_alias):
    result = []
    for c in columns_metadata or []:
        t = c.get("transformation")
        if not t:
            continue
        if re.search(r'^\s*' + re.escape(target_alias) + r'\.', t) or re.search(r'^\s*' + re.escape(target_alias) + r'\[', t):
            result.append(c)
    return result

def _parse_transformation_assignment(transformation):
    if not transformation or "=" not in transformation:
        return None
    lhs, rhs = transformation.split("=", 1)
    lhs = lhs.strip()
    rhs = rhs.strip()
    m = re.match(r'^([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)$', lhs)
    if not m:
        return None
    return {"target_alias": m.group(1), "target_column": m.group(2), "rhs_expr": rhs}

def _build_select_exprs_for_alias(columns_metadata, target_alias):
    exprs = []
    for c in _columns_for_target_alias(columns_metadata, target_alias):
        parsed = _parse_transformation_assignment(c.get("transformation"))
        if not parsed:
            continue
        exprs.append((parsed["rhs_expr"], parsed["target_column"]))
    return exprs

def _contains_aggregate_sql(rhs_expr):
    if not rhs_expr:
        return False
    return re.search(r'(?i)\b(sum|avg|min|max|count)\s*\(', rhs_expr) is not None

def _has_any_aggregate(exprs):
    for rhs, _ in exprs:
        if _contains_aggregate_sql(rhs):
            return True
    return False

def _attempt_groupby_from_description(description):
    if not description:
        return []
    m = re.search(r'(?i)\bper\s*\(([^)]*)\)', description)
    if not m:
        return []
    inside = m.group(1)
    cols = []
    for part in inside.split(","):
        p = part.strip()
        if p:
            cols.append(p)
    return cols

def _spark_sql_compat(sql_text):
    if sql_text is None:
        return None
    s = sql_text

    s = re.sub(r'(?i)\bDATEDIFF\s*\(\s*day\s*,', 'DATEDIFF(', s)
    s = re.sub(r'(?i)\bFETCH\s+FIRST\s+\d+\s+ROW\s+ONLY\b', 'LIMIT 1', s)

    s = re.sub(r'(?i)\bCONCAT\s*\(\s*([^)]+)\s*\)', lambda m: f"concat({m.group(1)})", s)

    return s

def _register_sources_as_temp_views(source_tables, global_read_conf):
    for t in source_tables:
        schema = t["schema"]
        table = t["table"]
        fmt = global_read_conf.get("read_format")
        opts = global_read_conf.get("read_options")

        read_opts = _csv_read_defaults_if_needed(fmt, opts)

        path = global_read_conf.get("source_path")
        if not path.endswith("/"):
            path = path + "/"
        table_path = f"{path}{schema}/{table}/"
        view_name = f"{schema}.{table}"
        df = _read_dataset(table_path, fmt, read_opts)
        df.createOrReplaceTempView(view_name)

def _register_target_deps_as_temp_views(dep_tables, global_read_conf, target_base_path):
    for dep in dep_tables:
        schema = dep["schema"]
        table = dep["table"]
        fmt = global_read_conf.get("read_format")
        opts = global_read_conf.get("read_options")

        read_opts = _csv_read_defaults_if_needed(fmt, opts)

        base = target_base_path
        if not base.endswith("/"):
            base = base + "/"
        table_path = f"{base}{table}/"
        view_name = f"{schema}.{table}"
        df = _read_dataset(table_path, fmt, read_opts)
        df.createOrReplaceTempView(view_name)

def _extract_schemas_tables_from_mapping_details(mapping_details):
    return _extract_tables_from_mapping_details(mapping_details)

def _split_source_tables(source_tables, target_schema):
    upstream = []
    deps = []
    for st in source_tables:
        if (st["schema"] or "").lower() == (target_schema or "").lower():
            deps.append(st)
        else:
            upstream.append(st)
    return upstream, deps

def _deduce_fail_fast(table_meta):
    for k in ["fail_fast", "failFast", "failfast"]:
        if k in table_meta:
            return bool(table_meta.get(k))
    return False

def _process_table(table_meta, columns_meta, global_read_conf, global_write_conf):
    table_name = table_meta.get("target_table")
    target_schema = table_meta.get("target_schema")
    target_alias = table_meta.get("target_alias")
    mapping_details = table_meta.get("mapping_details")
    description = table_meta.get("description")

    start_ts = _now_utc_iso()

    _safe_log_json("info", {
        "event": "table_start",
        "ts": start_ts,
        "target_schema": target_schema,
        "target_table": table_name,
        "target_alias": target_alias
    })

    if not mapping_details or not target_alias:
        raise ValueError("Missing required metadata keys for table processing (mapping_details and/or target_alias).")

    source_tables = _extract_schemas_tables_from_mapping_details(mapping_details)
    upstream, deps = _split_source_tables(source_tables, target_schema)

    global_source_conf = {
        "source_path": global_read_conf.get("source_path"),
        "read_format": global_read_conf.get("read_format"),
        "read_options": global_read_conf.get("read_options")
    }

    _register_sources_as_temp_views(upstream, global_source_conf)

    _register_target_deps_as_temp_views(deps, global_source_conf, global_write_conf.get("target_path"))

    spark_sql = _spark_sql_compat(mapping_details)
    base_query = f"SELECT * FROM {spark_sql}"
    base_df = spark.sql(base_query)

    base_df.createOrReplaceTempView(target_alias)

    select_exprs = _build_select_exprs_for_alias(columns_meta, target_alias)

    if select_exprs:
        has_aggs = _has_any_aggregate(select_exprs)
        if has_aggs:
            group_by_cols = _attempt_groupby_from_description(description)
            if not group_by_cols:
                raise ValueError("Aggregate transformations detected but no group-by keys could be derived from metadata.")
            agg_sql_parts = []
            gb_sql_parts = []
            for c in group_by_cols:
                gb_sql_parts.append(c)

            for rhs, out_col in select_exprs:
                agg_sql_parts.append(f"{rhs} AS {out_col}")

            agg_query = f"SELECT {', '.join(gb_sql_parts + agg_sql_parts)} FROM {target_alias} GROUP BY {', '.join(gb_sql_parts)}"
            result_df = spark.sql(_spark_sql_compat(agg_query))
        else:
            proj_parts = [f"{rhs} AS {out_col}" for rhs, out_col in select_exprs]
            proj_query = f"SELECT {', '.join(proj_parts)} FROM {target_alias}"
            result_df = spark.sql(_spark_sql_compat(proj_query))
    else:
        result_df = base_df

    write_fmt = global_write_conf.get("write_format")
    write_mode = global_write_conf.get("write_mode")
    write_options = _csv_write_defaults_if_needed(write_fmt, global_write_conf.get("write_options"))
    partition_cols = global_write_conf.get("partition_columns") or []

    target_base = global_write_conf.get("target_path")
    if not target_base.endswith("/"):
        target_base = target_base + "/"
    target_path = f"{target_base}{table_name}/"

    _write_dataset(result_df, target_path, write_fmt, write_mode, write_options, partition_cols)

    end_ts = _now_utc_iso()
    _safe_log_json("info", {
        "event": "table_success",
        "ts": end_ts,
        "target_schema": target_schema,
        "target_table": table_name,
        "target_alias": target_alias,
        "target_path": target_path
    })

def _get_global_conf(source_path, target_path, file_format):
    return {
        "read": {
            "source_path": source_path,
            "read_format": file_format,
            "read_options": {}
        },
        "write": {
            "target_path": target_path,
            "write_format": file_format,
            "write_mode": "overwrite",
            "write_options": {},
            "partition_columns": []
        }
    }

global_conf = _get_global_conf(SOURCE_PATH, TARGET_PATH, FILE_FORMAT)

failures = []
for t in metadata.get("tables", []):
    fail_fast = _deduce_fail_fast(t)
    try:
        _process_table(
            table_meta=t,
            columns_meta=metadata.get("columns", []),
            global_read_conf=global_conf["read"],
            global_write_conf=global_conf["write"]
        )
    except Exception as e:
        err = {
            "event": "table_failure",
            "ts": _now_utc_iso(),
            "target_schema": t.get("target_schema"),
            "target_table": t.get("target_table"),
            "target_alias": t.get("target_alias"),
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc()
        }
        _safe_log_json("error", err)
        failures.append(err)
        if fail_fast:
            raise

if failures:
    _safe_log_json("error", {"event": "job_completed_with_failures", "ts": _now_utc_iso(), "failures": failures})
else:
    _safe_log_json("info", {"event": "job_completed_successfully", "ts": _now_utc_iso()})

job.commit()