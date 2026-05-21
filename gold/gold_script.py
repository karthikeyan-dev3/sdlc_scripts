from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
import json
import re
import sys
import traceback
from functools import reduce

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("gold_job", {})

logger = glueContext.get_logger()

metadata = {
    "source_path": "s3://sdlc-agent-bucket/engineering-agent/silver/",
    "target_path": "s3://sdlc-agent-bucket/engineering-agent/gold/",
    "file_format": "csv",
    "tables": [
        {
            "target_schema": "gold",
            "target_table": "gold_loan_transactions",
            "target_alias": "glt",
            "mapping_details": "silver.loan_transactions_summary_silver lts",
            "description": "Create the gold loan transactions fact directly from the conformed silver loan transaction summary. Column mapping: loan_id, transaction_date, amount, customer_id, branch_id are selected as-is from lts (already standardized and deduplicated in silver)."
        },
        {
            "target_schema": "gold",
            "target_table": "gold_customers",
            "target_alias": "gc",
            "mapping_details": "silver.customer_profiles_silver cps",
            "description": "Create the gold customer dimension directly from the conformed silver customer profiles. Column mapping: customer_id, customer_name, contact_information, customer_risk_score are selected as-is from cps (risk score already derived and customer records deduplicated in silver)."
        },
        {
            "target_schema": "gold",
            "target_table": "gold_branches",
            "target_alias": "gb",
            "mapping_details": "silver.branch_profiles_silver bps",
            "description": "Create the gold branch dimension directly from the conformed silver branch profiles. Column mapping: branch_id, branch_name, location, manager are selected as-is from bps (location standardized and branch records deduplicated in silver)."
        },
        {
            "target_schema": "gold",
            "target_table": "gold_loan_performance",
            "target_alias": "glp",
            "mapping_details": "silver.branch_performance_silver bpsf LEFT JOIN silver.branch_profiles_silver bps ON bpsf.branch_id = bps.branch_id LEFT JOIN silver.customer_profiles_silver cps ON bpsf.customer_id = cps.customer_id",
            "description": "Create the gold loan performance table using the pre-aggregated silver performance dataset at (branch_id, loan_id, customer_id) grain. Select branch_id, loan_id, customer_id, total_amount, average_repayment_time, overdue_loans_count from bpsf. Join to bps and cps on their respective IDs only to ensure referential consistency to the gold dimensions (no additional attributes required for this table)."
        }
    ],
    "columns": [
        {
            "source_columns": [
                "lts.loan_id"
            ],
            "source_type": "varchar(10)",
            "source_nullable": "not_accepted",
            "target_column": "loan_id",
            "target_type": "varchar(10)",
            "target_nullable": "not_accepted",
            "transformation": "glt.loan_id = lts.loan_id"
        },
        {
            "source_columns": [
                "lts.transaction_date"
            ],
            "source_type": "date",
            "source_nullable": "accepted",
            "target_column": "transaction_date",
            "target_type": "date",
            "target_nullable": "accepted",
            "transformation": "glt.transaction_date = lts.transaction_date"
        },
        {
            "source_columns": [
                "lts.amount"
            ],
            "source_type": "int",
            "source_nullable": "accepted",
            "target_column": "amount",
            "target_type": "int",
            "target_nullable": "accepted",
            "transformation": "glt.amount = lts.amount"
        },
        {
            "source_columns": [
                "lts.customer_id"
            ],
            "source_type": "varchar(10)",
            "source_nullable": "accepted",
            "target_column": "customer_id",
            "target_type": "varchar(10)",
            "target_nullable": "accepted",
            "transformation": "glt.customer_id = lts.customer_id"
        },
        {
            "source_columns": [
                "lts.branch_id"
            ],
            "source_type": "varchar(10)",
            "source_nullable": "accepted",
            "target_column": "branch_id",
            "target_type": "varchar(10)",
            "target_nullable": "accepted",
            "transformation": "glt.branch_id = lts.branch_id"
        },
        {
            "source_columns": [
                "cps.customer_id"
            ],
            "source_type": "varchar(10)",
            "source_nullable": "not_accepted",
            "target_column": "customer_id",
            "target_type": "varchar(10)",
            "target_nullable": "not_accepted",
            "transformation": "gc.customer_id = cps.customer_id"
        },
        {
            "source_columns": [
                "cps.customer_name"
            ],
            "source_type": "varchar(255)",
            "source_nullable": "accepted",
            "target_column": "customer_name",
            "target_type": "varchar(255)",
            "target_nullable": "accepted",
            "transformation": "gc.customer_name = cps.customer_name"
        },
        {
            "source_columns": [
                "cps.contact_information"
            ],
            "source_type": "varchar(255)",
            "source_nullable": "accepted",
            "target_column": "contact_information",
            "target_type": "varchar(255)",
            "target_nullable": "accepted",
            "transformation": "gc.contact_information = cps.contact_information"
        },
        {
            "source_columns": [
                "cps.customer_risk_score"
            ],
            "source_type": "int",
            "source_nullable": "accepted",
            "target_column": "customer_risk_score",
            "target_type": "int",
            "target_nullable": "accepted",
            "transformation": "gc.customer_risk_score = cps.customer_risk_score"
        },
        {
            "source_columns": [
                "bps.branch_id"
            ],
            "source_type": "varchar(10)",
            "source_nullable": "not_accepted",
            "target_column": "branch_id",
            "target_type": "varchar(10)",
            "target_nullable": "not_accepted",
            "transformation": "gb.branch_id = bps.branch_id"
        },
        {
            "source_columns": [
                "bps.branch_name"
            ],
            "source_type": "varchar(255)",
            "source_nullable": "accepted",
            "target_column": "branch_name",
            "target_type": "varchar(255)",
            "target_nullable": "accepted",
            "transformation": "gb.branch_name = bps.branch_name"
        },
        {
            "source_columns": [
                "bps.location"
            ],
            "source_type": "varchar(255)",
            "source_nullable": "accepted",
            "target_column": "location",
            "target_type": "varchar(255)",
            "target_nullable": "accepted",
            "transformation": "gb.location = bps.location"
        },
        {
            "source_columns": [
                "bps.manager"
            ],
            "source_type": "varchar(255)",
            "source_nullable": "accepted",
            "target_column": "manager",
            "target_type": "varchar(255)",
            "target_nullable": "accepted",
            "transformation": "gb.manager = bps.manager"
        },
        {
            "source_columns": [
                "bpsf.branch_id"
            ],
            "source_type": "varchar(10)",
            "source_nullable": "accepted",
            "target_column": "branch_id",
            "target_type": "varchar(10)",
            "target_nullable": "accepted",
            "transformation": "glp.branch_id = bpsf.branch_id"
        },
        {
            "source_columns": [
                "bpsf.loan_id"
            ],
            "source_type": "varchar(10)",
            "source_nullable": "not_accepted",
            "target_column": "loan_id",
            "target_type": "varchar(10)",
            "target_nullable": "not_accepted",
            "transformation": "glp.loan_id = bpsf.loan_id"
        },
        {
            "source_columns": [
                "bpsf.customer_id"
            ],
            "source_type": "varchar(10)",
            "source_nullable": "accepted",
            "target_column": "customer_id",
            "target_type": "varchar(10)",
            "target_nullable": "accepted",
            "transformation": "glp.customer_id = bpsf.customer_id"
        },
        {
            "source_columns": [
                "bpsf.total_amount"
            ],
            "source_type": "int",
            "source_nullable": "accepted",
            "target_column": "total_amount",
            "target_type": "int",
            "target_nullable": "accepted",
            "transformation": "glp.total_amount = bpsf.total_amount"
        },
        {
            "source_columns": [
                "bpsf.average_repayment_time"
            ],
            "source_type": "double",
            "source_nullable": "accepted",
            "target_column": "average_repayment_time",
            "target_type": "double",
            "target_nullable": "accepted",
            "transformation": "glp.average_repayment_time = bpsf.average_repayment_time"
        },
        {
            "source_columns": [
                "bpsf.overdue_loans_count"
            ],
            "source_type": "int",
            "source_nullable": "accepted",
            "target_column": "overdue_loans_count",
            "target_type": "int",
            "target_nullable": "accepted",
            "transformation": "glp.overdue_loans_count = bpsf.overdue_loans_count"
        }
    ]
}

def _safe_get(dct, key, default=None):
    if dct is None:
        return default
    v = dct.get(key, default)
    return default if v is None else v

def _is_empty(x):
    if x is None:
        return True
    if isinstance(x, (list, dict, str)) and len(x) == 0:
        return True
    return False

def _parse_mapping_details(mapping_details):
    s = (mapping_details or "").strip()
    if not s:
        return None

    s_norm = re.sub(r"\s+", " ", s).strip()

    if " JOIN " not in s_norm.upper():
        m = re.match(r"^(?P<table>[^\s]+)\s+(?P<alias>[^\s]+)$", s_norm)
        if not m:
            raise ValueError("Unable to parse single-source mapping_details")
        return {
            "base": {"table": m.group("table"), "alias": m.group("alias")},
            "joins": []
        }

    from_re = re.compile(
        r"^(?P<base_table>[^\s]+)\s+(?P<base_alias>[^\s]+)\s*(?P<rest>.*)$",
        flags=re.IGNORECASE
    )
    m = from_re.match(s_norm)
    if not m:
        raise ValueError("Unable to parse join-based mapping_details")

    base_table = m.group("base_table")
    base_alias = m.group("base_alias")
    rest = m.group("rest").strip()

    join_re = re.compile(
        r"^(?P<join_type>LEFT|RIGHT|FULL|INNER|CROSS)\s+JOIN\s+(?P<table>[^\s]+)\s+(?P<alias>[^\s]+)\s+ON\s+(?P<on>.*)$",
        flags=re.IGNORECASE
    )

    joins = []
    remaining = rest
    while remaining:
        jm = join_re.match(remaining)
        if not jm:
            raise ValueError("Unable to parse join clause in mapping_details")

        join_type = jm.group("join_type").upper()
        table = jm.group("table")
        alias = jm.group("alias")
        on_and_more = jm.group("on").strip()

        next_join_pos = re.search(r"\s+(LEFT|RIGHT|FULL|INNER|CROSS)\s+JOIN\s+", on_and_more, flags=re.IGNORECASE)
        if next_join_pos:
            on_expr = on_and_more[:next_join_pos.start()].strip()
            remaining = on_and_more[next_join_pos.start():].strip()
        else:
            on_expr = on_and_more.strip()
            remaining = ""

        joins.append({"join_type": join_type, "table": table, "alias": alias, "on": on_expr})

    return {
        "base": {"table": base_table, "alias": base_alias},
        "joins": joins
    }

def _extract_eq_join_keys(on_expr):
    expr = (on_expr or "").strip()
    if not expr:
        return []

    parts = re.split(r"\s+(?i:AND)\s+", expr)
    keys = []
    for p in parts:
        m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\s*$", p.strip())
        if not m:
            continue
        left = m.group(1)
        right = m.group(2)
        keys.append((left, right))
    return keys

def _read_dataset(source_path, table_name, read_format, read_options):
    reader = spark.read.format(read_format)

    if (read_format or "").lower() == "csv" and _is_empty(read_options):
        reader = reader.option("header", "true").option("inferSchema", "true")
    else:
        for k, v in (read_options or {}).items():
            reader = reader.option(str(k), str(v))

    full_path = source_path.rstrip("/") + "/" + table_name.strip("/") + "/"
    return reader.load(full_path)

def _write_dataset(df, target_path, target_table, write_format, write_mode, write_options, partition_columns):
    writer = df.write.format(write_format).mode(write_mode)

    if (write_format or "").lower() == "csv" and _is_empty(write_options):
        writer = writer.option("header", "true")
    else:
        for k, v in (write_options or {}).items():
            writer = writer.option(str(k), str(v))

    if not _is_empty(partition_columns):
        writer = writer.partitionBy(*partition_columns)

    out_path = target_path.rstrip("/") + "/" + target_table.strip("/") + "/"
    writer.save(out_path)

def _build_alias_to_table(mapping_spec):
    alias_to_table = {}
    alias_to_table[mapping_spec["base"]["alias"]] = mapping_spec["base"]["table"]
    for j in mapping_spec["joins"]:
        alias_to_table[j["alias"]] = j["table"]
    return alias_to_table

def _infer_required_sources_from_columns(columns_meta, target_alias):
    required_aliases = set()
    for c in columns_meta:
        t = c.get("transformation")
        if not t:
            continue
        m = re.search(r"^\s*" + re.escape(target_alias) + r"\.[A-Za-z_][A-Za-z0-9_]*\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\.", t.strip())
        if m:
            required_aliases.add(m.group(1))
    return required_aliases

def _select_projection_from_transformations(df, cols_meta_for_table, target_alias):
    projections = []
    for c in cols_meta_for_table:
        t = (c.get("transformation") or "").strip()
        if not t:
            continue

        m = re.match(
            r"^\s*" + re.escape(target_alias) + r"\.(?P<target>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<expr>.+)\s*$",
            t
        )
        if not m:
            raise ValueError("Unsupported transformation pattern for target_alias: " + target_alias)

        target_col = m.group("target")
        rhs_expr = m.group("expr").strip()

        projections.append(F.expr(rhs_expr).alias(target_col))

    if _is_empty(projections):
        return df
    return df.select(*projections)

def _apply_validations(df, validations):
    if _is_empty(validations):
        return df, None

    conditions = []
    for v in validations:
        expr_str = v.get("expression") or v.get("condition")
        if not expr_str:
            continue
        conditions.append(F.expr(expr_str))

    if _is_empty(conditions):
        return df, None

    valid_cond = reduce(lambda a, b: a & b, conditions)
    valid_df = df.filter(valid_cond)
    invalid_df = df.filter(~valid_cond)
    return valid_df, invalid_df

def _process_table(table_meta, all_columns_meta, runtime_conf):
    target_table = table_meta.get("target_table")
    target_alias = table_meta.get("target_alias")
    mapping_details = table_meta.get("mapping_details")

    if not target_table or not target_alias or not mapping_details:
        raise ValueError("Missing required table metadata keys for processing")

    mapping_spec = _parse_mapping_details(mapping_details)
    alias_to_table = _build_alias_to_table(mapping_spec)

    cols_meta_for_table = []
    for c in (all_columns_meta or []):
        t = (c.get("transformation") or "").strip()
        if t.startswith(target_alias + "."):
            cols_meta_for_table.append(c)

    required_source_aliases = _infer_required_sources_from_columns(cols_meta_for_table, target_alias)
    if _is_empty(required_source_aliases):
        required_source_aliases = {mapping_spec["base"]["alias"]}

    read_format = _safe_get(table_meta, "read_format", _safe_get(runtime_conf, "file_format"))
    write_format = _safe_get(table_meta, "write_format", _safe_get(runtime_conf, "file_format"))
    read_options = _safe_get(table_meta, "read_options", _safe_get(runtime_conf, "read_options", None))
    write_options = _safe_get(table_meta, "write_options", _safe_get(runtime_conf, "write_options", None))
    write_mode = _safe_get(table_meta, "write_mode", _safe_get(runtime_conf, "write_mode", "overwrite"))
    partition_columns = _safe_get(table_meta, "partition_columns", _safe_get(runtime_conf, "partition_columns", None))

    source_path = _safe_get(table_meta, "source_path", _safe_get(runtime_conf, "source_path"))
    target_path = _safe_get(table_meta, "target_path", _safe_get(runtime_conf, "target_path"))

    if not source_path or not target_path or not read_format or not write_format:
        raise ValueError("Missing required IO metadata for table: " + str(target_table))

    dataframes = {}
    for alias, tbl in alias_to_table.items():
        if alias in required_source_aliases or alias == mapping_spec["base"]["alias"]:
            source_table_name = tbl.split(".", 1)[1] if "." in tbl else tbl
            df = _read_dataset(source_path, source_table_name, read_format, read_options)
            dataframes[alias] = df.alias(alias)

    base_alias = mapping_spec["base"]["alias"]
    if base_alias not in dataframes:
        base_table_name = alias_to_table[base_alias].split(".", 1)[1] if "." in alias_to_table[base_alias] else alias_to_table[base_alias]
        dataframes[base_alias] = _read_dataset(source_path, base_table_name, read_format, read_options).alias(base_alias)

    df_joined = dataframes[base_alias]

    for j in mapping_spec["joins"]:
        alias = j["alias"]
        if alias not in dataframes:
            continue

        join_keys = _extract_eq_join_keys(j.get("on"))
        if _is_empty(join_keys):
            raise ValueError("Join ON expression could not be interpreted as equality join keys")

        join_cond = None
        for left_ref, right_ref in join_keys:
            l_alias, l_col = left_ref.split(".", 1)
            r_alias, r_col = right_ref.split(".", 1)

            cond_piece = (F.col(l_alias + "." + l_col) == F.col(r_alias + "." + r_col))
            join_cond = cond_piece if join_cond is None else (join_cond & cond_piece)

        df_joined = df_joined.join(dataframes[alias], join_cond, j["join_type"].lower())

    filters = _safe_get(table_meta, "filters", None)
    if not _is_empty(filters):
        for f in filters:
            expr_str = f.get("expression") or f.get("condition")
            if expr_str:
                df_joined = df_joined.filter(F.expr(expr_str))

    window_cfgs = _safe_get(table_meta, "window_functions", None)
    if not _is_empty(window_cfgs):
        from pyspark.sql.window import Window
        for w in window_cfgs:
            out_col = w.get("target_column")
            func_expr = w.get("expression")
            if not out_col or not func_expr:
                continue
            df_joined = df_joined.withColumn(out_col, F.expr(func_expr))

    df_projected = _select_projection_from_transformations(df_joined, cols_meta_for_table, target_alias)

    agg_cfg = _safe_get(table_meta, "aggregation", None)
    if not _is_empty(agg_cfg):
        group_by_cols = agg_cfg.get("group_by")
        aggregations = agg_cfg.get("metrics") or agg_cfg.get("aggregations")
        if _is_empty(group_by_cols) or _is_empty(aggregations):
            raise ValueError("Aggregation config present but missing group_by and/or metrics")

        agg_exprs = []
        for m in aggregations:
            alias = m.get("alias") or m.get("target_column")
            expr_str = m.get("expression")
            if not alias or not expr_str:
                continue
            agg_exprs.append(F.expr(expr_str).alias(alias))

        if _is_empty(agg_exprs):
            raise ValueError("Aggregation metrics did not yield any expressions")

        df_projected = df_projected.groupBy(*group_by_cols).agg(*agg_exprs)

    op_meta = _safe_get(table_meta, "operational_metadata", None)
    if not _is_empty(op_meta):
        for col_def in op_meta:
            col_name = col_def.get("column_name")
            expr_str = col_def.get("expression")
            if col_name and expr_str:
                df_projected = df_projected.withColumn(col_name, F.expr(expr_str))

    validations = _safe_get(table_meta, "validations", None)
    valid_df, invalid_df = _apply_validations(df_projected, validations)

    if invalid_df is not None:
        reject_cfg = _safe_get(table_meta, "rejects", None)
        if reject_cfg and reject_cfg.get("reject_path") and reject_cfg.get("write_format"):
            rej_path = reject_cfg.get("reject_path").rstrip("/") + "/" + target_table.strip("/") + "/"
            rej_format = reject_cfg.get("write_format")
            rej_mode = reject_cfg.get("write_mode", "append")
            rej_options = reject_cfg.get("write_options", None)

            rej_writer = invalid_df.write.format(rej_format).mode(rej_mode)
            if (rej_format or "").lower() == "csv" and _is_empty(rej_options):
                rej_writer = rej_writer.option("header", "true")
            else:
                for k, v in (rej_options or {}).items():
                    rej_writer = rej_writer.option(str(k), str(v))
            rej_writer.save(rej_path)

    _write_dataset(valid_df, target_path, target_table, write_format, write_mode, write_options, partition_columns)

    return {"target_table": target_table, "status": "SUCCESS"}

fail_fast = bool(_safe_get(metadata, "fail_fast", False))
results = []
errors = []

for t in (metadata.get("tables") or []):
    tgt = t.get("target_table", "<unknown>")
    try:
        logger.info(json.dumps({"event": "TABLE_START", "target_table": tgt}))
        res = _process_table(t, metadata.get("columns") or [], metadata)
        results.append(res)
        logger.info(json.dumps({"event": "TABLE_SUCCESS", "target_table": tgt}))
    except Exception as e:
        err = {
            "event": "TABLE_FAILURE",
            "target_table": tgt,
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc()
        }
        errors.append(err)
        logger.error(json.dumps(err))
        if fail_fast:
            raise

logger.info(json.dumps({"event": "JOB_SUMMARY", "tables_total": len(metadata.get("tables") or []), "tables_success": len(results), "tables_failed": len(errors)}))

job.commit()