#!/usr/bin/env python
# coding: utf-8

# ## Postgres to Snowflake Export

# In[ ]:


from __future__ import annotations
from pathlib import Path
import csv
import gzip
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, List, Optional, Tuple
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as Psycopg2Connection
from psycopg2 import sql
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import copy
import math
from pathlib import Path
import yaml
import subprocess
import shlex
from dateutil import parser
#from snowflake.ingest import SimpleIngestManager, StagedFile
import json
import uuid
import socket
import requests
from cryptography.hazmat.primitives import serialization


# ## 1) Config file

# ### Load config file

# In[ ]:


def load_config(config_path: str) -> Dict[str, Any]:
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            return config or {}
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found: {config_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in config file: {e}")


# ### Validate config file

# In[ ]:


def validate_config(cfg: Dict[str, Any]) -> None:
    # Top-level keys
    required_top_keys = {"version", "source", "export", "tables"}
    missing = required_top_keys - cfg.keys()
    if missing:
        raise KeyError(f"Missing top-level keys: {', '.join(sorted(missing))}")

    # Validate source
    if not isinstance(cfg["source"], dict) or not cfg["source"]:
        raise ValueError("'source' must be a non-empty dictionary")

    for source_name, conn in cfg["source"].items():
        if not isinstance(conn, dict):
            raise TypeError(f"Source '{source_name}' must be a dictionary")
        for key in ("host", "port", "database", "user", "password_env", "schema"):
            if key not in conn:
                raise KeyError(f"Missing '{key}' in source '{source_name}'")
                
    # Validate target
    if not isinstance(cfg["target"], dict) or not cfg["target"]:
        raise ValueError("'target' must be a non-empty dictionary")

    for target_name, conn in cfg["target"].items():
        if not isinstance(conn, dict):
            raise TypeError(f"Target '{target_name}' must be a dictionary")
        for key in ("account", "user", "password_env", "role", "database", "schema"):
            if key not in conn:
                raise KeyError(f"Missing '{key}' in target '{target_name}'")

    # Validate export
    export = cfg["export"]
    if not isinstance(export, dict):
        raise TypeError("'export' must be a dictionary")
    for key in ("output_dir", "format"):
        if key not in export:
            raise KeyError(f"Missing '{key}' in 'export'")

    # Validate tables
    tables = cfg["tables"]
    if not isinstance(tables, dict) or not tables:
        raise ValueError("'tables' must be a non-empty list")
    for i, table in enumerate(tables):
        t=tables[table][0]
        if not isinstance(t, dict):
            raise TypeError(f"Table at index {i} must be a dictionary")
        if "name" not in t or "mode" not in t or "order_by" not in t:
            raise KeyError(f"Table {t} missing required keys")
        if t["mode"] == "table" and "table" not in t:
            raise KeyError(f"Table {t['name']} missing 'table' key for mode 'table'")
        if t["mode"] == "query" and "query" not in t:
            raise KeyError(f"Table {t['name']} missing 'query' key for mode 'query'")


# In[ ]:


def validate_yaml_file(path: str) -> Dict[str, Any]:
    # Load YAML
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"YAML file not found: {path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML syntax: {e}")

    if not isinstance(cfg, dict):
        raise ValueError("YAML root must be a dictionary")

    validate_config(cfg)
    return True


# ## 2) Connections

# ### Create postgres database connection

# In[ ]:


@dataclass(frozen=True)
class PostgresCreds:
    host: str
    port: str
    dbname: str
    user: str
    password: str # use password_env in YAML (recommended)
    schema: Optional[str]
    


# In[ ]:


def create_pg_connection(creds: PostgresCreds) -> Psycopg2Connection:
    """
    Create a PostgreSQL connection using psycopg2.
    """
    conn = psycopg2.connect(
        host=creds.host,
        port=creds.port,
        dbname=creds.dbname,
        user=creds.user,
        password=creds.password,
    )

    # Optionally set search_path if schema is provided
    if creds.schema:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SET search_path TO {}").format(
                    sql.Identifier(creds.schema)
                )
            )
            conn.commit()

    return conn


# ### Create Snowflake connection

# In[ ]:


@dataclass(frozen=True)
class SnowflakeCreds:
    account: str
    user: str
    role: Optional[str]
    warehouse: Optional[str]
    database: Optional[str]
    schema: Optional[str]
    password: Optional[str]
    sf_landing_stage: Optional[str]


# In[ ]:


def _snowsql_base_cmd(creds: SnowflakeCreds, snowsql_path: str = "snowsql") -> list[str]:
    cmd = [
        snowsql_path,
        "-a", creds.account,
        "-u", creds.user,
        "-o", "exit_on_error=true",
        "-o", "friendly=false",
        "-o", "quiet=false",
    ]
    return cmd


# In[ ]:


def _run_snowsql(creds: SnowflakeCreds, sql: str, *, snowsql_path: str = "snowsql", timeout_sec: int = 300) -> str:
    env = os.environ.copy()
    if creds.password:
        env["SNOWSQL_PWD"] = creds.password

    cmd = _snowsql_base_cmd(creds, snowsql_path=snowsql_path) + ["-q", sql]

    proc = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "SnowSQL command failed.\n"
            f"Command: {shlex.join(cmd)}\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )
    return proc.stdout


# In[ ]:


def configure_network_for_snowflake(
    proxy_url: str = "http://proxy.mycorp.com:8080",
    snowflake_host: str = "account_name",
    timeout_s: int = 8,
):
    """
    Makes Snowflake + stage (S3) uploads work without manual toggling.

    Strategy:
    - If proxy hostname resolves (likely on VPN/corp network):
        Use proxy for general internet (so S3 works),
        but bypass proxy for *.snowflakecomputing.com (so Snowflake doesn't get stuck on proxy DNS rules).
    - If proxy hostname does NOT resolve (likely off VPN/home network):
        Disable proxy env vars entirely and go direct.
    - Then run quick connectivity checks for Snowflake host DNS + S3 reachability.
    """
    
    def can_resolve(host: str) -> bool:
        try:
            socket.getaddrinfo(host, 80)
            return True
        except OSError:
            return False

    proxy_host = proxy_url.replace("http://", "").replace("https://", "").split(":")[0]
    proxy_resolves = can_resolve(proxy_host)

    if proxy_resolves:
        # Proxy is usable (VPN/corp). Keep it for S3, but bypass for Snowflake.
        for k in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
            os.environ[k] = proxy_url

        no_proxy = ",".join([
            "localhost", "127.0.0.1",
            snowflake_host,
            ".snowflakecomputing.com",
        ])
        os.environ["NO_PROXY"] = no_proxy
        os.environ["no_proxy"] = no_proxy
        mode = "PROXY_FOR_S3__BYPASS_FOR_SNOWFLAKE"
    else:
        # Proxy not resolvable (home/off VPN). Go fully direct.
        for k in ["HTTP_PROXY","HTTPS_PROXY","ALL_PROXY","http_proxy","https_proxy","all_proxy"]:
            os.environ.pop(k, None)
        os.environ["NO_PROXY"] = ",".join(["localhost","127.0.0.1",snowflake_host,".snowflakecomputing.com"])
        os.environ["no_proxy"] = os.environ["NO_PROXY"]
        mode = "DIRECT_NO_PROXY"

    # --- Fast checks ---
    # 1) Snowflake DNS
    try:
        socket.getaddrinfo(snowflake_host, 443)
        snowflake_dns_ok = True
    except OSError:
        snowflake_dns_ok = False

    # 2) S3 reachability (HEAD is enough)
    s3_ok = None
    try:
        r = requests.head("https://s3.amazonaws.com", timeout=timeout_s)
        s3_ok = (r.status_code < 500)
    except Exception:
        s3_ok = False

    return {
        "mode": mode,
        "proxy_resolves": proxy_resolves,
        "snowflake_dns_ok": snowflake_dns_ok,
        "s3_ok": s3_ok,
        "http_proxy": os.environ.get("HTTP_PROXY"),
        "no_proxy": os.environ.get("NO_PROXY"),
    }



# ## 3) Pipeline

# ### Extract data from postgres

# In[ ]:


def get_table_columns(conn: psycopg2.extensions.connection, schema: str, table: str) -> List[str]:
    """
    Fetch column names for a given schema.table in PostgreSQL.

    Args:
        conn: psycopg2 connection
        schema: schema name (e.g., 'public')
        table: table name (e.g., 'hotel')

    Returns:
        List of column names in order.
    """
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position;
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        columns = [row[0] for row in cur.fetchall()]
    return columns


# In[ ]:


def get_table_pk(conn: psycopg2.extensions.connection, schema: str, table: str) -> Optional[List[str]]:
    """
    Fetch the primary key column names for a given schema.table in PostgreSQL.

    Args:
        conn: psycopg2 connection
        schema: schema name (e.g., 'public')
        table: table name (e.g., 'hotel')

    Returns:
        List of primary key column names in order, or None if table has no PK.
    """
    query = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    WHERE tc.table_schema = %s
      AND tc.table_name = %s
      AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY kcu.ordinal_position;
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        pk_columns = [row[0] for row in cur.fetchall()]

    return pk_columns if pk_columns else None


# In[ ]:


def estimate_rowcount(conn: psycopg2.extensions.connection, sql: str) -> int:
    """
    Estimate the number of rows a SQL query will return.
    Uses COUNT(*) wrapped around the query.

    Args:
        conn: psycopg2 connection
        sql: SQL query (string)

    Returns:
        Estimated row count (int)
    """
    # Wrap the original query as a subquery
    count_sql = f"SELECT COUNT(*) FROM ({sql}) AS subquery"
    try:
        with conn.cursor() as cur:
            cur.execute(count_sql)
            rowcount = cur.fetchone()[0]
    except Exception:
        conn.rollback()
        raise
    return rowcount


# In[ ]:


def build_base_query(table_cfg: Dict, schema_default: str, columns: List[str] = None) -> str:
    """
    Build the base SQL query for a table export.

    Args:
        table_cfg: dictionary containing table config from YAML
                   Must include 'mode' and either 'table' or 'query'
        schema_default: default schema to use if table_cfg does not specify one
        columns: optional list of columns to select (for mode='table')

    Returns:
        SQL string (no trailing semicolon)
    """
    mode = table_cfg.get("mode")
    
    if mode == "table":
        table_name = table_cfg.get("table")
        if not table_name:
            raise KeyError(f"Table config '{table_cfg.get('name')}' missing 'table' key for mode='table'")
        
        schema = table_cfg.get("schema", schema_default)
        
        # Use all columns if not specified
        cols_sql = ", ".join(columns) if columns else "*"
        
        sql = f"SELECT {cols_sql} FROM {schema}.{table_name}"
        return sql

    elif mode == "query":
        query = table_cfg.get("query")
        if not query:
            raise KeyError(f"Table config '{table_cfg.get('name')}' missing 'query' key for mode='query'")
        return query.rstrip().rstrip(";")  # strip trailing semicolon

    else:
        raise ValueError(f"Unknown mode '{mode}' for table '{table_cfg.get('name')}'")


# In[ ]:


def apply_partition_clause(
    base_sql: str,
    partition_spec: Dict
) -> List[str]:
    """
    Partition base_sql across [start_date, end_date) and return a list of SQL strings,
    one per partition window.

    Supported:
      partition_spec = {"type": "date_range", "column": "checkin_date", "granularity": "month"|"day"}

    Notes:
      - end_date is EXCLUSIVE (so end_date="2026-01-01" includes all of 2025).
      - If base_sql already contains a WHERE (case-insensitive), appends with AND.
    """
    column = partition_spec.get("column")
    start_date=partition_spec.get("start")
    end_date=partition_spec.get("end")
    if not column:
        raise KeyError("partition_spec must have a 'column' key")

    if partition_spec.get("type") != "date_range":
        raise ValueError(f"Unsupported partition type '{partition_spec.get('type')}'")

    granularity = partition_spec.get("granularity", "month").lower()
    if granularity not in {"month", "day"}:
        raise ValueError(f"Unsupported granularity '{granularity}' (use 'month' or 'day')")

    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if start >= end:
        raise ValueError(f"start_date must be < end_date. Got {start_date} .. {end_date}")

    # Helper: next month boundary from a given date
    def next_month(d: date) -> date:
        if d.month == 12:
            return date(d.year + 1, 1, 1)
        return date(d.year, d.month + 1, 1)

    # Helper: attach WHERE/AND
    has_where = " where " in base_sql.lower()

    sqls: List[str] = []
    cur = start

    while cur < end:
        if granularity == "day":
            nxt = cur + timedelta(days=1)
        else:  # month
            # advance to first of next month; works best when cur is 1st-of-month (your case)
            nxt = next_month(cur)

        window_end = nxt if nxt < end else end

        clause = (
            f"{column} >= '{cur.strftime('%Y-%m-%d')}' "
            f"AND {column} < '{window_end.strftime('%Y-%m-%d')}'"
        )

        if has_where:
            sqls.append(f"{base_sql} AND {clause}")
        else:
            sqls.append(f"{base_sql} WHERE {clause}")

        cur = window_end

    return sqls


# In[ ]:


def plan_file_splits(rowcount: int, max_rows_per_file: int) -> List[Dict[str, int]]:
    """
    Plan file splits for a given rowcount and max rows per file.

    Args:
        rowcount: total number of rows in the partition
        max_rows_per_file: maximum rows per output file

    Returns:
        List of dicts with keys:
            - start_row: 0-based inclusive start
            - end_row: exclusive end
    """
    if rowcount <= 0:
        return []

    chunks = []
    start = 0

    while start < rowcount:
        end = min(start + max_rows_per_file, rowcount)
        chunks.append({"start_row": start, "end_row": end})
        start = end

    return chunks


# In[ ]:


def build_chunk_query(
    sql: str,
    order_by: Optional[str],
    chunk: Dict[str, int]
) -> str:
    """
    Build a SQL query for a specific chunk of rows.

    Args:
        sql: base SQL (should include WHERE/filters/partition/order)
        order_by: column(s) used for deterministic ordering
        chunk: dict with 'start_row' (inclusive) and 'end_row' (exclusive)

    Returns:
        SQL string with ORDER BY + OFFSET/LIMIT applied
    """
    if not order_by:
        raise ValueError("order_by must be specified for chunking")

    start = chunk["start_row"]
    limit = chunk["end_row"] - chunk["start_row"]
    
    # Ensure SQL has ORDER BY
    sql_ordered = sql if " order by " in sql.lower() else f"{sql} ORDER BY {order_by}"

    return f"{sql_ordered} OFFSET {start} LIMIT {limit}"


# #### Load data into snowflake

# In[ ]:


def postgres_query_to_snowflake_table(
    pg: PostgresCreds,
    sf: SnowflakeCreds,
    data_dir: str,
    sql: str,                               # your dynamic SELECT
    stage_fqn: str,                         # e.g. "@HOTEL_ANALYTICS.RAW.LANDING_STAGE"
    file_format_fqn: str,                   # e.g. "HOTEL_ANALYTICS.RAW.CSV_FMT" (CSV, SKIP_HEADER=1, COMPRESSION=GZIP)
    target_table_fqn: str,                  # e.g. "HOTEL_ANALYTICS.RAW.BOOKING_SLICE"
    overwrite_table: bool = True,
) -> dict:
    """
    Executes SELECT on Postgres, stages results as gzipped CSV, then loads into Snowflake.
    - Requires Snowflake INTERNAL stage (PUT must work).
    - Creates/overwrites target table with inferred column types as VARCHAR by default.
      (Good enough for a project slice; you can tighten types later.)

    Returns basic run stats.
    """
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # --- Connect Postgres ---
    pg_conn = psycopg2.connect(
        host=pg.host, port=pg.port, dbname=pg.dbname, user=pg.user, password=pg.password
    )
    pg_cur = pg_conn.cursor()

    pg_cur.execute(sql)
    cols: List[str] = [d.name for d in pg_cur.description]

    # --- Write gzipped CSV ---
    tmp_dir = Path(data_dir) / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    local_file = os.path.join(tmp_dir, f"extract__{run_id}.csv.gz")
    file_uri= "file://" + Path(local_file).resolve().as_posix()
    
    rows_extracted = 0
    with gzip.open(local_file, "wt", newline="", encoding="utf-8") as gz:
        w = csv.writer(gz)
        w.writerow(cols)  # header
        for row in pg_cur:
            w.writerow(row)
            rows_extracted += 1

    pg_cur.close()
    pg_conn.close()

    if rows_extracted == 0:
        return {"status": "SUCCESS", "rows_extracted": 0, "rows_loaded": 0, "target_table": target_table_fqn}


        # --- Connect Snowflake ---
    sf_conn = snowflake.connector.connect(
        account=sf.account,
        user=sf.user,
        password=sf.password,
        role=sf.role,
        warehouse=sf.warehouse,
        database=sf.database,
        schema=sf.schema,
        autocommit=True,
    )
    sf_cur = sf_conn.cursor()

    # Put into a deterministic folder under the stage
    stage_path = f"{stage_fqn}/pg_extract/{run_id}"
    sf_cur.execute(f"PUT '{file_uri}' {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

    # Optionally drop and re-create the table (simple project-friendly behavior)
    if overwrite_table:
        sf_cur.execute(f"DROP TABLE IF EXISTS {target_table_fqn}")

    # Create a table with simple VARCHAR columns (fast + robust for a demo)
    col_ddl = ", ".join([f'"{c.upper()}" VARCHAR' for c in cols])
    sf_cur.execute(f"CREATE TABLE IF NOT EXISTS {target_table_fqn} ({col_ddl})")
    
        # 2) COPY into RAW TABLE
    sf_cur.execute(f"""
        COPY INTO {target_table_fqn}
        FROM {stage_path}
        FILE_FORMAT = (FORMAT_NAME = {file_format_fqn})
        PATTERN = '.*\\.csv\\.gz'
        ON_ERROR = 'ABORT_STATEMENT'
    """)
    results = sf_cur.fetchall()
    loaded = sum(int(r[3]) for r in results if len(r) > 3)

    sf_cur.close()
    sf_cur.close()

    return {
        "status": "SUCCESS",
        "rows_extracted": rows_extracted,
        "rows_loaded": loaded,
        "local_file": str(local_file),
        "stage_path": stage_path,
    }


# In[ ]:


def main():
    ##### Get information from config files
    BASE_DIR = Path.cwd().parent 
    config_path = BASE_DIR / "config"
    config_yaml=config_path/ "postgres_to_snowflake_config.yaml"
    data_dir=BASE_DIR/"data"
    passwords_path=config_path/".env"

    ##### Store all passwords
    load_dotenv(dotenv_path=passwords_path,override=True)  # loads .env into environment variables
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
    config=load_config(config_yaml)
           
    ##### Validate config file
    if validate_yaml_file(config_yaml): 
        config=load_config(config_yaml)
    else:
        raise FileNotFoundError(f"YAML config file not found or invalid: {config_yaml}")

    ##### Connect to Postgres DB
    pg_cfg = config.get("source", {}).get("postgres", {})
    
    pg_creds=PostgresCreds(
        host=pg_cfg.get("host", "localhost"),
        port=str(pg_cfg.get("port", 5432)),
        dbname=pg_cfg.get("database", ""),
        user=pg_cfg.get("user", ""),
        password=postgres_password,
        schema=pg_cfg.get("schema")  # can be None
    )
    
    pg_conn=create_pg_connection(pg_creds)

    ###### Connect to Snowflake
    sf_cfg = config.get("target", {}).get("snowflake", {})
    
    sf_creds=SnowflakeCreds(
        account=sf_cfg.get("account"),
        user=sf_cfg.get("user"),
        role=sf_cfg.get("role"),
        warehouse=sf_cfg.get("warehouse"),
        database=sf_cfg.get("database"),
        schema=sf_cfg.get("schema"),
        password=snowflake_password,
        sf_landing_stage=sf_cfg.get("stage")
    )

    net = configure_network_for_snowflake(sf_creds.account)
    ####### Hotel Config
    table_schema=pg_cfg.get("schema")
    sqls=[]
    max_rows_per_file=config.get("export", {}).get("row_grouping",{}).get("max_rows_per_file",{})
    for table in config.get("tables", {}).keys():
        table_name=config.get("tables", {})[table][0].get("name")
        print(f"{sf_creds.database}.{sf_creds.schema}.{table_name}_RAW")
        table_cfg=config.get("tables", {})[table][0]
        globals()[f"{table_name}_columns"]=get_table_columns(pg_conn,table_schema,table_name)
        globals()[f"{table_name}_pk"]=get_table_pk(pg_conn,table_schema,table_name)
        globals()[f"{table_name}_rowcount"]=estimate_rowcount(pg_conn,f'SELECT * FROM {table_schema}.{table_name}')
        globals()[f"{table_name}_base_query"]= build_base_query(table_cfg, table_schema, globals()[f"{table_name}_columns"])
        globals()[f"{table_name}_partition_spec"]=table_cfg.get("partition")

        if globals()[f"{table_name}_partition_spec"]!=None:
            sqls=apply_partition_clause(globals()[f"{table_name}_base_query"],globals()[f"{table_name}_partition_spec"])

            for sql in sqls:
                chunk=plan_file_splits(globals()[f"{table_name}_rowcount"], max_rows_per_file)
                chunk_sql_query=build_chunk_query(sql,table_cfg['order_by'],chunk[0])
                postgres_query_to_snowflake_table( pg_creds,sf_creds,data_dir,chunk_sql_query,
                                              f"@{sf_creds.database}.{sf_creds.schema}.{sf_creds.sf_landing_stage}",
                                              f"{sf_creds.database}.{sf_creds.schema}.CSV_FMT",
                                              f"{sf_creds.database}.{sf_creds.schema}.{table_name}_RAW",False)

        else:
            chunk=plan_file_splits(globals()[f"{table_name}_rowcount"], max_rows_per_file)
            chunk_sql_query=build_chunk_query(globals()[f"{table_name}_base_query"],table_cfg['order_by'],chunk[0])
            postgres_query_to_snowflake_table( pg_creds,sf_creds,data_dir,chunk_sql_query,
                                              f"@{sf_creds.database}.{sf_creds.schema}.{sf_creds.sf_landing_stage}",
                                              f"{sf_creds.database}.{sf_creds.schema}.CSV_FMT",
                                              f"{sf_creds.database}.{sf_creds.schema}.{table_name}_RAW")        


if __name__=="__main__":main()


# In[ ]:





# In[ ]:





# In[ ]:




