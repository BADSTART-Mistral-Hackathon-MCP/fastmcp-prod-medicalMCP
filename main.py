#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified MCP Server - BigQuery + Google Calendar Integration
Combines BigQuery data access and Calendar management in a single service
"""

import os
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date, time, timedelta

from fastmcp import FastMCP
from pydantic import Field
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from dateutil import parser as dtp
from dateutil import tz

# ============= Configuration unifiée =============

# Server runtime config
PORT = int(os.getenv("PORT", "3000"))
DEBUG = os.getenv("DEBUG", "true").lower() in {"1", "true", "yes"}
STATELESS = True  # Pour FastMCP Cloud

# BigQuery Configuration
PROJECT_ID = "mcp-hackathon-mistral"
LOCATION = os.getenv("BQ_LOCATION", "EU")

# Datasets autorisés (vide = tous)
_ALLOWED = [d.strip() for d in os.getenv("ALLOWED_DATASETS", "").split(",") if d.strip()]
ALLOWED_DATASETS = set(_ALLOWED) if _ALLOWED else set()

# Limites BigQuery
MAX_BYTES_BILLED = int(os.getenv("MAX_BYTES_BILLED", "200000000"))
MAX_ROWS = int(os.getenv("MAX_ROWS", "1000"))

# Défauts BigQuery
DEFAULT_DATASET = "prod_public"

HOSPITALS_DATASET = os.getenv("HOSPITALS_DATASET", DEFAULT_DATASET).strip()
HOSPITALS_TABLE = os.getenv("HOSPITALS_TABLE", "hospital").strip()
HOSPITALS_CONTACT_ALLOWED = os.getenv("HOSPITALS_CONTACT_ALLOWED", "false").lower() in {"1", "true", "yes"}

SURGEONS_DATASET = (os.getenv("SURGEONS_DATASET") or HOSPITALS_DATASET).strip()
SURGEONS_TABLE = os.getenv("SURGEONS_TABLE", "surgeons").strip()
SURGEONS_SENSITIVE_ALLOWED = os.getenv("SURGEONS_SENSITIVE_ALLOWED", "false").lower() in {"1", "true", "yes"}

PATIENTS_DATASET = os.getenv("PATIENTS_DATASET", DEFAULT_DATASET).strip()
PATIENTS_TABLE = os.getenv("PATIENTS_TABLE", "patients").strip()

CASES_DATASET = os.getenv("CASES_DATASET", DEFAULT_DATASET).strip()
CASES_TABLE = os.getenv("CASES_TABLE", "cases").strip()

LABS_DATASET = os.getenv("LABS_DATASET", DEFAULT_DATASET).strip()
LABS_TABLE = os.getenv("LABS_TABLE", "lab_results").strip()

# Calendar Configuration
TIMEZONE = os.getenv("TIMEZONE", "Europe/Paris")
DEFAULT_SCOPES = ["https://www.googleapis.com/auth/calendar"]
SCOPES: List[str] = DEFAULT_SCOPES
TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "token.json")

# BigQuery Service Account Credentials
BQ_SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "mcp-hackathon-mistral",
    "private_key_id": "b2b3d966d5b3bd579c901e6be63b0d13212a2e14",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7Y4nD9/twKbfX\ny0r/u4vZaMvxCO/7+B+PANseDp+JFVhMCN1XALxKE/tNpmCU41iZPVHpT9zaJfye\nrBTpPOxmbKXP+HOLA10UnWV5M9Uz2MEbJrii72X4dwKcSHd35scId9yF+Wj4tawH\njTsl+b3xEHPZO/Xk9igIHwcZydmFSrw2h/McIOpYDV5QFZvHiNjgm6ij91sVOvse\nnFn5mEAm+BdvPMSNaI5LD71wBAzHF0ID9+xU7XSwb91ptRGVKF8L9hZksNvTQUeS\nbUh42m9TmVLFstPA3AJpBTxOyAKsA9S2Uo7vAyW6aaz1SfC6PV6tNrJE92NpitDt\nREFQT9pJAgMBAAECggEAQyfuFIRH4S+iSjz6GOJewUC0biKU1wlaTgaxgHkfJaK3\nrTA0Gt0Rnb7BfleVH2bGtsxqEaJkdO3ONhNXvyrtUdu4JOtWhUhkUGIEHsa7rsQM\nmK1s2D/RnJUSI245GohjZh6GsqDqxM9e4qnzu61gLAeIbR73BeJOAHMWOWDEiuba\niUgg9+qCFbgjjBsh+GpuzejSp+akhAifSH5CUrAtXzQL9IR/nTLzDfCUuSxvrXBL\nfJlRR6zyHegcTk2ZuZqM65xuC4Ejk2rRBd33gbGPGGqoWuaGr91gqNn554I7icwb\nGlrWlkSWJRzq9fPc0SmGG3wIUu2cdtccR0qSqMS/bQKBgQD4Sgtz0ckVwCavG9iH\nc9UAVC/WD5q54lyWw8uYmE+rGjDmgDS0qdw3pHnAGjeYOIDTsmaq4Gp53dc9woAI\nHrMjUq7uI5h8dgXkJSU/VCS/deNK4Z/TPN8r/c3c9r1RRgVhdWn5IUu8FkWY/dVq\nGvl15T+r5uBrMyk0TidcPKxnXwKBgQDBNVJHuThRzleoBL3Nq2LC0mERwcQ57ZbM\nt7UM7RDn+y52rKXPq+NyGOT7pfxROMDSCmfe/N/plLz7iFy45rXczAWDLgpl9puH\nhOEVoI/KjHT2YUeefaSyg0wuueiAEPg9DjQ6ts2yrA2q4nFNVVRJ3TqHSfsHjeCc\n+hJxTs7nVwKBgGsVPDU6cDhiRAzXvJ5GtcHLjUoMNtYeq4IWdbOdVRbdV+PBvXmB\nnMmetSfF5t5O2Dj1Q1RFL4bZx6AKR7+4xdfhLDLmxThAiq/n2VWjy6mLhXjhMFYh\ndbr6XpQDEol/4ogy5H6e/pPjIyclqqp1ccuIENrp2zZAvW+imVUtkcmPAoGARfd+\nTXT4vT9BJRpadcGL6UtwVZLa8bNlectJKF4tUiT3JYjOHw97NVVoju0EG5G22hlk\nli7zE06GxXwTP+5ki4nisSeaImSU3BW1wTQ8/jexH4wI+I89dlvv2bf/R2ldzBZ5\nuY17nimKZYjNSRkOhhU0XcvfuVOatJ4m0Zudd88CgYEAp20twaSy2G08O6oapaPp\ngH+WZKRJJ54bMgFAuilbrSG0bnr4iTSJHkbySjwim2rXWb5lZvO86HACodDUfG3y\n8BJDicnKjFz2GmkOtBeEHK6Yips9dlUhxA17bU7bxpTSEiFfETtnH+pGi1GQPjV2\nCigEk6Z7rceyKJnxU+lKD/4=\n-----END PRIVATE KEY-----\n",
    "client_email": "mcp-bq-sa@mcp-hackathon-mistral.iam.gserviceaccount.com",
    "client_id": "108403027016152214340",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/mcp-bq-sa%40mcp-hackathon-mistral.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

# Calendar OAuth Credentials
CALENDAR_CREDENTIALS_INFO: Dict[str, Any] = {
    "token": "ya29.a0AS3H6NximJxBnDooAP0WgYdQrIb7V5HcCfLIcuOoJSbUyeUjoNU3aAhcC3Dk1ymEzPDlCFIkC2NdwV2iZ9zPu3fTWYEWoDzLlH7o675l4Vm6HyqygWuUNVG_WjKDY6dAA6o3zffGS0jwcvqAu3nx2d5FaqWAv9leZ63R6hEb4Hklw41P9ryYRVF3HD-t9dPXdr74uPQaCgYKAWsSARISFQHGX2Mi9aVrMIHs8AYEdVfcAvgH_A0206",
    "refresh_token": "1//03mnS08DmW66GCgYIARAAGAMSNwF-L9Irf5TMBKCY4j2Pf3Yzd6jdLKnGXPMrsH5PCR7yIUC23ges4Arzw54hlM74XMEsTfXtNlA",
    "token_uri": "https://oauth2.googleapis.com/token",
    "client_id": "236231780397-sth57fqritpnsmta8enftoj8qddsgcd8.apps.googleusercontent.com",
    "client_secret": "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",
    "scopes": ["https://www.googleapis.com/auth/calendar"],
    "universe_domain": "googleapis.com",
    "account": "",
    "expiry": "2025-09-10T22:09:47Z",
}

if isinstance(CALENDAR_CREDENTIALS_INFO.get("scopes"), list) and CALENDAR_CREDENTIALS_INFO["scopes"]:
    SCOPES = CALENDAR_CREDENTIALS_INFO["scopes"]

_TZINFO = tz.gettz(TIMEZONE)

# ============= Create MCP Server =============

mcp = FastMCP(
    name="Unified Medical MCP Server (BigQuery + Calendar)",
    port=PORT,
    stateless_http=STATELESS,
    debug=DEBUG,
    instructions=(
        "Unified MCP Server combining:\n"
        "1. BigQuery: Read-only access to medical data (hospital, surgeons, patients, cases, lab_results)\n"
        "2. Google Calendar: Full read/write access for calendar and event management\n\n"
        "BigQuery Tools:\n"
        "  - list_datasets(), list_tables(dataset), get_schema(dataset, table)\n"
        "  - execute_query(dataset, sql, params?)\n"
        "  - search_hospitals(), get_hospital(), list_specialties()\n"
        "  - search_surgeons(), get_surgeon(), list_surgeon_subspecialties()\n\n"
        "Calendar Tools:\n"
        "  - list_calendars(), get_calendar(), update_calendar_meta()\n"
        "  - list_events(), get_freebusy(), create_event(), upsert_event()\n"
        "  - create_blocks_week() for batch event creation\n"
    ),
)

# ============= BigQuery Client Management =============

_bq_client: Optional[bigquery.Client] = None

def bq_client() -> bigquery.Client:
    """Retourne une instance du client BigQuery avec authentification par service account"""
    global _bq_client
    if _bq_client is None:
        try:
            credentials = service_account.Credentials.from_service_account_info(BQ_SERVICE_ACCOUNT_INFO)
            _bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        except Exception as e:
            print(f"Warning: Service account auth failed ({e}), using default auth")
            _bq_client = bigquery.Client(project=PROJECT_ID) if PROJECT_ID else bigquery.Client()
    return _bq_client

# ============= Calendar Service Management =============

_calendar_service = None
_calendar_creds = None

def _build_calendar_creds() -> Credentials:
    """Create OAuth credentials from embedded JSON."""
    global _calendar_creds
    if _calendar_creds is None:
        _calendar_creds = Credentials.from_authorized_user_info(CALENDAR_CREDENTIALS_INFO, scopes=SCOPES)
    return _calendar_creds

def _calendar_svc():
    """Return a Google Calendar service client (created lazily at first tool call)."""
    global _calendar_service
    if _calendar_service is None:
        creds = _build_calendar_creds()
        _calendar_service = build("calendar", "v3", credentials=creds, cache_discovery=False)
    return _calendar_service

# ============= BigQuery Utility Functions =============

def _require_allowed(dataset: str) -> None:
    if ALLOWED_DATASETS and dataset not in ALLOWED_DATASETS:
        raise ValueError(f"Dataset '{dataset}' is not allowed. Allowed: {sorted(ALLOWED_DATASETS)}")

def _forbid_select_star(sql: str) -> None:
    if re.search(r"(?is)\bselect\s*\*", sql):
        raise ValueError("Forbidden: 'SELECT *' is not allowed. Please list columns explicitly.")

def _infer_bq_param(name: str, value: Any) -> bigquery.ScalarQueryParameter:
    if isinstance(value, bool):
        return bigquery.ScalarQueryParameter(name, "BOOL", value)
    if isinstance(value, int):
        return bigquery.ScalarQueryParameter(name, "INT64", value)
    if isinstance(value, float):
        return bigquery.ScalarQueryParameter(name, "FLOAT64", value)
    return bigquery.ScalarQueryParameter(name, "STRING", str(value))

def _rows_to_dicts(rows: bigquery.table.RowIterator, limit: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        if i >= limit:
            break
        out.append(dict(row.items()))
    return out

def _qual_table(dataset: str, table: str) -> str:
    client = bq_client()
    return f"`{client.project}.{dataset}.{table}`"

def _list_ds_ids(client: bigquery.Client) -> List[str]:
    if ALLOWED_DATASETS:
        return sorted(ALLOWED_DATASETS)
    return sorted(ds.dataset_id for ds in client.list_datasets())

def _list_table_ids(client: bigquery.Client, dataset: str) -> List[str]:
    try:
        return [t.table_id for t in client.list_tables(bigquery.DatasetReference(client.project, dataset))]
    except Exception:
        return []

def _table_exists(client: bigquery.Client, dataset: str, table: str) -> bool:
    try:
        client.get_table(bigquery.TableReference(bigquery.DatasetReference(client.project, dataset), table))
        return True
    except Exception:
        return False

def _schema_fields(dataset: str, table: str) -> Dict[str, bigquery.SchemaField]:
    client = bq_client()
    tbl = client.get_table(bigquery.TableReference(bigquery.DatasetReference(client.project, dataset), table))
    return {f.name.lower(): f for f in tbl.schema}

def _col_exists(dataset: str, table: str, column: str) -> bool:
    try:
        fields = _schema_fields(dataset, table)
        return column.lower() in fields
    except Exception:
        return False

def _select_if_exists(dataset: str, table: str, column: str, alias: Optional[str] = None) -> Optional[str]:
    if _col_exists(dataset, table, column):
        return f"{column} AS {alias or column}"
    return None

def _get_table_schema(dataset: str, table: str):
    client = bq_client()
    ref = bigquery.TableReference(bigquery.DatasetReference(client.project, dataset), table)
    return client.get_table(ref).schema

@lru_cache(maxsize=8)
def _resolve_hospitals(dataset_override: Optional[str] = None, table_override: Optional[str] = None) -> Tuple[str, str]:
    client = bq_client()
    ds_candidates: List[str] = []
    if dataset_override:
        ds_candidates.append(dataset_override)
    if HOSPITALS_DATASET and HOSPITALS_DATASET not in ds_candidates:
        ds_candidates.append(HOSPITALS_DATASET)
    if DEFAULT_DATASET not in ds_candidates:
        ds_candidates.append(DEFAULT_DATASET)
    for d in _list_ds_ids(client):
        if d not in ds_candidates:
            ds_candidates.append(d)

    tbl_candidates: List[str] = []
    if table_override:
        tbl_candidates.append(table_override)
    for n in (HOSPITALS_TABLE, "hospital", "hospitals"):
        if n and n not in tbl_candidates:
            tbl_candidates.append(n)

    required = ("hospital_id", "hospital_name")
    for ds in ds_candidates:
        for tb in tbl_candidates:
            if _table_exists(client, ds, tb):
                fields = _schema_fields(ds, tb)
                if all(r in fields for r in required):
                    return ds, tb

    for ds in ds_candidates:
        for tb in _list_table_ids(client, ds):
            if "hospital" in tb.lower():
                fields = _schema_fields(ds, tb)
                if all(r in fields for r in required):
                    return ds, tb

    raise ValueError("Cannot locate hospital table. Set HOSPITALS_DATASET/HOSPITALS_TABLE or pass dataset/table.")

@lru_cache(maxsize=8)
def _resolve_surgeons(dataset_override: Optional[str] = None, table_override: Optional[str] = None) -> Tuple[str, str]:
    client = bq_client()
    ds_candidates: List[str] = []
    if dataset_override:
        ds_candidates.append(dataset_override)
    if SURGEONS_DATASET and SURGEONS_DATASET not in ds_candidates:
        ds_candidates.append(SURGEONS_DATASET)
    if DEFAULT_DATASET not in ds_candidates:
        ds_candidates.append(DEFAULT_DATASET)
    for d in _list_ds_ids(client):
        if d not in ds_candidates:
            ds_candidates.append(d)

    tbl_candidates: List[str] = []
    if table_override:
        tbl_candidates.append(table_override)
    for n in (SURGEONS_TABLE, "surgeons", "hospital_surgeons"):
        if n and n not in tbl_candidates:
            tbl_candidates.append(n)

    required = ("specialist_id", "hospital_id")
    for ds in ds_candidates:
        for tb in tbl_candidates:
            if _table_exists(client, ds, tb):
                fields = _schema_fields(ds, tb)
                if all(r in fields for r in required):
                    return ds, tb

    for ds in ds_candidates:
        for tb in _list_table_ids(client, ds):
            if "surgeon" in tb.lower():
                fields = _schema_fields(ds, tb)
                if all(r in fields for r in required):
                    return ds, tb

    raise ValueError("Cannot locate surgeons table. Set SURGEONS_DATASET/SURGEONS_TABLE or pass dataset/table.")

def _detect_specialties_shape(dataset: str, table: str) -> Tuple[Optional[str], str, Optional[str]]:
    """
    Retourne un triplet (spec_col, shape, leaf_field)
    shape ∈ {"none","scalar_string","scalar_csv","array_string","array_record"}
    """
    schema = _get_table_schema(dataset, table)
    fields = {f.name.lower(): f for f in schema}

    if "specialties" not in fields:
        return (None, "none", None)

    f = fields["specialties"]
    ftype = f.field_type.upper()
    fmode = (f.mode or "NULLABLE").upper()

    if ftype == "STRING" and fmode != "REPEATED":
        return ("specialties", "scalar_csv", None)

    if ftype == "STRING" and fmode == "REPEATED":
        return ("specialties", "array_string", None)

    if ftype == "RECORD" and fmode == "REPEATED" and getattr(f, "fields", None):
        candidates = ["specialty", "name", "value", "label", "description", "s"]
        leaf = None
        for c in candidates:
            sub = next((sf for sf in f.fields if sf.name.lower() == c and sf.field_type.upper() == "STRING"), None)
            if sub:
                leaf = sub.name
                break
        return ("specialties", "array_record", leaf)

    return ("specialties", "scalar_string", None)

# ============= Calendar Utility Functions =============

def _rfc3339_local(dt_naive: datetime) -> str:
    tzinfo = _TZINFO or tz.gettz("UTC")
    return dt_naive.replace(tzinfo=tzinfo, microsecond=0).isoformat()

def _date_only(s: str) -> bool:
    return len(s) == 10 and s.count("-") == 2 and "T" not in s

def _normalize_dt_field(val: str) -> Tuple[str, str]:
    """Return ("date"| "dateTime", value) in RFC3339, interpreting naive values in TIMEZONE."""
    if _date_only(val):
        return ("date", val)
    dt = dtp.parse(val)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_TZINFO or tz.gettz("UTC"))
    return ("dateTime", dt.replace(microsecond=0).isoformat())

def _parse_date(s: str) -> date:
    return dtp.parse(s).date()

def _list_calendar_items(svc) -> List[Dict[str, str]]:
    out = []
    token = None
    while True:
        resp = svc.calendarList().list(pageToken=token).execute()
        out.extend({"summary": it.get("summary"), "id": it.get("id")} for it in resp.get("items", []))
        token = resp.get("nextPageToken")
        if not token:
            break
    return out

def resolve_calendar_id(calendar_id_or_summary_or_prefix: str) -> str:
    """
    Resolve a calendar id from:
      - full id (contains '@') or email
      - exact summary match
      - prefix of id (before '@group.calendar.google.com')
    """
    svc = _calendar_svc()
    x = calendar_id_or_summary_or_prefix.strip()
    if "@" in x:
        return x
    try:
        svc.calendars().get(calendarId=x).execute()
        return x
    except Exception:
        pass
    items = _list_calendar_items(svc)
    for it in items:
        if it["summary"] == x:
            return it["id"]
    for it in items:
        if it["id"].startswith(x):
            return it["id"]
    raise ValueError(
        f"Calendrier introuvable pour '{x}'. Utilise un summary exact, l'ID complet, "
        "ou un préfixe d'ID (voir list_calendars())."
    )

# ============= Common Tools =============

@mcp.tool()
def echo_tool(text: str) -> str:
    """Echo the input text"""
    return text

# ============= BigQuery Tools =============

@mcp.tool()
def list_datasets() -> List[str]:
    """List all available BigQuery datasets in the project"""
    try:
        if ALLOWED_DATASETS:
            return sorted(ALLOWED_DATASETS)
        client = bq_client()
        datasets = sorted(ds.dataset_id for ds in client.list_datasets())
        return datasets if datasets else ["No datasets found"]
    except Exception as e:
        return [f"Error: {str(e)}"]

@mcp.tool()
def list_tables(dataset: str = "prod_public") -> List[str]:
    """List all tables in a BigQuery dataset"""
    try:
        if ALLOWED_DATASETS and dataset not in ALLOWED_DATASETS:
            return [f"Dataset '{dataset}' is not allowed"]
        client = bq_client()
        dataset_ref = client.dataset(dataset)
        tables = [table.table_id for table in client.list_tables(dataset_ref)]
        return sorted(tables) if tables else ["No tables found"]
    except Exception as e:
        return [f"Error: {str(e)}"]

@mcp.tool()
def get_table_schema(dataset: str = "prod_public", table: str = "hospital") -> List[Dict[str, str]]:
    """Get the schema of a BigQuery table"""
    try:
        if ALLOWED_DATASETS and dataset not in ALLOWED_DATASETS:
            return [{"error": f"Dataset '{dataset}' is not allowed"}]
        client = bq_client()
        table_ref = client.dataset(dataset).table(table)
        table_obj = client.get_table(table_ref)
        schema = []
        for field in table_obj.schema:
            schema.append({
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode or "NULLABLE"
            })
        return schema
    except Exception as e:
        return [{"error": str(e)}]

@mcp.tool(name="get_schema", description="Get column schema for a table.")
def get_schema(dataset: str, table: str) -> List[Dict[str, str]]:
    _require_allowed(dataset)
    client = bq_client()
    table_ref = bigquery.TableReference(bigquery.DatasetReference(client.project, dataset), table)
    tbl = client.get_table(table_ref)
    return [{"name": f.name, "type": f.field_type, "mode": f.mode} for f in tbl.schema]

@mcp.tool(
    name="execute_query",
    description="Execute a parameterized SELECT query (no SELECT *). Params = JSON object of named parameters.",
)
def execute_query(
    dataset: str,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    dry_run_check: bool = True,
    row_limit: Optional[int] = None,
) -> Dict[str, Any]:
    _require_allowed(dataset)
    _forbid_select_star(sql)

    client = bq_client()
    max_rows = int(row_limit or MAX_ROWS)

    base_cfg = QueryJobConfig()
    base_cfg.default_dataset = bigquery.DatasetReference(client.project, dataset)
    base_cfg.maximum_bytes_billed = MAX_BYTES_BILLED
    if params:
        base_cfg.query_parameters = [_infer_bq_param(k, v) for k, v in params.items()]

    if dry_run_check:
        dry_cfg = QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
            default_dataset=base_cfg.default_dataset,
            maximum_bytes_billed=base_cfg.maximum_bytes_billed,
            query_parameters=base_cfg.query_parameters,
        )
        dry_job = client.query(sql, job_config=dry_cfg)
        if getattr(dry_job, "total_bytes_processed", None) and dry_job.total_bytes_processed > MAX_BYTES_BILLED:
            raise ValueError(
                f"Query would scan {dry_job.total_bytes_processed} bytes (> {MAX_BYTES_BILLED}). "
                "Tighten your WHERE clause or reduce columns."
            )

    job = client.query(sql, job_config=base_cfg)
    result = job.result(page_size=max_rows)
    rows = _rows_to_dicts(result, max_rows)

    duration_ms = None
    try:
        if getattr(job, "started", None) and getattr(job, "ended", None):
            duration_ms = int((job.ended - job.started).total_seconds() * 1000)
    except Exception:
        duration_ms = None

    meta = {
        "bytes_billed": job.total_bytes_billed or 0,
        "bytes_processed": getattr(job, "total_bytes_processed", None) or 0,
        "rows_returned": len(rows),
        "duration_ms": duration_ms,
        "cache_hit": bool(getattr(job, "cache_hit", False)),
    }
    return {"status": "ok", "rows": rows, "meta": meta}

@mcp.tool(
    name="list_specialties",
    description="List distinct hospital specialties (agrège primary_specialty + specialties sous toutes les formes)."
)
def list_specialties(
    limit: int = 200,
    dataset: Optional[str] = None,
    table: Optional[str] = None,
) -> List[str]:
    try:
        ds, tbl = _resolve_hospitals(dataset, table)
    except Exception:
        ds, tbl = (dataset or "prod_public"), (table or "hospital")

    _require_allowed(ds)
    client = bq_client()
    table_qual = _qual_table(ds, tbl)

    schema = _get_table_schema(ds, tbl)
    cols = {f.name.lower() for f in schema}
    has_primary = "primary_specialty" in cols

    spec_col, shape, leaf = _detect_specialties_shape(ds, tbl)

    parts: List[str] = []

    if has_primary:
        parts.append(
            f"""
            SELECT TRIM(CAST(primary_specialty AS STRING)) AS specialty
            FROM {table_qual}
            WHERE primary_specialty IS NOT NULL
              AND LENGTH(TRIM(CAST(primary_specialty AS STRING))) > 0
            """
        )

    if spec_col and shape != "none":
        if shape in ("scalar_string", "scalar_csv"):
            parts.append(
                f"""
                SELECT TRIM(x) AS specialty
                FROM {table_qual},
                     UNNEST(SPLIT(CAST({spec_col} AS STRING), ';')) AS x
                WHERE {spec_col} IS NOT NULL
                  AND LENGTH(TRIM(CAST({spec_col} AS STRING))) > 0
                  AND LENGTH(TRIM(x)) > 0
                """
            )
        elif shape == "array_string":
            parts.append(
                f"""
                SELECT TRIM(CAST(x AS STRING)) AS specialty
                FROM {table_qual}, UNNEST({spec_col}) AS x
                WHERE x IS NOT NULL
                  AND LENGTH(TRIM(CAST(x AS STRING))) > 0
                """
            )
        elif shape == "array_record":
            if leaf:
                parts.append(
                    f"""
                    SELECT TRIM(CAST(x.{leaf} AS STRING)) AS specialty
                    FROM {table_qual}, UNNEST({spec_col}) AS x
                    WHERE x.{leaf} IS NOT NULL
                      AND LENGTH(TRIM(CAST(x.{leaf} AS STRING))) > 0
                    """
                )
            else:
                parts.append(
                    f"""
                    SELECT TRIM(CAST(TO_JSON_STRING(x) AS STRING)) AS specialty
                    FROM {table_qual}, UNNEST({spec_col}) AS x
                    WHERE x IS NOT NULL
                      AND LENGTH(TRIM(CAST(TO_JSON_STRING(x) AS STRING))) > 0
                    """
                )

    if not parts:
        return []

    union_sql = "\nUNION ALL\n".join(parts)

    sql = f"""
    WITH U AS (
      {union_sql}
    )
    SELECT DISTINCT specialty
    FROM U
    WHERE specialty IS NOT NULL
      AND LENGTH(TRIM(specialty)) > 0
    ORDER BY specialty
    LIMIT @lim
    """

    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, ds),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[bigquery.ScalarQueryParameter("lim", "INT64", int(max(1, limit)))],
    )

    rows = client.query(sql, job_config=job_cfg).result()
    return [r["specialty"] for r in rows]

@mcp.tool(
    name="search_hospitals",
    description="Search hospitals by optional filters. Returns curated columns.",
)
def search_hospitals(
    city: Optional[str] = None,
    specialty: Optional[str] = None,
    name_contains: Optional[str] = None,
    include_contact: bool = False,
    limit: int = 50,
    dataset: Optional[str] = None,
    table: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ds, tbl = _resolve_hospitals(dataset, table)
    _require_allowed(ds)
    client = bq_client()
    table_qual = _qual_table(ds, tbl)

    fields = _schema_fields(ds, tbl)

    select_cols: List[str] = []
    for col, alias in [
        ("hospital_id", "hospital_id"),
        ("hospital_code", "hospital_code"),
        ("hospital_name", "hospital_name"),
        ("legal_name", "legal_name"),
        ("type", "type"),
        ("ownership", "ownership"),
        ("address", "address"),
        ("postal_code", "postal_code"),
        ("city", "city"),
        ("department_code", "department_code"),
        ("region", "region"),
        ("country", "country"),
        ("primary_specialty", "primary_specialty"),
        ("specialties", "specialties"),
        ("has_emergency", "has_emergency"),
        ("er_level", "er_level"),
        ("capacity_beds", "capacity_beds"),
        ("icu_beds", "icu_beds"),
        ("surgery_rooms", "surgery_rooms"),
        ("accreditation", "accreditation"),
        ("latitude", "latitude"),
        ("longitude", "longitude"),
        ("opening_hours", "opening_hours"),
        ("active", "active"),
        ("website", "website"),
    ]:
        if col in fields:
            select_cols.append(f"{col} AS {alias}")

    if include_contact and HOSPITALS_CONTACT_ALLOWED:
        if "contact_phone" in fields:
            select_cols.append("contact_phone AS contact_phone")
        if "email" in fields:
            select_cols.append("email AS email")

    if not select_cols:
        select_cols.append("hospital_id AS hospital_id")
        select_cols.append("hospital_name AS hospital_name")

    where: List[str] = []
    params: List[bigquery.ScalarQueryParameter] = []

    if city and "city" in fields:
        where.append("city = @city")
        params.append(bigquery.ScalarQueryParameter("city", "STRING", city))

    if name_contains and "hospital_name" in fields:
        where.append("LOWER(hospital_name) LIKE @namepat")
        params.append(bigquery.ScalarQueryParameter("namepat", "STRING", f"%{name_contains.lower()}%"))

    if specialty:
        conds: List[str] = []
        if "primary_specialty" in fields:
            conds.append("LOWER(primary_specialty) = @spec")
        if "specialties" in fields:
            conds.append("EXISTS (SELECT 1 FROM UNNEST(SPLIT(specialties,';')) AS s WHERE LOWER(TRIM(s)) = @spec)")
        if conds:
            where.append("(" + " OR ".join(conds) + ")")
            params.append(bigquery.ScalarQueryParameter("spec", "STRING", specialty.lower()))

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    order_bits: List[str] = []
    if "capacity_beds" in fields:
        order_bits.append("capacity_beds DESC")
    if "hospital_name" in fields:
        order_bits.append("hospital_name")
    order_sql = ("ORDER BY " + ", ".join(order_bits)) if order_bits else ""

    lim = int(max(1, min(limit, MAX_ROWS)))
    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {table_qual}
    {where_sql}
    {order_sql}
    LIMIT @lim
    """

    params.append(bigquery.ScalarQueryParameter("lim", "INT64", lim))
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, ds),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=params,
    )
    rows = client.query(sql, job_config=job_cfg).result()
    return [dict(r.items()) for r in rows]

@mcp.tool(
    name="get_hospital",
    description="Get one hospital by ID. Set include_contact=True to return phone/email if allowed.",
)
def get_hospital(
    hospital_id: int,
    include_contact: bool = False,
    dataset: Optional[str] = None,
    table: Optional[str] = None,
) -> Dict[str, Any]:
    ds, tbl = _resolve_hospitals(dataset, table)
    _require_allowed(ds)
    client = bq_client()
    table_qual = _qual_table(ds, tbl)
    fields = _schema_fields(ds, tbl)

    select_cols: List[str] = []
    base_aliases = [
        ("hospital_id", "hospital_id"),
        ("hospital_code", "hospital_code"),
        ("hospital_name", "hospital_name"),
        ("legal_name", "legal_name"),
        ("type", "type"),
        ("ownership", "ownership"),
        ("address", "address"),
        ("postal_code", "postal_code"),
        ("city", "city"),
        ("department_code", "department_code"),
        ("region", "region"),
        ("country", "country"),
        ("primary_specialty", "primary_specialty"),
        ("specialties", "specialties"),
        ("has_emergency", "has_emergency"),
        ("er_level", "er_level"),
        ("capacity_beds", "capacity_beds"),
        ("icu_beds", "icu_beds"),
        ("surgery_rooms", "surgery_rooms"),
        ("accreditation", "accreditation"),
        ("latitude", "latitude"),
        ("longitude", "longitude"),
        ("opening_hours", "opening_hours"),
        ("active", "active"),
        ("website", "website"),
    ]
    for col, alias in base_aliases:
        if col in fields:
            select_cols.append(f"{col} AS {alias}")

    if include_contact and HOSPITALS_CONTACT_ALLOWED:
        if "contact_phone" in fields:
            select_cols.append("contact_phone AS contact_phone")
        if "email" in fields:
            select_cols.append("email AS email")

    if not select_cols:
        select_cols = ["hospital_id AS hospital_id", "hospital_name AS hospital_name"]

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {table_qual}
    WHERE hospital_id = @hid
    LIMIT 1
    """
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, ds),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[bigquery.ScalarQueryParameter("hid", "INT64", int(hospital_id))],
    )
    it = client.query(sql, job_config=job_cfg).result()
    row = next(iter(it), None)
    if not row:
        return {"status": "not_found", "hospital_id": hospital_id}
    return {"status": "ok", "hospital": dict(row.items())}

@mcp.tool(name="list_surgeon_subspecialties", description="List distinct surgeon sub-specialties.")
def list_surgeon_subspecialties(
    limit: int = 3000,
    dataset: Optional[str] = None,
    table: Optional[str] = None,
) -> List[str]:
    ds, tbl = _resolve_surgeons(dataset, table)
    _require_allowed(ds)
    client = bq_client()
    table_qual = _qual_table(ds, tbl)

    sql = f"""
    SELECT DISTINCT sub_specialty
    FROM {table_qual}
    WHERE sub_specialty IS NOT NULL
    ORDER BY sub_specialty
    LIMIT @lim
    """
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, ds),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[bigquery.ScalarQueryParameter("lim", "INT64", int(max(1, limit)))],
    )
    rows = client.query(sql, job_config=job_cfg).result()
    return [r["sub_specialty"] for r in rows]

@mcp.tool(
    name="list_hospital_surgeons",
    description="List surgeons working at a given hospital_id. include_sensitive exposes email/phone/RPPS/licence if allowed.",
)
def list_hospital_surgeons(
    hospital_id: int,
    include_sensitive: bool = False,
    limit: int = 100,
    dataset: Optional[str] = None,
    table: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ds, tbl = _resolve_surgeons(dataset, table)
    _require_allowed(ds)
    client = bq_client()
    table_qual = _qual_table(ds, tbl)
    fields = _schema_fields(ds, tbl)

    select_cols: List[str] = []
    base_cols = [
        ("specialist_id", "specialist_id"),
        ("hospital_id", "hospital_id"),
        ("department", "department"),
        ("sub_specialty", "sub_specialty"),
        ("first_name", "first_name"),
        ("last_name", "last_name"),
        ("years_experience", "years_experience"),
        ("languages", "languages"),
        ("on_call", "on_call"),
        ("accepts_new_patients", "accepts_new_patients"),
        ("consultation_fee_eur", "consultation_fee_eur"),
        ("surgery_methods", "surgery_methods"),
        ("weekly_schedule", "weekly_schedule"),
        ("rating", "rating"),
    ]
    for col, alias in base_cols:
        if col in fields:
            select_cols.append(f"{col} AS {alias}")

    if include_sensitive and SURGEONS_SENSITIVE_ALLOWED:
        for col in ("email", "phone", "rpps_number", "license_number"):
            if col in fields:
                select_cols.append(f"{col} AS {col}")

    if not select_cols:
        select_cols = ["specialist_id AS specialist_id", "hospital_id AS hospital_id"]

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {table_qual}
    WHERE hospital_id = @hid
    ORDER BY years_experience DESC, last_name, first_name
    LIMIT @lim
    """
    params = [
        bigquery.ScalarQueryParameter("hid", "INT64", int(hospital_id)),
        bigquery.ScalarQueryParameter("lim", "INT64", int(max(1, min(limit, MAX_ROWS)))),
    ]
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, ds),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=params,
    )
    rows = client.query(sql, job_config=job_cfg).result()
    return [dict(r.items()) for r in rows]

@mcp.tool(
    name="search_surgeons",
    description=(
        "Search surgeons with optional filters. Filters: hospital_id, name_contains, sub_specialty, languages "
        "(CSV list; any match), on_call, accepts_new_patients. include_sensitive exposes sensitive fields if allowed."
    ),
)
def search_surgeons(
    hospital_id: Optional[int] = None,
    name_contains: Optional[str] = None,
    sub_specialty: Optional[str] = None,
    languages: Optional[str] = None,
    on_call: Optional[bool] = None,
    accepts_new_patients: Optional[bool] = None,
    include_sensitive: bool = False,
    limit: int = 50,
    dataset: Optional[str] = None,
    table: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ds, tbl = _resolve_surgeons(dataset, table)
    _require_allowed(ds)
    client = bq_client()
    s_table = _qual_table(ds, tbl)
    fields = _schema_fields(ds, tbl)

    select_cols: List[str] = []
    base_cols = [
        ("specialist_id", "specialist_id"),
        ("hospital_id", "hospital_id"),
        ("department", "department"),
        ("sub_specialty", "sub_specialty"),
        ("first_name", "first_name"),
        ("last_name", "last_name"),
        ("years_experience", "years_experience"),
        ("languages", "languages"),
        ("on_call", "on_call"),
        ("accepts_new_patients", "accepts_new_patients"),
        ("consultation_fee_eur", "consultation_fee_eur"),
        ("surgery_methods", "surgery_methods"),
        ("weekly_schedule", "weekly_schedule"),
        ("rating", "rating"),
    ]
    for col, alias in base_cols:
        if col in fields:
            select_cols.append(f"{col} AS {alias}")

    if include_sensitive and SURGEONS_SENSITIVE_ALLOWED:
        for col in ("email", "phone", "rpps_number", "license_number"):
            if col in fields:
                select_cols.append(f"{col} AS {col}")

    if not select_cols:
        select_cols = ["specialist_id AS specialist_id", "hospital_id AS hospital_id"]

    where: List[str] = []
    params: List[bigquery.ScalarQueryParameter] = []

    if hospital_id is not None and "hospital_id" in fields:
        where.append("hospital_id = @hid")
        params.append(bigquery.ScalarQueryParameter("hid", "INT64", int(hospital_id)))

    if name_contains and ("first_name" in fields and "last_name" in fields):
        where.append("(LOWER(first_name) LIKE @namepat OR LOWER(last_name) LIKE @namepat)")
        params.append(bigquery.ScalarQueryParameter("namepat", "STRING", f"%{name_contains.lower()}%"))

    if sub_specialty and "sub_specialty" in fields:
        where.append("LOWER(sub_specialty) = @sub")
        params.append(bigquery.ScalarQueryParameter("sub", "STRING", sub_specialty.lower()))

    if languages and "languages" in fields:
        langs = [x.strip().lower() for x in languages.replace(",", ";").split(";") if x.strip()]
        for i, lg in enumerate(langs):
            where.append(f"REGEXP_CONTAINS(LOWER(languages), @lg{i})")
            params.append(bigquery.ScalarQueryParameter(f"lg{i}", "STRING", fr"(^|;)\s*{re.escape(lg)}\s*(;|$)"))

    if on_call is not None and "on_call" in fields:
        where.append("on_call = @onc")
        params.append(bigquery.ScalarQueryParameter("onc", "BOOL", bool(on_call)))

    if accepts_new_patients is not None and "accepts_new_patients" in fields:
        where.append("accepts_new_patients = @anp")
        params.append(bigquery.ScalarQueryParameter("anp", "BOOL", bool(accepts_new_patients)))

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    lim = int(max(1, min(limit, MAX_ROWS)))

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {s_table}
    {where_sql}
    ORDER BY rating DESC, years_experience DESC, last_name, first_name
    LIMIT @lim
    """
    params.append(bigquery.ScalarQueryParameter("lim", "INT64", lim))
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, ds),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=params,
    )
    rows = client.query(sql, job_config=job_cfg).result()
    return [dict(r.items()) for r in rows]

@mcp.tool(
    name="get_surgeon",
    description="Get one surgeon by specialist_id. include_sensitive exposes email/phone/RPPS/licence if allowed.",
)
def get_surgeon(
    specialist_id: int,
    include_sensitive: bool = False,
    dataset: Optional[str] = None,
    table: Optional[str] = None,
) -> Dict[str, Any]:
    ds, tbl = _resolve_surgeons(dataset, table)
    _require_allowed(ds)
    client = bq_client()
    table_qual = _qual_table(ds, tbl)
    fields = _schema_fields(ds, tbl)

    select_cols: List[str] = []
    base_cols = [
        ("specialist_id", "specialist_id"),
        ("hospital_id", "hospital_id"),
        ("department", "department"),
        ("sub_specialty", "sub_specialty"),
        ("first_name", "first_name"),
        ("last_name", "last_name"),
        ("years_experience", "years_experience"),
        ("languages", "languages"),
        ("on_call", "on_call"),
        ("accepts_new_patients", "accepts_new_patients"),
        ("consultation_fee_eur", "consultation_fee_eur"),
        ("surgery_methods", "surgery_methods"),
        ("weekly_schedule", "weekly_schedule"),
        ("rating", "rating"),
    ]
    for col, alias in base_cols:
        if col in fields:
            select_cols.append(f"{col} AS {alias}")

    if include_sensitive and SURGEONS_SENSITIVE_ALLOWED:
        for col in ("email", "phone", "rpps_number", "license_number"):
            if col in fields:
                select_cols.append(f"{col} AS {col}")

    if not select_cols:
        select_cols = ["specialist_id AS specialist_id", "hospital_id AS hospital_id"]

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {table_qual}
    WHERE specialist_id = @sid
    LIMIT 1
    """
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, ds),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[bigquery.ScalarQueryParameter("sid", "INT64", int(specialist_id))],
    )
    it = client.query(sql, job_config=job_cfg).result()
    row = next(iter(it), None)
    if not row:
        return {"status": "not_found", "specialist_id": specialist_id}
    return {"status": "ok", "surgeon": dict(row.items())}

# ============= Calendar Tools =============

@mcp.tool(title="List Calendars", description="Lister les calendriers accessibles (summary, id).")
def list_calendars() -> List[Dict[str, str]]:
    svc = _calendar_svc()
    return _list_calendar_items(svc)

@mcp.tool(title="Get Calendar", description="Lire les métadonnées d'un calendrier.")
def get_calendar(calendar_id: str = Field(description="ID, summary exact, ou préfixe d'ID")) -> Dict[str, Any]:
    svc = _calendar_svc()
    cid = resolve_calendar_id(calendar_id)
    cal = svc.calendars().get(calendarId=cid).execute()
    return {
        "id": cal.get("id"),
        "summary": cal.get("summary"),
        "description": cal.get("description"),
        "timeZone": cal.get("timeZone"),
        "location": cal.get("location"),
    }

@mcp.tool(title="Update Calendar Meta", description="Mettre à jour summary/description/timeZone/location d'un calendrier (patch).")
def update_calendar_meta(
    calendar_id: str = Field(description="ID/summary/prefix du calendrier"),
    summary: Optional[str] = Field(default=None, description="Nouveau nom (summary)"),
    description: Optional[str] = Field(default=None, description="Nouvelle description"),
    timeZone: Optional[str] = Field(default=None, description="Nouveau timeZone IANA"),
    location: Optional[str] = Field(default=None, description="Nouvelle localisation"),
) -> Dict[str, Any]:
    svc = _calendar_svc()
    cid = resolve_calendar_id(calendar_id)
    body: Dict[str, Any] = {}
    if summary is not None:
        body["summary"] = summary
    if description is not None:
        body["description"] = description
    if timeZone is not None:
        body["timeZone"] = timeZone
    if location is not None:
        body["location"] = location
    cal = svc.calendars().patch(calendarId=cid, body=body).execute()
    return {
        "status": "ok",
        "calendar": {
            "id": cal["id"],
            "summary": cal.get("summary"),
            "description": cal.get("description"),
            "timeZone": cal.get("timeZone"),
            "location": cal.get("location"),
        },
    }

@mcp.tool(title="List Events", description="Lister les événements d'un calendrier entre deux dates (YYYY-MM-DD).")
def list_events(
    calendar_id: str = Field(description="ID/summary/prefix du calendrier"),
    date_from: str = Field(description="Date début incluse (YYYY-MM-DD)"),
    date_to: str = Field(description="Date fin incluse (YYYY-MM-DD)"),
    q: Optional[str] = Field(default=None, description="Filtre texte"),
    max_results: int = Field(default=2500, description="Nombre max d'événements"),
) -> Dict[str, Any]:
    svc = _calendar_svc()
    cid = resolve_calendar_id(calendar_id)
    start_naive = datetime.combine(_parse_date(date_from), time(0, 0))
    end_naive = datetime.combine(_parse_date(date_to) + timedelta(days=1), time(0, 0))
    tmin = _rfc3339_local(start_naive)
    tmax = _rfc3339_local(end_naive)

    params = {
        "calendarId": cid,
        "timeMin": tmin,
        "timeMax": tmax,
        "singleEvents": True,
        "orderBy": "startTime",
        "maxResults": int(max_results),
    }
    if q:
        params["q"] = q

    resp = svc.events().list(**params).execute()
    items = resp.get("items", [])
    events = []
    for e in items:
        events.append(
            {
                "id": e.get("id"),
                "status": e.get("status"),
                "summary": e.get("summary"),
                "description": e.get("description"),
                "location": e.get("location"),
                "start": (e.get("start", {}).get("dateTime") or e.get("start", {}).get("date")),
                "end": (e.get("end", {}).get("dateTime") or e.get("end", {}).get("date")),
                "htmlLink": e.get("htmlLink"),
                "transparency": e.get("transparency"),
            }
        )
    return {"calendar_id": cid, "events": events}

@mcp.tool(title="Get FreeBusy", description="Lire les périodes occupées (busy) d'un calendrier sur une fenêtre.")
def get_freebusy(
    calendar_id: str = Field(description="ID/summary/prefix du calendrier"),
    date_from: str = Field(description="Date début (YYYY-MM-DD)"),
    date_to: str = Field(description="Date fin (YYYY-MM-DD)"),
) -> Dict[str, Any]:
    svc = _calendar_svc()
    cid = resolve_calendar_id(calendar_id)
    start_naive = datetime.combine(_parse_date(date_from), time(0, 0))
    end_naive = datetime.combine(_parse_date(date_to) + timedelta(days=1), time(0, 0))
    body = {
        "timeMin": _rfc3339_local(start_naive),
        "timeMax": _rfc3339_local(end_naive),
        "timeZone": TIMEZONE,
        "items": [{"id": cid}],
    }
    fb = svc.freebusy().query(body=body).execute()
    return {"calendar_id": cid, "busy": fb["calendars"][cid].get("busy", [])}

@mcp.tool(
    title="Create Event",
    description="Créer un événement (all-day ou horodaté). Paramètres: start_iso/end_iso (date 'YYYY-MM-DD' ou RFC3339), summary, description?, location?, block?, send_updates?",
)
def create_event(
    calendar_id: str = Field(description="ID/summary/prefix du calendrier"),
    start_iso: str = Field(description="Date/DateTime début (YYYY-MM-DD ou RFC3339)"),
    end_iso: str = Field(description="Date/DateTime fin (YYYY-MM-DD ou RFC3339)"),
    summary: str = Field(description="Titre de l'événement"),
    description: Optional[str] = Field(default=None, description="Description"),
    location: Optional[str] = Field(default=None, description="Lieu"),
    block: bool = Field(default=True, description="Opaque=occupe (True) / transparent (False)"),
    send_updates: str = Field(default="none", description="none|all|externalOnly"),
) -> Dict[str, Any]:
    svc = _calendar_svc()
    cid = resolve_calendar_id(calendar_id)

    start_kind, start_val = _normalize_dt_field(start_iso)
    end_kind, end_val = _normalize_dt_field(end_iso)

    if start_kind != "date" and end_kind != "date":
        sdt = dtp.parse(start_val)
        edt = dtp.parse(end_val)
        if edt <= sdt:
            raise ValueError("end_iso doit être strictement après start_iso")

    if start_kind == "date" and end_kind == "date":
        start_field = {"date": start_val}
        end_field = {"date": end_val}
    else:
        start_field = {"dateTime": start_val, "timeZone": TIMEZONE}
        end_field = {"dateTime": end_val, "timeZone": TIMEZONE}

    body: Dict[str, Any] = {
        "summary": summary,
        "start": start_field,
        "end": end_field,
        "transparency": "opaque" if block else "transparent",
    }
    if description:
        body["description"] = description
    if location:
        body["location"] = location

    evt = svc.events().insert(calendarId=cid, body=body, sendUpdates=send_updates).execute()
    return {
        "status": "created",
        "event": {
            "id": evt.get("id"),
            "summary": evt.get("summary"),
            "description": evt.get("description"),
            "location": evt.get("location"),
            "start": (evt.get("start", {}).get("dateTime") or evt.get("start", {}).get("date")),
            "end": (evt.get("end", {}).get("dateTime") or evt.get("end", {}).get("date")),
            "transparency": evt.get("transparency"),
            "htmlLink": evt.get("htmlLink"),
        },
    }

@mcp.tool(
    title="Upsert Event",
    description="Créer OU mettre à jour. Si event_id est fourni ⇒ patch, sinon ⇒ create_event. Paramètres idem create_event + event_id?",
)
def upsert_event(
    calendar_id: str = Field(description="ID/summary/prefix du calendrier"),
    start_iso: Optional[str] = Field(default=None, description="Date/DateTime début (pour création/MAJ)"),
    end_iso: Optional[str] = Field(default=None, description="Date/DateTime fin (pour création/MAJ)"),
    summary: Optional[str] = Field(default=None, description="Titre (pour création/MAJ)"),
    description: Optional[str] = Field(default=None, description="Description"),
    location: Optional[str] = Field(default=None, description="Lieu"),
    block: Optional[bool] = Field(default=None, description="Force opaque/transparent"),
    event_id: Optional[str] = Field(default=None, description="ID de l'événement à mettre à jour"),
) -> Dict[str, Any]:
    if event_id:
        return update_event(
            calendar_id=calendar_id,
            event_id=event_id,
            summary=summary,
            description=description,
            location=location,
            start_iso=start_iso,
            end_iso=end_iso,
            block=block,
        )
    else:
        if not (start_iso and end_iso and summary):
            raise ValueError("Pour créer, fournir start_iso, end_iso et summary.")
        return create_event(
            calendar_id=calendar_id,
            start_iso=start_iso,
            end_iso=end_iso,
            summary=summary,
            description=description,
            location=location,
            block=(True if block is None else block),
            send_updates="none",
        )

@mcp.tool(
    title="Update Event",
    description="Mettre à jour un événement existant: summary/description/location et/ou horaires (start_iso/end_iso). Gère date-only.",
)
def update_event(
    calendar_id: str = Field(description="ID/summary/prefix du calendrier"),
    event_id: str = Field(description="ID de l'événement à patcher"),
    summary: Optional[str] = Field(default=None, description="Nouveau titre"),
    description: Optional[str] = Field(default=None, description="Nouvelle description"),
    location: Optional[str] = Field(default=None, description="Nouveau lieu"),
    start_iso: Optional[str] = Field(default=None, description="Nouvelle date/datetime de début"),
    end_iso: Optional[str] = Field(default=None, description="Nouvelle date/datetime de fin"),
    block: Optional[bool] = Field(default=None, description="Force la transparence"),
) -> Dict[str, Any]:
    svc = _calendar_svc()
    cid = resolve_calendar_id(calendar_id)
    evt = svc.events().get(calendarId=cid, eventId=event_id).execute()
    body: Dict[str, Any] = {}
    if summary is not None:
        body["summary"] = summary
    if description is not None:
        body["description"] = description
    if location is not None:
        body["location"] = location

    if start_iso is not None or end_iso is not None:
        start_val = start_iso or (evt.get("start", {}).get("dateTime") or evt.get("start", {}).get("date"))
        end_val = end_iso or (evt.get("end", {}).get("dateTime") or evt.get("end", {}).get("date"))
        sk, sv = _normalize_dt_field(start_val)
        ek, ev = _normalize_dt_field(end_val)

        if sk == "date" and ek == "date":
            body["start"] = {"date": sv}
            body["end"] = {"date": ev}
        else:
            body["start"] = {"dateTime": sv, "timeZone": TIMEZONE}
            body["end"] = {"dateTime": ev, "timeZone": TIMEZONE}

        if not _date_only(sv) and not _date_only(ev):
            sdt = dtp.parse(sv)
            edt = dtp.parse(ev)
            if edt <= sdt:
                raise ValueError("end_iso doit être strictement après start_iso")

    if block is not None:
        body["transparency"] = "opaque" if block else "transparent"

    patched = svc.events().patch(calendarId=cid, eventId=event_id, body=body).execute()
    return {
        "status": "ok",
        "event": {
            "id": patched.get("id"),
            "summary": patched.get("summary"),
            "description": patched.get("description"),
            "location": patched.get("location"),
            "start": (patched.get("start", {}).get("dateTime") or patched.get("start", {}).get("date")),
            "end": (patched.get("end", {}).get("dateTime") or patched.get("end", {}).get("date")),
            "transparency": patched.get("transparency"),
            "htmlLink": patched.get("htmlLink"),
        },
    }

@mcp.tool(
    title="Create Blocks Week",
    description="Créer en batch des créneaux quotidiens entre date_from et date_to, sur certains jours (ex: 1-5 = Lun-Ven). Heures HH:MM.",
)
def create_blocks_week(
    calendar_id: str = Field(description="ID/summary/prefix du calendrier"),
    date_from: str = Field(description="YYYY-MM-DD (inclus)"),
    date_to: str = Field(description="YYYY-MM-DD (inclus)"),
    start_time: str = Field(description="Heure début HH:MM"),
    end_time: str = Field(description="Heure fin HH:MM"),
    summary: str = Field(description="Titre des créneaux"),
    description: Optional[str] = Field(default=None, description="Description"),
    location: Optional[str] = Field(default=None, description="Lieu"),
    weekdays: str = Field(default="1-5", description="Jours 1-7, ranges ex: '1-5,7'"),
    block: bool = Field(default=True, description="Opaque (occupé)"),
) -> Dict[str, Any]:
    svc = _calendar_svc()
    cid = resolve_calendar_id(calendar_id)
    d0 = _parse_date(date_from)
    d1 = _parse_date(date_to)

    # parse weekdays
    allowed = set()
    for chunk in weekdays.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            a, b = [int(x) for x in chunk.split("-")]
            allowed.update(range(a, b + 1))
        else:
            allowed.add(int(chunk))

    sh, sm = [int(x) for x in start_time.split(":")]
    eh, em = [int(x) for x in end_time.split(":")]

    created: List[Dict[str, Any]] = []
    cur = d0
    while cur <= d1:
        if (cur.weekday() + 1) in allowed:
            start_iso = _rfc3339_local(datetime.combine(cur, time(sh, sm)))
            end_iso = _rfc3339_local(datetime.combine(cur, time(eh, em)))
            evt = svc.events().insert(
                calendarId=cid,
                body={
                    "summary": summary,
                    "description": description,
                    "location": location,
                    "start": {"dateTime": start_iso, "timeZone": TIMEZONE},
                    "end": {"dateTime": end_iso, "timeZone": TIMEZONE},
                    "transparency": "opaque" if block else "transparent",
                },
                sendUpdates="none",
            ).execute()
            created.append(
                {
                    "id": evt.get("id"),
                    "start": evt.get("start", {}).get("dateTime"),
                    "end": evt.get("end", {}).get("dateTime"),
                    "htmlLink": evt.get("htmlLink"),
                }
            )
        cur += timedelta(days=1)

    return {"status": "created", "count": len(created), "events": created}

# ============= Legacy BigQuery Tools (for compatibility) =============

@mcp.tool()
def query_hospitals(limit: int = 10) -> List[Dict[str, Any]]:
    """Query hospital data from BigQuery (legacy version)"""
    try:
        client = bq_client()
        query = f"""
        SELECT hospital_id, hospital_name, city, primary_specialty, capacity_beds
        FROM `{PROJECT_ID}.prod_public.hospital`
        WHERE hospital_name IS NOT NULL
        ORDER BY hospital_name
        LIMIT {limit}
        """
        results = client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        return [{"error": str(e)}]

@mcp.tool()
def search_hospitals_by_city(city: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Search hospitals by city name (legacy version)"""
    try:
        client = bq_client()
        query = f"""
        SELECT hospital_id, hospital_name, city, primary_specialty, capacity_beds
        FROM `{PROJECT_ID}.prod_public.hospital`
        WHERE LOWER(city) LIKE LOWER('%{city}%')
        ORDER BY hospital_name
        LIMIT {limit}
        """
        results = client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        return [{"error": str(e)}]

# ============= Resources =============

@mcp.resource("bq://datasets")
def bq_datasets_resource() -> str:
    """Resource to list BigQuery datasets"""
    datasets = list_datasets()
    return f"Available datasets: {', '.join(datasets)}"

@mcp.resource("echo://static")
def echo_resource() -> str:
    return "Echo!"

@mcp.resource("echo://{text}")
def echo_template(text: str) -> str:
    """Echo the input text"""
    return f"Echo: {text}"

@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"

# ============= Prompts =============

@mcp.prompt("echo")
def echo_prompt(text: str) -> str:
    return text

# ============= Main =============

if __name__ == "__main__":
    # For FastMCP Cloud, use streamable-http transport
    mcp.run(transport="streamable-http" if STATELESS else None)