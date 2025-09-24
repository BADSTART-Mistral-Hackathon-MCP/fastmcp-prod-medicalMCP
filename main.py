"""
FastMCP Echo Server with Advanced BigQuery Integration (parametrized)
- No dataset discovery: every tool takes explicit dataset/table parameters
- Read-only & parameterized queries (no SELECT *)
- Patient dossier summarization tool aligned to provided schemas
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from fastmcp import FastMCP
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from google.oauth2 import service_account


# =============================================================================
# Project / Limits / Tables (kept in code, can be overridden by real env)
# =============================================================================

PROJECT_ID = "mcp-hackathon-mistral"
LOCATION = os.getenv("BQ_LOCATION", "EU")

# Limits
MAX_BYTES_BILLED = int(os.getenv("MAX_BYTES_BILLED", "200000000"))  # 200 MB
MAX_ROWS = int(os.getenv("MAX_ROWS", "1000"))

# Default tables (EDIT here or via ENV)
DEFAULT_DATASET = "prod_public"

HOSPITALS_DATASET = os.getenv("HOSPITALS_DATASET", DEFAULT_DATASET).strip()
HOSPITALS_TABLE = os.getenv("HOSPITALS_TABLE", "hospital").strip()
HOSPITALS_CONTACT_ALLOWED = os.getenv("HOSPITALS_CONTACT_ALLOWED", "false").lower() in {"1", "true", "yes"}

SURGEONS_DATASET = (os.getenv("SURGEONS_DATASET") or HOSPITALS_DATASET).strip()
SURGEONS_TABLE = os.getenv("SURGEONS_TABLE", "surgeons").strip()
SURGEONS_SENSITIVE_ALLOWED = os.getenv("SURGEONS_SENSITIVE_ALLOWED", "false").lower() in {"1", "true", "yes"}

PATIENTS_DATASET = os.getenv("PATIENTS_DATASET", DEFAULT_DATASET).strip()
PATIENTS_TABLE = os.getenv("PATIENTS_TABLE", "patients").strip()
PATIENTS_SENSITIVE_ALLOWED = os.getenv("PATIENTS_SENSITIVE_ALLOWED", "false").lower() in {"1", "true", "yes"}

CASES_DATASET = os.getenv("CASES_DATASET", DEFAULT_DATASET).strip()
CASES_TABLE = os.getenv("CASES_TABLE", "cases").strip()

LABS_DATASET = os.getenv("LABS_DATASET", DEFAULT_DATASET).strip()
LABS_TABLE = os.getenv("LABS_TABLE", "lab_results").strip()

# Optionally restrict usable datasets
_ALLOWED = [d.strip() for d in os.getenv("ALLOWED_DATASETS", "").split(",") if d.strip()]
ALLOWED_DATASETS = set(_ALLOWED) if _ALLOWED else set()


# =============================================================================
# Credentials (HARD-CODED FOR TESTING — DO NOT USE IN PROD)
# =============================================================================

SERVICE_ACCOUNT_INFO = {
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
    "universe_domain": "googleapis.com",
}

# =============================================================================
# FastMCP server
# =============================================================================

mcp = FastMCP(
    name="Medical MCP BigQuery Server (Param)",
    instructions=(
        "Read-only, parameterized access to Google BigQuery.\n"
        "Explicit tables only (no auto-discovery).\n"
        "Default tables: prod_public.(hospital|surgeons|patients|cases|lab_results)\n"
        "Tools:\n"
        "  - get_schema(dataset, table)\n"
        "  - execute_query(dataset, sql, params?, dry_run_check?, row_limit?)\n"
        "  - list_specialties(dataset, table, limit?)\n"
        "  - search_hospitals(dataset, table, city?, specialty?, name_contains?, include_contact?, limit?)\n"
        "  - get_hospital(dataset, table, hospital_id, include_contact?)\n"
        "  - list_surgeon_subspecialties(dataset, table, limit?)\n"
        "  - list_hospital_surgeons(dataset, table, hospital_id, include_sensitive?, limit?)\n"
        "  - search_surgeons(dataset, table, hospital_id?, name_contains?, sub_specialty?, languages?, on_call?, accepts_new_patients?, include_sensitive?, limit?)\n"
        "  - get_surgeon(dataset, table, specialist_id, include_sensitive?)\n"
        "  - get_patient(dataset, table, patient_id:str, include_sensitive?)\n"
        "  - list_patient_cases(dataset, table, patient_id:str, limit?)\n"
        "  - list_patient_lab_results(dataset, table, patient_id:str, limit?)\n"
        "  - summarize_patient_dossier(patients_ds, patients_tb, cases_ds, cases_tb, labs_ds, labs_tb, patient_id:str, surgeons_ds?, surgeons_tb?, include_sensitive?)\n"
        "Notes:\n"
        "  • maximum_bytes_billed & row limits enforced.\n"
        "  • Optional ALLOWED_DATASETS restriction.\n"
        "  • Contacts & sensitive fields are gated by env flags.\n"
    ),
)

# =============================================================================
# BigQuery client
# =============================================================================

_client: Optional[bigquery.Client] = None

def bq_client() -> bigquery.Client:
    global _client
    if _client is None:
        try:
            credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO)
            _client = bigquery.Client(credentials=credentials, project=PROJECT_ID, location=LOCATION)
        except Exception as e:
            print(f"Warning: SA auth failed ({e}), using default auth")
            _client = bigquery.Client(project=PROJECT_ID, location=LOCATION) if PROJECT_ID else bigquery.Client(location=LOCATION)
    return _client


# =============================================================================
# Helpers
# =============================================================================

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

def _schema_fields(dataset: str, table: str) -> Dict[str, bigquery.SchemaField]:
    client = bq_client()
    ref = bigquery.TableReference(bigquery.DatasetReference(client.project, dataset), table)
    tbl = client.get_table(ref)
    return {f.name.lower(): f for f in tbl.schema}

def _col_exists(dataset: str, table: str, column: str) -> bool:
    try:
        fields = _schema_fields(dataset, table)
        return column.lower() in fields
    except Exception:
        return False

def _select_available(dataset: str, table: str, pairs: List[Tuple[str, str]]) -> List[str]:
    """Build a SELECT list keeping only columns that exist."""
    fields = _schema_fields(dataset, table)
    out = []
    for col, alias in pairs:
        if col.lower() in fields:
            out.append(f"{col} AS {alias}")
    return out

def _order_by_if_exists(dataset: str, table: str, candidates: List[str], desc: bool = True) -> str:
    for c in candidates:
        if _col_exists(dataset, table, c):
            return f"ORDER BY {c} {'DESC' if desc else 'ASC'}"
    return ""

def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def _parse_iso_date(s: Optional[str]) -> Optional[date]:
    try:
        return date.fromisoformat(str(s))
    except Exception:
        return None


# =============================================================================
# Core tools
# =============================================================================

@mcp.tool(name="get_schema", description="Return the schema for a given table.")
def get_schema(dataset: str, table: str) -> List[Dict[str, str]]:
    _require_allowed(dataset)
    client = bq_client()
    table_ref = bigquery.TableReference(bigquery.DatasetReference(client.project, dataset), table)
    tbl = client.get_table(table_ref)
    return [{"name": f.name, "type": f.field_type, "mode": f.mode or "NULLABLE"} for f in tbl.schema]

@mcp.tool(
    name="execute_query",
    description="Execute a parameterized SELECT query (no SELECT *). Params is a JSON object of named parameters."
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


# =============================================================================
# Hospitals
# =============================================================================

@mcp.tool(
    name="list_specialties",
    description="List distinct hospital specialties from primary_specialty + specialties (CSV ';')."
)
def list_specialties(
    dataset: str = HOSPITALS_DATASET,
    table: str = HOSPITALS_TABLE,
    limit: int = 200,
) -> List[str]:
    _require_allowed(dataset)
    client = bq_client()
    t = _qual_table(dataset, table)
    # Forme simple conforme à ton CSV
    sql = f"""
    WITH u AS (
      SELECT TRIM(CAST(primary_specialty AS STRING)) AS s
      FROM {t}
      WHERE primary_specialty IS NOT NULL AND LENGTH(TRIM(CAST(primary_specialty AS STRING)))>0
      UNION ALL
      SELECT TRIM(x) AS s
      FROM {t}, UNNEST(SPLIT(CAST(specialties AS STRING), ';')) AS x
      WHERE specialties IS NOT NULL AND LENGTH(TRIM(CAST(specialties AS STRING)))>0 AND LENGTH(TRIM(x))>0
    )
    SELECT DISTINCT s AS specialty
    FROM u
    WHERE s IS NOT NULL AND LENGTH(TRIM(s))>0
    ORDER BY specialty
    LIMIT @lim
    """
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, dataset),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[bigquery.ScalarQueryParameter("lim", "INT64", int(max(1, limit)))],
    )
    rows = client.query(sql, job_config=job_cfg).result()
    return [r["specialty"] for r in rows]


@mcp.tool(
    name="search_hospitals",
    description="Search hospitals with optional filters (city, specialty, name). Returns curated columns."
)
def search_hospitals(
    dataset: str = HOSPITALS_DATASET,
    table: str = HOSPITALS_TABLE,
    city: Optional[str] = None,
    specialty: Optional[str] = None,
    name_contains: Optional[str] = None,
    include_contact: bool = False,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    _require_allowed(dataset)
    client = bq_client()
    t = _qual_table(dataset, table)

    select_cols = [
        "hospital_id AS hospital_id",
        "hospital_code AS hospital_code",
        "hospital_name AS hospital_name",
        "legal_name AS legal_name",
        "type AS type",
        "ownership AS ownership",
        "address AS address",
        "postal_code AS postal_code",
        "city AS city",
        "department_code AS department_code",
        "region AS region",
        "country AS country",
        "primary_specialty AS primary_specialty",
        "specialties AS specialties",
        "has_emergency AS has_emergency",
        "er_level AS er_level",
        "capacity_beds AS capacity_beds",
        "icu_beds AS icu_beds",
        "surgery_rooms AS surgery_rooms",
        "accreditation AS accreditation",
        "latitude AS latitude",
        "longitude AS longitude",
        "opening_hours AS opening_hours",
        "active AS active",
        "website AS website",
    ]
    if include_contact and HOSPITALS_CONTACT_ALLOWED:
        select_cols += ["contact_phone AS contact_phone", "email AS email"]

    where: List[str] = []
    params: List[bigquery.ScalarQueryParameter] = []

    if city:
        where.append("city = @city")
        params.append(bigquery.ScalarQueryParameter("city", "STRING", city))

    if name_contains:
        where.append("LOWER(hospital_name) LIKE @namepat")
        params.append(bigquery.ScalarQueryParameter("namepat", "STRING", f"%{name_contains.lower()}%"))

    if specialty:
        where.append(
            "("
            "LOWER(primary_specialty) = @spec OR "
            "EXISTS (SELECT 1 FROM UNNEST(SPLIT(CAST(specialties AS STRING), ';')) s WHERE LOWER(TRIM(s)) = @spec)"
            ")"
        )
        params.append(bigquery.ScalarQueryParameter("spec", "STRING", specialty.lower()))

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    order_sql = "ORDER BY capacity_beds DESC, hospital_name"

    lim = int(max(1, min(limit, MAX_ROWS)))
    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {t}
    {where_sql}
    {order_sql}
    LIMIT @lim
    """
    params.append(bigquery.ScalarQueryParameter("lim", "INT64", lim))

    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, dataset),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=params,
    )
    rows = client.query(sql, job_config=job_cfg).result()
    return [dict(r.items()) for r in rows]


@mcp.tool(
    name="get_hospital",
    description="Get a hospital by ID. Set include_contact=True to return phone/email if allowed."
)
def get_hospital(
    dataset: str = HOSPITALS_DATASET,
    table: str = HOSPITALS_TABLE,
    hospital_id: int = 0,
    include_contact: bool = False,
) -> Dict[str, Any]:
    _require_allowed(dataset)
    client = bq_client()
    t = _qual_table(dataset, table)

    select_cols = [
        "hospital_id AS hospital_id",
        "hospital_code AS hospital_code",
        "hospital_name AS hospital_name",
        "legal_name AS legal_name",
        "type AS type",
        "ownership AS ownership",
        "address AS address",
        "postal_code AS postal_code",
        "city AS city",
        "department_code AS department_code",
        "region AS region",
        "country AS country",
        "primary_specialty AS primary_specialty",
        "specialties AS specialties",
        "has_emergency AS has_emergency",
        "er_level AS er_level",
        "capacity_beds AS capacity_beds",
        "icu_beds AS icu_beds",
        "surgery_rooms AS surgery_rooms",
        "accreditation AS accreditation",
        "latitude AS latitude",
        "longitude AS longitude",
        "opening_hours AS opening_hours",
        "active AS active",
        "website AS website",
    ]
    if include_contact and HOSPITALS_CONTACT_ALLOWED:
        select_cols += ["contact_phone AS contact_phone", "email AS email"]

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {t}
    WHERE hospital_id = @hid
    LIMIT 1
    """
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, dataset),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[bigquery.ScalarQueryParameter("hid", "INT64", int(hospital_id))],
    )
    it = client.query(sql, job_config=job_cfg).result()
    row = next(iter(it), None)
    if not row:
        return {"status": "not_found", "hospital_id": hospital_id}
    return {"status": "ok", "hospital": dict(row.items())}


# =============================================================================
# Surgeons
# =============================================================================

@mcp.tool(name="list_surgeon_subspecialties", description="List distinct surgeon sub-specialties.")
def list_surgeon_subspecialties(
    dataset: str = SURGEONS_DATASET,
    table: str = SURGEONS_TABLE,
    limit: int = 3000,
) -> List[str]:
    _require_allowed(dataset)
    client = bq_client()
    t = _qual_table(dataset, table)
    sql = f"""
    SELECT DISTINCT sub_specialty
    FROM {t}
    WHERE sub_specialty IS NOT NULL
    ORDER BY sub_specialty
    LIMIT @lim
    """
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, dataset),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[bigquery.ScalarQueryParameter("lim", "INT64", int(max(1, limit)))],
    )
    rows = client.query(sql, job_config=job_cfg).result()
    return [r["sub_specialty"] for r in rows]

@mcp.tool(
    name="list_hospital_surgeons",
    description="List surgeons for a given hospital_id. Sensitive fields gated by env flag."
)
def list_hospital_surgeons(
    dataset: str = SURGEONS_DATASET,
    table: str = SURGEONS_TABLE,
    hospital_id: int = 0,
    include_sensitive: bool = False,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    _require_allowed(dataset)
    client = bq_client()
    t = _qual_table(dataset, table)

    base = [
        "specialist_id AS specialist_id",
        "hospital_id AS hospital_id",
        "department AS department",
        "sub_specialty AS sub_specialty",
        "first_name AS first_name",
        "last_name AS last_name",
        "years_experience AS years_experience",
        "languages AS languages",
        "on_call AS on_call",
        "accepts_new_patients AS accepts_new_patients",
        "consultation_fee_eur AS consultation_fee_eur",
        "surgery_methods AS surgery_methods",
        "weekly_schedule AS weekly_schedule",
        "rating AS rating",
    ]
    sens = [
        "email AS email",
        "phone AS phone",
        "rpps_number AS rpps_number",
        "license_number AS license_number",
    ]
    select_cols = base + (sens if include_sensitive and SURGEONS_SENSITIVE_ALLOWED else [])

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {t}
    WHERE hospital_id = @hid
    ORDER BY rating DESC, years_experience DESC, last_name, first_name
    LIMIT @lim
    """
    params = [
        bigquery.ScalarQueryParameter("hid", "INT64", int(hospital_id)),
        bigquery.ScalarQueryParameter("lim", "INT64", int(max(1, min(limit, MAX_ROWS)))),
    ]
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, dataset),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=params,
    )
    rows = client.query(sql, job_config=job_cfg).result()
    return [dict(r.items()) for r in rows]

@mcp.tool(
    name="search_surgeons",
    description=("Search surgeons with optional filters. "
                 "Filters: hospital_id, name_contains, sub_specialty, languages (CSV; any), on_call, accepts_new_patients."))
def search_surgeons(
    dataset: str = SURGEONS_DATASET,
    table: str = SURGEONS_TABLE,
    hospital_id: Optional[int] = None,
    name_contains: Optional[str] = None,
    sub_specialty: Optional[str] = None,
    languages: Optional[str] = None,
    on_call: Optional[bool] = None,
    accepts_new_patients: Optional[bool] = None,
    include_sensitive: bool = False,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    _require_allowed(dataset)
    client = bq_client()
    t = _qual_table(dataset, table)

    base = [
        "specialist_id AS specialist_id",
        "hospital_id AS hospital_id",
        "department AS department",
        "sub_specialty AS sub_specialty",
        "first_name AS first_name",
        "last_name AS last_name",
        "years_experience AS years_experience",
        "languages AS languages",
        "on_call AS on_call",
        "accepts_new_patients AS accepts_new_patients",
        "consultation_fee_eur AS consultation_fee_eur",
        "surgery_methods AS surgery_methods",
        "weekly_schedule AS weekly_schedule",
        "rating AS rating",
    ]
    sens = [
        "email AS email",
        "phone AS phone",
        "rpps_number AS rpps_number",
        "license_number AS license_number",
    ]
    select_cols = base + (sens if include_sensitive and SURGEONS_SENSITIVE_ALLOWED else [])

    where: List[str] = []
    params: List[bigquery.ScalarQueryParameter] = []

    if hospital_id is not None:
        where.append("hospital_id = @hid")
        params.append(bigquery.ScalarQueryParameter("hid", "INT64", int(hospital_id)))

    if name_contains:
        where.append("(LOWER(first_name) LIKE @namepat OR LOWER(last_name) LIKE @namepat)")
        params.append(bigquery.ScalarQueryParameter("namepat", "STRING", f"%{name_contains.lower()}%"))

    if sub_specialty:
        where.append("LOWER(sub_specialty) = @sub")
        params.append(bigquery.ScalarQueryParameter("sub", "STRING", sub_specialty.lower()))

    if languages:
        langs = [x.strip().lower() for x in languages.replace(",", ";").split(";") if x.strip()]
        for i, lg in enumerate(langs):
            where.append(f"REGEXP_CONTAINS(LOWER(languages), @lg{i})")
            params.append(bigquery.ScalarQueryParameter(f"lg{i}", "STRING", fr"(^|;)\s*{re.escape(lg)}\s*(;|$)"))

    if on_call is not None:
        where.append("on_call = @onc")
        params.append(bigquery.ScalarQueryParameter("onc", "BOOL", bool(on_call)))

    if accepts_new_patients is not None:
        where.append("accepts_new_patients = @anp")
        params.append(bigquery.ScalarQueryParameter("anp", "BOOL", bool(accepts_new_patients)))

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    lim = int(max(1, min(limit, MAX_ROWS)))

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {t}
    {where_sql}
    ORDER BY rating DESC, years_experience DESC, last_name, first_name
    LIMIT @lim
    """
    params.append(bigquery.ScalarQueryParameter("lim", "INT64", lim))
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, dataset),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=params,
    )
    rows = client.query(sql, job_config=job_cfg).result()
    return [dict(r.items()) for r in rows]

@mcp.tool(
    name="get_surgeon",
    description="Get one surgeon by specialist_id. Sensitive fields gated by env flag."
)
def get_surgeon(
    dataset: str = SURGEONS_DATASET,
    table: str = SURGEONS_TABLE,
    specialist_id: int = 0,
    include_sensitive: bool = False,
) -> Dict[str, Any]:
    _require_allowed(dataset)
    client = bq_client()
    t = _qual_table(dataset, table)

    base = [
        "specialist_id AS specialist_id",
        "hospital_id AS hospital_id",
        "department AS department",
        "sub_specialty AS sub_specialty",
        "first_name AS first_name",
        "last_name AS last_name",
        "years_experience AS years_experience",
        "languages AS languages",
        "on_call AS on_call",
        "accepts_new_patients AS accepts_new_patients",
        "consultation_fee_eur AS consultation_fee_eur",
        "surgery_methods AS surgery_methods",
        "weekly_schedule AS weekly_schedule",
        "rating AS rating",
    ]
    sens = [
        "email AS email",
        "phone AS phone",
        "rpps_number AS rpps_number",
        "license_number AS license_number",
    ]
    select_cols = base + (sens if include_sensitive and SURGEONS_SENSITIVE_ALLOWED else [])

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {t}
    WHERE specialist_id = @sid
    LIMIT 1
    """
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, dataset),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[bigquery.ScalarQueryParameter("sid", "INT64", int(specialist_id))],
    )
    it = client.query(sql, job_config=job_cfg).result()
    row = next(iter(it), None)
    if not row:
        return {"status": "not_found", "specialist_id": specialist_id}
    return {"status": "ok", "surgeon": dict(row.items())}


# =============================================================================
# Patients + Cases + Labs
# =============================================================================

@mcp.tool(name="get_patient", description="Get one patient by patient_id (STRING). Sensitive columns gated.")
def get_patient(
    dataset: str = PATIENTS_DATASET,
    table: str = PATIENTS_TABLE,
    patient_id: str = "",
    include_sensitive: bool = False,
) -> Dict[str, Any]:
    _require_allowed(dataset)
    client = bq_client()
    t = _qual_table(dataset, table)

    base = [
        "patient_id AS patient_id",
        "first_name AS first_name",
        "last_name AS last_name",
        "dob AS dob",
        "sex AS sex",
        "city AS city",
        "postal_code AS postal_code",
    ]
    sens = ["email AS email", "phone AS phone", "address AS address", "insurance_id AS insurance_id"]
    select_cols = base + (sens if include_sensitive and PATIENTS_SENSITIVE_ALLOWED else [])

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {t}
    WHERE patient_id = @pid
    LIMIT 1
    """
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, dataset),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[bigquery.ScalarQueryParameter("pid", "STRING", str(patient_id))],
    )
    it = client.query(sql, job_config=job_cfg).result()
    row = next(iter(it), None)
    if not row:
        return {"status": "not_found", "patient_id": patient_id}
    p = dict(row.items())

    # derive age
    dob = _parse_iso_date(p.get("dob"))
    if dob:
        today = date.today()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        p["age_years"] = age
    return {"status": "ok", "patient": p}

@mcp.tool(name="list_patient_cases", description="List cases for a patient_id (STRING), most recent first.")
def list_patient_cases(
    dataset: str = CASES_DATASET,
    table: str = CASES_TABLE,
    patient_id: str = "",
    limit: int = 100,
) -> List[Dict[str, Any]]:
    _require_allowed(dataset)
    client = bq_client()
    t = _qual_table(dataset, table)

    select_cols = [
        "case_id AS case_id",
        "patient_id AS patient_id",
        "diagnosis_code AS diagnosis_code",
        "diagnosis_desc AS diagnosis_desc",
        "severity AS severity",
        "admitted_at AS admitted_at",
        "discharged_at AS discharged_at",
        "attending_physician AS attending_physician",
        "status AS status",
    ]
    order_sql = "ORDER BY admitted_at DESC"

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {t}
    WHERE patient_id = @pid
    {order_sql}
    LIMIT @lim
    """
    params = [
        bigquery.ScalarQueryParameter("pid", "STRING", str(patient_id)),
        bigquery.ScalarQueryParameter("lim", "INT64", int(max(1, min(limit, MAX_ROWS)))),
    ]
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, dataset),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=params,
    )
    rows = client.query(sql, job_config=job_cfg).result()
    out = [dict(r.items()) for r in rows]
    # compute LOS (length of stay) if dates present
    for c in out:
        ad = _parse_iso_date(c.get("admitted_at"))
        dd = _parse_iso_date(c.get("discharged_at"))
        if ad and dd:
            c["los_days"] = max(0, (dd - ad).days)
    return out

@mcp.tool(name="list_patient_lab_results", description="List lab results for a patient_id (STRING), most recent first.")
def list_patient_lab_results(
    dataset: str = LABS_DATASET,
    table: str = LABS_TABLE,
    patient_id: str = "",
    limit: int = 100,
) -> List[Dict[str, Any]]:
    _require_allowed(dataset)
    client = bq_client()
    t = _qual_table(dataset, table)

    select_cols = [
        "lab_id AS lab_id",
        "patient_id AS patient_id",
        "case_id AS case_id",
        "test_date AS test_date",
        "test_name AS test_name",
        "value AS value",
        "unit AS unit",
        "ref_range AS ref_range",
        "flag AS flag",
    ]
    order_sql = "ORDER BY test_date DESC"

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {t}
    WHERE patient_id = @pid
    {order_sql}
    LIMIT @lim
    """
    params = [
        bigquery.ScalarQueryParameter("pid", "STRING", str(patient_id)),
        bigquery.ScalarQueryParameter("lim", "INT64", int(max(1, min(limit, MAX_ROWS)))),
    ]
    job_cfg = QueryJobConfig(
        default_dataset=bigquery.DatasetReference(client.project, dataset),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=params,
    )
    rows = client.query(sql, job_config=job_cfg).result()
    return [dict(r.items()) for r in rows]


# =============================================================================
# Patient dossier summary
# =============================================================================

@mcp.tool(
    name="summarize_patient_dossier",
    description=("Compact summary for a patient (STRING id): demographics, age, case counts, latest case (LOS), "
                 "recent labs & abnormal flags. Optionally resolve attending surgeon by name.")
)
def summarize_patient_dossier(
    patients_dataset: str = PATIENTS_DATASET,
    patients_table: str = PATIENTS_TABLE,
    cases_dataset: str = CASES_DATASET,
    cases_table: str = CASES_TABLE,
    labs_dataset: str = LABS_DATASET,
    labs_table: str = LABS_TABLE,
    patient_id: str = "",
    include_sensitive: bool = False,
    surgeons_dataset: Optional[str] = SURGEONS_DATASET,
    surgeons_table: Optional[str] = SURGEONS_TABLE,
    labs_limit: int = 10,
) -> Dict[str, Any]:
    # Patient
    p = get_patient(dataset=patients_dataset, table=patients_table, patient_id=patient_id, include_sensitive=include_sensitive)
    if p.get("status") == "not_found":
        return {"status": "not_found", "patient_id": patient_id}
    patient = p["patient"]

    # Cases
    cases = list_patient_cases(dataset=cases_dataset, table=cases_table, patient_id=patient_id, limit=min(200, MAX_ROWS))
    total_cases = len(cases)
    open_cases = sum(1 for c in cases if str(c.get("status", "")).lower() in {"open", "active", "ongoing", "scheduled"})
    latest_case = cases[0] if cases else None

    labs = list_patient_lab_results(dataset=labs_dataset, table=labs_table, patient_id=patient_id, limit=min(labs_limit, MAX_ROWS))
    def is_abnormal(x: Dict[str, Any]) -> bool:
        v = str(x.get("flag", "")).strip().lower()
        return v not in {"", "normal", "n", "ok"}
    abnormal_labs = [l for l in labs if is_abnormal(l)]
    last_lab_date = labs[0]["test_date"] if labs else None

    surgeon_detail = None
    if latest_case and latest_case.get("attending_physician") and surgeons_dataset and surgeons_table:
        try:
            full = str(latest_case["attending_physician"]).strip()
            parts = [p for p in full.replace("  ", " ").split(" ") if p]
            if len(parts) >= 2:
                fn, ln = parts[0], parts[-1]
                client = bq_client()
                t = _qual_table(surgeons_dataset, surgeons_table)
                sql = f"""
                SELECT specialist_id, hospital_id, first_name, last_name, sub_specialty, years_experience, rating
                FROM {t}
                WHERE LOWER(first_name) = @fn AND LOWER(last_name) = @ln
                ORDER BY rating DESC, years_experience DESC
                LIMIT 1
                """
                cfg = QueryJobConfig(
                    default_dataset=bigquery.DatasetReference(client.project, surgeons_dataset),
                    maximum_bytes_billed=MAX_BYTES_BILLED,
                    query_parameters=[
                        bigquery.ScalarQueryParameter("fn", "STRING", fn.lower()),
                        bigquery.ScalarQueryParameter("ln", "STRING", ln.lower()),
                    ],
                )
                it = client.query(sql, job_config=cfg).result()
                one = next(iter(it), None)
                surgeon_detail = dict(one.items()) if one else None
        except Exception:
            surgeon_detail = None

    overview = {
        "total_cases": total_cases,
        "open_cases": open_cases,
        "total_lab_results": len(labs),
        "abnormal_lab_count": len(abnormal_labs),
        "last_lab_date": last_lab_date,
    }

    return {
        "status": "ok",
        "patient": patient,
        "overview": overview,
        "latest_case": latest_case,
        "attending_surgeon_match": surgeon_detail,
        "recent_labs": labs,
        "recent_abnormal_labs": abnormal_labs[:5],
    }

if __name__ == "__main__":
    mcp.run()
