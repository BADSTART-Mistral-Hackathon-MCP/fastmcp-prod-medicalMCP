"""
FastMCP Echo Server with Advanced BigQuery Integration
"""

import os
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
from fastmcp import FastMCP
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from google.oauth2 import service_account

# Configuration du projet
PROJECT_ID = "mcp-hackathon-mistral"
LOCATION = os.getenv("BQ_LOCATION", "EU")

# Datasets autorisés (vide = tous)
_ALLOWED = [d.strip() for d in os.getenv("ALLOWED_DATASETS", "").split(",") if d.strip()]
ALLOWED_DATASETS = set(_ALLOWED) if _ALLOWED else set()

# Limites
MAX_BYTES_BILLED = int(os.getenv("MAX_BYTES_BILLED", "200000000"))
MAX_ROWS = int(os.getenv("MAX_ROWS", "1000"))

# Défauts
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

# Credentials - hardcodés pour le test (À SÉCURISER EN PRODUCTION!)
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
    "universe_domain": "googleapis.com"
}

# Create server
mcp = FastMCP(
    name="Medical MCP BigQuery Server",
    instructions=(
        "Read-only & parameterized access to Google BigQuery (safe by design).\n"
        "Datasets/tables par défaut: prod_public.(hospital|surgeons|patients|cases|lab_results)\n"
        "Tools:\n"
        "  - echo_tool(text)                                # Echo the input text\n"
        "  - list_datasets()                                # List all available BigQuery datasets\n"
        "  - list_tables(dataset)                           # List all tables in a dataset\n"
        "  - get_table_schema(dataset, table)              # Get schema of a table (legacy)\n"
        "  - get_schema(dataset, table)                    # Get schema of a table (new)\n"
        "  - query_hospitals(limit)                        # Query hospital data (legacy)\n"
        "  - search_hospitals_by_city(city, limit)         # Search hospitals by city (legacy)\n"
        "  - execute_query(dataset, sql, params?)          # Execute parameterized query (no SELECT *)\n"
        "  - list_specialties(dataset?, table?, limit?)    # List hospital specialties\n"
        "  - search_hospitals(city?, specialty?, name_contains?, include_contact?, limit?, dataset?, table?)\n"
        "  - get_hospital(hospital_id, include_contact?, dataset?, table?)\n"
        "  - list_surgeon_subspecialties(dataset?, table?, limit?)\n"
        "  - list_hospital_surgeons(hospital_id, include_sensitive?, limit?, dataset?, table?)\n"
        "  - search_surgeons(hospital_id?, name_contains?, sub_specialty?, languages?, on_call?, accepts_new_patients?, include_sensitive?, limit?, dataset?, table?)\n"
        "  - get_surgeon(specialist_id, include_sensitive?, dataset?, table?)\n"
        "Notes:\n"
        "  • maximum_bytes_billed & row limits enforced.\n"
        "  • Only datasets in ALLOWED_DATASETS are permitted (if set).\n"
        "  • Contacts & sensitive fields gated by env flags.\n"
    ),
)

# Client BigQuery global
_client: Optional[bigquery.Client] = None

def bq_client() -> bigquery.Client:
    """Retourne une instance du client BigQuery avec authentification par service account"""
    global _client
    if _client is None:
        try:
            credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO)
            _client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        except Exception as e:
            # Fallback sur l'authentification par défaut
            print(f"Warning: Service account auth failed ({e}), using default auth")
            _client = bigquery.Client(project=PROJECT_ID) if PROJECT_ID else bigquery.Client()
    return _client

# ============= Fonctions utilitaires =============

def _require_allowed(dataset: str) -> None:
    if ALLOWED_DATASETS and dataset not in ALLOWED_DATASETS:
        raise ValueError(
            f"Dataset '{dataset}' is not allowed. Allowed: {sorted(ALLOWED_DATASETS)}"
        )

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
    ref = bigquery.TableReference(
        bigquery.DatasetReference(client.project, dataset), table
    )
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

    # fallback: contient hospital
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
    leaf_field: pour array_record, le sous-champ string plausible (ex: "specialty","name","value","s"), sinon None.
    """
    schema = _get_table_schema(dataset, table)
    fields = {f.name.lower(): f for f in schema}

    # 1) primary_specialty (utile mais 'scalar_string', géré en parallèle)
    # 2) specialties: on détecte sa vraie nature
    if "specialties" not in fields:
        return (None, "none", None)

    f = fields["specialties"]
    ftype = f.field_type.upper()
    fmode = (f.mode or "NULLABLE").upper()

    # STRING (NULLABLE): on suppose CSV "a;b;c"
    if ftype == "STRING" and fmode != "REPEATED":
        return ("specialties", "scalar_csv", None)

    # ARRAY<STRING>
    if ftype == "STRING" and fmode == "REPEATED":
        return ("specialties", "array_string", None)

    # ARRAY<RECORD> -> on cherche un champ string pertinent
    if ftype == "RECORD" and fmode == "REPEATED" and getattr(f, "fields", None):
        candidates = ["specialty", "name", "value", "label", "description", "s"]
        leaf = None
        for c in candidates:
            sub = next((sf for sf in f.fields if sf.name.lower() == c and sf.field_type.upper() == "STRING"), None)
            if sub:
                leaf = sub.name  # nom exact (respecte la casse)
                break
        # pas trouvé ? on renverra None et on fera TO_JSON_STRING comme fallback
        return ("specialties", "array_record", leaf)

    # RECORD non répété ou autre forme exotique: on tente au pire TO_JSON_STRING
    return ("specialties", "scalar_string", None)

# ============= Tools Legacy (pour compatibilité) =============

@mcp.tool()
def echo_tool(text: str) -> str:
    """Echo the input text"""
    return text

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
    """Get the schema of a BigQuery table (legacy version)"""
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

# ============= Tools Avancés =============

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
    # localise la table hôpitaux (auto + fallback)
    try:
        ds, tbl = _resolve_hospitals(dataset, table)
    except Exception:
        ds, tbl = (dataset or "prod_public"), (table or "hospital")

    _require_allowed(ds)
    client = bq_client()
    table_qual = _qual_table(ds, tbl)

    # inventaire des colonnes
    schema = _get_table_schema(ds, tbl)
    cols = {f.name.lower() for f in schema}
    has_primary = "primary_specialty" in cols

    spec_col, shape, leaf = _detect_specialties_shape(ds, tbl)

    parts: List[str] = []

    # 1) primary_specialty (STRING simple)
    if has_primary:
        parts.append(
            f"""
            SELECT TRIM(CAST(primary_specialty AS STRING)) AS specialty
            FROM {table_qual}
            WHERE primary_specialty IS NOT NULL
              AND LENGTH(TRIM(CAST(primary_specialty AS STRING))) > 0
            """
        )

    # 2) specialties selon la forme détectée
    if spec_col and shape != "none":
        if shape in ("scalar_string", "scalar_csv"):
            # STRING => CSV "a;b;c"
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
            # ARRAY<STRING>
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
                # ARRAY<RECORD<leaf STRING>>
                parts.append(
                    f"""
                    SELECT TRIM(CAST(x.{leaf} AS STRING)) AS specialty
                    FROM {table_qual}, UNNEST({spec_col}) AS x
                    WHERE x.{leaf} IS NOT NULL
                      AND LENGTH(TRIM(CAST(x.{leaf} AS STRING))) > 0
                    """
                )
            else:
                # Fallback: serialize en JSON puis TRIM
                parts.append(
                    f"""
                    SELECT TRIM(CAST(TO_JSON_STRING(x) AS STRING)) AS specialty
                    FROM {table_qual}, UNNEST({spec_col}) AS x
                    WHERE x IS NOT NULL
                      AND LENGTH(TRIM(CAST(TO_JSON_STRING(x) AS STRING))) > 0
                    """
                )

    if not parts:
        # Rien à agréger
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

    # colonnes existantes -> alias cohérents
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

    # Ajout contacts si autorisé et dispos
    if include_contact and HOSPITALS_CONTACT_ALLOWED:
        if "contact_phone" in fields:
            select_cols.append("contact_phone AS contact_phone")
        if "email" in fields:
            select_cols.append("email AS email")

    if not select_cols:
        # garde-fou
        select_cols.append("hospital_id AS hospital_id")
        select_cols.append("hospital_name AS hospital_name")

    # WHERE dynamique
    where: List[str] = []
    params: List[bigquery.ScalarQueryParameter] = []

    if city and "city" in fields:
        where.append("city = @city")
        params.append(bigquery.ScalarQueryParameter("city", "STRING", city))

    if name_contains and "hospital_name" in fields:
        where.append("LOWER(hospital_name) LIKE @namepat")
        params.append(bigquery.ScalarQueryParameter("namepat", "STRING", f"%{name_contains.lower()}%"))

    if specialty:
        # match primary_specialty exact (case-insensitive)
        conds: List[str] = []
        if "primary_specialty" in fields:
            conds.append("LOWER(primary_specialty) = @spec")
        if "specialties" in fields:
            # specialties est une STRING ; on split/trim et compare
            conds.append("EXISTS (SELECT 1 FROM UNNEST(SPLIT(specialties,';')) AS s WHERE LOWER(TRIM(s)) = @spec)")
        if conds:
            where.append("(" + " OR ".join(conds) + ")")
            params.append(bigquery.ScalarQueryParameter("spec", "STRING", specialty.lower()))

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    # Tri priorisant la capacité puis le nom si dispo
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

    # sub_specialty existe dans ton schéma sample
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

# ============= Prompts =============

@mcp.prompt("echo")
def echo_prompt(text: str) -> str:
    return text

if __name__ == "__main__":
    mcp.run()