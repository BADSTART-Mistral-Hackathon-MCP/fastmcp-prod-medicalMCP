"""
FastMCP Echo Server with BigQuery Integration
"""

import os
from fastmcp import FastMCP
from typing import List
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuration du projet
PROJECT_ID = "mcp-hackathon-mistral"

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
mcp = FastMCP("Echo Server with BigQuery")

# Client BigQuery global
_client = None

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
            _client = bigquery.Client(project=PROJECT_ID)
    return _client

@mcp.tool
def echo_tool(text: str) -> str:
    """Echo the input text"""
    return text

@mcp.tool
def list_datasets() -> List[str]:
    """List all available BigQuery datasets in the project"""
    try:
        client = bq_client()
        datasets = sorted(ds.dataset_id for ds in client.list_datasets())
        return datasets
    except Exception as e:
        return [f"Error: {str(e)}"]

@mcp.resource("echo://static")
def echo_resource() -> str:
    return "Echo!"

@mcp.resource("echo://{text}")
def echo_template(text: str) -> str:
    """Echo the input text"""
    return f"Echo: {text}"

@mcp.prompt("echo")
def echo_prompt(text: str) -> str:
    return text

if __name__ == "__main__":
    mcp.run()