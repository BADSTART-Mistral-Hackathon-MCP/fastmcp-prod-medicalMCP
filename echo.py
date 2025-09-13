"""
FastMCP Echo Server with BigQuery Integration
"""

import os
from fastmcp import FastMCP
from typing import List
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuration du projet
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "mcp-hackathon-mistral")

# Credentials - en production, utilisez des variables d'environnement
SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": os.getenv("GCP_PROJECT_ID", "mcp-hackathon-mistral"),
    "private_key_id": os.getenv("GCP_PRIVATE_KEY_ID"),
    "private_key": os.getenv("GCP_PRIVATE_KEY", "").replace('\\n', '\n'),
    "client_email": os.getenv("GCP_CLIENT_EMAIL"),
    "client_id": os.getenv("GCP_CLIENT_ID"),
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": os.getenv("GCP_CLIENT_CERT_URL"),
    "universe_domain": "googleapis.com"
}

# Create server
mcp = FastMCP("Echo Server with BigQuery")

# Client BigQuery global
_client = None

def bq_client() -> bigquery.Client:
    """Retourne une instance du client BigQuery"""
    global _client
    if _client is None:
        # Vérifier si on a les credentials en variables d'environnement
        if all(SERVICE_ACCOUNT_INFO[key] for key in ["private_key", "client_email"]):
            credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO)
            _client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        else:
            # Fallback sur l'authentification par défaut
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