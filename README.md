# Medical FastMCP BigQuery Server Mistral P1

serveur FastMCP oriente BigQuery pour explorer des donnees hospitalieres et produire des resumes de dossiers patients.

## Fonctionnalites cles
- Connexion BigQuery geree via compte de service ou authentification locale Google Cloud.
- Ensemble complet d outils MCP couvrant schemas, recherches hospitalieres et resumes de dossiers.
- Protections natives : blocage de `SELECT *`, limites de lignes configurables et `MAX_BYTES_BILLED`, restriction optionnelle par dataset.
- Colonnes sensibles renvoyees uniquement lorsque les drapeaux d environnement correspondants sont actives.

## Prerequis
- Python 3.10 ou version plus recente.
- Acces a un projet Google Cloud BigQuery et aux datasets hospitaliers cibles.
- Compte de service Google Cloud ou authentification `gcloud auth application-default login`.

## Installation
```bash
python -m venv .venv
. .venv/Scripts/Activate.ps1  # PowerShell
pip install --upgrade pip
pip install -e .
```
Vous pouvez egalement utiliser [uv](https://docs.astral.sh/uv/) : `uv pip install -e .`.

## Configuration
Le fichier `main.py` lit plusieurs variables d environnement pour adapter le serveur :

| Variable | Valeur par defaut | Description |
| --- | --- | --- |
| `BQ_LOCATION` | `EU` | Region BigQuery utilisee pour les requetes. |
| `MAX_BYTES_BILLED` | `200000000` (200 MB) | Plafond de lecture par requete. |
| `MAX_ROWS` | `1000` | Nombre maximum de lignes renvoyees par outil. |
| `DEFAULT_DATASET` | `prod_public` | Dataset utilise si aucun dataset n est fourni. |
| `HOSPITALS_DATASET` / `HOSPITALS_TABLE` | `prod_public` / `hospital` | Source par defaut des hopitaux. |
| `SURGEONS_DATASET` / `SURGEONS_TABLE` | meme valeur que ci dessus | Source des chirurgiens. |
| `PATIENTS_DATASET` / `PATIENTS_TABLE` | `prod_public` / `patients` | Source des patients. |
| `CASES_DATASET` / `CASES_TABLE` | `prod_public` / `cases` | Source des cas cliniques. |
| `LABS_DATASET` / `LABS_TABLE` | `prod_public` / `lab_results` | Source des resultats de laboratoire. |
| `HOSPITALS_CONTACT_ALLOWED` | `false` | Autorise le renvoi du telephone et de l email d un hopital. |
| `SURGEONS_SENSITIVE_ALLOWED` | `false` | Autorise email/telephone/numeros RPPS pour les chirurgiens. |
| `PATIENTS_SENSITIVE_ALLOWED` | `false` | Autorise email/telephone/adresse/assurance pour les patients. |
| `ALLOWED_DATASETS` | vide | Liste separee par virgules de datasets autorises. |

- L identifiant de projet (`PROJECT_ID`) est defini dans le code (`mcp-hackathon-mistral`). Modifiez le ou surchargez le selon votre environnement.
- `SERVICE_ACCOUNT_INFO` contient des identifiants de demonstration. Remplacez le bloc par le JSON de votre compte de service ou supprimez le pour utiliser `GOOGLE_APPLICATION_CREDENTIALS`.
- Si l authentification par compte de service echoue, la librairie retombe sur l authentification implicite fournie par `gcloud`.

## Lancement du serveur
```bash
python main.py
```
FastMCP demarre alors un serveur nomme "Medical MCP BigQuery Server (Param)". Connectez le avec votre client MCP.

## Outils MCP disponibles

### BigQuery generique
- `get_schema(dataset, table)` : renvoie la liste des colonnes (nom/type/mode).
- `execute_query(dataset, sql, params=None, dry_run_check=True, row_limit=None)` : execute une requete `SELECT` parametree. `SELECT *` est refuse et un dry run peut verifier les octets analyses.

### Hopitaux
- `list_specialties(dataset=HOSPITALS_DATASET, table=HOSPITALS_TABLE, limit=200)` : renvoie les specialites distinctes.
- `search_hospitals(..., city=None, specialty=None, name_contains=None, include_contact=False, limit=50)` : recherche multi criteres; les contacts ne sont exposes que si `HOSPITALS_CONTACT_ALLOWED` vaut true et que `include_contact=True`.
- `get_hospital(..., hospital_id, include_contact=False)` : recupere la fiche d un hopital.

### Chirurgiens
- `list_surgeon_subspecialties(dataset=SURGEONS_DATASET, table=SURGEONS_TABLE, limit=3000)` : renvoie les sous specialites disponibles.
- `list_hospital_surgeons(..., hospital_id, include_sensitive=False, limit=100)` : liste les chirurgiens d un hopital avec options sur les colonnes sensibles.
- `search_surgeons(..., hospital_id=None, name_contains=None, sub_specialty=None, languages=None, on_call=None, accepts_new_patients=None, include_sensitive=False, limit=50)` : recherche avancee sur les chirurgiens.
- `get_surgeon(..., specialist_id, include_sensitive=False)` : retourne la fiche detaillee d un chirurgien.

### Patients, cas et examens
- `get_patient(..., patient_id, include_sensitive=False)` : renvoie un patient et calcule l age en annees si la date de naissance est disponible.
- `list_patient_cases(..., patient_id, limit=100)` : liste les cas cliniques les plus recents (ordre descendant sur les dates disponibles).
- `list_patient_lab_results(..., patient_id, limit=100)` : extrait les derniers resultats de laboratoire.
- `summarize_patient_dossier(...)` : assemble patient, resume des cas, derniers examens et tentative de rapprochement du chirurgien cite dans `attending_physician`.

## Garde fous integres
- Toutes les requetes sont strictement en lecture et parametrees via `QueryJobConfig`.
- `MAX_ROWS` et `MAX_BYTES_BILLED` limitent la taille des reponses et le volume d octets scannes.
- `ALLOWED_DATASETS` peut restreindre la liste blanche des datasets accessibles.
- Les colonnes sensibles ne sont renvoyees que si la variable d environnement correspondante est a `true` et que l appel indique `include_contact=True` ou `include_sensitive=True`.

## Ressources utiles
- [Documentation FastMCP](https://gofastmcp.com/)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [Client Python BigQuery](https://cloud.google.com/python/docs/reference/bigquery/latest)
