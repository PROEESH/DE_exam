// ...existing code...
# DE_exam

Lightweight Data engineering sample project — ingest football data from two external APIs, transform, persist as Parquet and load to BigQuery. Includes Beam pipelines, Airflow DAG, SQL models and deployment artifacts for local testing or running on GCP Dataflow.

## Architecture Overview

![Pipeline Architecture](docs/architecture.png)

## Contents
- .env — environment variables (not committed)
- Dockerfile, cloudbuild.yaml — container and Cloud Build config
- requirements.txt — Python dependencies
- dags/
  - football.py — Airflow DAG(s)
- src/
  - Pipelines/
    - ingest-api1.py — pipeline for API 1
    - ingest-api2.py — pipeline for API 2 (teams & standings)
  - bq_models/
    - my_model.sql — example BigQuery model
  - templates_metadata/ — JSON templates used by ingestion scripts
- sql/
  - silver.sql, gold.sql — transformation queries for silver/gold layers

## Project goals
- Ingest team and standings data from external football APIs
- Normalize and write data as Parquet (GCS/local)
- Append data to BigQuery tables
- Be runnable locally (DirectRunner) and on GCP Dataflow (DataflowRunner)
- Provide CI/CD and containerization examples

## Prerequisites
- Python 3.8+ (match versions in requirements.txt)
- pip
- Recommended for Windows: WSL2 or Linux container for pyarrow / Beam native wheels if you hit build issues
- (GCP) gcloud SDK, project with billing enabled
- (GCP) Enabled APIs: Dataflow, Compute Engine, BigQuery, Cloud Storage



🏗 Project Setup & Instructions
1️⃣ Clone the Repository
git clone https://github.com/PROEESH/DE_exam.git
cd DE_exam


Make sure the repository contains:

cloudbuild.yaml → Cloud Build pipeline

src/Pipelines/ → Dataflow Python scripts

templates_metadata/ → metadata for Flex Templates

dags/ → Airflow DAGs

sql/ → SQL files for BigQuery (silver/gold tables)

requirements.txt → Python dependencies

.env (optional for local testing API keys)

2️⃣ Set Environment Variables
Local Development

Create .env in repo root:

API_KEY_1=<your_api_key_1>
API_KEY_2=<your_api_key_2>
PROJECT_ID=<your_gcp_project_id>


.env allows pipelines to run locally without hardcoding secrets.

Use python-dotenv to load these in your scripts.

Cloud / Production

Use Google Secret Manager for API keys.

Example:

gcloud secrets create API_KEY_1 --data-file=<(echo "your_key_1")
gcloud secrets create API_KEY_2 --data-file=<(echo "your_key_2")


Grant Cloud Build service account access to read secrets.

3️⃣ Buckets Setup

Create required buckets:

# For Dataflow staging/templates
gsutil mb -l us-central1 gs://$PROJECT_ID-templates
gsutil mkdir gs://$PROJECT_ID-templates/staging
gsutil mkdir gs://$PROJECT_ID-templates/temp

# For raw Parquet output
gsutil mb -l us-central1 gs://sport__bucket

4️⃣ Composer (Airflow) Setup
# I did it drom the UI, 
# its possible to do it with the gcloud CLI
Create Composer environment (if not already):

gcloud composer environments create my-composer \
    --location=us-central1 \
    --zone=us-central1-a \
    --machine-type=n1-standard-1 \
    --python-version=3 \
    --image-version=composer-2.2.5-airflow-2.7.1


Copy DAGs and SQL to Composer bucket:

From cloudbuild.yaml this is automated, or if you want manually:

# DAG Python files
gsutil cp dags/*.py gs://<COMPOSER_BUCKET>/dags/

# SQL folder
gsutil -m cp -r sql gs://<COMPOSER_BUCKET>/dags/


The DAG will appear in the Airflow UI, ready to schedule.

5️⃣ Cloud Build Pipeline

Trigger build from GitHub:

gcloud builds submit --config cloudbuild.yaml .


What Cloud Build does:

Builds Docker image for Dataflow pipelines

Pushes Docker image to gcr.io/$PROJECT_ID/beam-pipelines:latest

Creates Flex Templates for ingest-api1.py and ingest-api2.py

Uploads Airflow DAGs & SQL to Composer bucket

Variables used in Cloud Build:

$PROJECT_ID → auto-filled by Cloud Build

_COMPOSER_BUCKET → specify your Composer bucket name in cloudbuild.yaml | or put in cloud build trigger "Substitution variables" (inside UI)

6️⃣ Running Dataflow Jobs

Dataflow Flex Templates are ready in GCS:

gs://$PROJECT_ID-templates/ingest-api1.json
gs://$PROJECT_ID-templates/ingest-api2.json


Trigger manually:

gcloud dataflow flex-template run "ingest-api1-$(date +%Y%m%d-%H%M%S)" \
    --template-file-gcs-location gs://$PROJECT_ID-templates/ingest-api1.json \
    --region us-central1


Or let Airflow DAG handle scheduling.

7️⃣ Airflow DAG Scheduling

DAG: football_ingest

Schedule: daily (@daily) by default.

Tasks in DAG:

ingest_api1 → triggers first Dataflow job

ingest_api2 → triggers second Dataflow job

run_sql_models → executes BigQuery SQL to build silver/gold tables

Trigger DAG manually from UI if needed.

8️⃣ BigQuery Setup

Tables created by pipeline:

teams.teams → raw data

Silver/Gold tables → processed via SQL models (sql/silver.sql, sql/gold.sql)

BigQuery will automatically create tables if they don’t exist using WRITE_APPEND and CREATE_IF_NEEDED in Dataflow.

9️⃣ Local Testing (Optional)

Install dependencies:

pip install -r requirements.txt



Test Dataflow pipelines locally:

python src/Pipelines/ingest-api1.py
python src/Pipelines/ingest-api2.py


Local run uses .env for API keys and writes to your local temp path or bucket if configured.

10️⃣ Secrets & Security

Never commit .env to GitHub.

For production, use Secret Manager and inject into Cloud Build and Composer via environment variables.

11️⃣ Summary Checklist

✅ Repo cloned with required structure

✅ .env (for local) or Secret Manager (cloud)

✅ Buckets created (templates, temp, staging, sport__bucket)

✅ Composer environment created, DAGs & SQL uploaded

✅ Cloud Build triggered → Docker image + Flex Templates built

✅ Airflow DAG schedules ingestion + BigQuery models

✅ Local testing possible using .env
