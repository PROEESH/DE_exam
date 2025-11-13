from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage

# ------------- Helper function to read SQL from GCS -------------
def read_sql_from_gcs(bucket_name, file_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    return blob.download_as_text()

# ------------- DAG default args -------------
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# ------------- Variables -------------
COMPOSER_BUCKET = "us-central1-airflow-de-exam-1de749f2-bucket"  # replace with your Composer bucket
PROJECT_ID = "voltaic-tooling-471807-t5"
REGION = "us-central1"

DATAFLOW_TEMPLATE_1 = f"gs://{PROJECT_ID}-templates/ingest_api1.json"
DATAFLOW_TEMPLATE_2 = f"gs://{PROJECT_ID}-templates/ingest_api2.json"

SILVER_SQL_PATH = "dags/sql/silver.sql"
GOLD_SQL_PATH   = "dags/sql/gold.sql"

# ------------- DAG definition -------------
with DAG(
    'dataflow_ingest_bq',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # every day at 6AM
    catchup=False,
) as dag:

    # Step 1: Run ingest_api1 Dataflow template
    ingest_api1 = DataflowTemplatedJobStartOperator(
        task_id='ingest_api1',
        template=DATAFLOW_TEMPLATE_1,
        location=REGION,
        project_id=PROJECT_ID,
        parameters={},
    )

    # Step 2: Run ingest_api2 Dataflow template
    ingest_api2 = DataflowTemplatedJobStartOperator(
        task_id='ingest_api2',
        template=DATAFLOW_TEMPLATE_2,
        location=REGION,
        project_id=PROJECT_ID,
        parameters={},
    )

    # Step 3: Run BigQuery Silver SQL
    bq_silver = BigQueryInsertJobOperator(
        task_id='bq_silver',
        configuration={
            "query": {
                "query": read_sql_from_gcs(COMPOSER_BUCKET, SILVER_SQL_PATH),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location=REGION,
        project_id=PROJECT_ID,
    )

    # Step 4: Run BigQuery Gold SQL
    bq_gold = BigQueryInsertJobOperator(
        task_id='bq_gold',
        configuration={
            "query": {
                "query": read_sql_from_gcs(COMPOSER_BUCKET, GOLD_SQL_PATH),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location=REGION,
        project_id=PROJECT_ID,
    )

    # ------------- DAG dependencies -------------
    ingest_api1 >> ingest_api2 >> bq_silver >> bq_gold
