from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
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
COMPOSER_BUCKET = "us-central1-airflow-de-exam-1de749f2-bucket"
PROJECT_ID = "voltaic-tooling-471807-t5"
REGION_BQ = "US"
REGION_DF = "us-central1"

DATAFLOW_TEMPLATE_1 = f"gs://{PROJECT_ID}-templates/ingest-api1.json"
DATAFLOW_TEMPLATE_2 = f"gs://{PROJECT_ID}-templates/ingest-api2.json"

TEAMS_SQL_PATH = "dags/sql/teams.sql"
STANDINGS_SQL_PATH = "dags/sql/standings.sql"
TEAMS_STAND_SQL_PATH = "dags/sql/teams_standings.sql"


from airflow.models import Variable
API_KEY_2 = Variable.get("API_KEY_2")

# ------------- DAG definition -------------
with DAG(
    'dataflow_ingest_bq',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # every day at 6AM
    catchup=False,
) as dag:

    # Step 1: Run ingest_api1 Flex Template
    # ingest_api1 = DataflowStartFlexTemplateOperator(
    #     task_id='ingest_api1',
    #     body={
    #         "launchParameter": {
    #             "jobName": "ingest-api1-{{ ds_nodash }}",
    #             "containerSpecGcsPath": DATAFLOW_TEMPLATE_1,
    #             "environment": {
    #                 "tempLocation": f"gs://{PROJECT_ID}-templates/temp",
    #                 "zone": "us-central1-f"
    #             },
    #             "parameters": {}
    #         }
    #     },
    #     location=REGION,
    #     project_id=PROJECT_ID
    # )

    # Step 2: Run ingest_api2 Flex Template
    ingest_api2 = DataflowStartFlexTemplateOperator(
        task_id='ingest_api2',
        body={
            "launchParameter": {
                "jobName": "ingest-api2-{{ ds_nodash }}",
                "containerSpecGcsPath": DATAFLOW_TEMPLATE_2,
                "environment": {
                    "tempLocation": f"gs://{PROJECT_ID}-templates/temp",
                    "zone": "us-central1-f"
                },
                "parameters": {
                    "API_KEY_2": API_KEY_2,
                    "FLEX_TEMPLATE_PYTHON_PY_FILE": "/template/src/Pipelines/ingest-api2.py"
                }
        }},
        location=REGION_DF,
        project_id=PROJECT_ID
    )

    # Step 3: Run BigQuery Teams SQL
    bq_teams = BigQueryInsertJobOperator(
        task_id='bq_teams',
        configuration={
            "query": {
                "query": read_sql_from_gcs(COMPOSER_BUCKET, TEAMS_SQL_PATH),
                "useLegacySql": False #,
                #"writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location=REGION_BQ,
        project_id=PROJECT_ID,
    )

    # Step 4: Run BigQuery Standings SQL
    bq_standings = BigQueryInsertJobOperator(
        task_id='bq_standings',
        configuration={
            "query": {
                "query": read_sql_from_gcs(COMPOSER_BUCKET, STANDINGS_SQL_PATH),
                "useLegacySql": False,
                #"writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location=REGION_BQ,
        project_id=PROJECT_ID,
    )

    # Step 4: Run BigQuery Teams Standings SQL
    bq_teams_standings = BigQueryInsertJobOperator(
        task_id='bq_teams_standings',
        configuration={
            "query": {
                "query": read_sql_from_gcs(COMPOSER_BUCKET, TEAMS_STAND_SQL_PATH),
                "useLegacySql": False,
                #"writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location=REGION_BQ,
        project_id=PROJECT_ID,
    )

    # ------------- DAG dependencies -------------
    #ingest_api1 >> 
    [ingest_api2, bq_teams] >> bq_standings >> bq_teams_standings
