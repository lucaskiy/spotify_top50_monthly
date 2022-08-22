from airflow import DAG
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from datetime import datetime

DBT_PROFILES_DIR = "/opt/airflow/.dbt/"

args = {
    "owner": "Lucas Kiy",
    "email": ["lucask.kiy@gmail.com"],
    "email_on_failure": False
}

with DAG("dbt_test_dag", start_date=datetime(2022, 1, 1), schedule_interval="@daily", catchup=False) as dag:

    validate_tracks_bq = BigQueryTableExistenceSensor(
        task_id="validate_tracks_bq",
        project_id="third-hangout-359213",
        dataset_id="spotify_top",
        table_id="stg_top_tracks",
        gcp_conn_id='google-cloud'
    )

    validate_artists_bq = BigQueryTableExistenceSensor(
        task_id="validate_artists_bq",
        project_id="third-hangout-359213",
        dataset_id="spotify_top",
        table_id="stg_top_artists",
        gcp_conn_id='google-cloud'
    )

validate_tracks_bq >> validate_artists_bq