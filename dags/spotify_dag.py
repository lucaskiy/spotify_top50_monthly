# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from datetime import datetime
from python_scripts.spotify import Spotify

DBT_PROFILES_DIR = "/opt/airflow/.dbt/"

spotify = Spotify()

args = {
    "owner": "Lucas Kiy",
    "start_date": datetime(2022, 6, 1),
    "email": ["lucask.kiy@gmail.com"],
    "email_on_failure": False
}
dag = DAG("spotify_dag", schedule_interval="@monthly", default_args=args)

with dag:

    start_dag = BashOperator(
        task_id="start_dag",
        bash_command="echo DAG STARTED"
    )

    get_top_artists = PythonOperator(
        task_id="get_top_artists",
        python_callable=spotify.get_spotify_user_top_artists_to_gcp_storage
    )

    get_top_tracks = PythonOperator(
        task_id="get_top_tracks",
        python_callable=spotify.get_spotify_user_top_tracks_to_gcp_storage
    )

    validate_tracks_gcs = GCSObjectExistenceSensor(
        task_id="validate_tracks_gcs",
        bucket="spotify-top-api",
        google_cloud_conn_id="google-cloud",
        object="{{ ti.xcom_pull('get_top_tracks') }}"
    )

    validate_artists_gcs = GCSObjectExistenceSensor(
        task_id="validate_artists_gcs",
        bucket="spotify-top-api",
        google_cloud_conn_id="google-cloud",
        object="{{ ti.xcom_pull('get_top_artists') }}"
    )

    gcs_artists_to_bq = GCSToBigQueryOperator(
        task_id="gcs_artists_to_bq",
        bucket="spotify-top-api",
        source_format="csv",
        source_objects=["top_artists_*.csv"],
        destination_project_dataset_table="third-hangout-359213.spotify_top.raw_artists",
        gcp_conn_id='google-cloud',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )

    gcs_tracks_to_bq = GCSToBigQueryOperator(
        task_id="gcs_tracks_to_bq",
        bucket="spotify-top-api",
        source_format="csv",
        source_objects=["top_tracks_*.csv"],
        destination_project_dataset_table="third-hangout-359213.spotify_top.raw_tracks",
        gcp_conn_id='google-cloud',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd ${AIRFLOW_HOME}/dags/spotify-dbt && \
                    export DBT_PROFILES_DIR={{ params.DBT_PROFILES_DIR }} && \
                    dbt run",
        params={"DBT_PROFILES_DIR": DBT_PROFILES_DIR}
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd ${AIRFLOW_HOME}/dags/spotify-dbt && \
                    export DBT_PROFILES_DIR={{ params.DBT_PROFILES_DIR }} && \
                    dbt test",
        params={"DBT_PROFILES_DIR": DBT_PROFILES_DIR}
    )

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

    finish_dag = BashOperator(
        task_id="finish_dag",
        bash_command="echo DAG FINISHED"
    )

start_dag >> get_top_artists >> validate_artists_gcs >> gcs_artists_to_bq
start_dag >> get_top_tracks >> validate_tracks_gcs >> gcs_tracks_to_bq
[gcs_artists_to_bq, gcs_tracks_to_bq] >> dbt_run >> dbt_test
dbt_test >> validate_artists_bq
dbt_test >> validate_tracks_bq
[validate_artists_bq, validate_tracks_bq] >> finish_dag
