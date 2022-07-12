# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from python.spotify import Spotify

spotify = Spotify()

args = {
    "owner": "Lucas Kiy",
    "start_date": datetime(2022, 6, 1),
    "email": ["lucask.kiy@gmail.com"],
    "email_on_failure": False
}
dag = DAG("spotify_dag", schedule_interval="@monthly", default_args=args)

with dag:

    top_artists = PythonOperator(
        task_id="get_top_artists",
        python_callable=spotify.get_spotify_user_top_artists
    )

    top_tracks = PythonOperator(
        task_id="get_top_tracks",
        python_callable=spotify.get_spotify_user_top_tracks
    )

    insert_artists_query = PythonOperator(
        task_id="insert_artists_query",
        python_callable=spotify.generate_artists_query
    )

    insert_tracks_query = PythonOperator(
        task_id="insert_tracks_query",
        python_callable=spotify.generate_tracks_query
    )

    save_artists_table = PostgresOperator(
        task_id="save_artists_table",
        sql="""{{ti.xcom_pull(task_ids='insert_artists_query', key='return_value')}}""",
        postgres_conn_id="postgres_local"
    )

    save_tracks_table = PostgresOperator(
        task_id="save_tracks_table",
        sql="""{{ti.xcom_pull(task_ids='insert_tracks_query', key='return_value')}}""",
        postgres_conn_id="postgres_local"
    )

    # email_csv = PythonOperator(
    #     task_id="send_email",
    #     python_callable=spotify.email_csvs,
    #     provide_context=True
    # )

top_artists >> insert_artists_query >> save_artists_table
top_tracks >> insert_tracks_query >> save_tracks_table
# [save_artists, save_tracks] >> email_csv
