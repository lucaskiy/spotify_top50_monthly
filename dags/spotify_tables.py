# -*- coding: utf-8 -*-
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

args = {
    "owner": "Lucas Kiy",
    "start_date": datetime(2022, 7, 4),
    "email": ["lucask.kiy@gmail.com"],
    "email_on_failure": False
}

dag = DAG("create_spotify_tables", schedule_interval="@once", default_args=args)


with dag:

    create_artists_table = PostgresOperator(
        task_id="create_artists_table",
        sql="""create table top_artists ("rank" integer NOT NULL,"date" date NOT NULL,artist varchar(100) 
                ,followers integer,genre varchar(100),main_subgenre varchar(100),PRIMARY KEY("rank", "date"))""",
        postgres_conn_id="postgres_local"
    )

    create_tracks_table = PostgresOperator(
        task_id="create_tracks_table",
        sql="""create table top_tracks ("rank" integer NOT NULL,"date" date NOT NULL,song_name varchar(100),
            artist varchar(100) ,album varchar(100),track_time time,release_date date,PRIMARY KEY("rank", "date"))""",
        postgres_conn_id="postgres_local"
    )


[create_artists_table, create_tracks_table]
