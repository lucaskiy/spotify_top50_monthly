# -*- coding: utf-8 -*-
import pandas as pd
import base64
import requests
import json
from airflow.models import Variable
from airflow.utils.email import send_email
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
from python.boto3_utils import Boto3Utils
from python.postgres_object import PostgresObject


class Spotify(PostgresObject):
    def __init__(self):
        super().__init__()
        self.__token = self.get_spotify_access_token()
        self.user_uri = "https://api.spotify.com/v1/me/top/"
        # self.time_range = ['short_term', 'medium_term', 'long_term']
        self.time_range = ['short_term']
        self.pg_hook = PostgresHook(postgres_conn_id="postgres_local")
        self.conn = self.pg_hook.get_conn()
        self.cursor = self.conn.cursor()
        self.bucket = "spotify-api-bucket"
        self.boto3 = Boto3Utils()

    @staticmethod
    def config() -> dict:
        creds = {'client_id': Variable.get("spotify_client_id"),
                 'client_secret': Variable.get("spotify_client_secret"),
                 'access_token': Variable.get("spotify_access_token"),
                 'refresh_token': Variable.get("spotify_refresh_token")}
        return creds

    def get_spotify_access_token(self) -> str:
        credentials = self.config()
        token_request_uri = "https://accounts.spotify.com/api/token"

        message = f"{credentials['client_id']}:{credentials['client_secret']}"
        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')

        headers = {'Authorization': f"Basic {base64_message}"}
        data = {
            "grant_type": "refresh_token",
            "refresh_token": credentials["refresh_token"],
            "redirect_uri": "http://localhost:8080/"
        }

        response = requests.post(token_request_uri, data=data, headers=headers)
        if response.status_code == 200:
            token = response.json()['access_token']
            return "Bearer " + token
        else:
            raise Exception(response.status_code, response.text)

    def get_spotify_user_top_tracks(self) -> str:
        uri = self.user_uri + "tracks"
        headers = {"Authorization": self.__token}

        results = list()
        for time in self.time_range:
            params = {'time_range': time, 'limit': 50}
            try:
                response = requests.get(uri, headers=headers, params=params)
                if response.status_code == 200:
                    res = json.loads(response.text)
                    for i, item in enumerate(res['items']):
                        song_time = f"{int((item['duration_ms']/(1000*60))%60)}:{int((item['duration_ms']/1000)%60)}"
                        data = {
                            'rank': int(i+1),
                            'date': datetime.strftime(datetime.now(), "%Y-%m-%d"),
                            'song_name': str(item['name']).strip().lower().replace("'", ''),
                            'artist': str(item['artists'][0]['name']).strip().lower().replace("'", ''),
                            'album': str(item['album']['name']).strip().lower().replace("'", ''),
                            'track_time': song_time,
                            'release_date': str(item['album']['release_date'])
                        }
                        results.append(data)
            except Exception as e:
                raise e

        print("Top tracks retrieved successfully")
        key = f"top_tracks_{datetime.strftime(datetime.now(), '%Y-%m-%d')}.csv"
        save_s3 = self.boto3.save_s3_files(bucket=self.bucket, key=key, data=results)
        if save_s3:
            print("Top tracks file saved on s3!!")
            return key

    def get_spotify_user_top_artists(self) -> str:
        uri = self.user_uri + "artists"
        headers = {"Authorization": self.__token}

        results = list()
        for time in self.time_range:
            params = {'time_range': time, 'limit': 50}
            try:
                response = requests.get(uri, headers=headers, params=params)
                if response.status_code == 200:
                    res = json.loads(response.text)
                    for i, item in enumerate(res['items']):
                        data = {
                            'rank': int(i+1),
                            'date': datetime.strftime(datetime.now(), "%Y-%m-%d"),
                            'artist': str(item['name']).strip().lower().replace("'", ''),
                            'followers': int(item['followers']['total']),
                            'genre': str(item['genres'][0]).strip().lower().replace("'", ''),
                            'main_subgenre': str(item['genres'][1]).strip().lower().replace("'", '') if len(item['genres']) > 1 else None
                        }
                        results.append(data)
            except Exception as e:
                raise e

        print("Top artists retrieved successfully")
        key = f"top_artists_{datetime.strftime(datetime.now(), '%Y-%m-%d')}.csv"
        save_s3 = self.boto3.save_s3_files(bucket=self.bucket, key=key, data=results)
        if save_s3:
            print("Top artists file saved on s3!!")
            return key

    def generate_artists_query(self, **kwargs) -> str:
        print("Starting method to save artists data into postgres database")
        key = kwargs['ti'].xcom_pull(task_ids="get_top_artists")
        top_artists = self.boto3.get_s3_files(bucket=self.bucket, key=key)
        print(top_artists.head())

        query = self.generate_query(df=top_artists, table_name="top_artists")
        return query


    def generate_tracks_query(self, **kwargs) -> str:
        print("Starting method to save tracks data into postgres database")
        key = kwargs['ti'].xcom_pull(task_ids="get_top_tracks")
        top_tracks = self.boto3.get_s3_files(bucket=self.bucket, key=key)
        print(top_tracks.head())
        
        query = self.generate_query(df=top_tracks, table_name="top_tracks")
        return query

    @staticmethod
    def email_csvs(**kwargs):
        top_tracks = kwargs['ti'].xcom_pull(key="email_tracks", task_ids="get_top_tracks")
        top_artists = kwargs['ti'].xcom_pull(key="email_artist", task_ids="get_top_artists")
        print("Spotify files retrieved successfully!")

        df_tracks = pd.DataFrame(top_tracks)
        df_artists = pd.DataFrame(top_artists)
        print(df_artists.head())
        print(df_tracks.head())

        df_tracks.to_csv("top_tracks.csv", index=False)
        df_artists.to_csv("top_artists.csv", index=False)

        receivers = ["lucask.kiy@gmail.com"]

        send_email(
            to=receivers,
            subject="Os seus top Artistas e Músicas do último mês - Spotify",
            html_content=("Olá!!!<br /><br />"
                          "Aqui estão os seus top 50 artistas e músicas escutadas neste úlimo mês no Spotify!"),
            files=["top_tracks.csv", "top_artists.csv"]
        )
        print(f"Email sent successfully to {receivers[0]}!")
