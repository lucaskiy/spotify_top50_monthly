# -*- coding: utf-8 -*-
from re import M
import pandas as pd
import base64
import requests
import json
from airflow.models import Variable
from airflow.utils.email import send_email
from datetime import datetime
from pathlib import Path
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class Spotify():
    def __init__(self):
        super().__init__()
        self.__token = self.get_spotify_access_token()
        self.user_uri = "https://api.spotify.com/v1/me/top/"
        # self.time_range = ['short_term', 'medium_term', 'long_term']
        self.time_range = ['short_term']
        self.bucket = "spotify-top-api"
        self.gcloud = GCSHook(gcp_conn_id="google-cloud")

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

    def get_spotify_user_top_tracks_to_gcp_storage(self) -> str:
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
                        data = {
                            'rank': int(i+1),
                            'date': datetime.strftime(datetime.now(), "%Y-%m-%d"),
                            'song_name': str(item['name']).strip().lower().replace("'", ''),
                            'artist': str(item['artists'][0]['name']).strip().lower().replace("'", ''),
                            'album': str(item['album']['name']).strip().lower().replace("'", ''),
                            'track_time_ms': int(item['duration_ms']),
                            'release_date': str(item['album']['release_date']),
                            'external_urls': str(item['external_urls']['spotify']),
                            'images': str(item['album']['images'][0]['url']),
                            'popularity': int(item['popularity']) if 'popularity' in item else None,
                            'uri': str(item['uri'])
                        }
                        results.append(data)

            except Exception as e:
                raise e

        print("Top tracks retrieved successfully")
        df = pd.DataFrame(results)
        file = df.to_csv(index=False)
        filename = f"top_tracks_{datetime.strftime(datetime.now(), '%Y-%m-%d')}.csv"

        self.gcloud.upload(
            bucket_name=self.bucket,
            object_name=filename,
            data=file)
        return filename

    def get_spotify_user_top_artists_to_gcp_storage(self) -> str:
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
                            'main_subgenre': str(item['genres'][1]).strip().lower().replace("'", '') if len(item['genres']) > 1 else None,
                            'external_urls': str(item['external_urls']['spotify']),
                            'images': str(item['images'][0]['url']),
                            'popularity': int(item['popularity']) if 'popularity' in item else None, 
                            'uri': str(item['uri'])
                        }
                        results.append(data)

            except Exception as e:
                raise e

        print("Top artists retrieved successfully")
        df = pd.DataFrame(results)
        file = df.to_csv(index=False)
        filename = f"top_artists_{datetime.strftime(datetime.now(), '%Y-%m-%d')}.csv"

        self.gcloud.upload(
            bucket_name=self.bucket,
            object_name=filename,
            data=file)
        return filename

    def create_tracks_bigquery_table():
        pass


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
