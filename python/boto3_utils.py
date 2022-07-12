# -*- coding: utf-8 -*-
import pandas as pd
import boto3
from botocore.client import Config
import io
from airflow.models import Variable


class Boto3Utils:
    def __init__(self):
        self.client = self.get_s3_client()

    def get_s3_client(self) -> Config:
        aws_access_key_id = Variable.get("aws_access_key")
        aws_secret_access_key = Variable.get("aws_secret_key")
        config_s3 = Config(connect_timeout=200, retries={'max_attempts': 5})
        client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key, config=config_s3)
        return client

    def get_s3_files(self, bucket: str, key: str) -> pd.DataFrame:
        obj = self.client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df

    def save_s3_files(self, bucket: str, key: str, data: list) -> bool:
        try:
            df = pd.DataFrame(data)
            file = df.to_csv(index=False)

            self.client.put_object(Bucket=bucket, Key=key, Body=file, ACL='private')
            return True
            
        except Exception as error:
            raise error
