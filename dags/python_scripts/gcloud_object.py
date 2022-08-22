from google.cloud import storage, bigquery
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os


class GcloudObject:
    def __init__(self):
        self.client = self._gcp_credentials()

    @staticmethod
    def _gcp_credentials():
        gcp_hook = GCSHook(gcp_conn_id="google-cloud")
        return gcp_hook

    def upload_to_bucket(self, blob_name: str, path_to_file: str, bucket_name: str) -> bool:
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(path_to_file)

        return True

    def get_files_inside_bucket(self, bucket_name: str) -> list:
        bucket = self.client.get_bucket(bucket_name)
        blobs = bucket.list_blobs()
        results = list()

        for blob in blobs:
            if blob.name in results:
                print(f"Duplicate file found -> {blob.name}")
            results.append(blob.name)

        return results

    @staticmethod
    def load_bucket_data_to_big_query():
        bq_client = bigquery.Client()

        print(f"Processing enem files from Storage to BigQuery!")

        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
        uri = "gs://enem-script-teste/MICRODADOS_ENEM_*.parquet"
        table_id = "dataengproject-355818.enem.raw_data"

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config) 
        load_job.result()  

        destination_table = bq_client.get_table(table_id)  

        print("Table {} uploaded.".format(table_id))
        print("Loaded {} rows.".format(destination_table.num_rows))
