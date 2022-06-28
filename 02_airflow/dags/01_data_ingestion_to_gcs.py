import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

from utils import Configs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def download_data(dataset_name = Configs.dataset_name):
    # extracting data from Kaggle
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_name)

def unzip_data(zipfile_name = Configs.zipfile_name):
    from zipfile import ZipFile
    zf = ZipFile(zipfile_name)
    zf.extractall() #save files in cwd
    zf.close()

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG( 
    dag_id="data_ingestion_to_gcs",
    schedule_interval="@once",
    start_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=False,
    max_active_runs=2,
) as dag:

    download_data_task = PythonOperator(
        task_id="download_data_task",
        python_callable=download_data
    )

    unzip_dataset_task = PythonOperator(
        task_id="unzip_dataset_task",
        python_callable=unzip_data
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{Configs.dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{Configs.raw_folder}/{Configs.parquet_file}",
            "local_file": f"{path_to_local_home}/{Configs.parquet_file}",
        },
    )
    
    trigger_process = TriggerDagRunOperator(
        task_id='trigger',  
        trigger_dag_id="process-food-waste-data",
        wait_for_completion=False
        )
    
    rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {path_to_local_home}/{Configs.dataset_file} {path_to_local_home}/{Configs.zipfile_name}"
        )

    download_data_task >> unzip_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> trigger_process >> rm_task