from importlib.resources import path
import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from google.cloud import storage

from utils import Configs
from process_food_waste_data import process_food_waste_data

import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="process-food-waste-data",
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    
    process_data_task = PythonOperator(
        task_id="process_data_task",
        python_callable=process_food_waste_data,
        op_kwargs={
            "raw_folder": Configs.raw_folder,
            "parquet_file": Configs.parquet_file,
            "processed_folder": Configs.processed_folder,
            "cols_to_drop": ["image_id", "id", 'label_explanation']
        },
    )
    
    trigger_bq = TriggerDagRunOperator(
        task_id='trigger',  
        trigger_dag_id="data_to_dw_bq",
        wait_for_completion = False
        )
    

    process_data_task >> trigger_bq