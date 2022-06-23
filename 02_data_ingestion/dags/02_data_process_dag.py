from importlib.resources import path
import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

from scripts.process_food_waste_data import process_food_waste_data

import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


raw_folder = 'raw'
parquet_file = 'brooklyn.parquet'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
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
            "raw_folder": raw_folder,
            "parquet_file": parquet_file,
            "cols_to_drop": ["image_id", "id"]
        },
    )
    '''
    process_data_task = SparkSubmitOperator(
                task_id="spark_job",
                application="./dags/scripts/process_food_waste.py", # Spark application path created in airflow and spark cluster
                name="process_data",
                conn_id="spark_default",
                verbose=1,
                conf={"spark.master":"local[*]"},
                dag=dag)'''

    process_data_task

    