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
processed_folder = 'processed'
parquet_file = 'brooklyn.parquet'
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
            "raw_folder": raw_folder,
            "parquet_file": parquet_file,
            "processed_folder": processed_folder,
            "cols_to_drop": ["image_id", "id"]
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{colour}_{DATASET}_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{colour}_{DATASET}_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                "sourceUris": [f"gs://{BUCKET}/{colour}/*"],
            },
        },
    )

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
        PARTITION BY DATE({ds_col}) \
        AS \
        SELECT * FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
    )

    # Create a partitioned table from external table
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )
    
    # remove processed data 


    process_data_task

    