import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

from utils import Configs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "dtc_project_bq")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{Configs.bq_food_waste_table_name} \
        PARTITION BY date_collected \
        AS \
        SELECT * FROM {BIGQUERY_DATASET}.{Configs.bq_food_waste_table_name}_external_table;"
    )


with DAG(
    dag_id="data_to_dw_bq",
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{Configs.bq_food_waste_table_name}_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": Configs.input_filetype,
                "sourceUris": [f"gs://{BUCKET}/{Configs.processed_folder}/{Configs.bq_food_waste_table_name}/*.parquet"],
            },
        },
    )

    # Create a partitioned table from external table
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_{Configs.bq_food_waste_table_name}_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )
    

    bigquery_external_table_task >> bq_create_partitioned_table_job 

    