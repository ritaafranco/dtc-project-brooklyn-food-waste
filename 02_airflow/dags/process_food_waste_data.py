from flask import Config
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
import os

from utils import Configs




PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

spark = SparkSession.builder.master("local[*]")\
        .appName('process')\
        .getOrCreate()

spark._jsc.hadoopConfiguration() \
    .set("google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)


food_waste_schema = types.StructType([
    types.StructField("date_collected", types.DateType(), True),
    types.StructField("retailer_type", types.StringType(), True),
    types.StructField("retailer_detail", types.StringType(), True),
    types.StructField("food_type", types.StringType(), True),
    types.StructField("food_detail", types.StringType(), True),
    types.StructField("label_type", types.StringType(), True),
    types.StructField("label_language", types.StringType(), True),
    types.StructField("label_date", types.DateType(), True),
    types.StructField("approximate_dollar_value", types.DoubleType(), True),
    types.StructField("collection_lat", types.DoubleType(), True),
    types.StructField("collection_long", types.DoubleType(), True),
    types.StructField("label_explanation", types.StringType(), True)
])


def process_date_columns(df, date_cols = ['date_collected', 'label_date']):
    ''' Prepares date columns to date format '''
    no_records = df.count()
    
    for col in date_cols:
        df = df.filter(F.length(F.col(col)) == 10)\
                .withColumn(col, F.to_date(F.col(col), 'yyyy-MM-dd'))

    print('There were ', no_records - df.count(), ' records eliminated by ensuring date format.')

    return df


def process_food_waste_data(
                        raw_folder = Configs.raw_folder, 
                        parquet_file = Configs.parquet_file,  
                        processed_folder = Configs.processed_folder, 
                        bq_food_waste_table_name = Configs.bq_food_waste_table_name,
                        cols_to_drop = []):
    
    # open spark session
    spark = SparkSession.builder.master("local[*]")\
            .appName('process')\
            .getOrCreate()

    spark._jsc.hadoopConfiguration() \
        .set("google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)

    # reading raw file
    df = spark.read \
        .option("header", "true") \
        .parquet(f"gs://{BUCKET}/{raw_folder}/{parquet_file}")
    
    # drop unwanted columns 
    if len(cols_to_drop) > 0:   
        df = df.drop(*cols_to_drop)
    
    # data quality checks
    df = process_date_columns(df)

    # camel case category columns
    for col in ['retailer_type', 'food_type', 'food_detail']:
        df = df.withColumn(col, F.initcap(F.col(col)))

    # new metric
    df = df.withColumn('no_days_untill_expire', F.datediff(F.col('label_date'), F.col('date_collected')))
        
    # save processed df
    df.write.parquet(f"gs://{BUCKET}/{processed_folder}/{bq_food_waste_table_name}")