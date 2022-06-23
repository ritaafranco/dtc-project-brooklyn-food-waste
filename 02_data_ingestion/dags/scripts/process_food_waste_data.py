import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types
import pandas as pd
import os
import sys




PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
print("########" , BUCKET)
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

'''

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('process') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

'''

spark = SparkSession.builder.master("local[*]")\
        .appName('process')\
        .getOrCreate()

spark._jsc.hadoopConfiguration() \
    .set("google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)



food_waste_schema = types.StructType([
    types.StructField("id", types.StringType(), True),
    types.StructField("date_collected", types.TimestampType(), True),
    types.StructField("retailer_type", types.StringType(), True),
    types.StructField("retailer_detail", types.StringType(), True),
    types.StructField("food_type", types.StringType(), True),
    types.StructField("food_detail", types.StringType(), True),
    types.StructField("label_type", types.StringType(), True),
    types.StructField("label_language", types.StringType(), True),
    types.StructField("label_date", types.TimestampType(), True),
    types.StructField("approximate_dollar_value", types.DoubleType(), True),
    types.StructField("image_id", types.StringType(), True),
    types.StructField("collection_lat", types.DoubleType(), True),
    types.StructField("collection_long", types.DoubleType(), True),
    types.StructField("label_explanation", types.StringType(), True)
])

def process_food_waste_data(raw_folder = 'raw', parquet_file = 'brooklyn.parquet', cols_to_drop = []):
    print('ENTERED FUNCTION')

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
    if len(cols_to_drop)>0:
        df.drop(*cols_to_drop)

    print(df.head())
    
'''raw_folder = sys.argv[1]
parquet_file = sys.argv[2]
cols_to_drop = sys.argv[3]'''

#process_food_waste_data(raw_folder='raw', parquet_file='brooklyn.parquet', cols_to_drop=['id', 'image_id'])



df = pd.read_parquet("gs://dtc-project-data_dtc-project-ritaafranco/raw/brooklyn.parquet")
print(df.head(5))
