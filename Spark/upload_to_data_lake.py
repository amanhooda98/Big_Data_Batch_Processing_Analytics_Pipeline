import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col,isnan,when,count
credentials_location = '/home/amanhd9/.google/credentials/google_credentials.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "/home/amanhd9/SPARK/spark-3.3.3-bin-hadoop3/jars/gcs-connector-hadoop3-latest.jar") \
    .set("spark.jars", "/home/amanhd9/SPARK/spark-3.3.3-bin-hadoop3/jars/spark-3.3-bigquery-0.32.2.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
    
sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()
    

df_sales=spark.read.csv('/home/amanhd9/notebooks/data/sales.csv',header=True,inferSchema=True)
df_stores=spark.read.csv('/home/amanhd9/notebooks/data/store_cities.csv',header=True,inferSchema=True)

df_sales.write.mode('overwrite').parquet('gs://amanhd9-bucket/retail_data/sales.parquet')
df_stores.write.mode('overwrite').parquet('gs://amanhd9-bucket/retail_data/stores.parquet')

spark.stop()