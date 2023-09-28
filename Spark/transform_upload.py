import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col,isnan,when,year,month
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
    

sales_table_name = "data-eng-399920.stg.sales-data"
stores_table_name = "data-eng-399920.stg.stores-data"
prod_table_name = "data-eng-399920.prod.revenue-data" 

df_sales = spark.read \
    .format("bigquery") \
    .option("table", sales_table_name) \
    .load()
    
df_stores= spark.read \
    .format("bigquery") \
    .option("table", stores_table_name) \
    .load()


df_sales=df_sales.drop('stock','promo_type_1','promo_bin_1','promo_type_2','promo_bin_2','promo_discount_2','promo_discount_type_2')
df_sales=df_sales.orderBy('date').filter('sales != 0.0')

df_join = df_sales.join(df_stores,on='store_id',how='outer').orderBy('date','store_id')

df_join.write \
    .format("bigquery") \
    .option("table", prod_table_name) \
    .option("writeMethod","direct") \
    .mode("overwrite") \
    .save()

df_join_with_year_month = df_join.withColumn("year", year("date")).withColumn("month", month("date"))
unique_year_month = df_join_with_year_month.select("year", "month").distinct().collect()

year_month_data_frames = {}

for row in unique_year_month:
    year = row.year
    month = row.month
    year_month_data_frames[(year, month)] = df_join_with_year_month.filter((df_join_with_year_month.year == year) & (df_join_with_year_month.month == month))
    
for year_month in year_month_data_frames:
    year,month=year_month
    table_name=f"data-eng-399920.prod.revenue-data-{year}-{month}"
    year_month_data_frames[year_month].write\
    .format("bigquery") \
    .option("table",table_name ) \
    .option("writeMethod","direct") \
    .mode("overwrite") \
    .save()    

spark.stop()