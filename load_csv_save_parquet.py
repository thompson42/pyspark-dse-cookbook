from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Convert CSV/DSEFS files into Parquet/DSEFS files")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#load both CSV/DSEFS files into DataFrames
df_load = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("dsefs:///user_sessions_2.csv")
df_load_2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("dsefs:///user_transactions_2.csv")

#write out parquet files to DSEFS
df_load.write.parquet("dsefs:///user_sessions_2.parquet")
df_load_2.write.parquet("dsefs:///user_transactions_2.parquet")