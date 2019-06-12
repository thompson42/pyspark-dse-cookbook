from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Load a CSV/DSEFS into a DataFrame and save it back into DSEFS as a Parquet file")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df_load = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("dsefs:///user_sessions_2.csv")
df_load.write.parquet("dsefs:///user_sessions_2.parquet")