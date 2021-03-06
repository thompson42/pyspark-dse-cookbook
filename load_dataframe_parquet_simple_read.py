from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Load DataFrame from Parquet via a simple read()")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#load a parquet file in DSEFS into a DataFrame
df = sqlContext.read.parquet("dsefs:///user_sessions_2.parquet")
df.show()