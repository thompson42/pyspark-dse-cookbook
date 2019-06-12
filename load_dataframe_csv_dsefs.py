from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Load a CSV/DSEFS file into a DataFrame")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("dsefs:///user_sessions_2.csv")
df.show()