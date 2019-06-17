from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Load a CSV/DSEFS file into a DataFrame")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#load a CSV file form DSEFS into a DataFrame
df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("dsefs:///user_sessions_2.csv")
df.show()