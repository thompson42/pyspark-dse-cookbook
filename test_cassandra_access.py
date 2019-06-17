from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Test Cassandra Access")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#load the DF
sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="compaction_history", keyspace="system").load().show()