from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Load DataFrame from Cassandra via a simple read()")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#load and show DataFrame
sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="compaction_history", keyspace="system").load().show()