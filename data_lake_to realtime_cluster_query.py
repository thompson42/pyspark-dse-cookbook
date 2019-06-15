from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

#override the default SparkConf that points to the local cluster and point it at a list of node/s in a remote cluster
conf = SparkConf()
conf.setAppName("Data Lake to Real Time cluster query")
conf.set('spark.cassandra.connection.host', '18.236.92.116,54.218.72.168,34.222.16.245')
conf.set('spark.cassandra.connection.port', '9042')
conf.set('spark.cassandra.output.consistency.level','ONE')

#load a DataFrame form the remote real-time cluster
spark = SparkContext(conf=conf)
sqlContext = SQLContext(spark)
sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="compaction_history", keyspace="system").load().show()

#load a Dataframe from the historic Data Lake

#JOIN the two DataFrames