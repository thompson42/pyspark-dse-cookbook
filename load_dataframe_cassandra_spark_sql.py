from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Load DataFrame from Cassandra via SparkSQL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#register the cassandra table
createDDL = """CREATE TEMPORARY VIEW compaction_history
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "compaction_history",
     keyspace "system",
     cluster "Cluster 1",
     pushdown "true")"""

sqlContext.sql(createDDL) # Creates Catalog Entry registering an existing Cassandra Table
sqlContext.sql("SELECT * FROM compaction_history").show()