from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Offloading using parquet partitions 1")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#register the cassandra table
transactions_ddl = """CREATE TEMPORARY VIEW transactions
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "transactions",
     keyspace "pyspark_dse_cookbook",
     cluster "Cluster 1",
     pushdown "true")"""

# Creates Catalog Entry registering an existing Cassandra Table
sqlContext.sql(transactions_ddl)

#load the DF with a SparkSQL statement, predicate pushdown will occur here as this is a valid CQL query
transactions_df = sqlContext.sql("SELECT * FROM transactions WHERE transaction_day = '2005-01-01'")

#save the DF to parquet/DSEFS
transactions_df.coalesce(1).write.parquet("dsefs:///transactions_paritioned.parquet")