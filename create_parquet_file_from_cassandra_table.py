from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Create Parquet file from Cassandra table")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#register the transactions cassandra table
transactions_ddl = """CREATE TEMPORARY VIEW transactions
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "transactions",
     keyspace "pyspark_dse_cookbook",
     cluster "Cluster 1",
     pushdown "true")"""

#load the DF
sqlContext.sql(transactions_ddl) # Creates Catalog Entry registering an existing Cassandra Table
transactions_df = sqlContext.sql("SELECT * FROM transactions WHERE transaction_day = '2005-01-01'")

#save the DF to parquet/DSEFS
transactions_df.write.parquet("dsefs:///transactions.parquet")