from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Cassandra SparkSQL JOIN")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#register the user_transactions cassandra table
user_transactions_ddl = """CREATE TEMPORARY VIEW user_transactions
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "user_transactions",
     keyspace "pyspark_dse_cookbook",
     cluster "Cluster 1",
     pushdown "true")"""

#load the DF
sqlContext.sql(user_transactions_ddl) # Creates Catalog Entry registering an existing Cassandra Table
user_transactions_df = sqlContext.sql("SELECT * FROM user_transactions WHERE user_id = 1")

# Read in the Parquet file.
# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
user_sessions = sqlContext.read.parquet("dsefs:///user_sessions_2.parquet")

# Parquet files can also be used to create a temporary view and then used in SQL statements.
user_sessions.createOrReplaceTempView("user_sessions")
user_sessions_df = sqlContext.sql("SELECT * FROM user_sessions WHERE user_id = 1")

#JOIN the dataframes
final_df = user_transactions_df.join(user_sessions_df, ['user_id','session_id'], how='full')
final_df.show()