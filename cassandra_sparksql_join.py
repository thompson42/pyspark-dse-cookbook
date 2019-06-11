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
user_transactions_df = sqlContext.sql("SELECT * FROM user_transactions")

#register the user_sessions cassandra table
user_sessions_ddl = """CREATE TEMPORARY VIEW user_sessions
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "user_sessions",
     keyspace "pyspark_dse_cookbook",
     cluster "Cluster 1",
     pushdown "true")"""

#load the DF
sqlContext.sql(user_sessions_ddl) # Creates Catalog Entry registering an existing Cassandra Table
user_sessions_df = sqlContext.sql("SELECT * FROM user_sessions")

#JOIN the dataframes
final_df = user_transactions_df.join(user_sessions_df, ['user_id','session_id'], how='full')
final_df.show()