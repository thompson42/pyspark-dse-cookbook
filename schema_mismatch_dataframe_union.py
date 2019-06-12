from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Cassandra and Parquet UNION with different schemas")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#register the user_transactions cassandra table
user_sessions_ddl = """CREATE TEMPORARY VIEW user_sessions
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "user_sessions",
     keyspace "pyspark_dse_cookbook",
     cluster "Cluster 1",
     pushdown "true")"""

#load the DF
sqlContext.sql(user_sessions_ddl) # Creates Catalog Entry registering an existing Cassandra Table
user_sessions_df = sqlContext.sql("SELECT * FROM user_sessions WHERE user_id = 1 LIMIT 5")

# Read in the Parquet file.
# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
user_sessions_2 = sqlContext.read.parquet("dsefs:///user_sessions_2.parquet")

# Parquet files can also be used to create a temporary view and then used in SQL statements.
user_sessions_2.createOrReplaceTempView("user_sessions_2")
user_sessions_2_df = sqlContext.sql("SELECT * FROM user_sessions_2 WHERE user_id = 1")

#following UNION will fail due to difference in number of columns
#COMMENT OUT THE FOLLOWING LINE
final_df = user_sessions_df.union(user_sessions_2_df)

#OUTER JOIN the dataframes
#UNCOMMENT THE FOLLOWING LINE
#final_df = user_sessions_df.join(user_sessions_2_df, user_sessions_2_df.columns if (len(user_sessions_2_df.columns) < len(user_sessions_df.columns)) else user_sessions_df.columns, "outer")

final_df.show()