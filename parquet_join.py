from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Load DataFrame from Parquet via a simple read()")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#load DataFrames
user_sessions_2_df = sqlContext.read.parquet("dsefs:///user_sessions_2.parquet")
user_transactions_2_df = sqlContext.read.parquet("dsefs:///user_transactions_2.parquet")

#JOIN the dataframes
final_df = user_transactions_2_df.join(user_sessions_2_df, ['user_id','session_id'], how='full')

#output to DSEFS as JSON
final_df.write.json("dsefs:///parquet_join.json")

