from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Load DataFrame from Parquet via SparkSQL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read in the Parquet file.
#parquet files are self-describing so the schema is preserved.
#the result of loading a parquet file is also a DataFrame.
user_sessions_2 = sqlContext.read.parquet("dsefs:///user_sessions_2.parquet")

#parquet files can also be used to create a temporary view and then used in SQL statements.
user_sessions_2.createOrReplaceTempView("user_sessions_2")
df = sqlContext.sql("SELECT * FROM user_sessions_2 WHERE user_id = 1")
df.show()