from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Save a Dataframe to an existing Cassandra table")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Read in the Parquet file.
# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
df = sqlContext.read.parquet("dsefs:///some_file.parquet")

#save the dataframe to an existing Cassandra table
df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="kv", keyspace="test")\
    .save()