# pyspark-dse-cookbook

A series of PySpark recipes for interacting with the Spark/Cassandra/DSEFS* components of the Datastax Enterprise platform.

Note that there are two clusters with 2 differing purposes: a DSE Analytics cluster (Real-time) and a DSE Analytics Solo cluster (Data Lake).

## Setup Notes:

DSBulk tool: Although not Spark, the ... TODO

#### Sample data: 

See the .CSV files in the sample-data directory of this project TODO

#### Spark GUI:

https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/sparkWebInterface.html

To use the Spark web interface enter the listen IP address of any Spark node in a browser followed by port number 7080 (configured in the spark-env.sh configuration file). Starting in DSE 5.1, all Spark nodes within an Analytics datacenter will redirect to the current Spark Master.

#### Spark Job Server:

https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/sparkJobserverOverview.html

DataStax Enterprise includes a bundled copy of the open-source Spark Jobserver, an optional component for submitting and managing Spark jobs, Spark contexts, and JARs on DSE Analytics clusters. 

#### Spark history server: 

https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/sparkConfiguringfHistoryServer.html

The Spark history server provides a way to load the event logs from Spark jobs that were run with event logging enabled. 

#### Per application memory and CPU allocation:

https://spark.apache.org/docs/2.2.0/configuration.html#dynamically-loading-spark-properties


Resource allocation on dse-submit, 




This library is split into two sections: 

1. PySpark scripts for Cassandra resident real-time data (Cluster 1: DSE Analytics)
2. PySpark scripts for Data Lake resident historic data in .parquet file based format (Cluster 2: DSE Analytics Solo)
3. PySpark scripts for JOINING/UNION of real-time and historic data in both clusters
4. PySpark scripts for ARCHIVING data from real-time cluster -> Data Lake

## PySpark scripts for Cassandra resident real-time data:

Cluster Purpose: real-time analytics component of a big data platform
Components of Datastax Enterprise used: Spark, Cassandra, DSEFS
Data storage: Cassandra tables
Access types: OLTP and OLAP
Spark Execution location: these scripts are executed on the DSE Analytics nodes
Cluster Name: DSE Analytics


First lets load some data into DSEFS and then on into Cassandra tables (using the spark-scassandra-connector):

#### Load .CSV files into DSEFS - see Resources above
#### Load one of the .CSV files in DSEFS into a DataFrame
#### Load one of the .CSV files in DSEFS into a DataFrame and save to Cassandra as a table

Lets run some JOINS taking note of the effect of partition key choice on Spark performance:

#### Load a DataFrame from a Cassandra table using SparkSQL
#### Load two DataFrames from Cassandra tables using SparkSQL and perform a JOIN (poorly chosen partition key -> table scan)
#### Load two DataFrames from Cassandra tables using SparkSQL and perform a JOIN (correctly chosen partition key -> efficient local data aware join -> NO SHUFFLE!)
#### Load two DataFrames one from a Cassandra table and the other one from DSEFS and perform a JOIN

Saving DataFrames to .parquet format:

#### Load a DataFrame from a Cassandra table using SparkSQL
#### Write out a DataFrame to DSEFS as a .parquet file

## PySpark scripts for Data Lake resident historic data (in .parquet format)

Cluster Purpose: big data querying with real-time join capabilities
Components of Datastax Enterprise used: Spark, DSEFS
Data storage: File based, .parquet format
Access types: OLAP only
Spark Execution location: these scripts are executed on the DSE Analytics Solo nodes
Cluster Name: DSE Data Lake

#### Load a DataFrame from parquet data via SparkSQL
#### Load two DataFrames from parquet data via SparkSQL and perform a JOIN


## PySpark scripts for JOINING/UNION of real-time and historic data in both clusters

https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/byosIntro.html

## PySpark scripts for ARCHIVING data from real-time cluster -> Data Lake

#### Read a Cassandra table with timebased key



*DSEFS: Datastax Enterprise File System, an HDFS compatible distributed file system - store up to 20TB per node.

