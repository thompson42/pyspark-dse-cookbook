# pyspark-dse-cookbook

A series of PySpark recipes for interacting with the Spark/Cassandra/DSEFS* components of the [Datastax Enterprise](http://www.datasatx.com) platform.

## Setup notes, actions and basic introduction to Spark diagnostics

#### Load the Spark Master UI in your browser

[Spark UI documentation](https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/sparkWebInterface.html)

To use the Spark web interface enter the listen IP address of any Spark node in a browser followed by port number 7080 (configured in the spark-env.sh configuration file). Starting in DSE 5.1, all Spark nodes within an Analytics datacenter will redirect to the current Spark Master.

#### SSH into one of the DSE Analytics (Spark) nodes

```
>ssh <options> <user>@<ip-address>
```

#### Deploy python test job

Deploy pyspark-dse-cookbook/pi.py to /home/your-user/pi.py to the node you just SSH'd into

#### Run the sample pi.py script with 1GB/1xCore directly on the SSH node

[For command line options](https://spark.apache.org/docs/2.2.0/configuration.html#dynamically-loading-spark-properties)

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/pi.py \
  1000
```

Note the job running in the Spark Master UI.
The job should run for about 2 minutes.

#### Run the sample pi.py script with 2GB/2xCore directly on the SSH node

[For command line options](https://spark.apache.org/docs/2.2.0/configuration.html#dynamically-loading-spark-properties)

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 2G \
  --total-executor-cores 2 \
  /home/your-user/pi.py \
  1000
```

Note that due to parallelism (the job spreading in parallel across 2x cores) the job now only takes about 1 minute to run.

#### Introduction to the Spark Application UI

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/pi.py \
  1000
```

1. Note the job running in the Spark Master UI
2. Click thru to the Application UI and take note of job details
3. Go back to the Spark Master UI and wait till the job finishes
4. Note that you can no longer view the Application UI

The application logs are quite verbose and as such are destroyed at the end of a successful run, to keep them for debugging, analysis and performance tuning you will need to activate the Spark history server.

#### Activate the Spark history server

[Spark history server documentation](https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/sparkConfiguringfHistoryServer.html)

The Spark history server provides a way to load the event logs from Spark jobs that were run with event logging enabled.

Due to the verbosity of files generated at the Application level pay attention to the log rolling/cleanup configuration at the bottom of documentation link above.

# Cookbook 

These scripts are split into four (4) sections: 

1. PySpark scripts for Cassandra resident real-time data (executed against Cluster1: DSE Analytics DC)
2. PySpark scripts for Data Lake resident historic data in .parquet file based format (executed against Cluster 2: DSE Analytics Solo DC)
3. PySpark scripts for JOINING/UNION of real-time and historic data in both clusters (executed against Cluster 2: DSE Analytics Solo DC but will also pull data from Cluster1: DSE Analytics DC)
4. PySpark scripts for ARCHIVING data from real-time cluster -> Data Lake (executed against Cluster 2: DSE Analytics Solo DC but will also pull data from Cluster1: DSE Analytics DC)

DC = datacenter (A Cassandra logical datacenter)

#### Cluster Topology:

1. Cluster 1: (DSE Analytics DC)
2. Cluster 2: (DSE Analytics Solo DC)

## Section 1: PySpark scripts for Cassandra resident real-time data

Cluster Purpose: real-time analytics component of a big data platform
Components of Datastax Enterprise used: Spark, Cassandra, DSEFS
Data storage: Cassandra tables
Access types: OLTP and OLAP
Spark Execution location: these scripts are executed on the DSE Analytics nodes
Cluster Name: DSE Analytics

#### Test Cassandra Access

Deploy pyspark-dse-cookbook/test_cassandra_access.py to /home/your-user/test_cassandra_access.py to the node you just SSH'd into and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/test_cassandra_access.py
```

You should see a table like this:

![](/images/table_screen_shot.png)

If you fail to see the table you have a connectivity issue, check the Spark logs for problems.

#### Load a ENTIRE Cassandra table into a DataFrame using simple read() method

Deploy pyspark-dse-cookbook/load_dataframe_simple_read.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/load_dataframe_simple_read.py
```

#### Load a PARTIAL Cassandra table into a DataFrame using SparkSQL method

Deploy pyspark-dse-cookbook/load_dataframe_spark_sql.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/load_dataframe_spark_sql.py
```

Note: the spark-cassandra-connector will push down CQL predicates to Cassandra level (or another way: the connector has the smarts to push down the WHERE clause constraints to Cassandra as opposed to filtering at the Spark level)

#### Load .CSV files into DSEFS manually at the command line

Note: DSEFS commands are available only in the local logical datacenter.

```
>dsefs dsefs://127.0.0.1:5598/ > put file:/bluefile greenfile
```


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

## Section 2: PySpark scripts for Data Lake resident historic data (in .parquet format)

Cluster Purpose: big data querying with real-time join capabilities
Components of Datastax Enterprise used: Spark, DSEFS
Data storage: File based, .parquet format
Access types: OLAP only
Spark Execution location: these scripts are executed on the DSE Analytics Solo nodes
Cluster Name: DSE Data Lake

#### Load a DataFrame from parquet data via SparkSQL
#### Load two DataFrames from parquet data via SparkSQL and perform a JOIN


## Section 3: PySpark scripts for JOINING/UNION of real-time and historic data in both clusters

https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/byosIntro.html

## Section 4: PySpark scripts for ARCHIVING data from real-time cluster -> Data Lake

#### Read a Cassandra table with timebased key

## A note on automation and other available tools

This training demo module is intended to involve the user in manual aspects (getting hands dirty) of job scheduling, deployment etc. 

There are better ways to perform some of the actions mentioned above in a production environment; see the following tools and processes.

#### Spark Job Server

[Spark job server documentation](https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/sparkJobserverOverview.html)

DataStax Enterprise includes a bundled copy of the open-source Spark Jobserver, an optional component for submitting and managing Spark jobs, Spark contexts, and JARs on DSE Analytics clusters. 

#### DSBulk tool

Although not Spark, the ... TODO



*DSEFS: Datastax Enterprise File System, an HDFS compatible distributed file system - store up to 20TB per node.

