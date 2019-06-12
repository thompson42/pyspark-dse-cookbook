# pyspark-dse-cookbook

A series of PySpark recipes for interacting with the Spark/Cassandra/DSEFS* components of the [Datastax Enterprise](http://www.datasatx.com) platform, this cookbook was built using Datastax Enterprise 6.7.x and Datastax Studio 6.7.x.

## Setup notes, actions and basic introduction to Spark diagnostics

#### Download and start Datastax Studio

Go to [Datastax Downloads](http://downloads.datastax.com) and download Datastax Studio and start it up, install instructions here: [Datastax Studio](https://docs.datastax.com/en/studio/6.7/index.html)

#### Import the Datastax Stuio project

Import the datastax-studio-project/pyspark-dse-project.TODO into Studio 

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

1. While the job is running check it's progress in the Spark Master UI
2. Click thru to the Application UI and take note of job details
3. Go back to the Spark Master UI and wait till the job finishes
4. Note that you can no longer view the Application UI

The application logs are quite verbose and as such are destroyed at the end of a successful run, to keep them for debugging, analysis and performance tuning you will need to activate the Spark history server.

#### Activate the Spark history server

[Spark history server documentation](https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/sparkConfiguringfHistoryServer.html)

The Spark history server provides a way to load the event logs from Spark jobs that were run with event logging enabled.

Due to the verbosity of files generated at the Application level pay attention to the log rolling/cleanup configuration at the bottom of documentation link above.

## Cookbook contents

These scripts are split into four (4) sections: 

1. PySpark scripts for Cassandra resident real-time data interaction (executed against Cluster1: DSE Analytics DC)
2. PySpark scripts for Data Lake resident historic data interaction (executed against Cluster 2: DSE Analytics Solo DC)
3. PySpark scripts for JOINING/UNION of real-time and historic data in both Cassandra and Data Lake (executed against Cluster 2: DSE Analytics Solo DC but will also pull data from Cluster1: DSE Analytics DC)
4. PySpark scripts for ARCHIVING data from real-time cluster -> Data Lake (executed against Cluster 2: DSE Analytics Solo DC but will also pull data from Cluster1: DSE Analytics DC)

DC = datacenter (A Cassandra logical datacenter)

#### Cluster Topology:

1. Cluster 1: (DSE Analytics DC)
2. Cluster 2: (DSE Analytics Solo DC)

## Section 1: PySpark scripts for Cassandra resident real-time data interaction

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

#### Load a ENTIRE Cassandra table into a DataFrame using simple read()

Deploy pyspark-dse-cookbook/load_dataframe_cassandra_simple_read.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/load_dataframe_cassandra_simple_read.py
```

#### Load a PARTIAL Cassandra table into a DataFrame using SparkSQL

Deploy pyspark-dse-cookbook/load_dataframe_cassandra_spark_sql.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/load_dataframe_cassandra_spark_sql.py
```

Note: the spark-cassandra-connector will push down CQL predicates to Cassandra level (or another way: the connector has the smarts to push down the WHERE clause constraints to Cassandra as opposed to filtering at the Spark level)

### Lets run some JOINS taking note of the effect of partition key choice on Spark performance:

#### Load two DataFrames from Cassandra tables using SparkSQL and perform a JOIN

Datastax Studio project: run the keyspace, table creation and insetion steps (STEP 1, 2) prior to running these recipes.

Note the JOIN is on a correctly chosen partition key -> efficient local data aware JOIN -> NO SHUFFLE!

Deploy pyspark-dse-cookbook/cassandra_sparksql_join.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/cassandra_sparksql_join.py
```

#### Save a Dataframe to a new Cassandra table - TODO

## Section 2: PySpark scripts for Data Lake resident historic data interaction (.parquet format)

Cluster Purpose: big data querying with real-time join capabilities
Components of Datastax Enterprise used: Spark, DSEFS
Data storage: File based, .parquet format
Access types: OLAP only
Spark Execution location: these scripts are executed on the DSE Analytics Solo nodes
Cluster Name: DSE Data Lake

#### Load CSV files into DSEFS manually at the command line

Note: DSEFS commands are available only in the local logical datacenter.

Deploy sample-data/user_sessions_2.csv and sample-data/user_transactions_2.csv files to the node's local filesystem in the usual directory: /home/your-user/

[Start DSEFS shell](https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/analytics/commandsDsefs.html) :

```
>dse fs 
```

Load the CSV files into the DSEFS distributed:

```
dsefs dsefs://127.0.0.1:5598/ > put file:/home/your-user/user_sessions_2.csv user_sessions_2.csv
dsefs dsefs://127.0.0.1:5598/ > put file:/home/your-user/user_transactions_2.csv user_transactions_2.csv
```

Check the files are there:

```
dsefs dsefs://127.0.0.1:5598/ > ls
```
Note: that "file:/" in a DSEFS command refers to the local filesystem, DSEFS can operate on both local and distributed filesystems, the above commands copy a file from the local file system to the distributed filesystem

#### Load a CSV/DSEFS file into a DataFrame

Deploy pyspark-dse-cookbook/load_dataframe_csv_dsefs.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/load_dataframe_csv_dsefs.py
```

#### Convert CSV/DSEFS files into Parquet/DSEFS files

Deploy pyspark-dse-cookbook/load_csv_save_parquet.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/load_csv_save_parquet.py
```

Check the Parquet file: user_seesions_2.parquet was created in DSEFS:

```
>dse fs 
dsefs dsefs://127.0.0.1:5598/ > ls
```

#### Load an ENTIRE Parquet/DSEFS file into a DataFrame using simple read()

Deploy pyspark-dse-cookbook/load_dataframe_parquet_simple_read.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/load_dataframe_parquet_simple_read.py
```


#### Load a PARTIAL Parquet/DSEFS file into a DataFrame via SparkSQL

Deploy pyspark-dse-cookbook/load_dataframe_parquet_spark_sql.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/load_dataframe_parquet_spark_sql.py
```

#### Load two DataFrames from two Parquet/DSEFS files via SparkSQL, perform a JOIN, output the results as a JSON report to DSEFS

Deploy pyspark-dse-cookbook/parquet_join.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/parquet_join.py
```

Check the JSON report file: parquet_join.json was created in DSEFS:

```
>dse fs 
dsefs dsefs://127.0.0.1:5598/ > ls
```

Pull the JSON file down from DSEFS to the local filesystem on the node and open it:

```
dsefs dsefs://127.0.0.1:5598/ > cp dsefs:parquet_join.json file:/home/your-user/parquet_join.json
```

## Section 3: PySpark scripts for JOINING/UNION of real-time and historic data in both Cassandra and Data Lake

https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/spark/byosIntro.html

#### Select a subset of Cassandra data in one DSE Cluster from a different Cluster via SparkSQL

TODO - need multiple environments

#### Load two DataFrames one from a Cassandra table and the other one from a DSEFS Parquet file and perform a JOIN

Deploy pyspark-dse-cookbook/cassandra_parquet_join.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/cassandra_parquet_join.py
```

#### Change the schema in Cassandra and add some more rows

Go to your Datastax Studio session and run...TODO

#### Perform the JOIN again (notice Parquet nulls)

## Section 4: PySpark scripts for ARCHIVING data from real-time cluster -> Data Lake

#### Read a Cassandra table with timebased key into Data frame

#### Parquet Append


*DSEFS: Datastax Enterprise File System, an HDFS compatible distributed file system - store up to 20TB per node.



