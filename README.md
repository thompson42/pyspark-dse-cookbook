# pyspark-dse-cookbook

A series of PySpark recipes for interacting with the Spark/Cassandra/DSEFS* components of the [Datastax Enterprise](http://www.datasatx.com) platform, this cookbook was built using Datastax Enterprise 6.7.x and Datastax Studio 6.7.x.

## Setup notes, actions and basic introduction to Spark diagnostics

#### Download and start Datastax Studio

Go to [Datastax Downloads](http://downloads.datastax.com) and download Datastax Studio and start it up, install instructions here: [Datastax Studio](https://docs.datastax.com/en/studio/6.7/index.html)

#### Import the Datastax Studio project

Import the datastax-studio-project/pyspark-dse-cookbook.studio-nb.tar into Datastax Studio

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

#### Cluster 1: (DSE Analytics DC)

| Cluster Purpose | real-time analytics component of a big data platform |
| Components of Datastax Enterprise used | Spark, Cassandra, DSEFS |
| Data storage | Cassandra tables |
| Access types | OLTP and OLAP |
| Spark Execution location | these scripts are executed on the DSE Analytics nodes |
| Cluster Name | DSE Analytics |

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

Note: the spark-cassandra-connector will push down CQL predicates to Cassandra level (or said another way: the connector has the smarts to push down the WHERE clause constraints to Cassandra as opposed to filtering at the Spark level)

### Lets run some JOINS taking note of the effect of partition key choice on Spark performance:

#### Load two DataFrames from Cassandra tables using SparkSQL and perform a JOIN

Datastax Studio project: run the keyspace, table creation and insertion steps (STEP 1, 2) prior to running these recipes.

Note the JOIN is on a correctly chosen partition key -> efficient local data aware JOIN -> NO SHUFFLE!

Deploy pyspark-dse-cookbook/cassandra_sparksql_join.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/cassandra_sparksql_join.py
```

#### Save a Dataframe to an existing Cassandra table

This is easy to do example code is in the following file, do NOT this file directly, modify it with your source DataFrame and target kespace/table

```
save_dataframe_to_cassandra_table.py
```

## Section 2: PySpark scripts for Data Lake resident historic data interaction (.parquet format)

#### Cluster 2: (DSE Analytics Solo DC)

| Cluster Purpose | big data querying with real-time join capabilities |
| Components of Datastax Enterprise used | Spark, DSEFS |
| Data storage | File based, .parquet format |
| Access types | OLAP only |
| Spark Execution location | these scripts are executed on the DSE Analytics Solo nodes |
| Cluster Name | DSE Data Lake |

#### Load CSV files into DSEFS manually at the command line

Note: DSEFS commands are available only in the local logical datacenter.

Deploy sample-data/user_sessions_2.csv and sample-data/user_transactions_2.csv files to the node's local filesystem in the usual directory: /home/your-user/

[Start DSEFS shell](https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/analytics/commandsDsefs.html) :

```
>dse fs 
```

Load the CSV files into the DSEFS distributed filesystem:

```
dsefs dsefs://127.0.0.1:5598/ > put file:/home/your-user/user_sessions_2.csv user_sessions_2.csv
dsefs dsefs://127.0.0.1:5598/ > put file:/home/your-user/user_transactions_2.csv user_transactions_2.csv
```

Check the CSV files are there:

```
dsefs dsefs://127.0.0.1:5598/ > ls
```
Note: that "file:/" in a DSEFS command refers to the local filesystem, DSEFS can operate on both local (file:) and distributed (dsefs:) filesystems, the above commands copy a file from the local file system to the distributed filesystem

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

JOIN data in a historic Data Lake with real-time data in Cassandra.

#### Combined Data Lake an Real Time cluster query

Deploy pyspark-dse-cookbook/data_lake_to_realtime_cluster_query.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/data_lake_to_realtime_cluster_query.py
```

Note: to JOIN two Cassandra tables form tow separate clusters you would need to configure two instances of the spark-cassandra-connector starting at the cluster object.

#### Load two DataFrames one from a Cassandra table and the other one from a DSEFS Parquet file and perform a JOIN

Deploy pyspark-dse-cookbook/cassandra_parquet_join.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/cassandra_parquet_join.py
```

#### Change the schema in the user_sessions table Cassandra by adding a column, then and add some more rows

Go to your Datastax Studio session and run STEP 4 and 5

Then run the query in STEP 6 to see how Cassandra sees the data; Cassandra sees nulls for the field in the old record.

#### Perform the Cassandra and Parquet JOIN again with different schemas (notice Parquet nulls)

Pull the DataFrame with new schema from Cassandra for user_id = 1

Pull the Dataframe with old schema from Parquet/DSEFS for user_1

Run this first iteration:

Deploy pyspark-dse-cookbook/schema_mismatch_dataframe_union.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/schema_mismatch_dataframe_union.py
```

This will fail (correctly) with the following exception:

```
pyspark.sql.utils.AnalysisException: "Union can only be performed on tables with the same number of columns, but the first table has 9 columns and the second table has 8 columns..."
```

Edit the schema_mismatch_dataframe_union.py paying attention to:

COMMENT OUT THE FOLLOWING LINE

UNCOMMENT THE FOLLOWING LINE

Save the file and run the Spark job again -> Success: An OUTER JOIN has performed the schema merging for you where a UNION failed due to column count mismatch.


## Section 4: PySpark design and scripts for OFFLOADING data from real-time cluster -> Data Lake

#### An explanation of TimeWindowCompactionStrategy

TimeWindowCompactionStrategy allows us to store time oriented data in blocks on the file system, in Cassandra those blocks are SortedStringTables (SSTables for short).

Transactional data is typically time oriented so can take advantage of TWCS's ability to reduce query latency (IO latency) by holding slices of time in the same file (SSTable) on disk.

This is an example of TWCS applied to a transactional table, all transactions for an hour will eventually compact into a single SSTable on disk

```
CREATE TABLE my_keyspace.transactions (
    ...
) WITH ...
    AND compaction = {'compaction_window_size': '1', 
                      'compaction_window_unit': 'HOURS', 
                      'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}
```

There is a great explanation of TWCS in this short article: [TWCS part 1 - how does it work and when should you use it](https://thelastpickle.com/blog/2016/12/08/TWCS-part1.html)

#### An explanation of TTLs with Cassandra

TTL stands for "time to live", data in Cassandra by default lives forever, that's what databases are for, but there are use cases where data can and should expire at some point a classic examplae is airline flights.
Once a flight has landed and cleared it's passengers it is no longer mutable, it it now historic data. In a two tiered real-time -> historic data solution the flight needs to be move out of the real-time store and into the historic store.
The usual way to do this is to:

1. Query the R/T store for completed flights
2. Copy those flights over the Historic store
3. Delete the flights from the R/T store

In this use case example we can avoid Step (3) by placing a TTL on the flight's row, the flight will automatically be purged from the R/T store after "x" amount of time from insertion (or reaching a completed status..., or some other parameter...)

TTLs only exist on columns, not rows, however there is a convenience when inserting a row for the first time; if you place a TTL on the INSERT, all columns get the same TTL.
Here is an example:

```
INSERT INTO test (k,v) VALUES ('test', 1) USING TTL 10;
```

There is a good explanation of this behaviour and UPDATE behaviour in this StackOverflow link: [CQL INSERT UPDATE TTL](https://stackoverflow.com/questions/40730510/just-set-the-ttl-on-a-row)


#### CQL query a Cassandra table with a time based partition key

Go to your Datastax Studio session and run STEP 7, 8 and 9

#### Save the table to Parquet format in DSEFS

Were going to do this on the local cluster, but no reason why you can't do this on the Data Lake and saving the R/T cluster data to the Data Lake.

Deploy pyspark-dse-cookbook/create_parquet_file_from_cassandra_table.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/create_parquet_file_from_cassandra_table.py
```
Check the Parquet file transactions.parquet was writtine to DSEFS:

```
dse fs
dsefs dsefs://127.0.0.1:5598/ > ls
```

#### Parquet Append Offloading - Moving data from the real-time cluster to a Data Lake

[Incrementally loaded Parquet files](https://aseigneurin.github.io/2017/03/14/incrementally-loaded-parquet-files.html)

Take special notice in the blog post above the difference between many parquet files in a directory and a single parquet file with multiple partitions.

We want a single parquet file with multiple partitions (we can them query across the entire data set without Spark performing UNIONs with other files):

```
dse fs
dsefs dsefs://127.0.0.1:5598/ > ls
dsefs dsefs://127.0.0.1:5598/ > cd my-parquet-file.parquet
dsefs dsefs://127.0.0.1:5598/ > ls
...  14:53 part-00001-bd5d902d-fac9-4e03-b63e-6a8dfc4060b6.snappy.parquet
...  14:53 part-00000-bd5d902d-fac9-4e03-b63e-6a8dfc4060b6.snappy.parquet

```

Go to your Datastax Studio session and run STEP 10

Deploy pyspark-dse-cookbook/offload_from_cassandra_to_parquet.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/offload_from_cassandra_to_parquet.py
```

Open the Parquet file and notice the number of partitions, we have too many, what we really want is 2, one for each of the dates:

```
dse fs
dsefs dsefs://127.0.0.1:5598/ > ls
dsefs dsefs://127.0.0.1:5598/ > cd transactions.parquet
dsefs dsefs://127.0.0.1:5598/ > ls

```

#### Offloading correctly using Parquet partitions

Deploy pyspark-dse-cookbook/offloading_using_parquet_partitions_1.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/offloading_using_parquet_partitions_1.py
```

Deploy pyspark-dse-cookbook/offloading_using_parquet_partitions_2.py to the node and run it:

```
dse spark-submit \
  --deploy-mode client \
  --executor-memory 1G \
  --total-executor-cores 1 \
  /home/your-user/offloading_using_parquet_partitions_2.py
```

Now check the correctly partitioned parquet file:

```
dse fs
dsefs dsefs://127.0.0.1:5598/ > ls
dsefs dsefs://127.0.0.1:5598/ > cd transactions_partitioned.parquet
dsefs dsefs://127.0.0.1:5598/ > ls

```

Summary: what we managed to do here is place a TTL on data in Cassandra, this data will be deleted at some point in the future. 
The second phase was to move the data to the Data Lake.

#### Parquet Schema Merging (On read) TODO

TODO: Can you parquet append a different schema?

Above we saw merging two disperate schemas into one using an OUTER JOIN against DataFrames from (1) a Cassandra datasourse and a (2)Parquet datasource. 

If we are dealing with only Parquet files we have another schema merge mechanism:

[Parquet Schema Merging](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#schema-merging)

## Optimisation techniques

[Predicate Pushdown](https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/spark/sparkPredicatePushdown.html)

## DSEFS useful commands:

Copy a file to the local filesystem from DSEFS (DSE 6.7.0+), you may need to make the user_sessions_2.parquet directory in the local filesystem?:

```
>dse fs
dsefs dsefs://127.0.0.1:5598/ > cd file:
dsefs file:/home/your-user/ > cp -r dsefs:/user_sessions_2.parquet /home/your-user/user_sessions_2.parquet

```

Copy a file to the local filesystem from DSEFS (DSE prior 6.7.0):

```
dse hadoop fs -copyToLocal dsefs:/user_sessions_2.parquet /home/your-user/
```

*DSEFS: Datastax Enterprise File System, an HDFS compatible distributed file system - store up to 20TB per node.



