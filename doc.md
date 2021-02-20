## RDD (Resilient Distributed Dataset) 
Is an immutable distributed collections of objects.


##### Features of an RDD in Spark:
***
**Resilience** : If a node in one cluster happens to crap out in the middle of a computation, the RDD will be automatically recovered from the other nodes still in operation. It is also called fault tolerance. 

**Lazy evaluation**: Data does not get loaded in an RDD even if you define it. Transformations are actually computed when you call an action, such as count or collect, or save the output to a file system. 

**Immutability**: Data stored in an RDD is in the read-only mode━you cannot edit the data which is present in the RDD. But, you can create new RDDs by performing transformations on the existing RDDs.  

**In-memory computation**: Loads the data from disk and process in memory and keeps the data in memory (RAM) than on the disk so that it provides faster access.

**Partitioning**: Data present in an RDD resides on multiple nodes. A single RDD is divided into multiple logical partitions, which can be computed on different nodes of the cluster.

![RDD features](./Reference%20Images/RDD%20features.png)

##### When to use RDDs?
- You want low-level transformation and actions and control on your dataset.
- Your data is unstructured, such as media streams or streams of text.
- You want to manipulate your data with functional programming constructs than domain specific expressions.

#### Creating RDD
***
RDD’s are created primarily in two different ways,
- Parallelizing an existing collection 
- Referencing a dataset in an external storage system (HDFS, S3 and many more). 

In order to create an RDD, first, you need to create a SparkSession which is an entry point to the PySpark application. SparkSession can be created using a `builder() or newSession()` methods of the SparkSession.

Spark session internally creates a sparkContext variable of SparkContext. You can create multiple SparkSession objects but only one SparkContext per JVM. In case if you want to create another new SparkContext you should stop existing Sparkcontext (using stop()) before creating a new one.
```
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate() 
```
> `master()` – If you are running it on the cluster you need to use your master name as an argument to master(). usually, it would be either yarn (Yet Another Resource Negotiator) or mesos depends on your cluster setup.
In realtime application, you will pass master from spark-submit instead of hardcoding on Spark application.

> Use `local[x]` when running in Standalone mode. **'x'** should be an integer value and should be greater than **'0'**; this represents how many partitions it should create when using RDD, DataFrame, and Dataset. Ideally, **'x'** value should be the number of CPU cores you have.

> `appName()` – Used to set your application name.

> `getOrCreate()` – This returns a SparkSession object if already exists, creates new one if not exists.

***Note:*** Creating SparkSession object, it internally creates one SparkContext per JVM.

#### Create RDD using sparkContext.parallelize()
PySpark `parallelize()` is a function in SparkContext and is used to create an RDD from a list collection 

Example : [Parallelize.ipynb](Notebooks/pyspark-parallelize.ipynb)  

> For production applications, we mostly create RDD by using external storage systems like HDFS, S3, HBase e.t.c. To make it simple for this PySpark RDD learning we are using files from the local system or loading it from the python list to create RDD.

#### Create RDD from External Storage Systems
Using `textFile()` method we can read a text (.txt) file into RDD.
```
-- Create RDD from external Data source
rdd = spark.sparkContext.textFile("/path/textFile.txt")
```
`wholeTextFiles()` function returns a PairRDD with the key being the file path and value being file content.
```
-- Reads entire file into a RDD as single record.
rdd = spark.sparkContext.wholeTextFiles("/path/textFile.txt")
```
When we use parallelize() or textFile() or wholeTextFiles() methods of SparkContext to initiate RDD, it automatically splits the data into partitions based on resource availability. when you run it on a laptop it would create partitions as the same number of cores available on your system.

Besides using text files, we can also create RDD from CSV file, JSON, and more formats.

To learn more : 
[Reading text file into RDD |DataFrame](https://sparkbyexamples.com/spark/spark-read-text-file-rdd-dataframe/)

[Reading CSV](https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe/)

### Repartition and Coalesce
***
Some times we may need to repartition the RDD, PySpark provides two ways to repartition;
- `repartition()` method which shuffles data from all nodes also called **full shuffle**
- `coalesce()` method which shuffle data from minimum nodes

One important point to note is, PySpark **repartition() and coalesce()** are very expensive operations as they shuffle the data across many partitions hence try to minimize using these as much as possible.

#### 1. How PySpark Partitions data files
One main advantage of the PySpark is, it splits data into multiple partitions and executes operations on all partitions of data in parallel which allows us to complete the job faster. While working with partition data we often need to increase or decrease the partitions based on data distribution. Methods repartition and coalesce helps us to repartition.

When not specified programmatically or through configuration, PySpark by default partitions data based on a number of factors, and the factors differ were you running your job on and what mode.
##### 1.1 Local mode
When you running on local in standalone mode, PySpark partitions data into the number of CPU cores you have on your system or the value you specify at the time of creating SparkSession object
```
spark = SparkSession.builder.appName('PySparkLearning') \
        .master("local[5]").getOrCreate()
```
The above example provides local[5] as an argument to master() method meaning to run the job locally with 5 partitions. Though if you have just 2 cores on your system, it still creates 5 partition tasks.
```
df = spark.range(0,20)
print(df.rdd.getNumPartitions())
```
Above example yields output as 5 partitions.

##### 1.2 HDFS cluster mode
When you running PySpark jobs on the Hadoop cluster the default number of partitions is based on the following.

- On the HDFS cluster, by default, PySpark creates one Partition for each block of the file.              
- In Version 1 Hadoop the HDFS block size is 64 MB and in Version 2 Hadoop the HDFS block size is 128 MB          
- Total number of cores on all executor nodes in a cluster or 2, whichever is larger              
- For example if you have 640 MB file and running it on Hadoop version 2, creates 5 partitions with each consists on 128 MB blocks (5 blocks * 128 MB = 640 MB). If you repartition to 10 then it creates 2 partitions for each block.            

##### 1.3 PySpark configuration
`spark.default.parallelism` configuration default value set to the number of all cores on all nodes in a cluster, on local it is set to number of cores on your system.       

`spark.sql.shuffle.partitions` configuration default value is set to 200 and be used when you call shuffle operations like reduceByKey()  , groupByKey(), join() and many more. This property is available only in DataFrame API but not in RDD.                

You can change the values of these properties through programmatically using the below statement
```spark.conf.set("spark.sql.shuffle.partitions", "500")```

You can also set the partition value of these configurations using Pyspark-submit command.
```./bin/spark-submit --conf spark.sql.shuffle.partitions=500 --conf spark.default.parallelism=500```

### RDD Partition and repartition
##### RDD repartition()
Spark RDD `repartition()` method is used to increase or decrease the partitions.
This operation reshuffles the RDD randomly.
##### RDD coalesce()
Spark RDD `coalesce()` is used only to reduce the number of partitions. This is optimized or improved version of repartition() where betterment is achieved by reshuffling the data from fewer nodes compared with all nodes by repartition.

Ex : [Repartition-Coalesce.ipynb](Notebooks/pyspark-repartition-coalesce.ipynb)

### RDD Operations
***
On PySpark RDD, you can perform two kinds of operations.
- RDD transformations 
- RDD Actions

##### RDD Transformations 
###### RDD Transformation Types
There are two types are transformations.
- Narrow Transformation
- Wider Transformation

**Narrow Transformation**       
Narrow transformations compute data that live on a single partition meaning there will not be any data movement between partitions to execute narrow transformations.

![Narrow Transformation](./Reference%20Images/narrow-transformation.png)

Functions such as `map(), mapPartition(), flatMap(), filter(), union()` are some examples of narrow transformation      

**Wider Transformation**        
Wider transformations compute data that live on many partitions meaning there will be data movements between partitions to execute wider transformations. Since these shuffles the data, they also called `shuffle transformations`.

![Wider Transformation](./Reference%20Images/wider-transformation.png)

Functions such as `groupByKey(), aggregateByKey(), aggregate(), join(), repartition()` are some examples of a wider transformations.

**RDD Transformation functions**   
Transformations on Spark RDD returns another RDD and transformations are lazy meaning they don’t execute until you call an action on RDD.

Some transformations on RDD’s are

- flatMap
- map
- reduceByKey
- sortByKey
- filter

Ex :        [Transformations functions.ipynb](Notebooks/pyspark-rdd-transformations.ipynb)

Some more functions are :   
`mapPartitions(), mapPartitionsWithIndex(), randomSplit(), union(), intersection(), distinct(), repartition(), coalesce()` etc

**RDD Actions**         
RDD Action operation returns the values from an RDD to a driver node. In other words, any RDD function that returns non RDD[T] is considered as an action.

Some actions on RDD’s are :
- count()
- collect()
- first()
- max()
- reduce()

Some more actions are : `aggregate(), countByValue(), foreach(), min()`

Example : [RDD Actions.ipynb](Notebooks/pyspark-rdd-actions.ipynb)              

For more Info : [RDD Actions](https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-actions/)

## PySpark RDD Persistence 
***
PySpark `cache() and persist()` are optimization techniques to improve the performance of the RDD jobs that are iterative and interactive

Though PySpark provides computation 100 x times faster than traditional Map Reduce jobs, If you have not designed the jobs to reuse the repeating computations you will see degrade in performance when you are dealing with billions or trillions of data. Hence, we need to look at the computations and use optimization techniques as one of the ways to improve performance.

Using cache() and persist() methods, PySpark provides an optimization mechanism to store the intermediate computation of an RDD so they can be reused in subsequent actions.

When you persist or cache an RDD, each worker node stores it’s partitioned data in memory or disk and reuses them in other actions on that RDD. And Spark’s persisted data on nodes are fault-tolerant meaning if any partition is lost, it will automatically be recomputed using the original transformations that created it.

**Advantages of Persisting RDD**

- *Cost efficient* – PySpark computations are very expensive hence reusing the computations are used to save cost.
- *Time efficient* – Reusing the repeated computations saves lots of time.
- *Execution time* – Saves execution time of the job which allows us to perform more jobs on the same cluster.

**RDD Cache** 

PySpark RDD cache() method by default saves RDD computation to storage level `MEMORY_ONLY` meaning it will store the data in the JVM heap as unserialized objects.

PySpark cache() method in RDD class internally calls `persist()` method which in turn uses sparkSession.sharedState.cacheManager.cacheQuery to cache the result set of RDD. Let’s look at an example.
```
cachedRdd = rdd.cache()
```
**RDD Persist**  

PySpark `persist()` method is used to store the RDD to one of the storage levels `MEMORY_ONLY,MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY, MEMORY_ONLY_2,MEMORY_AND_DISK_2` and more.

PySpark persist has two signature first signature doesn’t take any argument which by default saves it to MEMORY_ONLY storage level and the second signature which takes StorageLevel as an argument to store it to different storage levels.
```
dfPersist = rdd.persist()
dfPersist = rdd.persist(pyspark.StorageLevel.MEMORY_ONLY)
```
> Note that RDD.cache() is an alias for persist(StorageLevel.MEMORY_ONLY) and it will store the data in the JVM heap as unserialized objects. When you write data to a disk, that data is always serialized. 

**RDD Unpersist**       

PySpark automatically monitors every persist() and cache() calls you make and it checks usage on each node and drops persisted data if not used or by using least-recently-used (LRU) algorithm. You can also manually remove using unpersist() method. unpersist() marks the RDD as non-persistent, and remove all blocks for it from memory and disk.
```
  rdd_unpersist = rdd.unpersist()
```

**Persistence Storage Levels**

All different storage level PySpark supports are available at org.apache.spark.storage.StorageLevel class. Storage Level defines how and where to store the RDD.

MEMORY_ONLY – This is the default behavior of the RDD cache() method and stores the RDD as deserialized objects to JVM memory. When there is no enough memory available it will not save to RDD of some partitions and these will be re-computed as and when required. This takes more storage but runs faster as it takes few CPU cycles to read from memory.

MEMORY_ONLY_SER – This is the same as MEMORY_ONLY but the difference being it stores RDD as serialized objects to JVM memory. It takes lesser memory (space-efficient) then MEMORY_ONLY as it saves objects as serialized and takes an additional few more CPU cycles in order to deserialize.

MEMORY_ONLY_2 – Same as MEMORY_ONLY storage level but replicate each partition to two cluster nodes.

MEMORY_ONLY_SER_2 – Same as MEMORY_ONLY_SER storage level but replicate each partition to two cluster nodes.

MEMORY_AND_DISK – In this Storage Level, The RDD will be stored in JVM memory as a deserialized objects. When required storage is greater than available memory, it stores some of the excess partitions in to disk and reads the data from disk when it required. It is slower as there is I/O involved.

MEMORY_AND_DISK_SER – This is same as MEMORY_AND_DISK storage level difference being it serializes the RDD objects in memory and on disk when space not available.

MEMORY_AND_DISK_2 – Same as MEMORY_AND_DISK storage level but replicate each partition to two cluster nodes.

MEMORY_AND_DISK_SER_2 – Same as MEMORY_AND_DISK_SER storage level but replicate each partition to two cluster nodes.

DISK_ONLY – In this storage level, RDD is stored only on disk and the CPU computation time is high as I/O involved.

DISK_ONLY_2 – Same as DISK_ONLY storage level but replicate each partition to two cluster nodes.

To get more understanding on "Why do we need to call cache or persist on a RDD"

[Visit StackOverFlow](https://stackoverflow.com/questions/28981359/why-do-we-need-to-call-cache-or-persist-on-a-rdd)

## PySpark Shared Variables
***
For parallel processing, Apache Spark uses shared variables. When the driver sends a task to the executor on the cluster, a copy of shared variable goes on each node of the cluster, so we can use it for performing tasks.
Shared variables supported by Apache Spark in PySpark are two types of −
- Broadcast
- Accumulator

**Broadcast variables**         
Broadcast variables are read-only shared variables and used to save the copy of data across all nodes. This variable is cached on all the machines and not sent on machines with tasks. 

**When to use :**               
Many times, we will need something like a lookup table or parameters to base our calculations. Those parameters will be static and won't change during the calculation, they will be read-only params.

Broadcast variables are used when static(read-only) variables need to be shared across executers.

**Why to use :**                       
Without broadcast variables, these variables would be shipped to each executor for every transformation and action; this can cause network overhead. However, with broadcast variables, they are shipped once to all executors and are cached for future reference.

Example : [Broadcast.ipynb](Notebooks/pyspark-rdd-broadcast.ipynb)

**Accumulators**                        
A shared variable that can be accumulated, i.e., has a commutative and associative “add” operation. Worker tasks on a Spark cluster can add values to an Accumulator with the += operator, but only the driver program is allowed to access its value, using value. Updates from the workers get propagated automatically to the driver program.

For example, you can use an accumulator for a sum operation or counters (in MapReduce). 

Ex : [Accumulator.ipynb](Notebooks/pyspark-accumulator.ipynb)

For more : [Accumulators](https://sparkbyexamples.com/spark/spark-accumulators/) 

# DataFrames
***

Like an RDD, a DataFrame is an immutable distributed collection of data. Unlike an RDD, data is organized into named columns, like a table in a relational database. Designed to make large data sets processing even easier, DataFrame allows developers to impose a structure onto a distributed collection of data, allowing higher-level abstraction;

You may wonder what's the difference between RDD, DataFrame, DataSet

In short :

|                       | RDDs                                                                                                 | Dataframes                                                                                                             | Datasets                                                                                            |
|-----------------------|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| Data Representation   | RDD is a distributed collection of data elements without any schema.                                 | It is also the distributed collection organized into the named columns                                                 | It is an extension of Dataframes with more features like type-safety and object-oriented interface. |
| Optimization          | No in-built optimization engine for RDDs. Developers need to write the optimized code themselves.    | It uses a catalyst optimizer for optimization.                                                                         | It also uses a catalyst optimizer for optimization purposes.                                        |
| Projection of Schema  | Here, we need to define the schema manually.                                                         | It will automatically find out the schema of the dataset.                                                              | It will also automatically find out the schema of the dataset by using the SQL Engine.              |
| Aggregation Operation | RDD is slower than both Dataframes and Datasets to perform simple operations like grouping the data. | It provides an easy API to perform aggregation operations. It performs aggregation faster than both RDDs and Datasets. | Dataset is faster than RDDs but a bit slower than Dataframes.                                       |

For more Vist : [RDD vs DataFrame vs DataSet](https://medium.com/analytics-vidhya/datasets-vs-dataframes-vs-rdds-d3c2dba2d0b4) 

### Create DataFrame with Examples
***

You can Create a PySpark DataFrame using toDF() and createDataFrame() methods, both these function takes different signatures in order to create DataFrame from existing RDD, list, and DataFrame.

You can also create PySpark DataFrame from data sources like TXT, CSV, JSON, ORV, Avro, Parquet, XML formats by reading from HDFS, S3, DBFS, Azure Blob file systems e.t.c.

Finally, PySpark DataFrame also can be created by reading data from RDBMS Databases and NoSQL databases.

Ex : [Create Dataframe.ipynb](Notebooks/pyspark-create-dataframe.ipynb)  

#### Create DataFrame from Data sources
In real-time mostly you create DataFrame from data source files like CSV, Text, JSON, XML e.t.c.

PySpark by default supports many data formats out of the box without importing any libraries and to create DataFrame you need to use the appropriate method available in DataFrameReader class.

**Creating DataFrame from CSV**
Example : [DataFrame - CSV.ipynb](Notebooks/pyspark-read-write-csv.ipynb)  

**Creating DataFrame from JSON**
Example : [DataFrame - JSON.ipynb](Notebooks/pyspark-read-json.ipynb) 


**Creating DataFrame from Parquet**

Pyspark SQL provides methods to read Parquet file into DataFrame and write DataFrame to Parquet files, `parquet()` function

Apache Parquet is a free and open-source column-oriented data storage format of the Apache Hadoop ecosystem

###### Advantages:
- While querying columnar storage, it skips the nonrelevant data very quickly, making faster query execution. As a result aggregation queries consume less time compared to row-oriented databases.
- It is able to support advanced nested data structures.
- Parquet supports efficient compression options and encoding schemes.

Pyspark SQL provides support for both reading and writing Parquet files that automatically capture the schema of the original data, It also reduces data storage by 75% on average. 

For more on Parquet : [Parquet](https://www.upsolver.com/blog/apache-parquet-why-use)

Example : [DataFrame - Parquet.ipynb](Notebooks/pyspark-parquet.ipynb) 

#### PySpark Rename Column on DataFrame

Use PySpark `withColumnRenamed()` to rename a DataFrame column, we often need to rename one column or multiple columns on PySpark DataFrame, you can do this in several ways.

Example : [DataFrame - Rename Column.ipynb](Notebooks/pyspark-with-column-renamed.ipynb)



