### RDD (Resilient Distributed Dataset) 
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
`master()` – If you are running it on the cluster you need to use your master name as an argument to master(). usually, it would be either yarn (Yet Another Resource Negotiator) or mesos depends on your cluster setup.
In realtime application, you will pass master from spark-submit instead of hardcoding on Spark application.
Use `local[x]` when running in Standalone mode. **'x'** should be an integer value and should be greater than **'0'**; this represents how many partitions it should create when using RDD, DataFrame, and Dataset. Ideally, **'x'** value should be the number of CPU cores you have.
`appName()` – Used to set your application name.
`getOrCreate()` – This returns a SparkSession object if already exists, creates new one if not exists.
***Note:*** Creating SparkSession object, it internally creates one SparkContext per JVM.

##### Create RDD using sparkContext.parallelize()
PySpark `parallelize()` is a function in SparkContext and is used to create an RDD from a list collection
Example : [Parallelize.ipynb](Notebooks/pyspark-parallelize.ipynb)
For production applications, we mostly create RDD by using external storage systems like HDFS, S3, HBase e.t.c. To make it simple for this PySpark RDD learning we are using files from the local system or loading it from the python list to create RDD.

##### Create RDD from External Storage Systems
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

# Repartition and Coalesce
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

On the HDFS cluster, by default, PySpark creates one Partition for each block of the file.
In Version 1 Hadoop the HDFS block size is 64 MB and in Version 2 Hadoop the HDFS block size is 128 MB
Total number of cores on all executor nodes in a cluster or 2, whichever is larger
For example if you have 640 MB file and running it on Hadoop version 2, creates 5 partitions with each consists on 128 MB blocks (5 blocks * 128 MB = 640 MB). If you repartition to 10 then it creates 2 partitions for each block.

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
##### RDD coalesce()
Spark RDD `coalesce()` is used only to reduce the number of partitions. This is optimized or improved version of repartition() where the movement of the data across the partitions is lower using coalesce.

Ex : [Repartition-Coalesce.ipynb](Notebooks/pyspark-repartition-coalesce.ipynb)

## RDD Operations
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

**RDD Transformations functions**           
- flatMap
- map
- reduceByKey
- sortByKey
- filter

Ex :        [Transformations functions.ipynb](Notebooks/pyspark-rdd-transformations.ipynb)

Some more functions are :   
`mapPartitions(), mapPartitionsWithIndex(), randomSplit(), union(), intersection(), distinct(), repartition(), coalesce()` etc


DataFrames
Like an RDD, a DataFrame is an immutable distributed collection of data. Unlike an RDD, data is organized into named columns, like a table in a relational database. Designed to make large data sets processing even easier, DataFrame allows developers to impose a structure onto a distributed collection of data, allowing higher-level abstraction;
