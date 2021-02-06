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

Before we look into examples, first let’s initialize SparkSession
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

Example : [Parallelize.ipynb](./Notebooks/pyspark-parallelize.ipynb)

DataFrames
Like an RDD, a DataFrame is an immutable distributed collection of data. Unlike an RDD, data is organized into named columns, like a table in a relational database. Designed to make large data sets processing even easier, DataFrame allows developers to impose a structure onto a distributed collection of data, allowing higher-level abstraction;
