# SparkProfiler Overview

This project shows how "events" generated by Spark applications can be analyzed and used for profiling.
Profiling here means understanding how and where an application spent its time, the amount of data it processed, its memory footprint, etc.
Since Apache Spark is a distributed processing framework, this kind of anlysis helps understand resource utilization, application tuning and optimization.

Apache Spark internally uses events to record its activities. By setting the following configuration parameters, the events can be logged to a file as JSON entries
- spark.eventLog.enabled = true
- spark.eventLog.dir = dir-path

Note that events are logged by the driver, so for local (non-clustered) Spark applications and for YARN-client mode applications, <dir-path> needs to be a local directory. And for YARN cluster applications, <dir-path> is an HDFS location. The events are created underneath that base directory. This project also has a rudimentary analyzer that can process the events file and generate a summary for the application.

This project uses Spark itself to analyze Spark application events from Spark 1.6 or higher.

# Requirements
This project is based on Scala 2.11 and Spark 2.0 or higher. 

# How to Build and Run
Clone this project and build the project using `sbt assembly`. This will generate an assembly jar file `target/scala-2.11/sparkprofiler-assembly-1.0-SNAPSHOT.jar`.
 
Identify a location for an application events file and generate a summary for it as follows:

```
    spark-submit --class SummaryGenerator target/scala-2.11/sparkprofiler-assembly-0.1-SNAPSHOT.jar <events-file>
```

SummaryGenerator above is a Spark application that is run locally. For (very) large applications, it is not uncommon to have an events file that is a few GB large and if needed, SummaryGenerator can also be run as a distributed cluster application.

# Interactive Analysis
The project contains 3 main top-level Scala objects:
- Parser : This contains methods to parse the Spark events text lines into appropriate kind of events
- Profiler : This contains methods to generate a summary or profile of Spark jobs, stages and tasks
- SummaryGenerator: This is a sample program that shows how to process and analyze events file.

Note that you can also carry out interactive analysis of events using spark-shell as follows:

```
spark-shell --jars target/scala-2.11/sparkprofiler-assembly-1.0-SNAPSHOT.jar
```

```


scala> import com.conversantmedia.sparkprofiler.Parser._
import com.conversantmedia.sparkprofiler.Parser._


scala> import com.conversantmedia.sparkprofiler.Profiler._
import com.conversantmedia.sparkprofiler.Profiler._


scala> val events = spark.read.textFile("../application_1479761269966_255535_1")
events: org.apache.spark.sql.Dataset[String] = [value: string]

scala> val events = spark.read.textFile("../application_1479761269966_255535_1")
events: org.apache.spark.sql.Dataset[String] = [value: string]

scala> val application = parseApplication(spark, events)
application: com.conversantmedia.sparkprofiler.Application = Application(CPM,application_1479761269966_255535,1483011710370,1483024267784,12557414)

scala> val jobs = parseJobs(spark, events, application)
jobs: org.apache.spark.sql.Dataset[com.conversantmedia.sparkprofiler.Job] = [applicationId: string, applicationName: string ... 9 more fields]

scala> val stages = parseStages(spark, events, jobs)
stages: org.apache.spark.sql.Dataset[com.conversantmedia.sparkprofiler.Stage] = [applicationId: string, applicationName: string ... 16 more fields]

scala> val tasks = parseTasks(spark, events, stages)
tasks: org.apache.spark.sql.Dataset[com.conversantmedia.sparkprofiler.Task] = [applicationId: string, applicationName: string ... 45 more fields]

scala> jobs.printSchema
root
 |-- applicationId: string (nullable = true)
 |-- applicationName: string (nullable = true)
 |-- jobId: integer (nullable = true)
 |-- applicationStartTime: long (nullable = true)
 |-- applicationEndTime: long (nullable = true)
 |-- jobStartTime: long (nullable = true)
 |-- jobEndTime: long (nullable = true)
 |-- jobDuration: long (nullable = true)
 |-- applicationDuration: long (nullable = true)
 |-- stages: array (nullable = true)
 |    |-- element: integer (containsNull = false)
 |-- result: string (nullable = true)

scala> stages.printSchema
root
 |-- applicationId: string (nullable = true)
 |-- applicationName: string (nullable = true)
 |-- jobId: integer (nullable = true)
 |-- stageId: integer (nullable = true)
 |-- stageName: string (nullable = true)
 |-- stageAttemptId: integer (nullable = true)
 |-- details: string (nullable = true)
 |-- taskCount: integer (nullable = true)
 |-- rddCount: integer (nullable = true)
 |-- applicationStartTime: long (nullable = true)
 |-- applicationEndTime: long (nullable = true)
 |-- jobStartTime: long (nullable = true)
 |-- jobEndTime: long (nullable = true)
 |-- stageStartTime: long (nullable = true)
 |-- stageEndTime: long (nullable = true)
 |-- stageDuration: long (nullable = true)
 |-- jobDuration: long (nullable = true)
 |-- applicationDuration: long (nullable = true)

scala> tasks.printSchema
root
 |-- applicationId: string (nullable = true)
 |-- applicationName: string (nullable = true)
 |-- jobId: integer (nullable = true)
 |-- stageId: integer (nullable = true)
 |-- stageName: string (nullable = true)
 |-- stageAttemptId: integer (nullable = true)
 |-- taskId: integer (nullable = true)
 |-- taskAttemptId: integer (nullable = true)
 |-- taskType: string (nullable = true)
 |-- executorId: string (nullable = true)
 |-- host: string (nullable = true)
 |-- peakMemory: long (nullable = true)
 |-- inputRows: long (nullable = true)
 |-- outputRows: long (nullable = true)
 |-- locality: string (nullable = true)
 |-- speculative: boolean (nullable = true)
 |-- applicationStartTime: long (nullable = true)
 |-- applicationEndTime: long (nullable = true)
 |-- jobStartTime: long (nullable = true)
 |-- jobEndTime: long (nullable = true)
 |-- stageStartTime: long (nullable = true)
 |-- stageEndTime: long (nullable = true)
 |-- taskStartTime: long (nullable = true)
 |-- taskEndTime: long (nullable = true)
 |-- failed: boolean (nullable = true)
 |-- taskDuration: long (nullable = true)
 |-- stageDuration: long (nullable = true)
 |-- jobDuration: long (nullable = true)
 |-- applicationDuration: long (nullable = true)
 |-- gcTime: long (nullable = true)
 |-- gettingResultTime: long (nullable = true)
 |-- resultSerializationTime: long (nullable = true)
 |-- resultSize: long (nullable = true)
 |-- dataReadMethod: string (nullable = true)
 |-- bytesRead: long (nullable = true)
 |-- recordsRead: long (nullable = true)
 |-- memoryBytesSpilled: long (nullable = true)
 |-- diskBytesSpilled: long (nullable = true)
 |-- shuffleBytesWritten: long (nullable = true)
 |-- shuffleRecordsWritten: long (nullable = true)
 |-- shuffleWriteTime: long (nullable = true)
 |-- remoteBlocksFetched: long (nullable = true)
 |-- localBlocksFetched: long (nullable = true)
 |-- fetchWaitTime: long (nullable = true)
 |-- remoteBytesRead: long (nullable = true)
 |-- localBytesRead: long (nullable = true)
 |-- totalRecordsRead: long (nullable = true)

scala> // Lets identify long running jobs

scala> jobs.
     | orderBy($"jobDuration".desc)
res3: org.apache.spark.sql.Dataset[com.conversantmedia.sparkprofiler.Job] = [applicationId: string, applicationName: string ... 9 more fields]

scala> jobs.
     | orderBy($"jobDuration".desc).
     | select($"jobId", $"jobDuration", $"jobDuration"/$"applicationDuration" as "fraction").
     | show
+-----+-----------+--------------------+                                        
|jobId|jobDuration|            fraction|
+-----+-----------+--------------------+
|    4|    7722075|  0.6149414998979885|
|   11|    4140923|  0.3297592163482067|
|   14|     123947|0.009870423958308613|
|   15|       1846|1.470047893618861...|
|    0|       1220|9.715376111673948E-5|
|    2|       1036|  8.2501062718805E-5|
|    3|        987| 7.85989854280507E-5|
|    1|        978| 7.78822773542387E-5|
|    8|         66|5.255859207954758...|
|    6|         59| 4.69841959498986E-6|
|    9|         39|3.105734986518721E-6|
|   13|         34|2.707563834400936...|
|   10|         34|2.707563834400936...|
|   12|         28|2.229758451859595E-6|
|    7|         24|1.911221530165366...|
|    5|         21|1.672318838894696E-6|
+-----+-----------+--------------------+

scala> // Lets identify long running stages for the top 2 jobs

scala> stages.
     | filter($"jobId" === 4 || $"jobId" === 11).
     | select($"jobId", $"stageId", $"jobDuration", $"stageDuration", $"stageDuration"/$"jobDuration" as "fraction", $"stageName").
     | orderBy($"jobDuration".desc, $"stageDuration".desc).
     | show(50,150)
+-----+-------+-----------+-------------+---------------------+---------------------------------------+
|jobId|stageId|jobDuration|stageDuration|             fraction|                              stageName|
+-----+-------+-----------+-------------+---------------------+---------------------------------------+
|    4|     23|    7722075|      7603329|   0.9846225269762338|         map at BidECPMScores.scala:248|
|    4|     20|    7722075|      7468098|   0.9671102650518157|         map at BidECPMScores.scala:248|
|    4|     22|    7722075|      7421408|   0.9610639627302248|         map at BidECPMScores.scala:248|
|    4|     16|    7722075|      7356882|   0.9527079185322598|         map at BidECPMScores.scala:248|
|    4|     19|    7722075|      7306950|   0.9462417808684842|         map at BidECPMScores.scala:248|
|    4|     13|    7722075|      7237791|   0.9372857683977428|         map at BidECPMScores.scala:248|
|    4|      6|    7722075|      7205052|   0.9330461048358116|         map at BidECPMScores.scala:248|
|    4|     12|    7722075|      7046977|   0.9125755706853405|         map at BidECPMScores.scala:248|
|    4|     11|    7722075|      7019734|   0.9090476329224981|         map at BidECPMScores.scala:248|
|    4|      9|    7722075|      6999966|   0.9064876992259205|         map at BidECPMScores.scala:248|
|    4|      8|    7722075|      6037813|   0.7818899712836251|         map at BidECPMScores.scala:248|
|    4|      5|    7722075|      4544630|   0.5885244574806642|         map at BidECPMScores.scala:248|
|    4|      4|    7722075|      4176188|   0.5408116341786372|         map at BidECPMScores.scala:248|
|    4|     17|    7722075|        79704| 0.010321578073251037|         map at BidECPMScores.scala:248|
|    4|     10|    7722075|        43715|0.0056610431781613105|         map at BidECPMScores.scala:248|
|    4|      7|    7722075|        41908| 0.005427038716925178|         map at BidECPMScores.scala:248|
|    4|     15|    7722075|        40730| 0.005274489046014187|         map at BidECPMScores.scala:248|
|    4|     14|    7722075|        37207| 0.004818264520870362|         map at BidECPMScores.scala:248|
|    4|     24|    7722075|        36982| 0.004789127274728619|         map at BidECPMScores.scala:248|
|    4|     21|    7722075|        35924| 0.004652117468426556|         map at BidECPMScores.scala:248|
|    4|     25|    7722075|        33094|   0.0042856356613993|   map at ClusterBidECPMScores.scala:37|
|    4|     18|    7722075|         4940|  6.39724426400935E-4|         map at BidECPMScores.scala:248|
|    4|     26|    7722075|         3578| 4.633469630895841E-4|reduce at ClusterBidECPMScores.scala:39|
|   11|     52|    4140923|      3686673|   0.8903022345501233|            map at ECPMBuckets.scala:47|
|   11|     49|    4140923|      3611981|   0.8722647100658476|            map at ECPMBuckets.scala:47|
|   11|     45|    4140923|      3557824|   0.8591862249068626|            map at ECPMBuckets.scala:47|
|   11|     35|    4140923|      3495378|   0.8441060121137244|            map at ECPMBuckets.scala:47|
|   11|     42|    4140923|      3478641|   0.8400641596088602|            map at ECPMBuckets.scala:47|
|   11|     48|    4140923|      3420245|   0.8259619896337121|            map at ECPMBuckets.scala:47|
|   11|     41|    4140923|      3189200|   0.7701664580577808|            map at ECPMBuckets.scala:47|
|   11|     38|    4140923|      3186944|   0.7696216519843523|            map at ECPMBuckets.scala:47|
|   11|     40|    4140923|      3180009|   0.7679469045910778|            map at ECPMBuckets.scala:47|
|   11|     51|    4140923|      3102151|   0.7491448162643932|            map at ECPMBuckets.scala:47|
|   11|     37|    4140923|      2465505|   0.5953998661651038|            map at ECPMBuckets.scala:47|
|   11|     34|    4140923|      1304852|   0.3151113894172869|            map at ECPMBuckets.scala:47|
|   11|     33|    4140923|      1268903|  0.30642999157434225|            map at ECPMBuckets.scala:47|
|   11|     46|    4140923|        86384| 0.020861049577594173|            map at ECPMBuckets.scala:47|
|   11|     39|    4140923|        26783|0.0064678816775873395|            map at ECPMBuckets.scala:47|
|   11|     36|    4140923|        26103| 0.006303667080986534|            map at ECPMBuckets.scala:47|
|   11|     50|    4140923|        24544| 0.005927180969073803|            map at ECPMBuckets.scala:47|
|   11|     53|    4140923|        22421| 0.005414493338803933|            map at ECPMBuckets.scala:47|
|   11|     43|    4140923|        20020| 0.004834670917570793|            map at ECPMBuckets.scala:47|
|   11|     54|    4140923|        18311| 0.004421960997584355|            map at ECPMBuckets.scala:47|
|   11|     44|    4140923|        18069| 0.004363519920558774|            map at ECPMBuckets.scala:47|
|   11|     47|    4140923|         3439| 8.304911731031946E-4|            map at ECPMBuckets.scala:47|
|   11|     55|    4140923|         1682| 4.061896345331705E-4|            map at ECPMBuckets.scala:47|
|   11|     56|    4140923|         1154|  2.78681830113721E-4|        collect at ECPMBuckets.scala:47|
+-----+-------+-----------+-------------+---------------------+---------------------------------------+

scala> // For the most long running stage, lets get a "profile" of their tasks

scala> val expensiveTasks = tasks.filter($"jobId" === 4 && $"stageId" === 23)
expensiveTasks: org.apache.spark.sql.Dataset[Task] = [applicationId: string, applicationName: string ... 45 more fields]

scala> val taskInfo = taskProfile(spark, expensiveTasks)
taskInfo: org.apache.spark.sql.DataFrame = [summary: string, taskDuration: string ... 20 more fields]

scala> taskInfo.select($"summary", $"taskDuration", $"gcTime", $"peakMemory", $"inputRows", $"outputRows", $"remoteBytesRead", $"localBytesRead").show
+-------+------------------+-------------------+----------+--------------------+------------------+---------------+--------------+
|summary|      taskDuration|             gcTime|peakMemory|           inputRows|        outputRows|remoteBytesRead|localBytesRead|
+-------+------------------+-------------------+----------+--------------------+------------------+---------------+--------------+
|  count|            239521|             239521|    239521|              239521|            239521|         239521|        239521|
|   mean|212.27890247619206|0.21976778654063736|       0.0|5.387501543458062E10|1268936.9646544561|            0.0|           0.0|
| stddev| 1161.054508000039|  5.369256047367187|       0.0| 8.131317765061674E9| 153926.4945274276|            0.0|           0.0|
|    min|                 4|                  0|         0|                   0|                 0|              0|             0|
|    max|             48803|                401|         0|         57203175889|           1329783|              0|             0|
+-------+------------------+-------------------+----------+--------------------+------------------+---------------+--------------+

scala> // Hmmm....there's something interesting with the "inputRows" count of 57203175889....time for some investigation!

scala> //Happy SparkProfiling!!

```
