package com.conversantmedia.sparkprofiler

import org.apache.spark.sql.{Dataset, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
  * Parser does all the work of parsing events from the application events file.
  */
object Parser {

  def parseJSON[T](json: String)(implicit t: Manifest[T]): T = {
    implicit val formats = DefaultFormats
    parse(json).extract[T]
  }

  /**
    * Parse application start and end events to get an {@link Application} instance.
    * @param spark
    * @param events
    * @return
    */
  def parseApplication(spark: SparkSession,
                       events: Dataset[String]): Application = {

    import spark.implicits._

    val startEvent = events.filter(_.contains("SparkListenerApplicationStart"))
    val endEvent = events.filter(_.contains("SparkListenerApplicationEnd"))
    val start = parseJSON[SparkApplicationStart](startEvent.head())
    val end = parseJSON[SparkApplicationEnd](endEvent.head())
    val applicationName = start.`App Name`
    val applicationId = start.`App ID`
    val applicationStartTime = start.`Timestamp`
    val applicationEndTime = end.Timestamp
    val jobDuration = applicationEndTime - applicationStartTime
    Application(applicationName, applicationId, applicationStartTime, applicationEndTime, jobDuration)
  }

  /**
    * Parse executor add event to return a dataset of {@link Executor}
    * @param spark
    * @param events
    * @param sparkApplication
    * @return
    */
  def parseExecutors(spark: SparkSession,
                     events: Dataset[String],
                     sparkApplication: Application): Dataset[Executor] = {

    import spark.implicits._

    events.filter(_.contains("SparkListenerExecutorAdded")).
      map(string => parseJSON[SparkExecutorAdded](string)).
      map(e => {
        val executorId = e.`Executor ID`
        val host =   e.`Executor Info`.`Host`
        val cores = e.`Executor Info`.`Total Cores`
        val startTime = e.`Timestamp`
        Executor(sparkApplication.applicationName,
          sparkApplication.applicationId,
          executorId, host, cores, startTime)
      })
  }

  /**
    * Parse job start and end events to return a dataset of {@link Job}
    * @param spark
    * @param events
    * @param sparkApplication
    * @return
    */
  def parseJobs(spark: SparkSession,
                events: Dataset[String],
                sparkApplication: Application): Dataset[Job] = {

    import spark.implicits._

    val start = events.filter(_.contains("SparkListenerJobStart")).
      map(string => parseJSON[SparkJobStart](string))
    val end = events.filter(_.contains("SparkListenerJobEnd")).
      map(string => parseJSON[SparkJobEnd](string))

    start.joinWith(end, start("Job ID") === end("Job ID")).
      map(tuple => {
        val jobId = tuple._1.`Job ID`
        val jobStartTime = tuple._1.`Submission Time`
        val jobEndTime = tuple._2.`Completion Time`
        val stages = tuple._1.`Stage IDs`
        val jobDuration = jobEndTime - jobStartTime
        val result = tuple._2.`Job Result`.`Result`
        Job(sparkApplication.applicationId,
          sparkApplication.applicationName,
          jobId,
          sparkApplication.applicationStartTime,
          sparkApplication.applicationEndTime,
          jobStartTime, jobEndTime, jobDuration,
          sparkApplication.applicationDuration, stages, result)
      })
  }

  /**
    * Parse stage complete events to return a dataset of {@link Stage}
    * @param spark
    * @param events
    * @param sparkJob
    * @return
    */
  def parseStages(spark: SparkSession,
                  events: Dataset[String],
                  sparkJob: Dataset[Job]): Dataset[Stage] = {

    import spark.implicits._

    val jobs = sparkJob.collect()
    events.filter(_.contains("SparkListenerStageCompleted")).
      map(string => parseJSON[SparkStageComplete](string)).
      map(stage => {
        val stageInfo = stage.`Stage Info`
        val stageId = stageInfo.`Stage ID`
        val job = jobs.filter(j => j.stages.contains(stageId)).last
        val jobId = job.jobId
        val attempt = stageInfo.`Stage Attempt ID`
        val stageName = stageInfo.`Stage Name`
        val details = stageInfo.`Details`
        val taskCount = stageInfo.`Number of Tasks`
        val rddCount = stageInfo.`RDD Info`.size
        val applicationStartTime = job.applicationStartTime
        val applicationEndTime = job.applicationEndTime
        val jobStartTime = job.jobStartTime
        val jobEndTime = job.jobEndTime
        val stageStartTime = stageInfo.`Submission Time`
        val stageEndTime = stageInfo.`Completion Time`
        val stageDuration = stageEndTime - stageStartTime
        val jobDuration = job.jobDuration
        val applicationDuration = job.applicationDuration
        Stage(job.applicationId, job.applicationName, jobId, stageId, stageName,
          attempt, details, taskCount, rddCount, applicationStartTime, applicationEndTime,
          jobStartTime, jobEndTime, stageStartTime, stageEndTime, stageDuration, jobDuration, applicationDuration)
      })
  }

  /**
    * Parse task end events to return a dataset of {@link Task}.
    * Note that the task event has a number of nested entities that are
    * introspected to extract relevant metrics.
    * @param spark
    * @param events
    * @param sparkStage
    * @return
    */
  def parseTasks(spark: SparkSession,
                 events: Dataset[String],
                 sparkStage: Dataset[Stage]): Dataset[Task] = {

    import spark.implicits._

    val tasks = events.
      filter(_.contains("SparkListenerTaskEnd")).
      map(string => parseJSON[SparkTaskEnd](string))

    val joined = tasks.joinWith(sparkStage,
      tasks("Stage ID") === sparkStage("stageId") &&
        tasks("Stage Attempt ID") === sparkStage("stageAttemptId"))
      joined.map(tuple => {
        val applicationId = tuple._2.applicationId
        val applicationName = tuple._2.applicationName
        val taskInfo = tuple._1.`Task Info`
        val taskMetrics = tuple._1.`Task Metrics`
        val accumulableMemory = taskInfo.`Accumulables`.
          filter(_.`Name` == "peakExecutionMemory").
          map(i => i.`Value`.toLong)
        val peakMemory = if (accumulableMemory.nonEmpty) accumulableMemory.head else 0
        val accumulableInputRows = taskInfo.`Accumulables`.
          filter(_.`Name` == "number of input rows").
          map(i => i.`Value`.toLong)
        val inputRows = if (accumulableInputRows.nonEmpty) accumulableInputRows.head else 0
        val accumulableOutputRows = taskInfo.`Accumulables`.
          filter(_.`Name` == "number of output rows").
          map(i => i.`Value`.toLong)
        val outputRows = if (accumulableOutputRows.nonEmpty) accumulableOutputRows.head else 0
        val jobId = tuple._2.jobId
        val stageId = tuple._2.stageId
        val stageAttemptId = tuple._2.stageAttemptId
        val taskId = taskInfo.`Task ID`
        val taskType = tuple._1.`Task Type`
        val attempt = taskInfo.`Attempt`
        val executorId = taskInfo.`Executor ID`
        val host = taskInfo.`Host`
        val stageName = tuple._2.stageName
        val locality = taskInfo.`Locality`
        val speculative = taskInfo.`Speculative`
        val applicationStartTime = tuple._2.applicationStartTime
        val applicationEndTime = tuple._2.applicationEndTime
        val jobStartTime = tuple._2.jobStartTime
        val jobEndTime = tuple._2.jobEndTime
        val stageStartTime = tuple._2.stageStartTime
        val stageEndTime = tuple._2.stageEndTime
        val taskStartTime = taskInfo.`Launch Time`
        val taskEndTime = taskInfo.`Finish Time`
        val failed = taskInfo.`Failed`
        val taskDuration = taskEndTime - taskStartTime
        val stageDuration = tuple._2.stageDuration
        val jobDuration = tuple._2.jobDuration
        val applicationDuration = tuple._2.applicationDuration
        val gettingResultTime = taskInfo.`Getting Result Time`

        val gcTime = if (taskMetrics nonEmpty) taskMetrics.get.`JVM GC Time` else 0
        val resultSerializationTime = if (taskMetrics nonEmpty) taskMetrics.get.`Result Serialization Time` else 0
        val resultSize = if (taskMetrics nonEmpty) taskMetrics.get.`Result Size` else 0
        val memoryBytesSpilled = if (taskMetrics nonEmpty) taskMetrics.get.`Memory Bytes Spilled` else 0
        val diskBytesSpilled = if (taskMetrics nonEmpty) taskMetrics.get.`Disk Bytes Spilled` else 0

        val inputMetrics = if (taskMetrics nonEmpty) taskMetrics.get.`Input Metrics` else None
        val shuffleWriteMetrics = if (taskMetrics nonEmpty) taskMetrics.get.`Shuffle Write Metrics` else None
        val shuffleReadMetrics = if (taskMetrics nonEmpty) taskMetrics.get.`Shuffle Read Metrics` else None

        val dataReadMethod = "n/a"
        val bytesRead = if (inputMetrics nonEmpty) inputMetrics.get.`Bytes Read` else 0L
        val recordsRead =
          inputMetrics match {
            case None => 0L
            case Some(_) => inputMetrics.get.`Records Read`}
        val shuffleBytesWritten =
          shuffleWriteMetrics match {
            case None => 0L
            case Some(_) => shuffleWriteMetrics.get.`Shuffle Bytes Written`}
        val shuffleRecordsWritten =
          shuffleWriteMetrics match {
            case None => 0L
            case Some(_) => shuffleWriteMetrics.get.`Shuffle Records Written`}
        val shuffleWriteTime =
          shuffleWriteMetrics match {
            case None => 0L
            case Some(_) => shuffleWriteMetrics.get.`Shuffle Write Time`}
        val remoteBlocksFetched =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Remote Blocks Fetched`}
        val localBlocksFetched =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Local Blocks Fetched`}
        val fetchWaitTime =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Fetch Wait Time`}
        val remoteBytesRead =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Remote Bytes Read`}
        val localBytesRead =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Local Bytes Read`}
        val totalRecordsRead =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Total Records Read`}
        Task(applicationId,
          applicationName,
          jobId,
          stageId,
          stageName,
          stageAttemptId,
          taskId,
          attempt,
          taskType,
          executorId,
          host,
          peakMemory,
          inputRows,
          outputRows,
          locality,
          speculative,
          applicationStartTime,
          applicationEndTime,
          jobStartTime,
          jobEndTime,
          stageStartTime,
          stageEndTime,
          taskStartTime,
          taskEndTime,
          failed,
          taskDuration,
          stageDuration,
          jobDuration,
          applicationDuration,
          gcTime,
          gettingResultTime,
          resultSerializationTime,
          resultSize,
          dataReadMethod,
          bytesRead,
          recordsRead,
          memoryBytesSpilled,
          diskBytesSpilled,
          shuffleBytesWritten,
          shuffleRecordsWritten,
          shuffleWriteTime,
          remoteBlocksFetched,
          localBlocksFetched,
          fetchWaitTime,
          remoteBytesRead,
          localBytesRead,
          totalRecordsRead
      )

    })
  }

}
