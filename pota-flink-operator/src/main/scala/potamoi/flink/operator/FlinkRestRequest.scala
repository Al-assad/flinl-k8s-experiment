package potamoi.flink.operator

import potamoi.common.PathTool.getFileName
import potamoi.curTs
import potamoi.flink.share.FlinkOprErr._
import potamoi.flink.share.model.JobState.JobState
import potamoi.flink.share.model.SptFormatType.SptFormatType
import potamoi.flink.share.model._
import potamoi.flink.share._
import potamoi.flink.share.model.FlinkExecMode.FlinkExecMode
import potamoi.sttpx._
import potamoi.syntax._
import sttp.client3._
import sttp.client3.ziojson._
import zio.json.{jsonField, DeriveJsonCodec, JsonCodec}

import java.io.File

/**
 * Flink rest api request.
 * Reference to https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/
 */
case class FlinkRestRequest(restUrl: String) {
  import FlinkRestRequest._

  /**
   * Uploads jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars
   */
  def uploadJar(filePath: String): FlinkIO[JarId] = usingTypedSttp { backend =>
    request
      .post(uri"$restUrl/jars/upload")
      .multipartBody(
        multipartFile("jarfile", new File(filePath))
          .fileName(getFileName(filePath))
          .contentType("application/java-archive")
      )
      .send(backend)
      .narrowBody[FlinkOprErr]
      .attemptBody(ujson.read(_)("filename").str.split("/").last)
  }

  /**
   * Runs job from jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars-jarid-run
   */
  def runJar(jarId: String, jobRunReq: RunJobReq): FlinkIO[JobId] = usingTypedSttp { backend =>
    request
      .post(uri"$restUrl/jars/$jarId/run")
      .body(jobRunReq)
      .send(backend)
      .narrowBodyT[FlinkOprErr](JarNotFound(jarId))
      .attemptBody(ujson.read(_)("jobid").str)
  }

  /**
   * Deletes jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars-jarid
   */
  def deleteJar(jarId: String): FlinkIO[Unit] = usingTypedSttp { backend =>
    request
      .delete(uri"$restUrl/jars/$jarId")
      .send(backend)
      .narrowBodyT[FlinkOprErr](JarNotFound(jarId))
      .unit
  }

  /**
   * Cancels job.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-1
   */
  def cancelJob(jobId: String): FlinkIO[Unit] = usingTypedSttp { backend =>
    request
      .patch(uri"$restUrl/jobs/$jobId?mode=cancel")
      .send(backend)
      .narrowBodyT[FlinkOprErr](JobNotFound(jobId))
      .unit
  }

  /**
   * Stops job with savepoint.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-stop
   */
  def stopJobWithSavepoint(jobId: String, sptReq: StopJobSptReq): FlinkIO[TriggerId] = usingTypedSttp { backend =>
    request
      .post(uri"$restUrl/jobs/$jobId/stop")
      .body(sptReq)
      .send(backend)
      .narrowBodyT[FlinkOprErr](JobNotFound(jobId))
      .attemptBody(ujson.read(_)("request-id").str)
  }

  /**
   * Triggers a savepoint of job.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-savepoints
   */
  def triggerSavepoint(jobId: String, sptReq: TriggerSptReq): FlinkIO[TriggerId] = usingTypedSttp { backend =>
    request
      .post(uri"$restUrl/jobs/$jobId/savepoints")
      .body(sptReq)
      .send(backend)
      .narrowBodyT[FlinkOprErr](JobNotFound(jobId))
      .attemptBody(ujson.read(_)("request-id").str)
  }

  /**
   * Get status of savepoint operation.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-savepoints-triggerid
   */
  def getSavepointOperationStatus(jobId: String, triggerId: String): FlinkIO[FlinkSptTriggerStatus] = usingTypedSttp { backend =>
    request
      .get(uri"$restUrl/jobs/$jobId/savepoints/$triggerId")
      .send(backend)
      .narrowBodyT[FlinkOprErr](TriggerNotFound(triggerId))
      .attemptBody { body =>
        val rspJson      = ujson.read(body)
        val status       = rspJson("status")("id").str.contra(OprState.withName)
        val failureCause = rspJson("operation").objOpt.map(_("failure-cause")("stack-trace").str)
        FlinkSptTriggerStatus(status, failureCause)
      }
  }

  /**
   * Get all job and the current state.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs
   */
  def listJobsStatusInfo: FlinkIO[Vector[JobStatusInfo]] = usingTypedSttp { backend =>
    request
      .get(uri"$restUrl/jobs")
      .response(asJson[Vector[JobStatusInfo]])
      .send(backend)
      .narrowBody[FlinkOprErr]
  }

  /**
   * Get all job overview info
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-overview
   */
  def listJobOverviewInfo: FlinkIO[Vector[JobOverviewInfo]] = usingTypedSttp { backend =>
    request
      .get(uri"$restUrl/jobs/overview")
      .response(asJson[JobOverviewRsp])
      .send(backend)
      .narrowBody[FlinkOprErr]
      .map(_.jobs)
  }

  /**
   * Get cluster overview
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#overview-1
   */
  def getClusterOverview: FlinkIO[ClusterOverviewInfo] = usingTypedSttp { backend =>
    request
      .get(uri"$restUrl/overview")
      .response(asJson[ClusterOverviewInfo])
      .send(backend)
      .narrowBody[FlinkOprErr]
  }

  /**
   * Get job manager configuration.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobmanager-config
   */
  def getJobmanagerConfig: FlinkIO[Map[String, String]] = usingTypedSttp { backend =>
    request
      .get(uri"$restUrl/jobmanager/config")
      .send(backend)
      .narrowBody[FlinkOprErr]
      .attemptBody(ujson.read(_).arr.map(item => item("key").str -> item("value").str).toMap)
  }

  /**
   * Get job manager metrics.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobmanager-metrics
   */
  def getJmMetrics(metricsKeys: Set[String]): FlinkIO[Map[String, String]] = usingTypedSttp { backend =>
    request
      .get(uri"$restUrl/jobmanager/metrics?get=${metricsKeys.mkString(",")}")
      .send(backend)
      .narrowBody[FlinkOprErr]
      .attemptBody(ujson.read(_).arr.map(item => item("id").str -> item("value").str).toMap)
  }

  /**
   * List all task manager ids on cluster
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers
   */
  def listTaskManagerIds: FlinkIO[Vector[String]] = usingTypedSttp { backend =>
    request
      .get(uri"$restUrl/taskmanagers")
      .send(backend)
      .narrowBody[FlinkOprErr]
      .attemptBody(ujson.read(_)("taskmanagers").arr.map(_("id").str).toVector)
  }

  /**
   * Get task manager detail.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers-taskmanagerid
   */
  def getTaskManagerDetails(tmId: String): FlinkIO[TaskManagerDetail] = usingTypedSttp { backend =>
    request
      .get(uri"$restUrl/taskmanagers/$tmId")
      .response(asJson[TaskManagerDetail])
      .send(backend)
      .narrowBodyT[FlinkOprErr](TaskManagerNotFound(tmId))
  }

  /**
   * Get task manager metrics.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers-taskmanagerid-metrics
   */
  def getTmMetrics(tmId: String, metricsKeys: Set[String]): FlinkIO[Map[String, String]] = usingTypedSttp { backend =>
    request
      .get(uri"$restUrl/taskmanagers/$tmId/metrics?get=${metricsKeys.mkString(",")}")
      .send(backend)
      .narrowBodyT[FlinkOprErr](TaskManagerNotFound(tmId))
      .attemptBody(ujson.read(_).arr.map(item => item("id").str -> item("value").str).toMap)
  }

}

object FlinkRestRequest {

  def apply(restUrl: String): FlinkRestRequest = new FlinkRestRequest(restUrl)

  private val request                                      = basicRequest
  implicit private val narrowErr: Throwable => FlinkOprErr = RequestFlinkRestApiErr

  /**
   * see: [[FlinkRestRequest.runJar]]
   */
  case class RunJobReq(
      @jsonField("entry-class") entryClass: Option[String],
      programArgs: Option[String],
      parallelism: Option[Int],
      savepointPath: Option[String],
      restoreMode: Option[String],
      allowNonRestoredState: Option[Boolean])

  object RunJobReq {
    implicit def codec: JsonCodec[RunJobReq] = DeriveJsonCodec.gen[RunJobReq]

    def apply(jobDef: FlinkSessJobDef): RunJobReq = RunJobReq(
      entryClass = jobDef.appMain,
      programArgs = if (jobDef.appArgs.isEmpty) None else Some(jobDef.appArgs.mkString(" ")),
      parallelism = jobDef.parallelism,
      savepointPath = jobDef.savepointRestore.map(_.savepointPath),
      restoreMode = jobDef.savepointRestore.map(_.restoreMode.toString),
      allowNonRestoredState = jobDef.savepointRestore.map(_.allowNonRestoredState)
    )
  }

  /**
   * see: [[FlinkRestRequest.stopJobWithSavepoint]]
   */
  case class StopJobSptReq(
      drain: Boolean = false,
      formatType: Option[SptFormatType] = None,
      targetDirectory: Option[String],
      triggerId: Option[String] = None)

  object StopJobSptReq {
    implicit def codec: JsonCodec[StopJobSptReq]      = DeriveJsonCodec.gen[StopJobSptReq]
    def apply(sptConf: FlinkJobSptDef): StopJobSptReq = StopJobSptReq(sptConf.drain, sptConf.formatType, sptConf.savepointPath, sptConf.triggerId)
  }

  /**
   * see: [[FlinkRestRequest.triggerSavepoint]]
   */
  case class TriggerSptReq(
      @jsonField("cancel-job") cancelJob: Boolean = false,
      formatType: Option[SptFormatType] = None,
      @jsonField("target-directory") targetDirectory: Option[String],
      triggerId: Option[String] = None)

  object TriggerSptReq {
    implicit def codec: JsonCodec[TriggerSptReq]      = DeriveJsonCodec.gen[TriggerSptReq]
    def apply(sptConf: FlinkJobSptDef): TriggerSptReq = TriggerSptReq(cancelJob = false, sptConf.formatType, sptConf.savepointPath, sptConf.triggerId)
  }

  /**
   * see: [[FlinkRestRequest.listJobsStatusInfo]]
   */
  case class JobStatusInfo(id: String, status: FlinkJobOverview)

  object JobStatusInfo {
    implicit val codec: JsonCodec[JobStatusInfo] = DeriveJsonCodec.gen[JobStatusInfo]
  }

  /**
   * see: [[FlinkRestRequest.listJobOverviewInfo]]
   */
  case class JobOverviewRsp(jobs: Vector[JobOverviewInfo])
  case class JobOverviewInfo(
      @jsonField("jid") jid: String,
      name: String,
      state: JobState,
      @jsonField("start-time") startTime: Long,
      @jsonField("end-time") endTime: Long,
      @jsonField("last-modification") lastModifyTime: Long,
      tasks: TaskStats) {

    def toFlinkJobOverview(fcid: Fcid): FlinkJobOverview = model.FlinkJobOverview(
      clusterId = fcid.clusterId,
      namespace = fcid.namespace,
      jobId = jid,
      jobName = name,
      state = state,
      startTs = startTime,
      endTs = endTime,
      tasks = tasks,
      ts = curTs
    )
  }

  object JobOverviewRsp {
    implicit val taskStatsCodec: JsonCodec[TaskStats]     = DeriveJsonCodec.gen[TaskStats]
    implicit val jobOvCodec: JsonCodec[JobOverviewInfo]   = DeriveJsonCodec.gen[JobOverviewInfo]
    implicit val jobOvRspCodec: JsonCodec[JobOverviewRsp] = DeriveJsonCodec.gen[JobOverviewRsp]
  }

  case class ClusterOverviewInfo(
      @jsonField("flink-version") flinkVersion: String,
      @jsonField("taskmanagers") taskManagers: Int,
      @jsonField("slots-total") slotsTotal: Int,
      @jsonField("slots-available") slotsAvailable: Int,
      @jsonField("jobs-running") jobsRunning: Int,
      @jsonField("jobs-finished") jobsFinished: Int,
      @jsonField("jobs-cancelled") jobsCancelled: Int,
      @jsonField("jobs-failed") jobsFailed: Int) {

    def toFlinkClusterOverview(fcid: Fcid, execMode: FlinkExecMode): FlinkClusterOverview = model.FlinkClusterOverview(
      clusterId = fcid.clusterId,
      namespace = fcid.namespace,
      execMode = execMode,
      tmTotal = taskManagers,
      slotsTotal = slotsTotal,
      slotsAvailable = slotsAvailable,
      jobs = JobsStats(
        running = jobsRunning,
        finished = jobsFinished,
        canceled = jobsCancelled,
        failed = jobsFailed
      ),
      ts = curTs
    )
  }

  object ClusterOverviewInfo {
    implicit val codec: JsonCodec[ClusterOverviewInfo] = DeriveJsonCodec.gen[ClusterOverviewInfo]
  }

  case class TaskManagerDetail(
      id: String,
      path: String,
      dataPort: Int,
      timeSinceLastHeartbeat: Long,
      slotsNumber: Int,
      freeSlots: Int,
      totalResource: TmResource,
      freeResource: TmResource,
      hardware: TmHardware,
      memoryConfiguration: TmMemoryConfig)

  case class TmResource(cpuCores: Int, taskHeapMemory: Long, taskOffHeapMemory: Long, managedMemory: Long, networkMemory: Long)
  case class TmHardware(cpuCores: Int, physicalMemory: Long, freeMemory: Long, managedMemory: Long)
  case class TmMemoryConfig(
      frameworkHeap: Long,
      taskHeap: Long,
      frameworkOffHeap: Long,
      taskOffHeap: Long,
      networkMemory: Long,
      managedMemory: Long,
      jvmMetaspace: Long,
      jvmOverhead: Long,
      totalFlinkMemory: Long,
      totalProcessMemory: Long)

  object TaskManagerDetail {
    implicit val tmResourceCodec: JsonCodec[TmResource]               = DeriveJsonCodec.gen[TmResource]
    implicit val tmHardwareCodec: JsonCodec[TmHardware]               = DeriveJsonCodec.gen[TmHardware]
    implicit val tmMemoryConfigCodec: JsonCodec[TmMemoryConfig]       = DeriveJsonCodec.gen[TmMemoryConfig]
    implicit val taskManagerDetailCodec: JsonCodec[TaskManagerDetail] = DeriveJsonCodec.gen[TaskManagerDetail]
  }

}
