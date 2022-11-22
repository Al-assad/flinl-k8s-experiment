package potamoi.flink.operator

import potamoi.common.PathTool.getFileName
import potamoi.common.SttpExtension.{RequestBodyIOWrapper, RequestDeserializationBodyIOWrapper, usingSttp}
import potamoi.curTs
import potamoi.flink.operator.FlinkRestRequest._
import potamoi.flink.share.model.JobState.JobState
import potamoi.flink.share.model.SptFormatType.SptFormatType
import potamoi.flink.share.{model, _}
import potamoi.flink.share.model.{Fcid, FlinkJobOverview, FlinkJobSptDef, FlinkSessJobDef, FlinkSptTriggerStatus, OprState, TaskStats}
import potamoi.syntax._
import sttp.client3._
import sttp.client3.ziojson._
import zio.{IO, ZIO}
import zio.ZIO.attempt
import zio.json.{DeriveJsonCodec, JsonCodec, jsonField}

import java.io.File

/**
 * Flink rest api request.
 * Reference to https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/
 */
case class FlinkRestRequest(restUrl: String) {

  /**
   * Uploads jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars
   */
  def uploadJar(filePath: String): IO[FlinkOprErr, JarId] = usingSttp { backend =>
    basicRequest
      .post(uri"$restUrl/jars/upload")
      .multipartBody(
        multipartFile("jarfile", new File(filePath))
          .fileName(getFileName(filePath))
          .contentType("application/java-archive")
      )
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap(rsp => attempt(ujson.read(rsp)("filename").str.split("/").last))
  }

  /**
   * Runs job from jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars-jarid-run
   */
  def runJar(jarId: String, jobRunReq: RunJobReq): IO[Throwable, JobId] = usingSttp { backend =>
    basicRequest
      .post(uri"$restUrl/jars/$jarId/run")
      .body(jobRunReq)
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap(rsp => attempt(ujson.read(rsp)("jobid").str))
  }

  /**
   * Deletes jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars-jarid
   */
  def deleteJar(jarId: String): IO[Throwable, Unit] = usingSttp { backend =>
    basicRequest
      .delete(uri"$restUrl/jars/$jarId")
      .send(backend)
      .unit
  }

  /**
   * Cancels job.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-1
   */
  def cancelJob(jobId: String): IO[Throwable, Unit] = usingSttp { backend =>
    basicRequest
      .patch(uri"$restUrl/jobs/$jobId?mode=cancel")
      .send(backend)
      .unit
  }

  /**
   * Stops job with savepoint.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-stop
   */
  def stopJobWithSavepoint(jobId: String, sptReq: StopJobSptReq) = usingSttp { backend =>
    basicRequest
      .post(uri"$restUrl/jobs/$jobId/stop")
      .body(sptReq)
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap(rsp => attempt(ujson.read(rsp)("request-id").str))
  }

  /**
   * Triggers a savepoint of job.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-savepoints
   */
  def triggerSavepoint(jobId: String, sptReq: TriggerSptReq): IO[Throwable, TriggerId] = usingSttp { backend =>
    basicRequest
      .post(uri"$restUrl/jobs/$jobId/savepoints")
      .body(sptReq)
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap(rsp => attempt(ujson.read(rsp)("request-id").str))
  }

  /**
   * Get status of savepoint operation.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-savepoints-triggerid
   */
  def getSavepointOperationStatus(jobId: String, triggerId: String): IO[Throwable, FlinkSptTriggerStatus] = usingSttp { backend =>
    basicRequest
      .get(uri"$restUrl/jobs/$jobId/savepoints/$triggerId")
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap { rsp =>
        attempt {
          val rspJson      = ujson.read(rsp)
          val status       = rspJson("status")("id").str.contra(OprState.withName)
          val failureCause = rspJson("operation").objOpt.map(_("failure-cause")("stack-trace").str)
          FlinkSptTriggerStatus(status, failureCause)
        }
      }
  }

  /**
   * Get all job and the current state.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs
   */
  def listJobsStatusInfo: IO[Throwable, Vector[JobStatusInfo]] = usingSttp { backend =>
    basicRequest
      .get(uri"$restUrl/jobs")
      .response(asJson[Vector[JobStatusInfo]])
      .send(backend)
      .map(_.body)
      .narrowEither
  }

  /**
   * Get all job overview info
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-overview
   */
  def listJobOverviewInfo: IO[Throwable, Vector[JobOverviewInfo]] = usingSttp { backend =>
    basicRequest
      .get(uri"$restUrl/jobs/overview")
      .response(asJson[Vector[JobOverviewInfo]])
      .send(backend)
      .map(_.body)
      .narrowEither
  }

  /**
   * Get cluster overview
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#overview-1
   */
  def getClusterOverview: IO[Throwable, ClusterOverview] = usingSttp { backend =>
    basicRequest
      .get(uri"$restUrl/overview")
      .response(asJson[ClusterOverview])
      .send(backend)
      .map(_.body)
      .narrowEither
  }

  /**
   * Get cluster configuration.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#config
   */
  def getClusterConfig: IO[Throwable, Map[String, String]] = usingSttp { backend =>
    basicRequest
      .get(uri"$restUrl/config")
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap { rsp =>
        attempt {
          ujson.read(rsp).arr.map(item => item("key").str -> item("value").str).toMap
        }
      }
  }

  /**
   * Get job manager metrics.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobmanager-metrics
   */
  def getJmMetrics(metricsKeys: Set[String]): IO[Throwable, Map[String, String]] = usingSttp { backend =>
    basicRequest
      .get(uri"$restUrl/jobmanager/metrics?get=${metricsKeys.mkString(",")}")
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap { rsp =>
        attempt {
          ujson.read(rsp).arr.map(item => item("id").str -> item("value").str).toMap
        }
      }
  }

  /**
   * List all task manager ids on cluster
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers
   */
  def listTaskManagerIds: IO[Throwable, Vector[String]] = usingSttp { backend =>
    basicRequest
      .get(uri"$restUrl/taskmanagers")
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap { rsp =>
        attempt {
          ujson.read(rsp)("taskmanagers").arr.map(_("id").str).toVector
        }
      }
  }

  /**
   * Get task manager detail.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers-taskmanagerid
   */
  def getTaskManagerDetails(tmId: String): IO[Throwable, TaskManagerDetail] = usingSttp { backend =>
    basicRequest
      .get(uri"$restUrl/taskmanagers/$tmId")
      .response(asJson[TaskManagerDetail])
      .send(backend)
      .map(_.body)
      .narrowEither
  }

  /**
   * Get task manager metrics.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers-taskmanagerid-metrics
   */
  def getTmMetrics(metricsKeys: Set[String]): IO[Throwable, Map[String, String]] = usingSttp { backend =>
    basicRequest
      .get(uri"$restUrl/taskmanagers/metrics?get=${metricsKeys.mkString(",")}")
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap { rsp =>
        ZIO.attempt {
          ujson.read(rsp).arr.map(item => item("id").str -> item("value").str).toMap
        }
      }
  }

}

object FlinkRestRequest {

  case object NotFoundErr extends Throwable

  def apply(restUrl: String): FlinkRestRequest = new FlinkRestRequest(restUrl)

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
  case class JobOverviewInfo(
      @jsonField("jid") jid: String,
      name: String,
      state: JobState,
      @jsonField("start-time") startTime: Long,
      @jsonField("end-time") endTime: Long,
      duration: Long,
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

  object JobOverviewInfo {
    implicit val taskStatsCodec: JsonCodec[TaskStats] = DeriveJsonCodec.gen[TaskStats]
    implicit val codec: JsonCodec[JobOverviewInfo]    = DeriveJsonCodec.gen[JobOverviewInfo]
  }

  case class ClusterOverview(
      @jsonField("flink-version") flinkVersion: String,
      @jsonField("taskmanagers") taskManagers: Int,
      @jsonField("slots-total") slotsTotal: Int,
      @jsonField("slots-available") slotsAvailable: Int,
      @jsonField("jobs-running") jobsRunning: Int,
      @jsonField("jobs-finished") jobsFinished: Int,
      @jsonField("jobs-cancelled") jobsCancelled: Int,
      @jsonField("jobs-failed") jobsFailed: Int)

  object ClusterOverview {
    implicit val codec: JsonCodec[ClusterOverview] = DeriveJsonCodec.gen[ClusterOverview]
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
      memoryConfiguration: TmMemoryConfig
  )

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
      totalProcessMemory: Long
  )

  object TaskManagerDetail {
    implicit val tmResourceCodec: JsonCodec[TmResource]               = DeriveJsonCodec.gen[TmResource]
    implicit val tmHardwareCodec: JsonCodec[TmHardware]               = DeriveJsonCodec.gen[TmHardware]
    implicit val tmMemoryConfigCodec: JsonCodec[TmMemoryConfig]       = DeriveJsonCodec.gen[TmMemoryConfig]
    implicit val taskManagerDetailCodec: JsonCodec[TaskManagerDetail] = DeriveJsonCodec.gen[TaskManagerDetail]
  }

}
