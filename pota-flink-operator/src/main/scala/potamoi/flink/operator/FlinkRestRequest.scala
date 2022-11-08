package potamoi.flink.operator

import potamoi.common.PathTool.getFileName
import potamoi.common.SttpExtension.{usingSttp, RequestBodyIOWrapper, RequestDeserializationBodyIOWrapper}
import potamoi.common.{ComplexEnum, GenericPF}
import potamoi.flink.operator.FlinkRestRequest.FlkQueueStatus.FlkQueueStatus
import potamoi.flink.operator.FlinkRestRequest._
import potamoi.flink.share.FlinkJobStatus.FlinkJobStatus
import potamoi.flink.share.{FlinkSessJobDef, JarId, JobId, TriggerId}
import potamoi.flink.share.SptFormatType.SptFormatType
import sttp.client3._
import sttp.client3.ziojson._
import zio.IO
import zio.ZIO.attempt
import zio.json.{jsonField, DeriveJsonCodec, JsonCodec}

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
  def uploadJar(filePath: String): IO[Throwable, JarId] = usingSttp { backend =>
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
  def getSavepointOperationStatus(jobId: String, triggerId: String): IO[Throwable, SptOprStatus] = usingSttp { backend =>
    basicRequest
      .get(uri"$restUrl/jobs/$jobId/savepoints/$triggerId")
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap { rsp =>
        attempt {
          val rspJson      = ujson.read(rsp)
          val status       = rspJson("status")("id").str.contra(FlkQueueStatus.withName)
          val failureCause = rspJson("operation").objOpt.map(_("failure-cause")("stack-trace").str)
          SptOprStatus(status, failureCause)
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

}

object FlinkRestRequest {

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
    implicit def codec: JsonCodec[StopJobSptReq] = DeriveJsonCodec.gen[StopJobSptReq]
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
    implicit def codec: JsonCodec[TriggerSptReq] = DeriveJsonCodec.gen[TriggerSptReq]
  }

  /**
   * see: [[FlinkRestRequest.getSavepointOperationStatus]]
   */
  case class SptOprStatus(status: FlkQueueStatus, failureCause: Option[String])

  object SptOprStatus {
    implicit def codec: JsonCodec[SptOprStatus] = DeriveJsonCodec.gen[SptOprStatus]
  }

  object FlkQueueStatus extends ComplexEnum {
    type FlkQueueStatus = Value
    val Completed  = Value("COMPLETED")
    val InProgress = Value("IN_PROGRESS")
  }

  /**
   * see: [[FlinkRestRequest.listJobsStatusInfo]]
   */
  case class JobStatusInfo(id: String, status: FlinkJobStatus)

  object JobStatusInfo {
    implicit val codec: JsonCodec[JobStatusInfo] = DeriveJsonCodec.gen[JobStatusInfo]
  }

  /**
   * see: [[FlinkRestRequest.listJobOverviewInfo]]
   */
  case class JobOverviewInfo(
      @jsonField("jid") jid: String,
      name: String,
      state: FlinkJobStatus,
      @jsonField("start-time") startTime: Long,
      @jsonField("end-time") endTime: Long,
      duration: Long,
      @jsonField("last-modification") lastModifyTime: Long,
      tasks: TaskStats)

  case class TaskStats(
      total: Int,
      created: Int,
      scheduled: Int,
      deploying: Int,
      running: Int,
      finished: Int,
      canceling: Int,
      canceled: Int,
      failed: Int,
      reconciling: Int,
      initializing: Int)

  object JobOverviewInfo {
    implicit val codec: JsonCodec[JobOverviewInfo]    = DeriveJsonCodec.gen[JobOverviewInfo]
    implicit val taskStatsCodec: JsonCodec[TaskStats] = DeriveJsonCodec.gen[TaskStats]
  }

}
