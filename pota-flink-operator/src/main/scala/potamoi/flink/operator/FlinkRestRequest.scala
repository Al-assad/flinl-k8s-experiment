package potamoi.flink.operator

import potamoi.common.PathTool.getFileName
import potamoi.common.SttpExtension.{usingSttp, RequestBodyIOWrapper}
import potamoi.flink.operator.FlinkRestRequest.{FlinkJobRunReq, JarId}
import potamoi.flink.share.FlinkSessJobDef
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
   * Upload jar file.
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
   * Running job from jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars-jarid-run
   */
  def runJar(jarId: String, jobRunReq: FlinkJobRunReq) = usingSttp { backend =>
    basicRequest
      .post(uri"$restUrl/jars/$jarId/run")
      .body(jobRunReq)
      .send(backend)
      .map(_.body)
      .narrowEither
      .flatMap(rsp => attempt(ujson.read(rsp)("jobid").str))
  }

  /**
   * Delete jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars-jarid
   */
  def deleteJar(jarId: String): IO[Throwable, Unit] = usingSttp { backend =>
    basicRequest
      .delete(uri"$restUrl/jars/$jarId")
      .send(backend)
      .unit
  }
}

object FlinkRestRequest {

  def apply(restUrl: String): FlinkRestRequest = new FlinkRestRequest(restUrl)

  type JarId = String

  case class FlinkJobRunReq(
      @jsonField("entry-class") entryClass: Option[String],
      programArgs: Option[String],
      parallelism: Option[Int],
      savepointPath: Option[String],
      restoreMode: Option[String],
      allowNonRestoredState: Option[Boolean])

  object FlinkJobRunReq {
    implicit def codec: JsonCodec[FlinkJobRunReq] = DeriveJsonCodec.gen[FlinkJobRunReq]

    def apply(jobDef: FlinkSessJobDef): FlinkJobRunReq = FlinkJobRunReq(
      entryClass = jobDef.appMain,
      programArgs = if (jobDef.appArgs.isEmpty) None else Some(jobDef.appArgs.mkString(" ")),
      parallelism = jobDef.parallelism,
      savepointPath = jobDef.savepointRestore.map(_.savepointPath),
      restoreMode = jobDef.savepointRestore.map(_.restoreMode.toString),
      allowNonRestoredState = jobDef.savepointRestore.map(_.allowNonRestoredState)
    )
  }

}
