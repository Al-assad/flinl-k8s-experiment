package potamoi.flink.observer

import potamoi.config.FlinkConf
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.FlinkIO
import potamoi.flink.share.FlinkOprErr.WatchTimeout
import potamoi.flink.share.model.{Fjid, FlinkSptTriggerStatus}
import zio.ZIO.succeed
import zio.ZIOAspect.annotated
import potamoi.timex._
import zio.ZIO

import scala.concurrent.duration.Duration

/**
 * Flink Savepoint trigger observer.
 */
trait SavepointTriggerQuery {

  /**
   * Get current savepoint trigger status of the flink job.
   */
  def get(fjid: Fjid, triggerId: String): FlinkIO[FlinkSptTriggerStatus]

  /**
   * Watch flink savepoint trigger until it was completed.
   */
  def watch(fjid: Fjid, triggerId: String, timeout: Duration = Duration.Inf): FlinkIO[FlinkSptTriggerStatus]

}

object SavepointTriggerQuery {

  def live(flinkConf: FlinkConf, endpointQuery: RestEndpointQuery) = ZIO.succeed(Live(flinkConf, endpointQuery))

  /**
   * Implementation based on accessing api directly.
   */
  case class Live(flinkConf: FlinkConf, restEndpoint: RestEndpointQuery) extends SavepointTriggerQuery {
    implicit private val flkConf = flinkConf

    /**
     * Get trigger via rest api.
     */
    override def get(fjid: Fjid, triggerId: String): FlinkIO[FlinkSptTriggerStatus] = {
      for {
        restUrl <- restEndpoint.get(fjid.fcid).map(_.chooseUrl)
        rs      <- flinkRest(restUrl).getSavepointOperationStatus(fjid.jobId, triggerId)
      } yield rs
    } @@ annotated(fjid.toAnno :+ "triggerId" -> triggerId: _*)

    /**
     * Watch trigger by polling rest api.
     */
    override def watch(fjid: Fjid, triggerId: String, timeout: Duration): FlinkIO[FlinkSptTriggerStatus] = {
      for {
        restUrl <- restEndpoint.get(fjid.fcid).map(_.chooseUrl)
        rs <- flinkRest(restUrl)
          .getSavepointOperationStatus(fjid.jobId, triggerId)
          .repeatUntilZIO(r => if (r.isCompleted) succeed(true) else succeed(false).delay(flinkConf.tracking.savepointTriggerPolling))
          .timeoutFail(WatchTimeout)(timeout)
      } yield rs
    } @@ annotated(fjid.toAnno :+ "flink.triggerId" -> triggerId: _*)
  }

}
