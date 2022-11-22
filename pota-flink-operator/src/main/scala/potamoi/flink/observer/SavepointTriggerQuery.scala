package potamoi.flink.observer

import potamoi.config.FlinkConf
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.FlinkOprErr.RequestFlinkRestApiErr
import potamoi.flink.share.model.{Fjid, FlinkSptTriggerStatus}
import potamoi.flink.share.FlinkOprErr
import zio.IO

import scala.concurrent.duration.Duration

/**
 * Flink Savepoint trigger observer.
 */
trait SavepointTriggerQuery {

  /**
   * Get current savepoint trigger status of the flink job.
   */
  def get(fjid: Fjid, triggerId: String): IO[FlinkOprErr, FlinkSptTriggerStatus]

  /**
   * Watch flink savepoint trigger until it was completed.
   */
  def watch(fjid: Fjid, triggerId: String, timeout: Duration = Duration.Inf): IO[FlinkOprErr, FlinkSptTriggerStatus]

}

class SavepointTriggerQueryImpl(endpointQuery: RestEndpointQuery)(implicit flinkConfig: FlinkConf) extends SavepointTriggerQuery {

  override def get(fjid: Fjid, triggerId: String): IO[FlinkOprErr, FlinkSptTriggerStatus] = {
    for {
      restUrl <- endpointQuery.get(fjid.fcid).map(_.chooseUrl)
      rs      <- flinkRest(restUrl).getSavepointOperationStatus(fjid.jobId, triggerId).mapError(RequestFlinkRestApiErr)
    } yield ()
//    for {
//      restUrl <- endpointQuery.get(fjid.fcid)
//        map(_.chooseUrl)
//      rs      <- flinkRest(restUrl).getSavepointOperationStatus(fjid.jobId, triggerId).mapError(RequestFlinkRestApiErr)
//    } yield rs
  }

  override def watch(fjid: Fjid, triggerId: String, timeout: Duration): IO[FlinkOprErr, FlinkSptTriggerStatus] = ???

}
