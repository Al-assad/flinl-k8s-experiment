package potamoi.flink.observer

import potamoi.flink.share._
import zio.IO
import zio.macros.accessible

import scala.concurrent.duration.Duration

/**
 * Flink on Kubernetes observer, responsible for handling flink cluster/job tracking, monitoring.
 */
@accessible
trait FlinkK8sObserver {

  /**
   * Tracking flink cluster.
   */
  def trackCluster(fcid: Fcid): IO[FlinkObrErr, Unit]

  /**
   * UnTracking flink cluster.
   */
  def unTrackCluster(fcid: Fcid): IO[FlinkObrErr, Unit]

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   * Prioritize finding relevant records in DData cache, and call k8s api directly as fallback
   * when found nothing.
   * @param directly retrieve the endpoint via kubernetes api directly and reset the cache immediately.
   */
  def retrieveRestEndpoint(fcid: Fcid, directly: Boolean = false): IO[FlinkObrErr, FlinkRestSvcEndpoint]

  /**
   * Get current savepoint trigger status of the flink job.
   */
  def getSavepointTrigger(fjid: Fjid, triggerId: String): IO[FlinkObrErr, FlinkSptTriggerStatus]

  /**
   * Watch flink savepoint trigger until it was completed.
   */
  def watchSavepointTrigger(fjid: Fjid, triggerId: String, timeout: Duration = Duration.Inf): IO[FlinkObrErr, FlinkSptTriggerStatus]

}
