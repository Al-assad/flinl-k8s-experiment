package potamoi.flink.observer

import potamoi.flink.share._
import potamoi.flink.share.model.{Fcid, Fjid, FlinkJobOverview, FlinkSptTriggerStatus}
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
   * Get current savepoint trigger status of the flink job.
   */
  def getSavepointTrigger(fjid: Fjid, triggerId: String): IO[FlinkObrErr, FlinkSptTriggerStatus]

  /**
   * Watch flink savepoint trigger until it was completed.
   */
  def watchSavepointTrigger(fjid: Fjid, triggerId: String, timeout: Duration = Duration.Inf): IO[FlinkObrErr, FlinkSptTriggerStatus]

}

object FlinkK8sObserver {}

trait FlinkJobOverviewQuery {

  /**
   * Get flink job status
   */
  def get(fjid: Fjid): IO[FlinkObrErr, Option[FlinkJobOverview]]

  /**
   * Get all job id under the flink cluster.
   */
  def listJobIds(fcid: Fcid): IO[FlinkObrErr, Vector[JobId]]

  /**
   * Get all flink job status under cluster.
   */
  def listInCluster(fcid: Fcid): IO[FlinkObrErr, Vector[FlinkJobOverview]]

}
