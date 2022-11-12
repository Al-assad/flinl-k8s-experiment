package potamoi.flink.observer

import akka.actor.typed.ActorSystem
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import potamoi.cluster.ActorGuardian
import potamoi.conf.PotaConf
import potamoi.flink.share._
import zio.macros.accessible
import zio.{IO, ZIO, ZLayer}

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
   * Get all job id under the flink cluster.
   */
  def listJobIds(fcid: Fcid): IO[FlinkObrErr, Vector[JobId]]

  /**
   * Get flink job status
   */
  def getJobStatus(fjid: Fjid): IO[FlinkObrErr, Option[FlinkJobStatus]]

  /**
   * Get all flink job status under cluster.
   */
  def listJobStatus(fcid: Fcid): IO[FlinkObrErr, Vector[FlinkJobStatus]]

  /**
   * Select flink job status via custom filter function.
   */
  def selectJobStatus(
      filter: (Fjid, FlinkJobStatus) => Boolean,
      drop: Option[Int] = None,
      take: Option[Int] = None): IO[FlinkObrErr, Vector[FlinkJobStatus]]

  /**
   * Get current savepoint trigger status of the flink job.
   */
  def getSavepointTrigger(fjid: Fjid, triggerId: String): IO[FlinkObrErr, FlinkSptTriggerStatus]

  /**
   * Watch flink savepoint trigger until it was completed.
   */
  def watchSavepointTrigger(fjid: Fjid, triggerId: String, timeout: Duration = Duration.Inf): IO[FlinkObrErr, FlinkSptTriggerStatus]

}

object FlinkK8sObserver {

  val live = ZLayer {
    for {
      potaConf  <- ZIO.service[PotaConf]
      k8sClient <- ZIO.service[Kubernetes]
      guardian  <- ZIO.service[ActorSystem[ActorGuardian.Cmd]]
    } yield new FlinkK8sObserverLive(potaConf, k8sClient, guardian)
  }

}