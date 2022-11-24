package potamoi.flink.observer

import potamoi.cluster.PotaActorSystem.ActorGuardian
import potamoi.common.ActorInteropException
import potamoi.config.PotaConf
import potamoi.k8s.K8sClient
import zio.{ZIO, ZLayer}

/**
 * Flink cluster on kubernetes observer.
 * The service needs to be loaded eagerly.
 */
trait FlinkObserver {
  def tracker: FlinkTrackManager
  def restEndpoint: RestEndpointQuery
  def savepointTrigger: SavepointTriggerQuery
  def jobOverview: JobOverviewQuery
}

object FlinkObserver {
  val live: ZLayer[ActorGuardian with K8sClient with PotaConf, ActorInteropException, FlinkObserver] = ZLayer {
    for {
      potaConf         <- ZIO.service[PotaConf]
      k8sClient        <- ZIO.service[K8sClient]
      guardian         <- ZIO.service[ActorGuardian]
      endpointQuery    <- RestEndpointQuery.live(potaConf, k8sClient, guardian)
      savepointQuery   <- SavepointTriggerQuery.live(potaConf.flink, endpointQuery)
      jobOverviewQuery <- JobOverviewQuery.live(potaConf, guardian, endpointQuery)
      trackerManager   <- FlinkTrackManager.live(potaConf, guardian, jobOverviewQuery.trackers)
    } yield new FlinkObserver {
      val tracker          = trackerManager
      val restEndpoint     = endpointQuery
      val savepointTrigger = savepointQuery
      val jobOverview      = jobOverviewQuery
    }
  }

}
