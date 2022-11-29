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
  def manager: FlinkTrackManager
  def clusters: ClusterQuery
  def jobs: JobQuery
  def restEndpoints: RestEndpointQuery
  def savepointTriggers: SavepointTriggerQuery
}

object FlinkObserver {
  val live: ZLayer[ActorGuardian with K8sClient with PotaConf, ActorInteropException, FlinkObserver] = ZLayer {
    for {
      potaConf       <- ZIO.service[PotaConf]
      k8sClient      <- ZIO.service[K8sClient]
      guardian       <- ZIO.service[ActorGuardian]
      endpointQuery  <- RestEndpointQuery.live(potaConf, k8sClient, guardian)
      savepointQuery <- SavepointTriggerQuery.live(potaConf.flink, endpointQuery)
      jobQuery       <- JobQuery.live(potaConf, guardian, endpointQuery)
      clusterQuery   <- ClusterQuery.live(potaConf, guardian, endpointQuery)
      trackerManager <- FlinkTrackManager.live(potaConf, guardian, clusterQuery.ovTrackers, clusterQuery.tmDetailTrackers, jobQuery.trackers)
    } yield new FlinkObserver {
      val manager           = trackerManager
      val clusters          = clusterQuery
      val restEndpoints     = endpointQuery
      val savepointTriggers = savepointQuery
      val jobs              = jobQuery
    }
  }

}
