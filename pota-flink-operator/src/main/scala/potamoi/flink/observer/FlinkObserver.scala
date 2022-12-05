package potamoi.flink.observer

import potamoi.cluster.PotaActorSystem.ActorGuardian
import potamoi.common.ActorInteropException
import potamoi.config.PotaConf
import potamoi.k8s.K8sOperator
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
  def k8sRefs: K8sRefQuery
}

object FlinkObserver {
  val live: ZLayer[ActorGuardian with K8sOperator with PotaConf, ActorInteropException, FlinkObserver] = ZLayer {
    for {
      potaConf       <- ZIO.service[PotaConf]
      k8sOperator    <- ZIO.service[K8sOperator]
      guardian       <- ZIO.service[ActorGuardian]
      endpointQuery  <- RestEndpointQuery.live(potaConf, k8sOperator.client, guardian)
      savepointQuery <- SavepointTriggerQuery.live(potaConf.flink, endpointQuery)
      jobQuery       <- JobQuery.live(potaConf, guardian, endpointQuery)
      clusterQuery   <- ClusterQuery.live(potaConf, guardian, endpointQuery)
      k8sRefQuery    <- K8sRefQuery.live(potaConf, guardian, k8sOperator)
      trackerManager <- FlinkTrackManager.live(
        potaConf,
        guardian,
        k8sOperator.client,
        clusterQuery.ovTrackers,
        clusterQuery.tmDetailTrackers,
        clusterQuery.jmMetricTrackers,
        clusterQuery.tmMetricTrackers,
        jobQuery.ovTrackers,
        jobQuery.metricsTrackers,
        k8sRefQuery.k8sRefTrackers,
        k8sRefQuery.k8sPodMetricTrackers)
    } yield new FlinkObserver {
      val manager           = trackerManager
      val clusters          = clusterQuery
      val restEndpoints     = endpointQuery
      val savepointTriggers = savepointQuery
      val jobs              = jobQuery
      val k8sRefs           = k8sRefQuery
    }
  }

}
