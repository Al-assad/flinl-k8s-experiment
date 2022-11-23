package potamoi.flink.observer

import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.common.ActorInteropException
import potamoi.config.PotaConf
import potamoi.k8s.K8sClient
import potamoi.timex._
import zio.{ZIO, ZLayer}

/**
 * Flink cluster on kubernetes observer.
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
      potaConf        <- ZIO.service[PotaConf]
      k8sClient       <- ZIO.service[K8sClient]
      guardian        <- ZIO.service[ActorGuardian]
      clusterIdsCache <- guardian.spawn(TrackClusterIdsCache(potaConf.akka.ddata.getFlinkClusterIds), "flinkTrackClusterCache")

      //  RestEndpointQuery
      restEptCache <- guardian.spawn(RestEndpointCache(potaConf.akka.ddata.getFlinkRestEndpoint), "flkRestEndpointCache")
      queryTimeout  = potaConf.flink.queryAskTimeout
      endpointQuery = RestEndpointQuery.Live(k8sClient, restEptCache)(guardian.scheduler, queryTimeout)

      // SavepointTriggerQuery
      savepointQuery = SavepointTriggerQuery.Live(potaConf.flink, endpointQuery)

      // JobOverviewQuery
      jobOvIdxCache <- guardian.spawn(JobOvIndexCache(potaConf.akka.ddata.getFlinkJobsOvIndex), "flkJobOvIndexCache")
      jobTrackerDispatcher <- guardian.spawn(
        JobsTrackDispatcher(potaConf.log, potaConf.flink, jobOvIdxCache, endpointQuery),
        "flkJobsTrackDispatcher")
      jobOverviewQuery = JobOverviewQuery.Live(jobTrackerDispatcher, jobOvIdxCache)(guardian.scheduler, queryTimeout)

      // tracker manager
      trackerManager = FlinkTrackManager.Live(clusterIdsCache, jobTrackerDispatcher)(guardian.scheduler, queryTimeout)

    } yield new FlinkObserver {
      val tracker          = trackerManager
      val restEndpoint     = endpointQuery
      val savepointTrigger = savepointQuery
      val jobOverview      = jobOverviewQuery
    }
  }
}
