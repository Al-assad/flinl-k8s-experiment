package potamoi.flink.observer

import akka.actor.typed.ActorSystem
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import potamoi.cluster.ActorGuardian
import potamoi.cluster.ActorGuardian.SpawnActor
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.conf.PotaConf
import potamoi.flink.share.{Fcid, FlinkRestSvcEndpoint, JobId}
import zio._
import zio.macros.accessible

/**
 * Flink on Kubernetes observer, responsible for handling flink cluster/job tracking, monitoring.
 */
@accessible
trait FlinkK8sObserver {

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   * Prioritize finding relevant records in DData cache, and call k8s api directly as fallback
   * when found nothing.
   * @param directly retrieve the endpoint via kubernetes api directly and reset the cache immediately.
   */
  def retrieveRestEndpoint(fcid: Fcid, directly: Boolean = false): IO[FlinkObrErr, FlinkRestSvcEndpoint]

  def trackCluster(fcid: Fcid): IO[FlinkObrErr, Unit] = ???

  def unTrackCluster(fcid: Fcid): IO[FlinkObrErr, Unit] = ???

  /**
   * Get all job id under the flink cluster.
   */
  def listJobIds(fcid: Fcid): IO[FlinkObrErr, Set[JobId]]

}

object FlinkK8sObserver {

  val live = ZLayer {
    for {
      potaConf  <- ZIO.service[PotaConf]
      k8sClient <- ZIO.service[Kubernetes]
      guardian  <- ZIO.service[ActorSystem[ActorGuardian.Cmd]]
      sc = guardian.scheduler
      restEptCache <- guardian.askZIO(SpawnActor("flinkRestEndpointCache", RestEptCache(potaConf.akka), _))(sc)
      jobOvCache   <- guardian.askZIO(SpawnActor("flinkJobOverviewCache", JobOverviewCache(potaConf.akka), _))(sc)
    } yield new FlinkK8sObserverLive(potaConf.flink, k8sClient, guardian, restEptCache, jobOvCache)
  }
}
