package potamoi.flink.observer

import akka.actor.typed.ActorSystem
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import potamoi.cluster.ActorGuardian
import potamoi.conf.PotaConf
import potamoi.flink.share.{Fcid, FlinkRestSvcEndpoint}
import zio._
import zio.macros.accessible

/**
 * Flink on Kubernetes observer, responsible for handling flink cluster/job tracking, monitoring.
 */
@accessible
trait FlinkK8sObserver {

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   * @param directly retrieve the endpoint via kubernetes api directly and reset the cache immediately.
   */
  def retrieveRestEndpoint(fcid: Fcid, directly: Boolean = false): IO[FlinkObrErr, FlinkRestSvcEndpoint]

}

object FlinkK8sObserver {
  val live = ZLayer {
    for {
      potaConf  <- ZIO.service[PotaConf]
      k8sClient <- ZIO.service[Kubernetes]
      guardian  <- ZIO.service[ActorSystem[ActorGuardian.Cmd]]
    } yield new FlinkK8sObserverLive(potaConf.flink, k8sClient, guardian)
  }
}
