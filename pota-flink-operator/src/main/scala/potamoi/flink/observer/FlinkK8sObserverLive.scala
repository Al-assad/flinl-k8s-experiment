package potamoi.flink.observer

import akka.actor.typed.ActorSystem
import com.coralogix.zio.k8s.client.NotFound
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import potamoi.cluster.ActorGuardian
import potamoi.conf.FlinkConf
import potamoi.flink.share.{Fcid, FlinkRestSvcEndpoint}
import potamoi.k8s.stringToK8sNamespace
import zio.IO

/**
 * Default FlinkK8sObserver implementation.
 */
class FlinkK8sObserverLive(flinkConf: FlinkConf, k8sClient: Kubernetes, guardian: ActorSystem[ActorGuardian.Cmd]) extends FlinkK8sObserver {



  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  override def retrieveRestEndpoint(fcid: Fcid, directly: Boolean): IO[FlinkObrErr, FlinkRestSvcEndpoint] = {
    if (directly) {}
    ???
  }

  private def retrieveRestEndpointViaK8s(fcid: Fcid): IO[FlinkObrErr, FlinkRestSvcEndpoint] = {
    k8sClient.v1.services
      .get(s"${fcid.clusterId}-rest", fcid.namespace)
      .flatMap { svc =>
        for {
          metadata  <- svc.getMetadata
          name      <- metadata.getName
          ns        <- metadata.getNamespace
          spec      <- svc.getSpec
          clusterIp <- spec.getClusterIP
          ports     <- spec.getPorts
          restPort = ports
            .find(_.port == 8081)
            .flatMap(_.targetPort.map(_.value.fold(identity, _.toInt)).toOption)
            .getOrElse(8081)
        } yield FlinkRestSvcEndpoint(name, ns, restPort, clusterIp)
      }
      .mapError {
        case NotFound => FlinkObrErr.ClusterNotFound(fcid)
        case failure  => FlinkObrErr.RequestK8sApiErr(failure)
      }
  }

}
