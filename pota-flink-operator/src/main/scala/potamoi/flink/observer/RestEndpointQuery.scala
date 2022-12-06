package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import com.coralogix.zio.k8s.client.NotFound
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.config.PotaConf
import potamoi.flink.share.FlinkOprErr.{ActorInteropErr, ClusterNotFound}
import potamoi.flink.share.cache.FlinkRestEndpointCache
import potamoi.flink.share.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.share.{FlinkIO, FlinkOprErr}
import potamoi.k8s._
import potamoi.timex._
import zio.ZIO.succeed
import zio.{IO, ZIOAspect}

/**
 * Flink cluster rest endpoint snapshot query layer.
 */
trait RestEndpointQuery {

  /**
   * Get Flink rest endpoint via kubernetes api.
   * Prioritize finding relevant records in DData cache, and call k8s api directly as fallback
   * when found nothing.
   *
   * @param directly retrieve the endpoint via kubernetes api directly and reset the cache immediately.
   */
  def get(fcid: Fcid, directly: Boolean = false): FlinkIO[FlinkRestSvcEndpoint]

  /**
   * Similar to [[get]], but returns an Option instead of the ClusterNotFound error.
   */
  def retrieve(fcid: Fcid, directly: Boolean = false): FlinkIO[Option[FlinkRestSvcEndpoint]]

}

object RestEndpointQuery {

  def live(potaConf: PotaConf, k8sClient: K8sClient, guardian: ActorGuardian) =
    for {
      restEptCache <- guardian.spawn(FlinkRestEndpointCache(potaConf.akka.ddata.getFlinkRestEndpoint), "flkRestEndpointCache")
      queryTimeout = potaConf.flink.snapshotQuery.askTimeout
      sc           = guardian.scheduler
    } yield Live(k8sClient, restEptCache)(sc, queryTimeout)

  /**
   * Implementation based on Akka DData as cache.
   */
  case class Live(k8sClient: K8sClient, restEptCache: ActorRef[FlinkRestEndpointCache.Cmd])(implicit sc: Scheduler, queryTimeout: Timeout)
      extends RestEndpointQuery {

    /**
     * Get Flink rest endpoint.
     */
    override def retrieve(fcid: Fcid, directly: Boolean): FlinkIO[Option[FlinkRestSvcEndpoint]] = {
      get(fcid, directly).map(Some(_)).catchSome { case ClusterNotFound(_) => succeed(None) }
    } @@ ZIOAspect.annotated(fcid.toAnno: _*)

    /**
     * Get Flink rest endpoint.
     */
    override def get(fcid: Fcid, directly: Boolean): FlinkIO[FlinkRestSvcEndpoint] = {
      if (directly) retrieveRestEndpointViaK8s(fcid: Fcid)
      else
        restEptCache.get(fcid).mapError(ActorInteropErr).flatMap {
          case Some(ept) => succeed(ept)
          case None      => retrieveRestEndpointViaK8s(fcid: Fcid)
        }
    } @@ ZIOAspect.annotated(fcid.toAnno: _*)

    /**
     * Retrieve Flink endpoint via kubernetes api.
     */
    private def retrieveRestEndpointViaK8s(fcid: Fcid): IO[FlinkOprErr, FlinkRestSvcEndpoint] = {
      k8sClient.api.v1.services
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
          case NotFound => FlinkOprErr.ClusterNotFound(fcid)
          case failure  => FlinkOprErr.RequestK8sApiErr(failure, liftException(failure).orNull)
        }
    }
  }
}
