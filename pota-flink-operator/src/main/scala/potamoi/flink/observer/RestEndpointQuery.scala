package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import com.coralogix.zio.k8s.client.NotFound
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.flink.share.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.share.FlinkOprErr
import potamoi.k8s.{liftException, K8sClient}
import zio.{IO, ZIOAspect}
import potamoi.k8s._
import zio.ZIO.{fail, logDebug, succeed}

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
  def get(fcid: Fcid, directly: Boolean = false): IO[FlinkOprErr, FlinkRestSvcEndpoint]

  /**
   * Similar to [[get]], but returns an Option instead of the ClusterNotFound error.
   */
  def retrieve(fcid: Fcid, directly: Boolean = false): IO[FlinkOprErr, Option[FlinkRestSvcEndpoint]]

}

case class RestEndpointQueryImpl(k8sClient: K8sClient, restEptCache: ActorRef[RestEptCache.Cmd])(implicit sc: Scheduler, queryTimeout: Timeout)
    extends RestEndpointQuery {

  private case object NotFoundRecordFromCache

  override def retrieve(fcid: Fcid, directly: Boolean): IO[FlinkOprErr, Option[FlinkRestSvcEndpoint]] =
    get(fcid, directly)
      .map(Some(_))
      .catchSome { case FlinkOprErr.ClusterNotFound(_) => succeed(None) }

  override def get(fcid: Fcid, directly: Boolean): IO[FlinkOprErr, FlinkRestSvcEndpoint] = {
    if (directly) retrieveRestEndpointViaK8s(fcid: Fcid)
    else
      (restEptCache ?> (RestEptCache.Get(fcid, _)))
        .flatMap {
          case Some(r) => succeed(r)
          case None    => fail(NotFoundRecordFromCache)
        }
        .catchAll { err =>
          logDebug(s"Fallback to requesting k8s svc api directly due to $err") *>
          retrieveRestEndpointViaK8s(fcid)
        }
  } @@ ZIOAspect.annotated(fcid.toAnno: _*)

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
      .tapBoth(
        { case FlinkOprErr.ClusterNotFound(fcid) => restEptCache !!> RestEptCache.Remove(fcid) },
        endpoint => restEptCache !!> RestEptCache.Put(fcid, endpoint))
  }

}
