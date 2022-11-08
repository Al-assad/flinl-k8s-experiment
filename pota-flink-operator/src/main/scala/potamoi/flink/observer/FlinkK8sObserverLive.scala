package potamoi.flink.observer

import akka.actor.typed.ActorSystem
import akka.util.Timeout
import com.coralogix.zio.k8s.client.NotFound
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import potamoi.cluster.ActorGuardian
import potamoi.cluster.PotaActorSystem.ActorGuardianExtension
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.conf.PotaConf
import potamoi.flink.observer.FlinkObrErr.{ActorInteropErr, ClusterNotFound, RequestFlinkRestApiErr}
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.{Fcid, FlinkRestSvcEndpoint, JobId}
import potamoi.k8s.{liftException, stringToK8sNamespace}
import zio.ZIO.{fail, logDebug, succeed}
import zio._

/**
 * Default FlinkK8sObserver implementation.
 */
class FlinkK8sObserverLive(potaConf: PotaConf, k8sClient: Kubernetes, guardian: ActorSystem[ActorGuardian.Cmd]) extends FlinkK8sObserver {

  implicit private val actorSc             = guardian.scheduler
  implicit private val askTimeout: Timeout = Timeout(potaConf.akka.defaultAskTimeout)

  // initialize actors unsafely
  private val restEptCache    = guardian.spawnNow("flinkRestEndpointCache", RestEptCache(potaConf.akka))
  private val jobOvCache      = guardian.spawnNow("flinkJobOverviewCache", JobOverviewCache(potaConf.akka))
  private val trackDispatcher = guardian.spawnNow("flinkTrackersDispatcher", TrackersDispatcher(potaConf.flink, jobOvCache, this))

  implicit private val flinkConf = potaConf.flink

  /**
   * Tracking flink cluster.
   */
  override def trackCluster(fcid: Fcid): IO[FlinkObrErr, Unit] = {
    (trackDispatcher !> TrackersDispatcher.Track(fcid)) <*
    retrieveRestEndpoint(fcid, directly = true).ignore
  }.mapError(ActorInteropErr)

  /**
   * Cancel tracking flink cluster.
   */
  override def unTrackCluster(fcid: Fcid): IO[FlinkObrErr, Unit] = {
    (trackDispatcher !> TrackersDispatcher.UnTrack(fcid)) <*
    (restEptCache !> RestEptCache.Remove(fcid)) <*
    (jobOvCache !> JobOverviewCache.RemoveRecordUnderFcid(fcid))
  }.mapError(ActorInteropErr)

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   * Prioritize finding relevant records in ddata, and call k8s api directly as fallback when found nothing.
   */
  override def retrieveRestEndpoint(fcid: Fcid, directly: Boolean = false): IO[FlinkObrErr, FlinkRestSvcEndpoint] = {
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

  private case object NotFoundRecordFromCache

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
        case failure  => FlinkObrErr.RequestK8sApiErr(failure, liftException(failure).orNull)
      }
      .tapBoth(
        { case ClusterNotFound(fcid) => restEptCache !!> RestEptCache.Remove(fcid) },
        endpoint => restEptCache !!> RestEptCache.Put(fcid, endpoint))
  }

  /**
   * Get all job id under the flink cluster.
   */
  override def listJobIds(fcid: Fcid): IO[FlinkObrErr, Set[JobId]] = {
    val fromCache = jobOvCache.?>(JobOverviewCache.ListJobIdUnderFcid(fcid, _)).mapError(ActorInteropErr)
    val fromRestApi = retrieveRestEndpoint(fcid).flatMap { endpoint =>
      flinkRest(endpoint.chooseUrl).listJobsStatusInfo.mapBoth(RequestFlinkRestApiErr, _.map(_.id).toSet)
    }
    fromCache.catchAll { err =>
      logDebug(s"Fallback to requesting flink rest api directly due to $err") *>
      fromRestApi
    }
  }

}
