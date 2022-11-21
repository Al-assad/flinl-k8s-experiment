package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import com.coralogix.zio.k8s.client.NotFound
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.config.PotaConf
import potamoi.flink.observer.FlinkObrErr.{ActorInteropErr, ClusterNotFound, DbInteropErr, RequestFlinkRestApiErr, TriggerTimeout}
import potamoi.flink.operator.flinkRest
import potamoi.flink.share._
import potamoi.flink.share.repo.FlinkRepoHub
import potamoi.k8s._
import potamoi.timex._
import zio.ZIO.{fail, logDebug, succeed}
import zio._

import scala.concurrent.duration.Duration

/**
 * Default FlinkK8sObserver implementation.
 */
object FlinkK8sObserverImpl {
  val live: ZLayer[FlinkRepoHub with ActorGuardian with K8sClient with PotaConf, Throwable, FlinkK8sObserver] = ZLayer {
    for {
      potaConf        <- ZIO.service[PotaConf]
      k8sClient       <- ZIO.service[K8sClient]
      flinkRepo       <- ZIO.service[FlinkRepoHub]
      guardian        <- ZIO.service[ActorGuardian]
      restEptCache    <- guardian.spawn(RestEptCache(potaConf.akka.ddata.getFlinkRestEndpoint), "flinkRestEndpointCache")
      observer        <- ZIO.succeed(new FlinkK8sObserverImpl(potaConf, k8sClient, flinkRepo, restEptCache)(guardian.scheduler))
      trackDispatcher <- guardian.spawn(TrackersDispatcher(potaConf.flink, observer, flinkRepo), "flinkTrackersDispatcher")
      _               <- ZIO.succeed(observer.bindTrackDispatcher(trackDispatcher))
    } yield observer
  }
}

class FlinkK8sObserverImpl(potaConf: PotaConf, k8sClient: K8sClient, flinkRepo: FlinkRepoHub, restEptCache: ActorRef[RestEptCache.Cmd])(
    implicit sc: Scheduler)
    extends FlinkK8sObserver {

  private var trackDispatcher: ActorRef[TrackersDispatcher.Cmd]                      = _
  private def bindTrackDispatcher(trackDispatcher: ActorRef[TrackersDispatcher.Cmd]) = this.trackDispatcher = trackDispatcher
  implicit private val flinkConf                                                     = potaConf.flink
  implicit private val askTimeout: Timeout                                           = potaConf.akka.defaultAskTimeout

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
    {
      (trackDispatcher !> TrackersDispatcher.UnTrack(fcid)) <*
      (restEptCache !> RestEptCache.Remove(fcid))
    }.mapError(ActorInteropErr) *>
    flinkRepo.jobOverview.cur.removeInCluster(fcid).mapError(DbInteropErr)
  }

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
        case NotFound => FlinkObrErr.ClusterNotFound(fcid)
        case failure  => FlinkObrErr.RequestK8sApiErr(failure, liftException(failure).orNull)
      }
      .tapBoth(
        { case ClusterNotFound(fcid) => restEptCache !!> RestEptCache.Remove(fcid) },
        endpoint => restEptCache !!> RestEptCache.Put(fcid, endpoint))
  }

  /**
   * Get current flink savepoint status by trigger-id.
   */
  override def getSavepointTrigger(fjid: Fjid, triggerId: JobId): IO[FlinkObrErr, FlinkSptTriggerStatus] = {
    for {
      restUrl <- retrieveRestEndpoint(fjid.fcid).map(_.chooseUrl)
      rs      <- flinkRest(restUrl).getSavepointOperationStatus(fjid.jobId, triggerId).mapError(RequestFlinkRestApiErr)
    } yield rs
  } @@ ZIOAspect.annotated(fjid.toAnno :+ "flink.triggerId" -> triggerId: _*)

  /**
   * Watch flink savepoint trigger until it was completed or reach timeout settings.
   */
  override def watchSavepointTrigger(fjid: Fjid, triggerId: String, timeout: Duration = Duration.Inf): IO[FlinkObrErr, FlinkSptTriggerStatus] = {
    for {
      restUrl <- retrieveRestEndpoint(fjid.fcid).map(_.chooseUrl)
      rs <- flinkRest(restUrl)
        .getSavepointOperationStatus(fjid.jobId, triggerId)
        .mapError(RequestFlinkRestApiErr)
        .repeatUntilZIO(r => if (r.isCompleted) succeed(true) else succeed(false).delay(flinkConf.tracking.savepointTriggerPolling))
        .timeoutFail(TriggerTimeout)(timeout)
    } yield rs
  } @@ ZIOAspect.annotated(fjid.toAnno :+ "flink.triggerId" -> triggerId: _*)

}
