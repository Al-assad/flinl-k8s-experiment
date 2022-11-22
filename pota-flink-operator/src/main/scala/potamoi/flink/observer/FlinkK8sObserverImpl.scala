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
import potamoi.flink.share.model.{Fcid, Fjid, FlinkSptTriggerStatus}
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
      trackDispatcher <- guardian.spawn(TrackersDispatcher(potaConf.log, potaConf.flink, observer, flinkRepo), "flinkTrackersDispatcher")
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
