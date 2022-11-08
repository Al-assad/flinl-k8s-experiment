package potamoi.flink.observer

import akka.actor.typed.ActorSystem
import akka.util.Timeout
import com.coralogix.zio.k8s.client.NotFound
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import potamoi.cluster.ActorGuardian
import potamoi.cluster.PotaActorSystem.ActorGuardianExtension
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.common.ZIOExtension.scalaDurationToZIO
import potamoi.conf.PotaConf
import potamoi.flink.observer.FlinkObrErr.{ActorInteropErr, ClusterNotFound, RequestFlinkRestApiErr, TriggerTimeout}
import potamoi.flink.operator.flinkRest
import potamoi.flink.share._
import potamoi.k8s.{liftException, stringToK8sNamespace}
import zio.ZIO.{fail, logDebug, succeed}
import zio._

import scala.concurrent.duration.Duration

/**
 * Default FlinkK8sObserver implementation.
 */
class FlinkK8sObserverLive(potaConf: PotaConf, k8sClient: Kubernetes, guardian: ActorSystem[ActorGuardian.Cmd]) extends FlinkK8sObserver {

  implicit private val actorSc             = guardian.scheduler
  implicit private val askTimeout: Timeout = Timeout(potaConf.akka.defaultAskTimeout)

  // initialize actors unsafely
  private val restEptCache    = guardian.spawnNow("flinkRestEndpointCache", RestEptCache(potaConf.akka))
  private val jobStatusCache  = guardian.spawnNow("flinkJobStatusCache", JobStatusCache(potaConf.akka))
  private val trackDispatcher = guardian.spawnNow("flinkTrackersDispatcher", TrackersDispatcher(potaConf.flink, jobStatusCache, this))

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
    (jobStatusCache !> JobStatusCache.RemoveRecordUnderFcid(fcid))
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
   * Get flink job status
   */
  override def getJobStatus(fjid: Fjid): IO[FlinkObrErr, Option[FlinkJobStatus]] = {
    jobStatusCache ?> (JobStatusCache.Get(fjid, _))
  } @@ ZIOAspect.annotated(fjid.toAnno: _*)

  /**
   * Get all flink job status under cluster.
   */
  override def listJobStatus(fcid: Fcid): IO[FlinkObrErr, Vector[FlinkJobStatus]] = {
    jobStatusCache ?> (JobStatusCache.ListRecordUnderFcid(fcid, _))
  } @@ ZIOAspect.annotated(fcid.toAnno: _*)

  /**
   * Select flink job status via custom filter function.
   */
  override def selectJobStatus(
      filter: (Fjid, FlinkJobStatus) => Boolean,
      drop: Option[RuntimeFlags],
      take: Option[RuntimeFlags]): IO[FlinkObrErr, Vector[FlinkJobStatus]] = {
    jobStatusCache ?> (JobStatusCache.SelectRecord(filter, drop, take, _))
  }

  /**
   * Get all job id under the flink cluster.
   */
  override def listJobIds(fcid: Fcid): IO[FlinkObrErr, Vector[JobId]] = {
    val fromCache = jobStatusCache.?>(JobStatusCache.ListJobIdUnderFcid(fcid, _)).mapError(ActorInteropErr)
    val fromRestApi = retrieveRestEndpoint(fcid).flatMap { endpoint =>
      flinkRest(endpoint.chooseUrl).listJobsStatusInfo.mapBoth(RequestFlinkRestApiErr, _.map(_.id))
    }
    fromCache
      .flatMap(r => if (r.isEmpty) fromRestApi else succeed(r))
      .catchAll { err =>
        logDebug(s"Fallback to requesting flink rest api directly due to $err") *>
        fromRestApi
      }
  } @@ ZIOAspect.annotated(fcid.toAnno: _*)

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
        .repeatUntilZIO(r => if (r.isCompleted) succeed(true) else succeed(false).delay(flinkConf.trackingSptTriggerPollInterval))
        .timeoutFail(TriggerTimeout)(timeout)
    } yield rs
  } @@ ZIOAspect.annotated(fjid.toAnno :+ "flink.triggerId" -> triggerId: _*)

}
