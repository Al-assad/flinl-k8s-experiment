package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.flink.observer.FlinkObrErr.ActorInteropErr
import potamoi.flink.share.model.{Fcid, Fjid, FlinkJobOverview}
import potamoi.flink.share.JobId
import zio.IO

/**
 * Flink jobs overview snapshot query layer.
 */
trait JobOverviewQuery {

  /**
   * Get flink job status
   */
  def get(fjid: Fjid): IO[FlinkObrErr, Option[FlinkJobOverview]]

  /**
   * Get all job id under the flink cluster.
   */
  def listJobIds(fcid: Fcid): IO[FlinkObrErr, Set[JobId]]

  /**
   * Get all flink job status under cluster.
   */
  def listInCluster(fcid: Fcid): IO[FlinkObrErr, Set[FlinkJobOverview]]

}

/**
 * Akka Sharding/DData hybrid storage implementation.
 */
case class JobOverviewQueryImpl(trackers: ActorRef[JobsTraceDispatcher.Cmd], idxCache: ActorRef[JobOvIndexCache.Cmd])(
    implicit sc: Scheduler,
    queryTimeout: Timeout)
    extends JobOverviewQuery {

  override def get(fjid: Fjid): IO[FlinkObrErr, Option[FlinkJobOverview]] =
    trackers
      .askZIO(JobsTraceDispatcher.GetJobOv(fjid, _))
      .mapError(ActorInteropErr)

  override def listJobIds(fcid: Fcid): IO[FlinkObrErr, Set[JobId]] =
    idxCache
      .askZIO(JobOvIndexCache.SelectKeys(_.fcid == fcid, _))
      .mapBoth(ActorInteropErr, _.map(_.jobId))

  override def listInCluster(fcid: Fcid): IO[FlinkObrErr, Set[FlinkJobOverview]] =
    trackers
      .askZIO(JobsTraceDispatcher.ListJobOvInCluster(fcid, _))
      .mapError(ActorInteropErr)

}
