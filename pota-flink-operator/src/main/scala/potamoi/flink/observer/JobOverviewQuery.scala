package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.cluster.LWWMapDData
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.config.{DDataConf, PotaConf}
import potamoi.flink.observer.JobsTrackerProxy.Proxy
import potamoi.flink.share.FlinkOprErr.ActorInteropErr
import potamoi.flink.share.model.JobState.JobState
import potamoi.flink.share.model.{Fcid, Fjid, FlinkJobOverview}
import potamoi.flink.share.{FlinkIO, JobId}
import potamoi.timex._
import zio.stream.ZStream

/**
 * Flink jobs overview snapshot query layer.
 */
trait JobOverviewQuery {

  /**
   * Get flink job overview
   */
  def get(fjid: Fjid): FlinkIO[Option[FlinkJobOverview]]

  /**
   * Get all flink job status under cluster.
   */
  def list(fcid: Fcid): FlinkIO[Set[FlinkJobOverview]]

  /**
   * List all job overview in tracking.
   */
  def listAll: FlinkIO[Set[FlinkJobOverview]]

  /**
   * Get all job id under the flink cluster.
   */
  def listJobIds(fcid: Fcid): FlinkIO[Set[JobId]]

  /**
   * List all job id in tracking.
   */
  def listAllJobIds: FlinkIO[Set[Fjid]]

}

object JobOverviewQuery {

  def live(potaConf: PotaConf, guardian: ActorGuardian, endpointQuery: RestEndpointQuery) =
    for {
      idxCache      <- guardian.spawn(JobOvIndexCache(potaConf.akka.ddata.getFlinkJobsOvIndex), "flkJobOvIndexCache")
      trackersProxy <- guardian.spawn(JobsTrackerProxy(potaConf, endpointQuery), "flkJobsTrackerProxy")
      queryTimeout     = potaConf.flink.snapshotQuery.askTimeout
      queryParallelism = potaConf.flink.snapshotQuery.parallelism
      sc               = guardian.scheduler
    } yield Live(trackersProxy, idxCache, queryParallelism)(sc, queryTimeout)

  /**
   * Akka Sharding/DData hybrid storage implementation.
   */
  case class Live(
      trackers: ActorRef[JobsTrackerProxy.Cmd],
      idxCache: ActorRef[JobOvIndexCache.Cmd],
      queryParallelism: Int
    )(implicit sc: Scheduler,
      queryTimeout: Timeout)
      extends JobOverviewQuery {

    override def get(fjid: Fjid): FlinkIO[Option[FlinkJobOverview]] =
      trackers.askZIO[Option[FlinkJobOverview]] { ref =>
        Proxy(fjid.fcid, JobsTracker.GetJobOverview(fjid.jobId, ref))
      }

    override def list(fcid: Fcid): FlinkIO[Set[FlinkJobOverview]] = {
      trackers.askZIO[Set[FlinkJobOverview]] { ref =>
        Proxy(fcid, JobsTracker.ListJobOverviews(ref))
      }
    }

    override def listAll: FlinkIO[Set[FlinkJobOverview]] = {
      val listAllFcid: FlinkIO[Set[Fcid]] = idxCache.askZIO(JobOvIndexCache.ListFcid).mapError(ActorInteropErr)
      ZStream
        .fromIterableZIO(listAllFcid)
        .mapZIOParUnordered(queryParallelism)(list)
        .runFold(Set.empty[FlinkJobOverview])(_ ++ _)
    }

    override def listJobIds(fcid: Fcid): FlinkIO[Set[JobId]] =
      idxCache
        .askZIO(JobOvIndexCache.SelectKeys(_.fcid == fcid, _))
        .map(_.map(_.jobId))

    override def listAllJobIds: FlinkIO[Set[Fjid]] = idxCache.askZIO(JobOvIndexCache.ListKeys)
  }
}

/**
 * Job overview query index cache.
 */
private[observer] object JobOvIndexCache extends LWWMapDData[Fjid, JobOvIndex] {
  val cacheId = "flink-job-ov-index"
  final case class ListFcid(reply: ActorRef[Set[Fcid]]) extends GetCmd

  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)(
    get = { case (ListFcid(reply), cache) => reply ! cache.entries.keys.map(_.fcid).toSet },
    defaultNotFound = { case ListFcid(reply) => reply ! Set.empty }
  )
}

case class JobOvIndex(state: JobState)
