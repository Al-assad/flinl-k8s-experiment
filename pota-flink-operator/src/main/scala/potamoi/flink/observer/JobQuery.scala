package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.cluster.LWWMapDData
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.config.{DDataConf, PotaConf}
import potamoi.flink.observer.JobsTracker.{GetJobOverview, ListJobOverviews}
import potamoi.flink.share.model.JobState.JobState
import potamoi.flink.share.model.{Fcid, Fjid, FlinkJobOverview}
import potamoi.flink.share.{FlinkIO, FlinkOprErr, JobId}
import potamoi.timex._
import zio.stream.ZStream

/**
 * Flink jobs snapshot information query layer.
 */
trait JobQuery {
  def getOverview(fjid: Fjid): FlinkIO[Option[FlinkJobOverview]]
  def listOverview(fcid: Fcid): FlinkIO[Set[FlinkJobOverview]]
  def listAllOverview: FlinkIO[Set[FlinkJobOverview]]

  def listJobId(fcid: Fcid): FlinkIO[Set[JobId]]
  def listAllJobId: FlinkIO[Set[Fjid]]

  def getJobState(fjid: Fjid): FlinkIO[Option[JobState]]
  def listJobState(fcid: Fcid): FlinkIO[Map[JobId, JobState]]

//  def select(filter: (Fcid, FlinkJobOverview) => Boolean) = ???
  // filter, partitionHint, shape, pageable, sort
}

object JobQuery {

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
      extends JobQuery {

    private lazy val listAllFcid: FlinkIO[Set[Fcid]] = idxCache.listKeys.map(_.map(_.fcid).toSet)

    def getOverview(fjid: Fjid): FlinkIO[Option[FlinkJobOverview]] = trackers(fjid.fcid).ask(GetJobOverview(fjid.jobId, _))
    def listOverview(fcid: Fcid): FlinkIO[Set[FlinkJobOverview]]   = trackers(fcid).ask(ListJobOverviews)

    def listJobId(fcid: Fcid): FlinkIO[Set[JobId]] = idxCache.listKeys.map(_.map(_.jobId))
    def listAllJobId: FlinkIO[Set[Fjid]]           = idxCache.listKeys

    def getJobState(fjid: Fjid): FlinkIO[Option[JobState]] = idxCache.get(fjid).map(_.map(_.state))

    def listJobState(fcid: Fcid): FlinkIO[Map[JobId, JobState]] =
      idxCache.listAll.map {
        _.filter { case (k, _) => k.fcid == fcid }
          .map { case (k, v) => k.jobId -> v.state }
      }

    def listAllOverview: FlinkIO[Set[FlinkJobOverview]] =
      ZStream
        .fromIterableZIO[Any, FlinkOprErr, Fcid](listAllFcid)
        .mapZIOParUnordered(queryParallelism)(listOverview)
        .runFold(Set.empty[FlinkJobOverview])(_ ++ _)

  }
}

/**
 * Job overview query index cache.
 */
private[observer] object JobOvIndexCache extends LWWMapDData[Fjid, JobOvIndex] {
  val cacheId                               = "flink-job-ov-index"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)
}

case class JobOvIndex(state: JobState)
