package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.cluster.LWWMapDData
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.config.{DDataConf, PotaConf}
import potamoi.flink.share.model.JobState.JobState
import potamoi.flink.share.model.{Fcid, Fjid, FlinkJobOverview}
import potamoi.flink.share.{FlinkIO, JobId}
import potamoi.timex._

/**
 * Flink jobs overview snapshot query layer.
 */
trait JobOverviewQuery {

  /**
   * Get flink job status
   */
  def get(fjid: Fjid): FlinkIO[Option[FlinkJobOverview]]

  /**
   * Get all job id under the flink cluster.
   */
  def listJobIds(fcid: Fcid): FlinkIO[Set[JobId]]

  /**
   * Get all flink job status under cluster.
   */
  def listInCluster(fcid: Fcid): FlinkIO[Set[FlinkJobOverview]]

}

object JobOverviewQuery {

  def live(potaConf: PotaConf, guardian: ActorGuardian, endpointQuery: RestEndpointQuery) =
    for {
      idxCache          <- guardian.spawn(JobOvIndexCache(potaConf.akka.ddata.getFlinkJobsOvIndex), "flkJobOvIndexCache")
      trackerDispatcher <- guardian.spawn(JobsTrackDispatcher(potaConf, idxCache, endpointQuery), "flkJobsTrackDispatcher")
      queryTimeout = potaConf.flink.queryAskTimeout
      sc           = guardian.scheduler
    } yield Live(trackerDispatcher, idxCache)(sc, queryTimeout)

  /**
   * Akka Sharding/DData hybrid storage implementation.
   */
  case class Live(trackers: ActorRef[JobsTrackDispatcher.Cmd], idxCache: ActorRef[JobOvIndexCache.Cmd])(implicit sc: Scheduler, queryTimeout: Timeout)
      extends JobOverviewQuery {

    override def get(fjid: Fjid): FlinkIO[Option[FlinkJobOverview]] = {
      trackers.askZIO(JobsTrackDispatcher.GetJobOv(fjid, _))
    }

    override def listJobIds(fcid: Fcid): FlinkIO[Set[JobId]] = {
      idxCache.askZIO(JobOvIndexCache.SelectKeys(_.fcid == fcid, _)).map(_.map(_.jobId))
    }

    override def listInCluster(fcid: Fcid): FlinkIO[Set[FlinkJobOverview]] = {
      trackers.askZIO(JobsTrackDispatcher.ListJobOvInCluster(fcid, _))
    }
  }
}

/**
 * Job overview query index cache.
 */
private[observer] object JobOvIndexCache extends LWWMapDData[Fjid, JobOvIndex] {
  val cacheId                               = "flink-job-ov-index"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)()
}

case class JobOvIndex(state: JobState)
