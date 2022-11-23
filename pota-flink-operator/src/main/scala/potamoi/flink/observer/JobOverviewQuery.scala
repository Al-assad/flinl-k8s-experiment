package potamoi.flink.observer

import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.cluster.LWWMapDData
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.config.DDataConf
import potamoi.flink.share.model.JobState.JobState
import potamoi.flink.share.model.{Fcid, Fjid, FlinkJobOverview}
import potamoi.flink.share.{FlinkIO, JobId}

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
  val serviceKey                            = ServiceKey[Cmd](cacheId + "-cache")
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf, Some(serviceKey))()
}

case class JobOvIndex(state: JobState)
