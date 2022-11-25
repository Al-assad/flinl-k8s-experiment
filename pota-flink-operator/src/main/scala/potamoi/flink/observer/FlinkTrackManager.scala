package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.flink.share.FlinkIO
import potamoi.flink.share.model.Fcid
import potamoi.actorx._
import potamoi.timex._
import potamoi.cluster.ORSetDData
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.config.{DDataConf, PotaConf}

/**
 * Flink trackers manager.
 */
trait FlinkTrackManager {

  /**
   * Tracking flink cluster.
   */
  def trackCluster(fcid: Fcid): FlinkIO[Unit]

  /**
   * UnTracking flink cluster.
   */
  def untrackCluster(fcid: Fcid): FlinkIO[Unit]

  /**
   * Listing tracked cluster id.
   */
  def listTrackedCluster: FlinkIO[Set[Fcid]]

}

object FlinkTrackManager {

  def live(potaConf: PotaConf, guardian: ActorGuardian, jobsTrackers: ActorRef[JobsTrackerProxy.Cmd]) =
    for {
      clusterIdsCache <- guardian.spawn(TrackClusterIdsCache(potaConf.akka.ddata.getFlinkClusterIds), "flkTrackClusterCache")
      queryTimeout = potaConf.flink.snapshotQuery.askTimeout
      sc           = guardian.scheduler
    } yield Live(clusterIdsCache, jobsTrackers)(sc, queryTimeout)

  /**
   * Implementation based on Akka infra.
   */
  case class Live(
      clusterIdsCache: ActorRef[TrackClusterIdsCache.Cmd],
      jobsTrackers: ActorRef[JobsTrackerProxy.Cmd]
    )(implicit sc: Scheduler,
      queryTimeout: Timeout)
      extends FlinkTrackManager {

    override def trackCluster(fcid: Fcid): FlinkIO[Unit] = {
      clusterIdsCache.tellZIO(TrackClusterIdsCache.Put(fcid)) *>
      jobsTrackers.tellZIO(JobsTrackerProxy.Proxy(fcid, JobsTracker.Start))
    }

    override def untrackCluster(fcid: Fcid): FlinkIO[Unit] = {
      clusterIdsCache.tellZIO(TrackClusterIdsCache.Remove(fcid)) *>
      jobsTrackers.tellZIO(JobsTrackerProxy.Proxy(fcid, JobsTracker.Stop))
    }

    override def listTrackedCluster: FlinkIO[Set[Fcid]] = {
      clusterIdsCache.askZIO(TrackClusterIdsCache.List)
    }
  }
}

/**
 * Tracked Flink cluster id distributed data storage base on ORSet.
 */
private[observer] object TrackClusterIdsCache extends ORSetDData[Fcid] {
  val cacheId                               = "flink-cluster-id"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)()
}
