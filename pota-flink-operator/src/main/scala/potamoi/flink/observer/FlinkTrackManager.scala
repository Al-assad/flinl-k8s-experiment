package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.flink.share.FlinkIO
import potamoi.flink.share.model.Fcid
import potamoi.actorx._
import potamoi.cluster.ORSetDData
import potamoi.config.DDataConf

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
  def unTrackCluster(fcid: Fcid): FlinkIO[Unit]

  /**
   * Listing tracked cluster id.
   */
  def listTrackedCluster: FlinkIO[Set[Fcid]]

}

object FlinkTrackManager {

  /**
   * Implementation based on Akka infra.
   */
  case class Live(clusterIdsCache: ActorRef[TrackClusterIdsCache.Cmd], jobsTrackers: ActorRef[JobsTrackDispatcher.Cmd])(
      implicit sc: Scheduler,
      queryTimeout: Timeout)
      extends FlinkTrackManager {

    override def trackCluster(fcid: Fcid): FlinkIO[Unit] = {
      clusterIdsCache.tellZIO(TrackClusterIdsCache.Put(fcid)) *>
      jobsTrackers.tellZIO(JobsTrackDispatcher.Track(fcid))
    }

    override def unTrackCluster(fcid: Fcid): FlinkIO[Unit] = {
      clusterIdsCache.tellZIO(TrackClusterIdsCache.Remove(fcid)) *>
      jobsTrackers.tellZIO(JobsTrackDispatcher.UnTrack(fcid))
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
