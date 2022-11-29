package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.cluster.ORSetDData
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.config.{DDataConf, PotaConf}
import potamoi.flink.share.FlinkIO
import potamoi.flink.share.model.Fcid
import potamoi.timex._

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

  def live(
      potaConf: PotaConf,
      guardian: ActorGuardian,
      clustersOvTrackers: ActorRef[ClustersOvTrackerProxy.Cmd],
      tmDetailTrackers: ActorRef[TmDetailTrackerProxy.Cmd],
      jmMetricTrackers: ActorRef[JmMetricTrackerProxy.Cmd],
      jobsTrackers: ActorRef[JobsOvTrackerProxy.Cmd]) =
    for {
      clusterIdsCache   <- guardian.spawn(TrackClusterIdsCache(potaConf.akka.ddata.getFlinkClusterIds), "flkTrackClusterCache-tm")
      clusterIndexCache <- guardian.spawn(ClusterIndexCache(potaConf.akka.ddata.getFlinkClusterIndex), "flkClusterIdxCache-tm")
      queryTimeout = potaConf.flink.snapshotQuery.askTimeout
      sc           = guardian.scheduler
    } yield Live(clusterIdsCache, clusterIndexCache, clustersOvTrackers, tmDetailTrackers, jmMetricTrackers, jobsTrackers)(sc, queryTimeout)

  /**
   * Implementation based on Akka infra.
   */
  case class Live(
      clusterIdsCache: ActorRef[TrackClusterIdsCache.Cmd],
      clusterIndexCache: ActorRef[ClusterIndexCache.Cmd],
      clustersOvTrackers: ActorRef[ClustersOvTrackerProxy.Cmd],
      tmDetailTrackers: ActorRef[TmDetailTrackerProxy.Cmd],
      jmMetricTrackers: ActorRef[JmMetricTrackerProxy.Cmd],
      jobsTrackers: ActorRef[JobsOvTrackerProxy.Cmd]
    )(implicit sc: Scheduler,
      queryTimeout: Timeout)
      extends FlinkTrackManager {

    override def trackCluster(fcid: Fcid): FlinkIO[Unit] = {
      clusterIdsCache.put(fcid) *>
      clustersOvTrackers(fcid).tell(ClustersOvTracker.Start) *>
      tmDetailTrackers(fcid).tell(TmDetailTracker.Start) *>
      jmMetricTrackers(fcid).tell(JmMetricTracker.Start) *>
      jobsTrackers(fcid).tell(JobsOvTracker.Start)
    }

    override def untrackCluster(fcid: Fcid): FlinkIO[Unit] = {
      clusterIdsCache.remove(fcid) *>
      clusterIndexCache.remove(fcid) *>
      clustersOvTrackers(fcid).tell(ClustersOvTracker.Stop) *>
      tmDetailTrackers(fcid).tell(TmDetailTracker.Stop) *>
      jmMetricTrackers(fcid).tell(JmMetricTracker.Stop) *>
      jobsTrackers(fcid).tell(JobsOvTracker.Stop)
    }

    override def listTrackedCluster: FlinkIO[Set[Fcid]] = clusterIdsCache.list
  }
}

/**
 * Tracked Flink cluster id distributed data storage base on ORSet.
 */
private[observer] object TrackClusterIdsCache extends ORSetDData[Fcid] {
  val cacheId                               = "flink-cluster-id"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)
}
