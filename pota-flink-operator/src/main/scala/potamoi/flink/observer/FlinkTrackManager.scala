package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.cluster.{LWWMapDData, ORSetDData}
import potamoi.config.{DDataConf, PotaConf}
import potamoi.flink.share.FlinkIO
import potamoi.flink.share.model.Fcid
import potamoi.flink.share.model.FlinkExecMode.FlinkExecMode
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
      tmMetricTrackers: ActorRef[TmMetricTrackerProxy.Cmd],
      jobOvTrackers: ActorRef[JobOvTrackerProxy.Cmd],
      jobMetricsTrackers: ActorRef[JobMetricTrackerProxy.Cmd],
      k8sRefsTrackers: ActorRef[K8sRefTrackerProxy.Cmd]) =
    for {
      clusterIdsCache   <- guardian.spawn(TrackClusterIdCache(potaConf.akka.ddata.getFlinkClusterIds), "flkTrackClusterCache-tm")
      clusterIndexCache <- guardian.spawn(ClusterIndexCache(potaConf.akka.ddata.getFlinkClusterIndex), "flkClusterIdxCache-tm")
      queryTimeout = potaConf.flink.snapshotQuery.askTimeout
      sc           = guardian.scheduler
    } yield Live(
      clusterIdsCache,
      clusterIndexCache,
      clustersOvTrackers,
      tmDetailTrackers,
      jmMetricTrackers,
      tmMetricTrackers,
      jobOvTrackers,
      jobMetricsTrackers,
      k8sRefsTrackers
    )(sc, queryTimeout)

  /**
   * Implementation based on Akka infra.
   */
  case class Live(
      clusterIdsCache: ActorRef[TrackClusterIdCache.Cmd],
      clusterIndexCache: ActorRef[ClusterIndexCache.Cmd],
      clustersOvTrackers: ActorRef[ClustersOvTrackerProxy.Cmd],
      tmDetailTrackers: ActorRef[TmDetailTrackerProxy.Cmd],
      jmMetricTrackers: ActorRef[JmMetricTrackerProxy.Cmd],
      tmMetricTrackers: ActorRef[TmMetricTrackerProxy.Cmd],
      jobsTrackers: ActorRef[JobOvTrackerProxy.Cmd],
      jobMetricsTrackers: ActorRef[JobMetricTrackerProxy.Cmd],
      k8sRefsTrackers: ActorRef[K8sRefTrackerProxy.Cmd]
    )(implicit sc: Scheduler,
      queryTimeout: Timeout)
      extends FlinkTrackManager {

    override def trackCluster(fcid: Fcid): FlinkIO[Unit] = {
      clusterIdsCache.put(fcid) *>
      k8sRefsTrackers(fcid).tell(K8sRefTracker.Start) *>
      clustersOvTrackers(fcid).tell(ClustersOvTracker.Start) *>
      tmDetailTrackers(fcid).tell(TmDetailTracker.Start) *>
      jmMetricTrackers(fcid).tell(JmMetricTracker.Start) *>
      tmMetricTrackers(fcid).tell(TmMetricTracker.Start) *>
      jobsTrackers(fcid).tell(JobOvTracker.Start) *>
      jobMetricsTrackers(fcid).tell(JobMetricTracker.Start)
    }

    override def untrackCluster(fcid: Fcid): FlinkIO[Unit] = {
      clusterIdsCache.remove(fcid) *>
      clusterIndexCache.remove(fcid) *>
      k8sRefsTrackers(fcid).tell(K8sRefTracker.Stop) *>
      clustersOvTrackers(fcid).tell(ClustersOvTracker.Stop) *>
      tmDetailTrackers(fcid).tell(TmDetailTracker.Stop) *>
      jmMetricTrackers(fcid).tell(JmMetricTracker.Stop) *>
      tmMetricTrackers(fcid).tell(TmMetricTracker.Stop) *>
      jobsTrackers(fcid).tell(JobOvTracker.Stop) *>
      jobMetricsTrackers(fcid).tell(JobMetricTracker.Stop)
    }

    override def listTrackedCluster: FlinkIO[Set[Fcid]] = clusterIdsCache.list
  }
}

/**
 * Tracked Flink cluster id distributed data storage base on ORSet.
 */
private[observer] object TrackClusterIdCache extends ORSetDData[Fcid] {
  val cacheId                               = "flink-cluster-id"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)
}

/**
 * Flink cluster query index cache,
 */
private[observer] object ClusterIndexCache extends LWWMapDData[Fcid, ClusterIndex] {
  val cacheId                               = "flink-cluster-idx"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)
}

case class ClusterIndex(execMode: Option[FlinkExecMode] = None)
