package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.cluster.LWWMapDData
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.config.{DDataConf, PotaConf}
import potamoi.flink.observer.ClustersOvTracker.GetClusterOverview
import potamoi.flink.observer.JmMetricTracker.GetJmMetrics
import potamoi.flink.observer.TmDetailTracker.{GetTmDetail, ListTmDetails, ListTmIds}
import potamoi.flink.share.FlinkIO
import potamoi.flink.share.model.FlinkExecMode.FlinkExecMode
import potamoi.flink.share.model.{Fcid, FlinkClusterOverview, FlinkJmMetrics, FlinkRestSvcEndpoint, FlinkTmDetail, Ftid}
import potamoi.timex._
import zio.stream.ZStream

/**
 * Flink cluster snapshot information query layer.
 */
trait ClusterQuery {
  def getOverview(fcid: Fcid): FlinkIO[Option[FlinkClusterOverview]]
  def listOverview: FlinkIO[List[FlinkClusterOverview]]

  def getTmDetail(ftid: Ftid): FlinkIO[Option[FlinkTmDetail]]
  def listTmDetails(fcid: Fcid): FlinkIO[List[FlinkTmDetail]]
  def listTmIds(fcid: Fcid): FlinkIO[List[String]]

  def getJmMetrics(fcid: Fcid): FlinkIO[Option[FlinkJmMetrics]]
}

object ClusterQuery {

  def live(potaConf: PotaConf, guardian: ActorGuardian, endpointQuery: RestEndpointQuery) =
    for {
      clusterIdsCache       <- guardian.spawn(TrackClusterIdsCache(potaConf.akka.ddata.getFlinkClusterIds), "flkTrackClusterCache-cq")
      clusterIdxCache       <- guardian.spawn(ClusterIndexCache(potaConf.akka.ddata.getFlinkClusterIndex), "flkClusterIdxCache-cq")
      ovTrackersProxy       <- guardian.spawn(ClustersOvTrackerProxy(potaConf, endpointQuery), "flkClusterOvTrackerProxy")
      tmDetailTrackersProxy <- guardian.spawn(TmDetailTrackerProxy(potaConf, endpointQuery), "flkTmDetailTrackerProxy")
      jmMetricTrackersProxy <- guardian.spawn(JmMetricTrackerProxy(potaConf, endpointQuery), "flkJmMetricTrackerProxy")
      queryTimeout     = potaConf.flink.snapshotQuery.askTimeout
      queryParallelism = potaConf.flink.snapshotQuery.parallelism
      sc               = guardian.scheduler
    } yield Live(clusterIdsCache, clusterIdxCache, ovTrackersProxy, tmDetailTrackersProxy, jmMetricTrackersProxy, queryParallelism)(sc, queryTimeout)

  case class Live(
      clusterIdsCache: ActorRef[TrackClusterIdsCache.Cmd],
      clusterIndexCache: ActorRef[ClusterIndexCache.Cmd],
      ovTrackers: ActorRef[ClustersOvTrackerProxy.Cmd],
      tmDetailTrackers: ActorRef[TmDetailTrackerProxy.Cmd],
      jmMetricTrackers: ActorRef[JmMetricTrackerProxy.Cmd],
      queryParallelism: Int
    )(implicit sc: Scheduler,
      queryTimeout: Timeout)
      extends ClusterQuery {

    def listOverview: FlinkIO[List[FlinkClusterOverview]] = {
      ZStream
        .fromIterableZIO(clusterIdsCache.list)
        .mapZIOParUnordered(queryParallelism)(ovTrackers(_).ask(GetClusterOverview))
        .filter(_.isDefined)
        .map(_.get)
        .runFold(List.empty[FlinkClusterOverview])(_ :+ _)
        .map(_.sorted)
    }

    def getOverview(fcid: Fcid): FlinkIO[Option[FlinkClusterOverview]] = ovTrackers(fcid).ask(GetClusterOverview)
    def getTmDetail(ftid: Ftid): FlinkIO[Option[FlinkTmDetail]]        = tmDetailTrackers(ftid.fcid).ask(GetTmDetail(ftid.tid, _))
    def listTmDetails(fcid: Fcid): FlinkIO[List[FlinkTmDetail]]        = tmDetailTrackers(fcid).ask(ListTmDetails).map(_.toList.sorted)
    def listTmIds(fcid: Fcid): FlinkIO[List[String]]                   = tmDetailTrackers(fcid).ask(ListTmIds).map(_.toList.sorted)
    def getJmMetrics(fcid: Fcid): FlinkIO[Option[FlinkJmMetrics]]      = jmMetricTrackers(fcid).ask(GetJmMetrics)
  }
}

/**
 * Flink cluster query index cache,
 */
object ClusterIndexCache extends LWWMapDData[Fcid, ClusterIndex] {
  val cacheId                               = "flink-cluster-idx"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)
}

case class ClusterIndex(endpoint: Option[FlinkRestSvcEndpoint] = None, execMode: Option[FlinkExecMode] = None)
