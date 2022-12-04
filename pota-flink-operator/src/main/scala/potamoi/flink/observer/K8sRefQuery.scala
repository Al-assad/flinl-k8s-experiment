package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.config.PotaConf
import potamoi.flink.observer.K8sRefTracker.{GetDeployment, GetPod, GetRef, GetRefSnap, GetService, ListDeployments, ListPods, ListServices}
import potamoi.flink.share.model._
import potamoi.flink.share.{FlinkIO, K8sRsName}
import potamoi.k8s.K8sClient
import potamoi.timex._
import zio.stream.ZStream

/**
 * Flink kubernetes resource snapshot information query layer.
 */
trait K8sRefQuery {
  def getRef(fcid: Fcid): FlinkIO[Option[FlinkK8sRef]]
  def getRefSnapshot(fcid: Fcid): FlinkIO[Option[FlinkK8sRefSnap]]
  def listRefs: FlinkIO[List[FlinkK8sRef]]
  def listRefSnapshots: FlinkIO[List[FlinkK8sRefSnap]]

  def listDeploymentSnaps(fcid: Fcid): FlinkIO[List[FK8sDeploymentSnap]]
  def listServiceSnaps(fcid: Fcid): FlinkIO[List[FK8sServiceSnap]]
  def listPodSnaps(fcid: Fcid): FlinkIO[List[FK8sPodSnap]]

  def getDeploymentSnap(fcid: Fcid, name: K8sRsName): FlinkIO[Option[FK8sDeploymentSnap]]
  def getServiceSnap(fcid: Fcid, name: K8sRsName): FlinkIO[Option[FK8sServiceSnap]]
  def getPodSnap(fcid: Fcid, name: K8sRsName): FlinkIO[Option[FK8sPodSnap]]
}

object K8sRefQuery {

  def live(potaConf: PotaConf, guardian: ActorGuardian, k8sClient: K8sClient) = for {
    clusterIdsCache    <- guardian.spawn(TrackClusterIdCache(potaConf.akka.ddata.getFlinkClusterIds), "flkTrackClusterCache-k8")
    k8sRefTrackerProxy <- guardian.spawn(K8sRefTrackerProxy(potaConf, k8sClient), "flk8sRefTrackerProxy")
    queryTimeout     = potaConf.flink.snapshotQuery.askTimeout
    queryParallelism = potaConf.flink.snapshotQuery.parallelism
    sc               = guardian.scheduler
  } yield Live(
    clusterIdsCache,
    k8sRefTrackerProxy,
    k8sClient,
    queryParallelism
  )(sc, queryTimeout)

  /**
   * Akka Sharding/DData hybrid storage implementation.
   */
  case class Live(
      clusterIdsCache: ActorRef[TrackClusterIdCache.Cmd],
      k8sRefTrackers: ActorRef[K8sRefTrackerProxy.Cmd],
      k8sClient: K8sClient,
      queryParallelism: Int
    )(implicit sc: Scheduler,
      queryTimeout: Timeout)
      extends K8sRefQuery {

    def listRefs: FlinkIO[List[FlinkK8sRef]] =
      ZStream
        .fromIterableZIO(clusterIdsCache.list)
        .mapZIOParUnordered(queryParallelism)(k8sRefTrackers(_).ask(GetRef))
        .filter(_.isDefined)
        .map(_.get)
        .runFold(List.empty[FlinkK8sRef])(_ :+ _)
        .map(_.sorted)

    def listRefSnapshots: FlinkIO[List[FlinkK8sRefSnap]] =
      ZStream
        .fromIterableZIO(clusterIdsCache.list)
        .mapZIOParUnordered(queryParallelism)(k8sRefTrackers(_).ask(GetRefSnap))
        .filter(_.isDefined)
        .map(_.get)
        .runFold(List.empty[FlinkK8sRefSnap])(_ :+ _)
        .map(_.sorted)

    def getRef(fcid: Fcid): FlinkIO[Option[FlinkK8sRef]]                   = k8sRefTrackers(fcid).ask(GetRef)
    def getRefSnapshot(fcid: Fcid): FlinkIO[Option[FlinkK8sRefSnap]]       = k8sRefTrackers(fcid).ask(GetRefSnap)
    def listDeploymentSnaps(fcid: Fcid): FlinkIO[List[FK8sDeploymentSnap]] = k8sRefTrackers(fcid).ask(ListDeployments).map(_.sortBy(_.name))
    def listServiceSnaps(fcid: Fcid): FlinkIO[List[FK8sServiceSnap]]       = k8sRefTrackers(fcid).ask(ListServices).map(_.sortBy(_.name))
    def listPodSnaps(fcid: Fcid): FlinkIO[List[FK8sPodSnap]]               = k8sRefTrackers(fcid).ask(ListPods).map(_.sortBy(_.name))
    def getDeploymentSnap(fcid: Fcid, name: K8sRsName): FlinkIO[Option[FK8sDeploymentSnap]] = k8sRefTrackers(fcid).ask(GetDeployment(name, _))
    def getServiceSnap(fcid: Fcid, name: K8sRsName): FlinkIO[Option[FK8sServiceSnap]]       = k8sRefTrackers(fcid).ask(GetService(name, _))
    def getPodSnap(fcid: Fcid, name: K8sRsName): FlinkIO[Option[FK8sPodSnap]]               = k8sRefTrackers(fcid).ask(GetPod(name, _))
  }

}
