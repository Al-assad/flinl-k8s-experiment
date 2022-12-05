package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.config.PotaConf
import potamoi.flink.observer.K8sPodMetricTracker.{GetPodMetrics, ListPodMetrics}
import potamoi.flink.observer.K8sRefTracker._
import potamoi.flink.share.{FlinkIO, FlinkOprErr}
import potamoi.flink.share.FlinkOprErr.K8sOperationErr
import potamoi.flink.share.model._
import potamoi.k8s.{K8sErr, K8sOperator}
import potamoi.timex._
import zio.ZIO.succeed
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
  def listContainerNames(fcid: Fcid, podName: String): FlinkIO[List[String]]

  def getDeploymentSnap(fcid: Fcid, deployName: String): FlinkIO[Option[FK8sDeploymentSnap]]
  def getServiceSnap(fcid: Fcid, svcName: String): FlinkIO[Option[FK8sServiceSnap]]
  def getPodSnap(fcid: Fcid, podName: String): FlinkIO[Option[FK8sPodSnap]]
  def getConfigMapNames(fcid: Fcid): FlinkIO[List[String]]

  def getDeploymentSpec(fcid: Fcid, deployName: String): FlinkIO[Option[DeploymentSpec]]
  def getServiceSpec(fcid: Fcid, deployName: String): FlinkIO[Option[ServiceSpec]]
  def getPodSpec(fcid: Fcid, deployName: String): FlinkIO[Option[PodSpec]]

  def getConfigMapData(fcid: Fcid, configMapName: String): FlinkIO[Map[String, String]]
  def listConfigMapData(fcid: Fcid): FlinkIO[Map[String, String]]

  def getPodMetrics(fcid: Fcid, podName: String): FlinkIO[Option[FK8sPodMetrics]]
  def listPodMetrics(fcid: Fcid): FlinkIO[List[FK8sPodMetrics]]

  /**
   * Only for getting flink-main-container logs, side-car container logs should be obtained
   * through the [[K8sOperator.getPodLog()]].
   */
  def getLog(
      fcid: Fcid,
      podName: String,
      follow: Boolean = false,
      tailLines: Option[Int] = None,
      sinceSec: Option[Int] = None): ZStream[Any, FlinkOprErr, String]

}

object K8sRefQuery {

  def live(potaConf: PotaConf, guardian: ActorGuardian, k8sOperator: K8sOperator) = for {
    clusterIdsCache           <- guardian.spawn(TrackClusterIdCache(potaConf.akka.ddata.getFlinkClusterIds), "flkTrackClusterCache-k8")
    k8sRefTrackerProxy        <- guardian.spawn(K8sRefTrackerProxy(potaConf, k8sOperator.client), "flk8sRefTrackerProxy")
    k8sPodMetricsTrackerProxy <- guardian.spawn(K8sPodMetricTrackerProxy(potaConf, k8sOperator), "flk8sPodMetricsTrackerProxy")
    queryTimeout     = potaConf.flink.snapshotQuery.askTimeout
    queryParallelism = potaConf.flink.snapshotQuery.parallelism
    sc               = guardian.scheduler
  } yield Live(
    clusterIdsCache,
    k8sRefTrackerProxy,
    k8sPodMetricsTrackerProxy,
    k8sOperator,
    queryParallelism
  )(sc, queryTimeout)

  /**
   * Akka Sharding/DData hybrid storage implementation.
   */
  case class Live(
      clusterIdsCache: ActorRef[TrackClusterIdCache.Cmd],
      k8sRefTrackers: ActorRef[K8sRefTrackerProxy.Cmd],
      k8sPodMetricTrackers: ActorRef[K8sPodMetricTrackerProxy.Cmd],
      k8sOperator: K8sOperator,
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

    def getRef(fcid: Fcid): FlinkIO[Option[FlinkK8sRef]]             = k8sRefTrackers(fcid).ask(GetRef)
    def getRefSnapshot(fcid: Fcid): FlinkIO[Option[FlinkK8sRefSnap]] = k8sRefTrackers(fcid).ask(GetRefSnap)

    def listDeploymentSnaps(fcid: Fcid): FlinkIO[List[FK8sDeploymentSnap]]     = k8sRefTrackers(fcid).ask(ListDeployments).map(_.sortBy(_.name))
    def listServiceSnaps(fcid: Fcid): FlinkIO[List[FK8sServiceSnap]]           = k8sRefTrackers(fcid).ask(ListServices).map(_.sortBy(_.name))
    def listPodSnaps(fcid: Fcid): FlinkIO[List[FK8sPodSnap]]                   = k8sRefTrackers(fcid).ask(ListPods).map(_.sortBy(_.name))
    def listContainerNames(fcid: Fcid, podName: String): FlinkIO[List[String]] = k8sRefTrackers(fcid).ask(ListContainerNames(podName, _))

    def getDeploymentSnap(fcid: Fcid, name: String): FlinkIO[Option[FK8sDeploymentSnap]] = k8sRefTrackers(fcid).ask(GetDeployment(name, _))
    def getServiceSnap(fcid: Fcid, name: String): FlinkIO[Option[FK8sServiceSnap]]       = k8sRefTrackers(fcid).ask(GetService(name, _))
    def getPodSnap(fcid: Fcid, name: String): FlinkIO[Option[FK8sPodSnap]]               = k8sRefTrackers(fcid).ask(GetPod(name, _))
    def getConfigMapNames(fcid: Fcid): FlinkIO[List[String]]                             = k8sRefTrackers(fcid).ask(GetConfigMapNames)

    def getDeploymentSpec(fcid: Fcid, name: String): FlinkIO[Option[DeploymentSpec]] =
      k8sOperator
        .getDeploymentSpec(name, fcid.namespace)
        .map(Some(_))
        .catchSome { case K8sErr.DeploymentNotFound(_, _) => succeed(None) }
        .mapError(K8sOperationErr)

    def getServiceSpec(fcid: Fcid, name: String): FlinkIO[Option[ServiceSpec]] =
      k8sOperator
        .getServiceSpec(name, fcid.namespace)
        .map(Some(_))
        .catchSome { case K8sErr.ServiceNotFound(_, _) => succeed(None) }
        .mapError(K8sOperationErr)

    def getPodSpec(fcid: Fcid, name: String): FlinkIO[Option[PodSpec]] =
      k8sOperator
        .getPodSpec(name, fcid.namespace)
        .map(Some(_))
        .catchSome { case K8sErr.PodNotFound(_, _) => succeed(None) }
        .mapError(K8sOperationErr)

    def getConfigMapData(fcid: Fcid, configMapName: String): FlinkIO[Map[String, String]] =
      k8sOperator
        .getConfigMapsData(configMapName, fcid.namespace)
        .catchSome { case K8sErr.ConfigMapNotFound(_, _) => succeed(Map.empty[String, String]) }
        .mapError(K8sOperationErr)

    override def listConfigMapData(fcid: Fcid): FlinkIO[Map[String, String]] = {
      val listConfigMapNames: FlinkIO[List[String]] = k8sRefTrackers(fcid).ask(GetConfigMapNames)
      ZStream
        .fromIterableZIO(listConfigMapNames)
        .mapZIOParUnordered(queryParallelism)(getConfigMapData(fcid, _))
        .runFold(Map.empty[String, String])(_ ++ _)
    }

    def getPodMetrics(fcid: Fcid, podName: String): FlinkIO[Option[FK8sPodMetrics]] = k8sPodMetricTrackers(fcid).ask(GetPodMetrics(podName, _))
    def listPodMetrics(fcid: Fcid): FlinkIO[List[FK8sPodMetrics]] = k8sPodMetricTrackers(fcid).ask(ListPodMetrics).map(_.toList.sortBy(_.name))

    def getLog(fcid: Fcid, podName: String, follow: Boolean, tailLines: Option[Int], sinceSec: Option[Int]): ZStream[Any, FlinkOprErr, String] = {
      k8sOperator
        .getPodLog(
          podName = podName,
          namespace = fcid.namespace,
          containerName = Some("flink-main-container"),
          follow = follow,
          tailLines = tailLines,
          sinceSec = sinceSec)
        .mapError(K8sOperationErr)
    }
  }

}
