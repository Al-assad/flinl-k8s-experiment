package potamoi.flink.observer

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import com.coralogix.zio.k8s.client.model.{label, Added, Deleted, K8sNamespace, Modified, Reseted}
import potamoi.actorx._
import potamoi.cluster.{CborSerializable, ShardingProxy}
import potamoi.config.{NodeRole, PotaConf}
import potamoi.flink.share.K8sRsName
import potamoi.flink.share.model._
import potamoi.k8s.K8sClient
import potamoi.logger.PotaLogger
import potamoi.syntax._
import potamoi.ziox._
import zio.ZIO.logError
import zio.ZIOAspect.annotated
import zio.{durationInt, CancelableFuture, Clock, Schedule, ZLayer}

import scala.collection.mutable

/**
 * Akka cluster sharding proxy for [[K8sRefTracker]].
 */
private[observer] object K8sRefTrackerProxy extends ShardingProxy[Fcid, K8sRefTracker.Cmd] {
  val entityKey = EntityTypeKey[K8sRefTracker.Cmd]("flinkK8sRefTracker")

  val marshallKey   = fcid => s"k8R@${fcid.clusterId}@${fcid.namespace}"
  val unmarshallKey = _.split('@').contra(arr => arr(1) -> arr(2))

  def apply(potaConf: PotaConf, k8sClient: K8sClient): Behavior[Cmd] = action(
    createBehavior = entityId => K8sRefTracker(entityId, potaConf, k8sClient),
    stopMessage = K8sRefTracker.Stop,
    bindRole = NodeRole.FlinkOperator.toString
  )
}

/**
 * Flink k8s resource snapshot tracker
 */
private[observer] object K8sRefTracker {

  sealed trait Cmd        extends CborSerializable
  final case object Start extends Cmd
  final case object Stop  extends Cmd

  sealed trait Query                                                                           extends Cmd
  final case class GetRef(reply: ActorRef[Option[FlinkK8sRef]])                                extends Query
  final case class GetRefSnap(reply: ActorRef[Option[FlinkK8sRefSnap]])                        extends Query
  final case class ListDeployments(reply: ActorRef[List[FK8sDeploymentSnap]])                  extends Query
  final case class ListServices(reply: ActorRef[List[FK8sServiceSnap]])                        extends Query
  final case class ListPods(reply: ActorRef[List[FK8sPodSnap]])                                extends Query
  final case class ListContainerNames(podName: String, reply: ActorRef[List[String]])          extends Query
  final case class GetDeployment(name: K8sRsName, reply: ActorRef[Option[FK8sDeploymentSnap]]) extends Query
  final case class GetService(name: K8sRsName, reply: ActorRef[Option[FK8sServiceSnap]])       extends Query
  final case class GetPod(name: K8sRsName, reply: ActorRef[Option[FK8sPodSnap]])               extends Query
  final case class GetConfigMapNames(reply: ActorRef[List[K8sRsName]])                         extends Query

  sealed private trait Internal                                  extends Cmd
  private object ResetDeploySnaps                                extends Internal
  private case class RefreshDeploySnap(snap: FK8sDeploymentSnap) extends Internal
  private case class DeleteDeploySnap(name: K8sRsName)           extends Internal

  private object ResetServiceSnaps                             extends Internal
  private case class RefreshServiceSnap(snap: FK8sServiceSnap) extends Internal
  private case class DeleteServiceSnap(name: K8sRsName)        extends Internal

  private object ResetPodSnaps                         extends Internal
  private case class RefreshPodSnap(snap: FK8sPodSnap) extends Internal
  private case class DeletePodSnap(name: K8sRsName)    extends Internal

  private object ResetConfigMapNames                    extends Internal
  private case class CollectConfigMapName(name: String) extends Internal
  private case class DeleteConfigMapName(name: String)  extends Internal

  def apply(fcidStr: String, potaConf: PotaConf, k8sClient: K8sClient): Behavior[Cmd] = {
    Behaviors.setup { implicit ctx =>
      val fcid              = K8sRefTrackerProxy.unmarshallKey(fcidStr)
      val restEndpointCache = ctx.spawn(RestEndpointCache(potaConf.akka.ddata.getFlinkRestEndpoint), "flkRestEndpointCache")
      ctx.log.info(s"Flink K8sRefTracker actor initialized, ${fcid.show}")
      new K8sRefTracker(fcid, potaConf, k8sClient, restEndpointCache).action
    }
  }
}

private class K8sRefTracker(
    fcid: Fcid,
    potaConf: PotaConf,
    k8sClient: K8sClient,
    restEndpointCache: ActorRef[RestEndpointCache.Cmd]
  )(implicit ctx: ActorContext[K8sRefTracker.Cmd]) {
  import K8sEntityConverter._
  import K8sRefTracker._

  private var proc: Option[CancelableFuture[Unit]] = None
  private val deploySnaps                          = mutable.Map.empty[K8sRsName, FK8sDeploymentSnap]
  private val svcSnaps                             = mutable.Map.empty[K8sRsName, FK8sServiceSnap]
  private val podSnaps                             = mutable.Map.empty[K8sRsName, FK8sPodSnap]
  private val configMapNames                       = mutable.Set.empty[String]

  private def snapshotAll: Option[FlinkK8sRefSnap] = {
    if (deploySnaps.isEmpty && svcSnaps.isEmpty && podSnaps.isEmpty && configMapNames.isEmpty) None
    else
      FlinkK8sRefSnap(
        fcid.clusterId,
        fcid.namespace,
        deploySnaps.values.toList,
        svcSnaps.values.toList,
        podSnaps.values.toList
      )
  }

  private def snapshotListing: Option[FlinkK8sRef] = {
    if (deploySnaps.isEmpty && svcSnaps.isEmpty && podSnaps.isEmpty && configMapNames.isEmpty) None
    else
      FlinkK8sRef(
        fcid.clusterId,
        fcid.namespace,
        deploySnaps.keys.toList,
        svcSnaps.keys.toList,
        podSnaps.keys.toList,
        configMapNames.toList
      )
  }

  def action: Behavior[Cmd] = Behaviors.receiveMessage {
    case Start =>
      if (proc.isEmpty) {
        proc = Some(watchK8sRefs.provide(PotaLogger.layer(potaConf.log)).runToFuture)
        ctx.log.info(s"Flink K8sRefTracker started, ${fcid.show}")
      }
      Behaviors.same

    case Stop =>
      proc.foreach(_.cancel())
      ctx.log.info(s"Flink K8sRefTracker stopped, ${fcid.show}")
      Behaviors.same

    case ResetDeploySnaps        => deploySnaps.clear(); Behaviors.same
    case RefreshDeploySnap(snap) => deploySnaps(snap.name) = snap; Behaviors.same
    case DeleteDeploySnap(name)  => deploySnaps.remove(name); Behaviors.same

    case ResetPodSnaps        => podSnaps.clear(); Behaviors.same
    case RefreshPodSnap(snap) => podSnaps(snap.name) = snap; Behaviors.same
    case DeletePodSnap(name)  => podSnaps.remove(name); Behaviors.same

    case ResetServiceSnaps =>
      svcSnaps.clear()
      Behaviors.same

    case RefreshServiceSnap(snap) =>
      svcSnaps(snap.name) = snap
      // update RestSvcEndpointCache
      if (snap.isFlinkRestSvc) FlinkRestSvcEndpoint.of(snap).foreach { endpoint =>
        restEndpointCache ! RestEndpointCache.Put(fcid, endpoint)
      }
      Behaviors.same

    case DeleteServiceSnap(name) =>
      svcSnaps.remove(name)
      // update RestSvcEndpointCache
      if (name.endsWith("-rest")) restEndpointCache ! RestEndpointCache.Remove(fcid)
      Behaviors.same

    case ResetConfigMapNames        => configMapNames.clear(); Behaviors.same
    case CollectConfigMapName(name) => configMapNames.add(name); Behaviors.same
    case DeleteConfigMapName(name)  => configMapNames.remove(name); Behaviors.same

    case GetRefSnap(reply)          => reply ! snapshotAll; Behaviors.same
    case GetRef(reply)              => reply ! snapshotListing; Behaviors.same
    case ListDeployments(reply)     => reply ! deploySnaps.values.toList; Behaviors.same
    case ListServices(reply)        => reply ! svcSnaps.values.toList; Behaviors.same
    case ListPods(reply)            => reply ! podSnaps.values.toList; Behaviors.same
    case GetDeployment(name, reply) => reply ! deploySnaps.get(name); Behaviors.same
    case GetService(name, reply)    => reply ! svcSnaps.get(name); Behaviors.same
    case GetPod(name, reply)        => reply ! podSnaps.get(name); Behaviors.same
    case GetConfigMapNames(reply)   => reply ! configMapNames.toList; Behaviors.same

    case ListContainerNames(name, reply) =>
      reply ! podSnaps.values.find(_.name == name).map(_.containerSnaps.map(_.name).toList).getOrElse(List.empty)
      Behaviors.same
  }

  /**
   * Watching k8s deployments, services, pods api.
   */
  private val watchK8sRefs = {
    watchDeployments <&>
    watchServices <&>
    watchPods <&>
    watchConfigMapNames
  }.provide(ZLayer.succeed(Clock.ClockLive)) @@ annotated(fcid.toAnno :+ "akkaSource" -> ctx.self.path.toString: _*)

  private def appSelector = label("app") === fcid.clusterId && label("type") === "flink-native-kubernetes"

  private lazy val watchDeployments = {
    k8sClient.api.apps.v1.deployments
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector)
      .runForeach {
        case Reseted()        => ctx.self.tellZIO(ResetDeploySnaps)
        case Added(deploy)    => toDeploymentSnap(deploy).flatMap(snap => ctx.self.tellZIO(RefreshDeploySnap(snap)))
        case Modified(deploy) => toDeploymentSnap(deploy).flatMap(snap => ctx.self.tellZIO(RefreshDeploySnap(snap)))
        case Deleted(deploy)  => toDeploymentSnap(deploy).flatMap(snap => ctx.self.tellZIO(DeleteDeploySnap(snap.name)))
      }
      .tapError(e => logError(s"Some error occurs while watching k8s deployments: $e"))
      .retry(Schedule.spaced(1.seconds))
  }

  private lazy val watchServices = {
    k8sClient.api.v1.services
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector)
      .runForeach {
        case Reseted()     => ctx.self.tellZIO(ResetServiceSnaps)
        case Added(svc)    => toServiceSnap(svc).flatMap(snap => ctx.self.tellZIO(RefreshServiceSnap(snap)))
        case Modified(svc) => toServiceSnap(svc).flatMap(snap => ctx.self.tellZIO(RefreshServiceSnap(snap)))
        case Deleted(svc)  => toServiceSnap(svc).flatMap(snap => ctx.self.tellZIO(DeleteServiceSnap(snap.name)))
      }
      .tapError(e => logError(s"Some error occurs while watching k8s services: $e"))
      .retry(Schedule.spaced(1.seconds))
  }

  private lazy val watchPods = {
    k8sClient.api.v1.pods
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector)
      .runForeach {
        case Reseted()     => ctx.self.tellZIO(ResetPodSnaps)
        case Added(pod)    => toPodSnap(pod).flatMap(snap => ctx.self.tellZIO(RefreshPodSnap(snap)))
        case Modified(pod) => toPodSnap(pod).flatMap(snap => ctx.self.tellZIO(RefreshPodSnap(snap)))
        case Deleted(pod)  => toPodSnap(pod).flatMap(snap => ctx.self.tellZIO(DeletePodSnap(snap.name)))
      }
      .tapError(e => logError(s"Some error occurs while watching k8s pods: $e"))
      .retry(Schedule.spaced(1.seconds))
  }

  private lazy val watchConfigMapNames = {
    k8sClient.api.v1.configmaps
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector)
      .runForeach {
        case Reseted()           => ctx.self.tellZIO(ResetConfigMapNames)
        case Added(configMap)    => configMap.getName.flatMap(name => ctx.self.tellZIO(CollectConfigMapName(name)))
        case Modified(configMap) => configMap.getName.flatMap(name => ctx.self.tellZIO(CollectConfigMapName(name)))
        case Deleted(configMap)  => configMap.getName.flatMap(name => ctx.self.tellZIO(DeleteConfigMapName(name)))
      }
      .tapError(e => logError(s"Some error occurs while watching k8s configmaps: $e"))
      .retry(Schedule.spaced(1.seconds))
  }

}
