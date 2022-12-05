package potamoi.flink.observer

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import com.coralogix.zio.k8s.client.model.{label, Added, Deleted, K8sNamespace, Modified, Reseted}
import potamoi.actorx._
import potamoi.cluster.{CborSerializable, ShardingProxy}
import potamoi.common.Syntax.GenericPF
import potamoi.config.{NodeRole, PotaConf}
import potamoi.flink.share.FlinkOprErr.ActorInteropErr
import potamoi.flink.share.K8sRsName
import potamoi.flink.share.model.{FK8sPodMetrics, Fcid}
import potamoi.k8s.K8sOperator
import potamoi.logger.PotaLogger
import potamoi.syntax._
import potamoi.timex._
import potamoi.ziox._
import zio.ZIO.logError
import zio.ZIOAspect.annotated
import zio.stream.ZStream
import zio.{durationInt, CancelableFuture, Clock, Ref, Schedule, ZLayer}

import scala.collection.mutable

/**
 * Akka cluster sharding proxy for [[K8sPodMetricTracker]].
 */
object K8sPodMetricTrackerProxy extends ShardingProxy[Fcid, K8sPodMetricTracker.Cmd] {
  val entityKey = EntityTypeKey[K8sPodMetricTracker.Cmd]("flinkK8sPodMetricTracker")

  val marshallKey   = fcid => s"podMt@${fcid.clusterId}@${fcid.namespace}"
  val unmarshallKey = _.split('@').contra(arr => arr(1) -> arr(2))

  def apply(potaConf: PotaConf, k8sOperator: K8sOperator): Behavior[Cmd] = action(
    createBehavior = entityId => K8sPodMetricTracker(entityId, potaConf, k8sOperator),
    stopMessage = K8sPodMetricTracker.Stop,
    bindRole = NodeRole.FlinkOperator.toString
  )
}

/**
 * Flink k8s pod metrics tracker.
 */
object K8sPodMetricTracker {

  sealed trait Cmd                                                                         extends CborSerializable
  final case object Start                                                                  extends Cmd
  final case object Stop                                                                   extends Cmd
  sealed trait Query                                                                       extends Cmd
  final case class GetPodMetrics(podName: String, reply: ActorRef[Option[FK8sPodMetrics]]) extends Query
  final case class ListPodMetrics(reply: ActorRef[Set[FK8sPodMetrics]])                    extends Query
  sealed private trait Internal                                                            extends Cmd
  private case object ResetMetric                                                          extends Internal
  private case class RefreshMetrics(metrics: FK8sPodMetrics)                               extends Internal
  private case class RemoveMetrics(podName: String)                                        extends Internal

  def apply(fcidStr: String, potaConf: PotaConf, k8sOperator: K8sOperator): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      val fcid = K8sPodMetricTrackerProxy.unmarshallKey(fcidStr)
      ctx.log.info(s"Flink K8sPodMetricTracker actor initialized, ${fcid.show}")
      new K8sPodMetricTracker(fcid, potaConf, k8sOperator).action
    }
}

private class K8sPodMetricTracker(fcid: Fcid, potaConf: PotaConf, k8sOperator: K8sOperator)(implicit ctx: ActorContext[K8sPodMetricTracker.Cmd]) {
  import K8sPodMetricTracker._

  private var proc: Option[CancelableFuture[Unit]]          = None
  private var state: mutable.Map[K8sRsName, FK8sPodMetrics] = mutable.HashMap.empty

  def action: Behavior[Cmd] = Behaviors.receiveMessage {
    case Start =>
      if (proc.isEmpty) {
        proc = Some(pollingK8sMetricsApi.provide(PotaLogger.layer(potaConf.log)).runToFuture)
        ctx.log.info(s"Flink K8sPodMetricTracker actor started, ${fcid.show}")
      }
      Behaviors.same

    case Stop =>
      proc.map(_.cancel())
      ctx.log.info(s"Flink K8sPodMetricTracker actor stopped, ${fcid.show}")
      Behaviors.same

    case ResetMetric =>
      state = mutable.HashMap.empty
      Behaviors.same

    case RefreshMetrics(metrics) =>
      state.put(metrics.name, metrics)
      Behaviors.same

    case RemoveMetrics(name) =>
      state.remove(name)
      Behaviors.same

    case GetPodMetrics(podName, reply) =>
      reply ! state.get(podName)
      Behaviors.same

    case ListPodMetrics(reply) =>
      reply ! state.values.toSet
      Behaviors.same

  }

  private val pollingK8sMetricsApi = {
    def watchK8sPodNames(podNames: Ref[mutable.HashSet[String]]) =
      k8sOperator.client.api.v1.pods
        .watchForever(
          namespace = K8sNamespace(fcid.namespace),
          labelSelector = label("app") === fcid.clusterId && label("type") === "flink-native-kubernetes")
        .runForeach {
          case Added(pod)    => pod.getMetadata.flatMap(_.getName).flatMap(name => podNames.update(_ += name))
          case Modified(pod) => pod.getMetadata.flatMap(_.getName).flatMap(name => podNames.update(_ += name))
          case Reseted()     => podNames.set(mutable.HashSet.empty) *> ctx.self.tellZIO(ResetMetric)
          case Deleted(pod) =>
            pod.getMetadata.flatMap(_.getName).flatMap { name =>
              podNames.update(_ -= name) *>
              ctx.self.tellZIO(RemoveMetrics(name))
            }
        }
        .tapError(e => logError(s"Some error occurs while watching k8s pods: $e"))
        .retry(Schedule.spaced(1.seconds))

    def pollingMetric(podNames: Ref[mutable.HashSet[String]]) =
      ZStream
        .fromIterableZIO(podNames.get)
        .mapZIOParUnordered(5)(name => k8sOperator.getPodMetrics(name, fcid.namespace).map(name -> _))
        .map { case (name, metrics) => FK8sPodMetrics(fcid.clusterId, fcid.namespace, name, metrics.copy()) }
        .runForeach(e => ctx.self.tellZIO(RefreshMetrics(e)).mapError(ActorInteropErr))

    for {
      podNames <- Ref.make(mutable.HashSet.empty[String])
      _        <- watchK8sPodNames(podNames).fork
      _        <- loopTrigger(potaConf.flink.tracking.k8sPodMetricsPolling)(pollingMetric(podNames))
    } yield ()
  }.provide(ZLayer.succeed(Clock.ClockLive)) @@ annotated(fcid.toAnno :+ "akkaSource" -> ctx.self.path.toString: _*)

}
