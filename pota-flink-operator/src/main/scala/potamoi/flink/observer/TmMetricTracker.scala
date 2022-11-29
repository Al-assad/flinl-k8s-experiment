package potamoi.flink.observer

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import potamoi.actorx._
import potamoi.cluster.{CborSerializable, ShardingProxy}
import potamoi.config.{NodeRole, PotaConf}
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.FlinkOprErr.ActorInteropErr
import potamoi.flink.share.model.{Fcid, FlinkTmMetrics, Ftid}
import potamoi.logger.PotaLogger
import potamoi.syntax._
import potamoi.timex._
import potamoi.ziox._
import zio.CancelableFuture
import zio.ZIOAspect.annotated
import zio.stream.ZStream

import scala.collection.mutable

/**
 * Akka cluster sharding proxy for [[TmMetricTracker]].
 */
private[observer] object TmMetricTrackerProxy extends ShardingProxy[Fcid, TmMetricTracker.Cmd] {
  val entityKey = EntityTypeKey[TmMetricTracker.Cmd]("flinkTmMetricsTracker")

  val marshallKey   = fcid => s"tmMt@${fcid.clusterId}@${fcid.namespace}"
  val unmarshallKey = _.split('@').contra(arr => arr(1) -> arr(2))

  def apply(potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] = action(
    createBehavior = entityId => TmMetricTracker(entityId, potaConf, flinkEndpointQuery),
    stopMessage = TmMetricTracker.Stop,
    bindRole = NodeRole.FlinkOperator.toString
  )
}

/**
 * Flink taskmanager metrics tracker.
 */
private[observer] object TmMetricTracker {
  sealed trait Cmd                                                                    extends CborSerializable
  final case object Start                                                             extends Cmd
  final case object Stop                                                              extends Cmd
  sealed trait Query                                                                  extends Cmd
  final case class GetTmMetrics(tid: String, reply: ActorRef[Option[FlinkTmMetrics]]) extends Query
  final case class ListTmMetrics(reply: ActorRef[Set[FlinkTmMetrics]])                extends Query

  private case class RefreshTidListing(tidSeq: Vector[String]) extends Cmd
  private case class RefreshRecord(record: FlinkTmMetrics)     extends Cmd

  def apply(fcidStr: String, potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      val fcid = TmMetricTrackerProxy.unmarshallKey(fcidStr)
      ctx.log.info(s"Flink TmMetricTracker actor initialized, fcid=$fcid")
      new TmMetricTracker(fcid, potaConf, flinkEndpointQuery).action
    }
}

private class TmMetricTracker(
    fcid: Fcid,
    potaConf: PotaConf,
    endpointQuery: RestEndpointQuery,
)(implicit ctx: ActorContext[TmMetricTracker.Cmd]) {
  import TmMetricTracker._

  private var proc: Option[CancelableFuture[Unit]]           = None
  private val state: mutable.HashMap[String, FlinkTmMetrics] = mutable.HashMap.empty

  def action: Behavior[Cmd] = Behaviors.receiveMessage {
    case Start =>
      if (proc.isEmpty) {
        proc = Some(pollingTmMetricsApis.provide(PotaLogger.layer(potaConf.log)).runToFuture)
        ctx.log.info(s"Flink TmMetricTracker actor started, fcid=$fcid")
      }
      Behaviors.same

    case Stop =>
      proc.map(_.cancel())
      ctx.log.info(s"Flink TmMetricTracker actor stopped, fcid=$fcid")
      Behaviors.stopped

    case RefreshTidListing(tidSeq) =>
      (state.keys.toSet diff tidSeq.toSet).foreach(state.remove)
      Behaviors.same

    case RefreshRecord(record) =>
      state.put(record.tid, record)
      Behaviors.same

    case GetTmMetrics(tid, reply) =>
      reply ! state.get(tid)
      Behaviors.same

    case ListTmMetrics(reply) =>
      reply ! state.values.toSet
      Behaviors.same
  }

  private val pollingTmMetricsApis = {
    implicit val flkConf = potaConf.flink
    val queryParallelism = potaConf.flink.snapshotQuery.parallelism

    loopTrigger(potaConf.flink.tracking.tmMetricsPolling) {
      for {
        restUrl <- endpointQuery.get(fcid)
        tidSeq  <- flinkRest(restUrl.chooseUrl).listTaskManagerIds
        _       <- ctx.self.tellZIO(RefreshTidListing(tidSeq)).mapError(ActorInteropErr)
        _ <- ZStream
          .fromIterable(tidSeq)
          .mapZIOParUnordered(queryParallelism) { tid =>
            flinkRest(restUrl.chooseUrl)
              .getTmMetrics(tid, FlinkTmMetrics.metricsRawKeys)
              .map(FlinkTmMetrics.fromRaw(Ftid(fcid, tid), _)) @@ annotated("tid" -> tid)
          }
          .runForeach(e => ctx.self.tellZIO(RefreshRecord(e)).mapError(ActorInteropErr))
      } yield ()
    } @@ annotated(fcid.toAnno :+ "akkaSource" -> ctx.self.path.toString: _*)
  }

}
