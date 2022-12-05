package potamoi.flink.observer

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import potamoi.actorx._
import potamoi.cluster.{CborSerializable, ShardingProxy}
import potamoi.config.{NodeRole, PotaConf}
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.FlinkOprErr.ActorInteropErr
import potamoi.flink.share.model.{Fcid, FlinkTmDetail}
import potamoi.logger.PotaLogger
import potamoi.syntax._
import potamoi.timex._
import potamoi.ziox._
import zio.ZIOAspect.annotated
import zio.stream.ZStream
import zio.{CancelableFuture, Ref}

import scala.util.hashing.MurmurHash3

/**
 * Akka cluster sharding proxy for [[TmDetailTracker]].
 */
private[observer] object TmDetailTrackerProxy extends ShardingProxy[Fcid, TmDetailTracker.Cmd] {
  val entityKey = EntityTypeKey[TmDetailTracker.Cmd]("flinkTmDetailTracker")

  val marshallKey   = fcid => s"tmDt@${fcid.clusterId}@${fcid.namespace}"
  val unmarshallKey = _.split('@').contra(arr => arr(1) -> arr(2))

  def apply(potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] = action(
    createBehavior = entityId => TmDetailTracker(entityId, potaConf, flinkEndpointQuery),
    stopMessage = TmDetailTracker.Stop,
    bindRole = NodeRole.FlinkOperator.toString
  )
}

/**
 * Flink Taskmanager details tracker.
 */
private[observer] object TmDetailTracker {
  sealed trait Cmd                                                                   extends CborSerializable
  final case object Start                                                            extends Cmd
  final case object Stop                                                             extends Cmd
  sealed trait Query                                                                 extends Cmd
  final case class ListTmDetails(reply: ActorRef[Set[FlinkTmDetail]])                extends Query
  final case class GetTmDetail(tmId: String, reply: ActorRef[Option[FlinkTmDetail]]) extends Query
  final case class ListTmIds(reply: ActorRef[Set[String]])                           extends Query
  private case class RefreshRecords(records: Vector[FlinkTmDetail])                  extends Cmd

  def apply(fcidStr: String, potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      val fcid = TmDetailTrackerProxy.unmarshallKey(fcidStr)
      ctx.log.info(s"Flink TmDetailTracker actor initialized, ${fcid.show}")
      new TmDetailTracker(fcid, potaConf, flinkEndpointQuery).action
    }

}

private class TmDetailTracker(
    fcid: Fcid,
    potaConf: PotaConf,
    endpointQuery: RestEndpointQuery
  )(implicit ctx: ActorContext[TmDetailTracker.Cmd]) {
  import TmDetailTracker._

  private var proc: Option[CancelableFuture[Unit]] = None
  private var state: Set[FlinkTmDetail]            = Set.empty

  def action: Behavior[Cmd] = Behaviors.receiveMessage {
    case Start =>
      if (proc.isEmpty) {
        proc = Some(pollingTmDetailApi.provide(PotaLogger.layer(potaConf.log)).runToFuture)
        ctx.log.info(s"Flink TmDetailTracker actor started, ${fcid.show}")
      }
      Behaviors.same

    case Stop =>
      proc.map(_.cancel())
      ctx.log.info(s"Flink TmDetailTracker actor started, ${fcid.show}")
      Behaviors.same

    case RefreshRecords(records) =>
      state = records.toSet
      Behaviors.same

    case ListTmDetails(reply) =>
      reply ! state
      Behaviors.same

    case GetTmDetail(tmId, reply) =>
      reply ! state.find(_.id == tmId)
      Behaviors.same

    case ListTmIds(reply) =>
      reply ! state.map(_.id)
      Behaviors.same
  }

  private val pollingTmDetailApi = {
    implicit val flkConf = potaConf.flink
    val queryParallelism = potaConf.flink.snapshotQuery.parallelism

    def polling(mur: Ref[Int]) =
      for {
        restUrl <- endpointQuery.get(fcid)
        tmDetails <- ZStream
          .fromIterableZIO(flinkRest(restUrl.chooseUrl).listTaskManagerIds)
          .mapZIOParUnordered(queryParallelism)(flinkRest(restUrl.chooseUrl).getTaskManagerDetail)
          .runFold(Vector.empty[FlinkTmDetail])(_ :+ _)
        preMur <- mur.get
        curMur = MurmurHash3.arrayHash(tmDetails.toArray)
        _ <- ctx.self
          .tellZIO(RefreshRecords(tmDetails))
          .mapError(ActorInteropErr)
          .zip(mur.set(curMur))
          .when(preMur != curMur)
      } yield ()

    Ref.make(0).flatMap(mur => loopTrigger(potaConf.flink.tracking.tmdDetailPolling)(polling(mur)))
  } @@ annotated(fcid.toAnno :+ "akkaSource" -> ctx.self.path.toString: _*)
}
