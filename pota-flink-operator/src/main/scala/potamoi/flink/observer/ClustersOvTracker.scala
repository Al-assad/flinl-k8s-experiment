package potamoi.flink.observer

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import potamoi.cluster.{CborSerializable, ShardingProxy}
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.config.{NodeRole, PotaConf}
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.FlinkOprErr
import potamoi.flink.share.model.{Fcid, FlinkClusterOverview, FlinkExecMode}
import potamoi.logger.PotaLogger
import potamoi.syntax._
import potamoi.timex._
import potamoi.ziox._
import zio.ZIOAspect.annotated
import zio.{CancelableFuture, Ref}

import scala.util.hashing.MurmurHash3

/**
 * Akka cluster sharding proxy for [[ClustersOvTracker]].
 */
private[observer] object ClustersOvTrackerProxy extends ShardingProxy[Fcid, ClustersOvTracker.Cmd] {
  val entityKey = EntityTypeKey[ClustersOvTracker.Cmd]("flinkClusterTracker")

  val marshallKey   = fcid => s"clOv@${fcid.clusterId}@${fcid.namespace}"
  val unmarshallKey = _.split('@').contra(arr => arr(1) -> arr(2))

  def apply(potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] = action(
    createBehavior = entityId => ClustersOvTracker(entityId, potaConf, flinkEndpointQuery),
    stopMessage = ClustersOvTracker.Stop,
    bindRole = NodeRole.FlinkOperator.toString
  )
}

/**
 * FLink cluster overview tracker.
 */
private[observer] object ClustersOvTracker {
  sealed trait Cmd                                                                   extends CborSerializable
  final case object Start                                                            extends Cmd
  final case object Stop                                                             extends Cmd
  sealed trait Query                                                                 extends Cmd
  final case class GetClusterOverview(reply: ActorRef[Option[FlinkClusterOverview]]) extends Query
  private case class RefreshRecord(record: FlinkClusterOverview)                     extends Cmd

  def apply(fcidStr: String, potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      val fcid     = ClustersOvTrackerProxy.unmarshallKey(fcidStr)
      val idxCache = ctx.spawn(ClusterIndexCache(potaConf.akka.ddata.getFlinkClusterIndex), "flkClusterIdxCache")
      ctx.log.info(s"Flink ClustersOvTracker actor initialized, fcid=$fcid")
      new ClustersOvTracker(fcid, potaConf, flinkEndpointQuery, idxCache).action
    }
}

private class ClustersOvTracker(
    fcid: Fcid,
    potaConf: PotaConf,
    endpointQuery: RestEndpointQuery,
    idxCache: ActorRef[ClusterIndexCache.Cmd]
  )(implicit ctx: ActorContext[ClustersOvTracker.Cmd]) {
  import ClustersOvTracker._

  private var proc: Option[CancelableFuture[Unit]] = None
  private var state: Option[FlinkClusterOverview]  = None

  def action: Behavior[Cmd] = Behaviors.receiveMessage {
    case Start =>
      if (proc.isEmpty) {
        proc = Some(pollingClusterOverviewApi.provide(PotaLogger.layer(potaConf.log)).runToFuture)
        ctx.log.info(s"Flink ClustersOvTracker actor started, fcid=$fcid")
      }
      Behaviors.same

    case Stop =>
      proc.map(_.cancel())
      idxCache ! ClusterIndexCache.Update(fcid, _.copy(execMode = None))
      ctx.log.info(s"Flink ClustersOvTracker actor stopped, fcid=$fcid")
      Behaviors.stopped

    case RefreshRecord(record) =>
      idxCache ! ClusterIndexCache.Upsert(
        fcid,
        putValue = ClusterIndex(execMode = record.execMode),
        updateValue = _.copy(execMode = record.execMode)
      )
      state = record
      Behaviors.same

    case GetClusterOverview(reply) =>
      reply ! state
      Behaviors.same
  }

  private val pollingClusterOverviewApi = {
    implicit val flkConf = potaConf.flink
    def polling(mur: Ref[Int]) =
      for {
        restUrl        <- endpointQuery.get(fcid)
        clusterOvFiber <- flinkRest(restUrl.chooseUrl).getClusterOverview.fork
        execModeFiber  <- flinkRest(restUrl.chooseUrl).getJobmanagerConfig.map(r => FlinkExecMode.ofRawConfValue(r.get("execution.target"))).fork
        clusterOv      <- clusterOvFiber.join
        execMode       <- execModeFiber.join
        preMur         <- mur.get
        curMur = MurmurHash3.productHash(clusterOv -> execMode.id)
        _ <- ctx.self
          .tellZIO(RefreshRecord(clusterOv.toFlinkClusterOverview(fcid, execMode)))
          .mapError(FlinkOprErr.ActorInteropErr)
          .zip(mur.set(curMur))
          .when(curMur != preMur)
      } yield ()

    Ref.make(0).flatMap { mur => loopTrigger(potaConf.flink.tracking.clusterOvPolling)(polling(mur)) }
  } @@ annotated(fcid.toAnno :+ "akkaSource" -> ctx.self.path.toString: _*)

}
