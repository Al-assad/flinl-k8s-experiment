package potamoi.flink.observer

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import potamoi.cluster.{CborSerializable, ShardingProxy}
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.config.{NodeRole, PotaConf}
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.FlinkOprErr
import potamoi.flink.share.model.{Fcid, FlinkJobOverview}
import potamoi.logger.PotaLogger
import potamoi.syntax._
import potamoi.timex._
import potamoi.ziox._
import zio.ZIOAspect.annotated
import zio.{CancelableFuture, Ref}

import scala.util.hashing.MurmurHash3

/**
 * Akka cluster sharding proxy for [[JobsOvTracker]].
 */
private[observer] object JobsOvTrackerProxy extends ShardingProxy[Fcid, JobsOvTracker.Cmd] {
  val entityKey   = EntityTypeKey[JobsOvTracker.Cmd]("flinkJobTracker")
  val marshallKey = marshallFcid

  def apply(potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] = action(
    createBehavior = entityId => JobsOvTracker(entityId, potaConf, flinkEndpointQuery),
    stopMessage = JobsOvTracker.Stop,
    bindRole = NodeRole.FlinkOperator.toString
  )
}

/**
 * Job overview info tracker for single flink cluster.
 */
private[observer] object JobsOvTracker {

  sealed trait Cmd                                                                             extends CborSerializable
  final case object Start                                                                      extends Cmd
  final case object Stop                                                                       extends Cmd
  sealed trait Query                                                                           extends Cmd
  final case class ListJobOverviews(reply: ActorRef[Set[FlinkJobOverview]])                    extends Query
  final case class GetJobOverview(jobId: String, reply: ActorRef[Option[FlinkJobOverview]])    extends Query
  final case class GetJobOverviews(jobId: Set[String], reply: ActorRef[Set[FlinkJobOverview]]) extends Query
  private case class RefreshRecords(records: Set[FlinkJobOverview])                            extends Cmd

  def apply(fcidStr: String, potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] = {
    Behaviors.setup { implicit ctx =>
      val fcid     = unMarshallFcid(fcidStr)
      val idxCache = ctx.spawn(JobIdxCache(potaConf.akka.ddata.getFlinkJobIndex), "flkJobIndexCache")
      ctx.log.info(s"Flink JobsOvTracker actor initialized, fcid=$fcid")
      new JobsOvTracker(fcid, potaConf: PotaConf, flinkEndpointQuery, idxCache).action
    }
  }
}

private class JobsOvTracker(
    fcid: Fcid,
    potaConf: PotaConf,
    flinkEndpointQuery: RestEndpointQuery,
    idxCache: ActorRef[JobIdxCache.Cmd]
  )(implicit ctx: ActorContext[JobsOvTracker.Cmd]) {
  import JobsOvTracker._

  private var proc: Option[CancelableFuture[Unit]] = None
  private var firstRefresh: Boolean                = true
  private var state: Set[FlinkJobOverview]         = Set.empty

  def action: Behavior[Cmd] = Behaviors.receiveMessage {
    case Start =>
      if (proc.isEmpty) {
        proc = Some(pollingJobOverviewApi.provide(PotaLogger.layer(potaConf.log)).runToFuture)
        ctx.log.info(s"Flink JobsOvTracker actor started, fcid=$fcid")
      }
      Behaviors.same

    case Stop =>
      proc.map(_.cancel())
      idxCache ! JobIdxCache.RemoveAll(state.map(_.fjid))
      ctx.log.info(s"Flink JobsOvTracker actor stopped, fcid=$fcid")
      Behaviors.stopped

    case RefreshRecords(records) =>
      // update jobs index cache
      if (firstRefresh) {
        idxCache ! JobIdxCache.RemoveBySelectKey(_.fcid == fcid)
        idxCache ! JobIdxCache.PutAll(records.map(JobIndex.of).toMap)
        firstRefresh = false
      } else {
        val intersect = records intersect state
        val puts      = records diff intersect
        val removes   = records diff intersect
        idxCache ! JobIdxCache.RemoveAll(removes.map(_.fjid))
        idxCache ! JobIdxCache.PutAll(puts.map(JobIndex.of).toMap)
      }
      state = records
      Behaviors.same

    case ListJobOverviews(reply) =>
      reply ! state
      Behaviors.same

    case GetJobOverview(jobId, reply) =>
      reply ! state.find(_.jobId == jobId)
      Behaviors.same

    case GetJobOverviews(jobIds, reply) =>
      reply ! state.filter(job => jobIds.contains(job.jobId))
      Behaviors.same
  }

  /**
   * Polling flink job overview rest api.
   */
  private val pollingJobOverviewApi = {
    def polling(mur: Ref[Int]) =
      for {
        restUrl <- flinkEndpointQuery.get(fcid)
        ovInfo  <- flinkRest(restUrl.chooseUrl(potaConf.flink)).listJobOverviewInfo.map(_.toSet)
        preMur  <- mur.get
        curMur = MurmurHash3.setHash(ovInfo)
        ovs    = ovInfo.map(_.toFlinkJobOverview(fcid))
        _ <- ctx.self
          .tellZIO(RefreshRecords(ovs))
          .mapError(FlinkOprErr.ActorInteropErr)
          .zip(mur.set(curMur))
          .when(curMur != preMur)
      } yield ()

    Ref.make(0).flatMap { mur => loopTrigger(potaConf.flink.tracking.jobPolling)(polling(mur)) }
  } @@ annotated(fcid.toAnno :+ "akkaSource" -> ctx.self.path.toString: _*)
}
