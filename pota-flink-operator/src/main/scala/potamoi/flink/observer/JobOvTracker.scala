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
import zio.Schedule.spaced
import zio.ZIO.logError
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
  sealed private trait Internal                                                                extends Cmd
  private case class RefreshRecords(records: Set[FlinkJobOverview])                            extends Internal

  def apply(fcidStr: String, potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] = {
    Behaviors.setup { implicit ctx =>
      val fcid     = unMarshallFcid(fcidStr)
      val idxCache = ctx.spawn(JobOvIndexCache(potaConf.akka.ddata.getFlinkJobsOvIndex), "flkJobOvIndexCache")
      ctx.log.info(s"Flink JobsTracker actor initialized, fcid=$fcid")
      new JobsOvTracker(fcid, potaConf: PotaConf, flinkEndpointQuery, idxCache).action
    }
  }
}

private class JobsOvTracker(
    fcid: Fcid,
    potaConf: PotaConf,
    flinkEndpointQuery: RestEndpointQuery,
    idxCache: ActorRef[JobOvIndexCache.Cmd]
  )(implicit ctx: ActorContext[JobsOvTracker.Cmd]) {
  import JobsOvTracker._

  private var proc: Option[CancelableFuture[Unit]] = None
  private var firstRefresh: Boolean                = true
  private var state: Set[FlinkJobOverview]         = Set.empty
  private val actorSrc                             = ctx.self.path.toString

  def action: Behavior[Cmd] = Behaviors.receiveMessage {
    case Start =>
      if (proc.nonEmpty) Behaviors.same
      else {
        proc = Some(pollingJobOverviewApi.provide(PotaLogger.layer(potaConf.log)).runToFuture)
        ctx.log.info(s"Flink JobsTracker actor started, fcid=$fcid")
        Behaviors.same
      }

    case Stop =>
      proc.map(_.cancel())
      idxCache ! JobOvIndexCache.RemoveAll(state.map(_.fjid))
      ctx.log.info(s"Flink JobsTracker actor stopped, fcid=$fcid")
      Behaviors.stopped

    case RefreshRecords(records) =>
      // update jobs index cache
      if (firstRefresh) {
        idxCache ! JobOvIndexCache.RemoveBySelectKey(_.fcid == fcid)
        idxCache ! JobOvIndexCache.PutAll(records.map(JobOvIndex.of).toMap)
        firstRefresh = false
      } else {
        val intersect = records intersect state
        val puts      = records diff intersect
        val removes   = records diff intersect
        idxCache ! JobOvIndexCache.RemoveAll(removes.map(_.fjid))
        idxCache ! JobOvIndexCache.PutAll(puts.map(JobOvIndex.of).toMap)
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
  private def pollingJobOverviewApi = {
    def polling(mur: Ref[Int]) =
      for {
        restUrl <- flinkEndpointQuery.get(fcid)
        ovInfo  <- flinkRest(restUrl.chooseUrl(potaConf.flink)).listJobOverviewInfo.map(_.toSet)
        preMur  <- mur.get
        curMur = MurmurHash3.setHash(ovInfo)
        ovs    = ovInfo.map(_.toFlinkJobOverview(fcid))
        _ <- (ctx.self.tellZIO(RefreshRecords(ovs)) *> mur.set(curMur))
          .mapError(FlinkOprErr.ActorInteropErr)
          .when(curMur != preMur)
      } yield ()

    for {
      mur    <- Ref.make(0)
      preErr <- Ref.make[Option[FlinkOprErr]](None)
      effect <- polling(mur)
        .schedule(spaced(potaConf.flink.tracking.jobPolling))
        .tapError { err =>
          // logging non-repetitive error
          preErr.get.flatMap(pre => (logError(err.toPrettyStr) *> preErr.set(err)).when(!pre.contains(err)))
        }
        .ignore
        .forever
    } yield effect
  } @@ annotated(fcid.toAnno :+ "akkaSource" -> actorSrc: _*)
}
