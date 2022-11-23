package potamoi.flink.observer

import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.Receptionist.Listing
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import potamoi.cluster.CborSerializable
import potamoi.common.ActorExtension.{ActorRefWrapper, BehaviorWrapper}
import potamoi.config.{FlinkConf, LogConf}
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.model.{Fcid, FlinkJobOverview}
import potamoi.logger.PotaLogger
import potamoi.timex._
import potamoi.ziox._
import zio.Schedule.spaced
import zio.{CancelableFuture, Ref}

import scala.util.hashing.MurmurHash3

/**
 * Job overview info tracker for single flink cluster.
 */
object JobsTracker {

  sealed trait Cmd                                                                             extends CborSerializable
  final case object Start                                                                      extends Cmd
  final case object Stop                                                                       extends Cmd
  final case class ListJobOverviews(reply: ActorRef[Set[FlinkJobOverview]])                    extends Cmd
  final case class GetJobOverview(jobId: String, reply: ActorRef[Option[FlinkJobOverview]])    extends Cmd
  final case class GetJobOverviews(jobId: Set[String], reply: ActorRef[Set[FlinkJobOverview]]) extends Cmd

  private case class FoundJobOvCacheSvcListing(listing: Listing)    extends Cmd
  private case class RefreshRecords(records: Set[FlinkJobOverview]) extends Cmd

  def apply(fcidStr: String, logConf: LogConf, flinkConf: FlinkConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      Behaviors.withStash[Cmd](100) { stash =>
        Behaviors.receiveMessage {
          case FoundJobOvCacheSvcListing(listing) =>
            // find local job overview cache service
            listing.serviceInstances(JobOvIndexCache.serviceKey).find(_.path.address == ctx.system.address) match {
              case None =>
                ctx.log.error("Flink JobsTracker actor failed to initialize: JobOvIndexCache actor not found.")
                Behaviors.stopped
              case Some(cache) =>
                val fcid = unMarshallFcid(fcidStr)
                ctx.log.info(s"Flink JobsTracker actor initialized, fcid=$fcid")
                new JobsTracker(fcid, logConf, flinkConf, flinkEndpointQuery, cache).action
                Behaviors.same
            }
          case cmd =>
            stash.stash(cmd)
            Behaviors.same
        }
      } beforeIt {
        ctx.system.receptionist ! Receptionist.Find(JobOvIndexCache.serviceKey, ctx.messageAdapter(FoundJobOvCacheSvcListing))
      }
    }
}

private class JobsTracker(
    fcid: Fcid,
    logConf: LogConf,
    flinkConf: FlinkConf,
    flinkEndpointQuery: RestEndpointQuery,
    idxCache: ActorRef[JobOvIndexCache.Cmd])(implicit ctx: ActorContext[JobsTracker.Cmd]) {
  import JobsTracker._

  private var proc: Option[CancelableFuture[Unit]] = None
  private var firstRefresh: Boolean                = true
  private var state: Set[FlinkJobOverview]         = Set.empty

  def action: Behavior[Cmd] = Behaviors.receiveMessage {
    case Start =>
      if (proc.nonEmpty) Behaviors.same
      else {
        proc = Some(pollingJobOverviewApi.provide(PotaLogger.layer(logConf)).runToFuture)
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
        idxCache ! JobOvIndexCache.PutAll(records.map(e => e.fjid -> JobOvIndex(e.state)).toMap)
        firstRefresh = false
      } else {
        val intersect = records intersect state
        val puts      = records diff intersect
        val removes   = records diff intersect
        idxCache ! JobOvIndexCache.RemoveAll(removes.map(_.fjid))
        idxCache ! JobOvIndexCache.PutAll(puts.map(e => e.fjid -> JobOvIndex(e.state)).toMap)
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
        ovInfo  <- flinkRest(restUrl.chooseUrl(flinkConf)).listJobOverviewInfo.map(_.toSet)
        preMur  <- mur.get
        curMur = MurmurHash3.setHash(ovInfo)
        ovs    = ovInfo.map(_.toFlinkJobOverview(fcid))
        _ <- ((ctx.self !> RefreshRecords(ovs)) *> mur.set(curMur)).when(curMur != preMur)
      } yield ()
    Ref.make(0).flatMap(polling(_).ignore.schedule(spaced(flinkConf.tracking.jobPolling)).forever)
  }
}
