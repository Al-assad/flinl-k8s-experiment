package potamoi.flink.observer

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import potamoi.config.FlinkConf
import potamoi.flink.observer.TrackersDispatcher.unMarshallFcid
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.repo.FlinkJobOverviewRepo
import potamoi.flink.share.{Fcid, FlinkJobOverview}
import potamoi.timex._
import potamoi.ziox._
import zio.Schedule.spaced
import zio.{CancelableFuture, Ref}

/**
 * Job overview info tracker for single flink cluster.
 */
object JobsTracker {

  sealed trait Cmd
  final case object Start extends Cmd
  final case object Stop  extends Cmd

  def apply(fcidStr: String, flinkConf: FlinkConf, flinkObserver: FlinkK8sObserver, flkJobOvRepo: FlinkJobOverviewRepo): Behavior[Cmd] =
    Behaviors.setup[Cmd] { implicit ctx =>
      val fcid = unMarshallFcid(fcidStr)
      ctx.log.info(s"Flink JobsTracker actor initialized, fcid=$fcid")
      new JobsTracker(fcid, flinkConf, flinkObserver, flkJobOvRepo).action
    }
}

import potamoi.flink.observer.JobsTracker._

private class JobsTracker(fcid: Fcid, flinkConf: FlinkConf, flinkObserver: FlinkK8sObserver, flkJobOvRepo: FlinkJobOverviewRepo)(
    implicit ctx: ActorContext[JobsTracker.Cmd]) {

  private var proc: Option[CancelableFuture[Unit]] = None

  def action: Behavior[Cmd] = Behaviors.receiveMessage {
    case Start =>
      if (proc.isDefined) Behaviors.same
      else {
        proc = Some(pollingJobOverviewInfo.runToFuture)
        ctx.log.info(s"Flink JobsTracker actor started, fcid=$fcid")
        Behaviors.same
      }
    case Stop =>
      proc.map(_.cancel())
      ctx.log.info(s"Flink JobsTracker actor stopped, fcid=$fcid")
      Behaviors.stopped
  }

  private def pollingJobOverviewInfo = {
    def touchApi(state: Ref[Vector[FlinkJobOverview]]) = for {
      restUrl    <- flinkObserver.retrieveRestEndpoint(fcid)
      curCollect <- flinkRest(restUrl.chooseUrl(flinkConf)).listJobOverviewInfo.map(_.map(_.toFlinkJobOverview(fcid)))
      preCollect <- state.get
      (puts, removes) = {
        val intersect = curCollect intersect preCollect
        val puts      = curCollect diff intersect
        val removes   = preCollect diff intersect
        puts -> removes
      }
      _ <- flkJobOvRepo.collect(puts).when(puts.nonEmpty) *>
        flkJobOvRepo.cur.removeAll(removes.map(e => e.fjid)).when(removes.nonEmpty)
      _ <- state.set(curCollect)
    } yield ()

    for {
      state <- Ref.make(Vector.empty[FlinkJobOverview])
      _     <- touchApi(state).ignore.schedule(spaced(flinkConf.tracking.jobPolling)).forever
    } yield ()
  }

}
