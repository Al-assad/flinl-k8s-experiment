package potamoi.flink.observer

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.common.ziox
import potamoi.conf.FlinkConf
import potamoi.flink.observer.TrackersDispatcher.unMarshallFcid
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.Fjid
import zio.Schedule.spaced
import zio.{CancelableFuture, Duration}

/**
 * Job overview info tracker for single flink cluster.
 */
object JobsTracker {

  sealed trait Cmd
  final case object Start extends Cmd
  final case object Stop  extends Cmd

  def apply(fcidStr: String, flinkConf: FlinkConf, jobOvCache: ActorRef[JobOverviewCache.Cmd], flinkObserver: FlinkK8sObserver): Behavior[Cmd] =
    Behaviors.setup[Cmd] { implicit ctx =>
      val fcid                                 = unMarshallFcid(fcidStr)
      val pollInterval                         = Duration.fromMillis(flinkConf.jobTrackingPollInterval.toMillis)
      var proc: Option[CancelableFuture[Unit]] = None
      ctx.log.info(s"Flink JobsTracker actor initialized, fcid=$fcid")

      Behaviors.receiveMessage {
        case Start =>
          if (proc.isDefined) Behaviors.same
          else {
            val retrieveJobOvInfos = for {
              restUrl <- flinkObserver.retrieveRestEndpoint(fcid)
              jobOvs  <- flinkRest(restUrl.chooseUrl(flinkConf)).listJobOverviewInfo
              _       <- jobOvs.map(ov => jobOvCache !!> JobOverviewCache.Put(Fjid(fcid, ov.jid), ov)).reduce(_ zip _)
            } yield ()
            val io = ziox.zioRunToFuture(retrieveJobOvInfos.ignore.schedule(spaced(pollInterval)).forever)
            proc = Some(io)
            ctx.log.info(s"Flink JobsTracker actor started, fcid=$fcid")
            Behaviors.same
          }

        case Stop =>
          proc.map(_.cancel())
          ctx.log.info(s"Flink JobsTracker actor stopped, fcid=$fcid")
          Behaviors.stopped
      }
    }
}
