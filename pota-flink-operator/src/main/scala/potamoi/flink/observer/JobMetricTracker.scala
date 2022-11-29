package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import potamoi.actorx._
import potamoi.cluster.{CborSerializable, ShardingProxy}
import potamoi.config.{NodeRole, PotaConf}
import potamoi.flink.operator.flinkRest
import potamoi.flink.share.FlinkOprErr.ActorInteropErr
import potamoi.flink.share.JobId
import potamoi.flink.share.model.{Fcid, Fjid, FlinkJobMetrics}
import potamoi.logger.PotaLogger
import potamoi.syntax._
import potamoi.timex._
import potamoi.ziox._
import zio.CancelableFuture
import zio.ZIOAspect.annotated
import zio.stream.ZStream

import scala.collection.mutable

/**
 * Akka cluster sharding proxy for [[JobMetricTracker]].
 */
private[observer] object JobMetricTrackerProxy extends ShardingProxy[Fcid, JobMetricTracker.Cmd] {
  val entityKey   = EntityTypeKey[JobMetricTracker.Cmd]("flinkJobMetricsTracker")
  val marshallKey = marshallFcid

  def apply(potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] = action(
    createBehavior = entityId => JobMetricTracker(entityId, potaConf, flinkEndpointQuery),
    stopMessage = JobMetricTracker.Stop,
    bindRole = NodeRole.FlinkOperator.toString
  )
}

/**
 * Flink job metrics tracker.
 */
private[observer] object JobMetricTracker {
  sealed trait Cmd                                                                        extends CborSerializable
  final case object Start                                                                 extends Cmd
  final case object Stop                                                                  extends Cmd
  sealed trait Query                                                                      extends Cmd
  final case class GetJobMetrics(jobId: String, reply: ActorRef[Option[FlinkJobMetrics]]) extends Query
  final case class ListJobMetrics(reply: ActorRef[Set[FlinkJobMetrics]])                  extends Query

  private case class RefreshJobIdListing(jobIds: Vector[String]) extends Cmd
  private case class RefreshRecord(record: FlinkJobMetrics)      extends Cmd

  def apply(fcidStr: String, potaConf: PotaConf, flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      val fcid = unMarshallFcid(fcidStr)
      ctx.log.info(s"Flink JobMetricTracker actor initialized, fcid=$fcid")
      new JobMetricTracker(fcid, potaConf, flinkEndpointQuery).action
    }

}

private class JobMetricTracker(
    fcid: Fcid,
    potaConf: PotaConf,
    endpointQuery: RestEndpointQuery
  )(implicit ctx: ActorContext[JobMetricTracker.Cmd]) {
  import JobMetricTracker._

  private var proc: Option[CancelableFuture[Unit]]       = None
  private val state: mutable.Map[JobId, FlinkJobMetrics] = mutable.HashMap.empty

  def action: Behavior[Cmd] = Behaviors.receiveMessage {
    case Start =>
      if (proc.isEmpty) {
        proc = Some(pollingJobMetricsApis.provide(PotaLogger.layer(potaConf.log)).runToFuture)
        ctx.log.info(s"Flink JobMetricTracker actor started, fcid=$fcid")
      }
      Behaviors.same

    case Stop =>
      proc.map(_.cancel())
      ctx.log.info(s"Flink JobMetricTracker actor stopped, fcid=$fcid")
      Behaviors.stopped

    case RefreshJobIdListing(jobIds) =>
      (state.keys.toSet diff jobIds.toSet).foreach(state.remove)
      Behaviors.same

    case RefreshRecord(record) =>
      state.put(record.jobId, record)
      Behaviors.same

    case GetJobMetrics(jobId, reply) =>
      reply ! state.get(jobId)
      Behaviors.same

    case ListJobMetrics(reply) =>
      reply ! state.values.toSet
      Behaviors.same
  }

  private val pollingJobMetricsApis = {
    implicit val flkConf = potaConf.flink
    val queryParallelism = potaConf.flink.snapshotQuery.parallelism

    loopTrigger(potaConf.flink.tracking.tmMetricsPolling) {
      for {
        restUrl <- endpointQuery.get(fcid)
        jobIds  <- flinkRest(restUrl.chooseUrl).listJobsStatusInfo.map(_.map(_.id))
        _       <- ctx.self.tellZIO(RefreshJobIdListing(jobIds)).mapError(ActorInteropErr)
        _ <- ZStream
          .fromIterable(jobIds)
          .mapZIOParUnordered(queryParallelism) { jobId =>
            flinkRest(restUrl.chooseUrl)
              .getJobMetrics(jobId, FlinkJobMetrics.metricsRawKeys)
              .map(FlinkJobMetrics.fromRaw(Fjid(fcid, jobId), _)) @@ annotated("jobId" -> jobId)
          }
          .runForeach(e => ctx.self.tellZIO(RefreshRecord(e)).mapError(ActorInteropErr))
      } yield ()
    } @@ annotated(fcid.toAnno :+ "akkaSource" -> ctx.self.path.toString: _*)
  }

}
