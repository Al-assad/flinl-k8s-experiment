package potamoi.flink.observer

import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import potamoi.common.ActorExtension.BehaviorWrapper
import potamoi.config.{NodeRole, PotaConf}
import potamoi.flink.share.model.{Fcid, Fjid, FlinkJobOverview}

/**
 * Akka cluster sharding dispatcher for [[JobsTracker]].
 */
object JobsTrackDispatcher {

  sealed trait Cmd
  final case class Track(fcid: Fcid)                                                                          extends Cmd
  final case class UnTrack(fcid: Fcid)                                                                        extends Cmd
  sealed trait Query                                                                                          extends Cmd
  final case class GetJobOv(fjid: Fjid, reply: ActorRef[Option[FlinkJobOverview]])                            extends Query
  final case class GetJobOvInCluster(fcid: Fcid, jobIds: Set[String], reply: ActorRef[Set[FlinkJobOverview]]) extends Query
  final case class ListJobOvInCluster(fcid: Fcid, reply: ActorRef[Set[FlinkJobOverview]])                     extends Query
  private case class ValidateExistsThen(exists: Boolean, cmd: Query)                                          extends Cmd

  val JobsTrackerKey = EntityTypeKey[JobsTracker.Cmd]("flinkJobTracker")

  def apply(potaConf: PotaConf, idxCache: ActorRef[JobOvIndexCache.Cmd], flinkEndpointQuery: RestEndpointQuery): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      val sharding = ClusterSharding(ctx.system)
      val region = sharding.init(
        Entity(JobsTrackerKey)(entityCxt => JobsTracker(entityCxt.entityId, potaConf, flinkEndpointQuery))
          .withStopMessage(JobsTracker.Stop)
          .withRole(NodeRole.FlinkOperator.toString)
          .withSettings(ClusterShardingSettings(ctx.system).withNoPassivationStrategy())
      )
      ctx.log.info("Flink JobsTraceDispatcher actor started.")

      Behaviors
        .receiveMessage[Cmd] {
          case Track(fcid) =>
            region ! ShardingEnvelope(marshallFcid(fcid), JobsTracker.Start)
            Behaviors.same

          case UnTrack(fcid) =>
            region ! ShardingEnvelope(marshallFcid(fcid), JobsTracker.Stop)
            Behaviors.same

          case cmd: Query =>
            cmd match {
              case c @ GetJobOv(fjid, _) =>
                idxCache ! JobOvIndexCache.SelectKeyExists(_ == fjid, ctx.messageAdapter(ref => ValidateExistsThen(ref, c)))
              case c @ GetJobOvInCluster(fcid, _, _) =>
                idxCache ! JobOvIndexCache.SelectKeyExists(_.fcid == fcid, ctx.messageAdapter(ref => ValidateExistsThen(ref, c)))
              case c @ ListJobOvInCluster(fcid, _) =>
                idxCache ! JobOvIndexCache.SelectKeyExists(_.fcid == fcid, ctx.messageAdapter(ref => ValidateExistsThen(ref, c)))
            }
            Behaviors.same

          case ValidateExistsThen(true, cmd) =>
            cmd match {
              case GetJobOv(fjid, reply) =>
                region ! ShardingEnvelope(marshallFcid(fjid.fcid), JobsTracker.GetJobOverview(fjid.jobId, reply))
              case GetJobOvInCluster(fcid, jobIds, reply) =>
                region ! ShardingEnvelope(marshallFcid(fcid), JobsTracker.GetJobOverviews(jobIds, reply))
              case ListJobOvInCluster(fcid, reply) =>
                region ! ShardingEnvelope(marshallFcid(fcid), JobsTracker.ListJobOverviews(reply))
            }
            Behaviors.same

          case ValidateExistsThen(false, cmd) =>
            cmd match {
              case GetJobOv(_, reply)             => reply ! None
              case GetJobOvInCluster(_, _, reply) => reply ! Set.empty
              case ListJobOvInCluster(_, reply)   => reply ! Set.empty
            }
            Behaviors.same
        }
        .onFailure[Exception](restart)
    }
}
