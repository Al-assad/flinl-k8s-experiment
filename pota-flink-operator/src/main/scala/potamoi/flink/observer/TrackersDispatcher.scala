package potamoi.flink.observer

import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import potamoi.common.ActorExtension.BehaviorWrapper
import potamoi.common.GenericPF
import potamoi.conf.{FlinkConf, NodeRole}
import potamoi.flink.share.Fcid

/**
 * Flink tracker actors dispatcher.
 */
object TrackersDispatcher {
  sealed trait Cmd
  final case class Track(fcid: Fcid)   extends Cmd
  final case class UnTrack(fcid: Fcid) extends Cmd

  val JobsTrackerEntityKey = EntityTypeKey[JobsTracker.Cmd]("flink-job-tracker")

  def apply(flinkConf: FlinkConf, jobOvCache: ActorRef[JobStatusCache.Cmd], flinkObserver: FlinkK8sObserver): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      val sharding = ClusterSharding(ctx.system)

      val jobTrackerRegion = sharding.init(
        Entity(JobsTrackerEntityKey)(entityCxt => JobsTracker(entityCxt.entityId, flinkConf, jobOvCache, flinkObserver))
          .withSettings(ClusterShardingSettings(ctx.system).withNoPassivationStrategy)
          .withStopMessage(JobsTracker.Stop)
          .withRole(NodeRole.FlinkOperator.toString)
      )
      ctx.log.info("Flink TrackersDispatcher actor started.")

      Behaviors
        .receiveMessage[Cmd] {
          case Track(fcid) =>
            jobTrackerRegion ! ShardingEnvelope(marshallFcid(fcid), JobsTracker.Start)
            Behaviors.same
          case UnTrack(fcid) =>
            jobTrackerRegion ! ShardingEnvelope(marshallFcid(fcid), JobsTracker.Stop)
            Behaviors.same
        }
        .onFailure[Exception](restart)
    }

  // marshall/unmarshall between fcid and cluster-sharding entity id.
  def marshallFcid(fcid: Fcid): String   = s"${fcid.clusterId}@${fcid.namespace}"
  def unMarshallFcid(fcid: String): Fcid = fcid.split('@').contra { arr => Fcid(arr(0), arr(1)) }
}
