package potamoi.cluster

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import potamoi.common.ActorExtension.BehaviorWrapper
import potamoi.common.Syntax.GenericPF

/**
 * Cluster sharding proxy for Akka actor.
 *
 * @tparam ShardKey type of sharding key
 * @tparam ProxyCommand type of proxy actor
 */
trait ShardingProxy[ShardKey, ProxyCommand] {

  sealed trait Cmd
  final case class Proxy(key: ShardKey, cmd: ProxyCommand) extends Cmd

  /**
   * Sharding entity key
   */
  def entityKey: EntityTypeKey[ProxyCommand]

  /**
   * Marshall ShardKey type to String.
   */
  def marshallKey: ShardKey => String

  /**
   * Action behavior.
   */
  protected def action(region: (ActorContext[Cmd], ClusterSharding) => ActorRef[ShardingEnvelope[ProxyCommand]]): Behavior[Cmd] =
    Behaviors.setup { ctx =>
      val sharding    = ClusterSharding(ctx.system)
      val shardRegion = region(ctx, sharding)
      ctx.log.info(s"Sharding proxy actor for [${entityKey.name}] started.")
      Behaviors
        .receiveMessage[Cmd] { case Proxy(key, cmd) =>
          shardRegion ! ShardingEnvelope(marshallKey(key), cmd)
          Behaviors.same
        }
        .onFailure[Exception](restart)
    }

  type EntityId = String

  /**
   * Simpler action behavior.
   */
  protected def action(
      createBehavior: EntityId => Behavior[ProxyCommand],
      stopMessage: ProxyCommand,
      bindRole: Option[String] = None): Behavior[Cmd] = action { (ctx, sharding) =>
    sharding.init {
      Entity(entityKey)(entityCtx => createBehavior(entityCtx.entityId))
        .withStopMessage(stopMessage)
        .withSettings(ClusterShardingSettings(ctx.system).withNoPassivationStrategy())
        .contra { it =>
          bindRole match {
            case Some(role) => it.withRole(role)
            case None       => it
          }
        }
    }
  }

}
