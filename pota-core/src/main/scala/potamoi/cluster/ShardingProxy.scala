package potamoi.cluster

import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import akka.util.Timeout
import potamoi.common.ActorExtension.{ActorRefWrapper, BehaviorWrapper}
import potamoi.common.ActorInteropException
import potamoi.common.Syntax.GenericPF
import zio.IO

import scala.reflect.ClassTag

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

  /**
   * ZIO interop.
   */
  type InteropIO[A] = IO[ActorInteropException, A]

  implicit class ZIOOperation(actor: ActorRef[Cmd])(implicit sc: Scheduler, askTimeout: Timeout) {
    def apply(key: ShardKey): ProxyPartiallyApplied = ProxyPartiallyApplied(key)

    case class ProxyPartiallyApplied(key: ShardKey) {
      def tell(cmd: ProxyCommand): InteropIO[Unit]                               = actor.tellZIO(Proxy(key, cmd))
      def ask[Res: ClassTag](cmd: ActorRef[Res] => ProxyCommand): InteropIO[Res] = actor.askZIO[Res](ref => Proxy(key, cmd(ref)))
    }
  }

}
