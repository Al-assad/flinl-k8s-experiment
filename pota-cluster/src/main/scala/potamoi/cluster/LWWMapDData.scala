package potamoi.cluster

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.ddata.Replicator.{GetResponse, ReadConsistency, UpdateResponse, WriteConsistency}
import akka.cluster.ddata.typed.scaladsl.{DistributedData, Replicator, ReplicatorMessageAdapter}
import akka.cluster.ddata.{LWWMap, LWWMapKey, SelfUniqueAddress}
import akka.util.Timeout

/**
 * Akka LWWMap type DData structure wrapped implementation.
 */
trait LWWMapDData[Key, Value] {

  sealed trait Cmd
  trait GetCmd    extends Cmd
  trait UpdateCmd extends Cmd

  case class Get(key: Key, reply: ActorRef[Option[Value]]) extends GetCmd
  case class Contains(key: Key, reply: ActorRef[Boolean])  extends GetCmd
  case class ListKeys(reply: ActorRef[Set[Key]])           extends GetCmd
  case class ListAll(reply: ActorRef[Map[Key, Value]])     extends GetCmd
  case class Size(reply: ActorRef[Int])                    extends GetCmd

  case class Put(key: Key, value: Value) extends UpdateCmd
  case class Remove(key: Key)            extends UpdateCmd

  sealed trait InternalCmd                                                             extends Cmd
  final private case class InternalUpdate(rsp: UpdateResponse[LWWMap[Key, Value]])     extends InternalCmd
  final private case class InternalGet(rsp: GetResponse[LWWMap[Key, Value]], cmd: Cmd) extends InternalCmd

  /**
   * LWWMap cache key.
   */
  def cacheId: String

  /**
   * LWWMap initial value.
   */
  def init: LWWMap[Key, Value]

  /**
   * LWWMap write consistency level.
   */
  def writeLevel: WriteConsistency

  /**
   * LWWMap read consistency level.
   */
  def readLevel: ReadConsistency

  lazy val cacheKey = LWWMapKey[Key, Value](cacheId)

  /**
   * Receive message behavior.
   */
  protected def action(
      get: (GetCmd, LWWMap[Key, Value]) => Unit = (_, _) => (),
      update: (UpdateCmd, LWWMap[Key, Value]) => LWWMap[Key, Value] = (_, m) => m)(
      implicit ctx: ActorContext[Cmd],
      node: SelfUniqueAddress,
      askTimeout: Timeout): Behavior[Cmd] = {

    DistributedData.withReplicatorMessageAdapter[Cmd, LWWMap[Key, Value]] { implicit replicator =>
      Behaviors.receiveMessage {
        case cmd: GetCmd =>
          replicator.askGet(
            replyTo => Replicator.Get(cacheKey, readLevel, replyTo),
            rsp => InternalGet(rsp, cmd)
          )
          Behaviors.same

        case cmd: UpdateCmd =>
          cmd match {
            case Put(key, value) => modifyShape(_.put(node, key, value))
            case Remove(key)     => modifyShape(_.remove(node, key))
            case c               => modifyShape(update(c, _))
          }

        case InternalGet(rsp @ Replicator.GetSuccess(cacheId), cmd) =>
          val map = rsp.get(cacheId)
          cmd match {
            case Get(key, reply)      => reply ! map.get(key)
            case Contains(key, reply) => reply ! map.contains(key)
            case ListKeys(reply)      => reply ! map.entries.keys.toSet
            case ListAll(reply)       => reply ! map.entries
            case Size(reply)          => reply ! map.size
            case c: GetCmd            => get(c, map)
          }
          Behaviors.same

        case InternalUpdate(_ @Replicator.UpdateSuccess(_)) =>
          Behaviors.same

        case InternalUpdate(rsp) =>
          ctx.log.error(s"Update data replica failed: ${rsp.toString}")
          Behaviors.same

        case InternalGet(rsp, _) =>
          ctx.log.error(s"Get data replica failed: ${rsp.toString}")
          Behaviors.same
      }
    }
  }

  private def modifyShape(modify: LWWMap[Key, Value] => LWWMap[Key, Value])(
      implicit replicator: ReplicatorMessageAdapter[Cmd, LWWMap[Key, Value]]): Behavior[Cmd] = {
    replicator.askUpdate(
      replyTo => Replicator.Update(cacheKey, init, writeLevel, replyTo)(modify(_)),
      rsp => InternalUpdate(rsp)
    )
    Behaviors.same
  }

}
