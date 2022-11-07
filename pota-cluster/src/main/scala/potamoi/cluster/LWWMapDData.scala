package potamoi.cluster

import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.ddata.Replicator.{GetResponse, NotFound, ReadConsistency, UpdateResponse, WriteConsistency}
import akka.cluster.ddata.typed.scaladsl.{DistributedData, Replicator, ReplicatorMessageAdapter}
import akka.cluster.ddata.{LWWMap, LWWMapKey, SelfUniqueAddress}
import akka.util.Timeout
import potamoi.common.ActorExtension.BehaviorWrapper

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
  def init: LWWMap[Key, Value] = LWWMap.empty

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
   * Start actor behavior.
   */
  protected def start(askTimeout: Timeout)(
      get: (GetCmd, LWWMap[Key, Value]) => Unit = (_, _) => (),
      notYetInit: GetCmd => Unit = _ => (),
      update: (UpdateCmd, LWWMap[Key, Value]) => LWWMap[Key, Value] = (_, m) => m): Behavior[Cmd] = {

    Behaviors.setup { implicit ctx =>
      implicit val node             = DistributedData(ctx.system).selfUniqueAddress
      implicit val timeout: Timeout = askTimeout
      ctx.log.info(s"Distributed data actor[$cacheId] started.")
      action(get, notYetInit, update).onFailure[Exception](restart)
    }
  }

  /**
   * Receive message behavior.
   */
  protected def action(
      get: (GetCmd, LWWMap[Key, Value]) => Unit = (_, _) => (),
      notYetInit: GetCmd => Unit = _ => (),
      update: (UpdateCmd, LWWMap[Key, Value]) => LWWMap[Key, Value] = (_, m) => m)(
      implicit ctx: ActorContext[Cmd],
      node: SelfUniqueAddress,
      askTimeout: Timeout): Behavior[Cmd] = {

    var firstNotFoundRsp = true

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

        // get replica successfully
        case InternalGet(rsp @ Replicator.GetSuccess(cacheKey), cmd) =>
          val map = rsp.get(cacheKey)
          cmd match {
            case Get(key, reply)      => reply ! map.get(key)
            case Contains(key, reply) => reply ! map.contains(key)
            case ListKeys(reply)      => reply ! map.entries.keys.toSet
            case ListAll(reply)       => reply ! map.entries
            case Size(reply)          => reply ! map.size
            case c: GetCmd            => get(c, map)
          }
          Behaviors.same

        // update replica successfully
        case InternalUpdate(_ @Replicator.UpdateSuccess(_)) =>
          Behaviors.same

        // fail to get replica
        case InternalGet(rsp, cmd) =>
          rsp match {
            case NotFound(_, _) =>
              if (firstNotFoundRsp) cmd match {
                case Get(_, reply)      => reply ! None
                case Contains(_, reply) => reply ! false
                case ListKeys(reply)    => reply ! Set.empty
                case ListAll(reply)     => reply ! Map.empty
                case Size(reply)        => reply ! 0
                case c: GetCmd          => notYetInit(c)
              }
              else {
                firstNotFoundRsp = false
                ctx.log.error(s"Get data replica failed: ${rsp.toString}")
              }
            case _ => ctx.log.error(s"Get data replica failed: ${rsp.toString}")
          }
          Behaviors.same

        // fail to update replica
        case InternalUpdate(rsp) =>
          ctx.log.error(s"Update data replica failed: ${rsp.toString}")
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
