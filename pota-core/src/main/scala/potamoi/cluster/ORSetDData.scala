package potamoi.cluster

import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.ddata.Replicator.{GetResponse, NotFound, UpdateResponse, WriteConsistency}
import akka.cluster.ddata.typed.scaladsl.{DistributedData, Replicator, ReplicatorMessageAdapter}
import akka.cluster.ddata.{ORSet, ORSetKey, SelfUniqueAddress}
import potamoi.common.ActorExtension.BehaviorWrapper
import potamoi.config.DDataConf

/**
 * Akka ORSet type DData structure wrapped implementation.
 */
trait ORSetDData[Value] {

  sealed trait Cmd
  trait GetCmd    extends Cmd
  trait UpdateCmd extends Cmd

  final case class List(reply: ActorRef[Set[Value]])                                extends GetCmd
  final case class Size(reply: ActorRef[Int])                                       extends GetCmd
  final case class Contains(value: Value, reply: ActorRef[Boolean])                 extends GetCmd
  final case class SelectExists(filter: Value => Boolean, reply: ActorRef[Boolean]) extends GetCmd
  final case class Select(filter: Value => Boolean, reply: ActorRef[Set[Value]])    extends GetCmd

  final case class Put(value: Value)             extends UpdateCmd
  final case class PutAll(values: Set[Value])    extends UpdateCmd
  final case class Remove(value: Value)          extends UpdateCmd
  final case class RemoveAll(values: Set[Value]) extends UpdateCmd
  final case object Clear                        extends UpdateCmd

  sealed private trait InternalCmd                                               extends Cmd
  final private case class InternalUpdate(rsp: UpdateResponse[ORSet[Value]])     extends InternalCmd
  final private case class InternalGet(rsp: GetResponse[ORSet[Value]], cmd: Cmd) extends InternalCmd

  /**
   * ORSet cache key.
   */
  def cacheId: String

  /**
   * ORSet initial value.
   */
  def init: ORSet[Value] = ORSet.empty

  lazy val cacheKey = ORSetKey[Value](cacheId)

  /**
   * Start actor behavior.
   */
  // noinspection DuplicatedCode
  protected def start(conf: DDataConf)(
      get: (GetCmd, ORSet[Value]) => Unit = (_, _) => (),
      defaultNotFound: GetCmd => Unit = _ => (),
      update: (UpdateCmd, ORSet[Value]) => ORSet[Value] = (_, s) => s): Behavior[Cmd] = {
    Behaviors.setup { implicit ctx =>
      implicit val node = DistributedData(ctx.system).selfUniqueAddress
      action(conf)(get, defaultNotFound, update).onFailure[Exception](restart)
    }
  }

  // noinspection DuplicatedCode
  protected def action(conf: DDataConf)(
      get: (GetCmd, ORSet[Value]) => Unit = (_, _) => (),
      defaultNotFound: GetCmd => Unit = _ => (),
      update: (UpdateCmd, ORSet[Value]) => ORSet[Value] = (_, s) => s)(implicit ctx: ActorContext[Cmd], node: SelfUniqueAddress): Behavior[Cmd] = {

    implicit val timeout = conf.askTimeout
    val writeLevel       = conf.writeLevel.asAkka
    val readLevel        = conf.readLevel.asAkka

    DistributedData.withReplicatorMessageAdapter[Cmd, ORSet[Value]] { implicit replicator =>
      val modifyShapePF = modifyShape(writeLevel)(_)
      Behaviors.receiveMessage {
        case cmd: GetCmd =>
          replicator.askGet(
            replyTo => Replicator.Get(cacheKey, readLevel, replyTo),
            rsp => InternalGet(rsp, cmd)
          )
          Behaviors.same

        case cmd: UpdateCmd =>
          cmd match {
            case Put(value)        => modifyShapePF(_.add(node, value))
            case PutAll(values)    => modifyShapePF(values.foldLeft(_)((ac, c) => ac.add(node, c)))
            case Remove(value)     => modifyShapePF(_.remove(node, value))
            case RemoveAll(values) => modifyShapePF(values.foldLeft(_)((ac, c) => ac.remove(node, c)))
            case Clear             => modifyShapePF(_.clear(node))
            case c                 => modifyShapePF(update(c, _))
          }
          Behaviors.same

        // get replica successfully
        case InternalGet(rsp @ Replicator.GetSuccess(cacheKey), cmd) =>
          val set = rsp.get(cacheKey)
          cmd match {
            case List(reply)                 => reply ! set.elements
            case Size(reply)                 => reply ! set.size
            case Contains(value, reply)      => reply ! set.elements.contains(value)
            case SelectExists(filter, reply) => reply ! set.elements.exists(filter(_))
            case Select(filter, reply)       => reply ! set.elements.filter(filter(_))
            case c: GetCmd                   => get(c, set)
          }
          Behaviors.same

        // update replica successfully
        case InternalUpdate(_ @Replicator.UpdateSuccess(_)) =>
          Behaviors.same

        // fail to get replica
        case InternalGet(rsp, cmd) =>
          rsp match {
            case NotFound(_, _) =>
              cmd match {
                case List(reply)            => reply ! Set.empty
                case Size(reply)            => reply ! 0
                case Contains(_, reply)     => reply ! false
                case SelectExists(_, reply) => reply ! false
                case Select(_, reply)       => reply ! Set.empty
                case c: GetCmd              => defaultNotFound(c)
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

  // noinspection DuplicatedCode
  private def modifyShape(writeLevel: WriteConsistency)(modify: ORSet[Value] => ORSet[Value])(
      implicit replicator: ReplicatorMessageAdapter[Cmd, ORSet[Value]]): Behavior[Cmd] = {
    replicator.askUpdate(
      replyTo => Replicator.Update(cacheKey, init, writeLevel, replyTo)(modify(_)),
      rsp => InternalUpdate(rsp)
    )
    Behaviors.same
  }

}
