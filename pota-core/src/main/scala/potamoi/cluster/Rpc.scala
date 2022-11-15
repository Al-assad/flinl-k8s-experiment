package potamoi.cluster

import akka.actor.typed.ActorRef

object Rpc {

  /**
   * Rpc reply type
   */
  type ProtoReply[E, A] = ActorRef[MayBe[E, A]]

  /**
   * Rpc request proto trait type.
   */
  trait ProtoReq extends CborSerializable with Product

  /**
   * Either wrapper case class to bypass the band of Akka Jackson serialization for [[scala.util.Either]].
   */
  case class MayBe[E, A](value: Either[E, A]) extends CborSerializable

  /**
   * Not found remote rpc service error.
   */
  case class RpcServerNotFound(svcId: String) extends Exception(s"Rpc server for [$svcId] not found")

}
