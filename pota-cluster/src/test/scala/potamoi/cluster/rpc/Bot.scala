package potamoi.cluster.rpc

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import potamoi.cluster.{CborSerializable, RpcClientActor, RpcServerActor}
import potamoi.cluster.Rpc.{ProtoReply, ProtoReq}
import potamoi.cluster.rpc.BotErr.Err
import potamoi.common.ActorInteropException
import zio.Console.printLine
import zio.{IO, ZIO, ZLayer}
import zio.ZIO.succeed

import scala.concurrent.duration.DurationInt
import scala.language.implicitConversions

/**
 * Service effect definition.
 */
trait Bot {
  def touch(): IO[BotErr, Unit]
  def greet(msg: String): IO[BotErr, String]
}

/**
 * Side effect type.
 */
sealed trait BotErr extends CborSerializable
object BotErr {
  case class Err(str: String)                  extends BotErr
  case class ActorInteropErr(cause: Throwable) extends BotErr

  implicit def actorErrAdapter(ex: ActorInteropException): BotErr = ActorInteropErr(ex)
}

/**
 * Rpc proto definition.
 */
object BotProto {
  sealed trait Req                                                 extends ProtoReq
  case class Touch(reply: ProtoReply[BotErr, Unit])                extends Req
  case class Greet(msg: String, reply: ProtoReply[BotErr, String]) extends Req

  val SvcId = "BotService"
}

/**
 * Server effect implementation.
 */
class BotImpl extends Bot {
  override def touch(): IO[BotErr, Unit]              = printLine("Bot has been touched!").mapError(e => Err(e.getMessage))
  override def greet(msg: String): IO[BotErr, String] = succeed(s"Reply from Bot : Hello, $msg")
}

object BotImpl {
  val live = ZLayer.succeed(new BotImpl)
}

/**
 * Rpc server
 */
object BotRpcServer extends RpcServerActor[BotProto.Req] {
  import BotProto._

  val init = ZIO.service[Bot].flatMap { impl =>
    provideActor(SvcId) {
      case Touch(reply)      => bind(reply, impl.touch())
      case Greet(msg, reply) => bind(reply, impl.greet(msg))
    }
  }
}

/**
 * Rpc client
 */
object BotRemote extends RpcClientActor[BotProto.Req] {
  import BotProto._

  val live = ZLayer {
    provideActor(SvcId).map(ctx => new Live()(ctx.actor, ctx.sc))
  }

  class Live(implicit clientActor: ActorRef[BotRemote.Request], sc: Scheduler) extends Bot {
    implicit val timeout: Timeout                       = 2.seconds
    override def touch(): IO[BotErr, Unit]              = bind(ref => Touch(ref))
    override def greet(msg: String): IO[BotErr, String] = bind(ref => Greet(msg, ref))
  }
}