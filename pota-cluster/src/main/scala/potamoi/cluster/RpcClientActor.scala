package potamoi.cluster

import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed._
import akka.actor.typed.receptionist.Receptionist.Listing
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.cluster.Rpc.{MayBe, RpcServerNotFound}
import potamoi.cluster.RpcClientActor.RpcServiceNotFound
import potamoi.common.ActorExtension._
import potamoi.common.ActorInteropException
import potamoi.common.CollectionExtension.IterableWrapper
import zio.ZIO.{fail, succeed}
import zio.{IO, ZIO}

import scala.reflect.ClassTag

/**
 * Rpc client base on Akka.
 */
abstract class RpcClientActor[Proto <: Product: ClassTag] {

  sealed trait Request
  final case class CallRemote(cmd: Proto)                extends Request
  final private case class RefreshSvcList(list: Listing) extends Request

  /**
   * Provide rpc client actor.
   */
  protected def provideActor(svcId: String): ZIO[ActorGuardian, Throwable, LayerContext] = {
    for {
      system   <- ZIO.service[ActorGuardian]
      behavior <- ZIO.succeed(provide(svcId))
      actor    <- system.spawn[Request](behavior, s"${svcId}RpcClient")
    } yield LayerContext(actor, system.scheduler)
  }

  case class LayerContext(actor: ActorRef[Request], sc: Scheduler)

  /**
   * Provide rpc client actor behavior.
   */
  protected def provide(svcId: String): Behavior[Request] = Behaviors.setup { ctx =>
    ctx.log.info(s"Rpc client actor for [$svcId] started")
    val svcKey  = ServiceKey[Proto](svcId)
    var svcList = Set.empty[ActorRef[Proto]]
    ctx.system.receptionist ! Receptionist.Subscribe(svcKey, ctx.messageAdapter[Listing](RefreshSvcList))

    Behaviors
      .receiveMessage[Request] { msg =>
        println("@yulin" + msg.getClass.getName)
        msg match {
          case RefreshSvcList(list) =>
            svcList = list.serviceInstances(svcKey)
            ctx.log.info(s"Discover remote rpc service for [$svcId], address=[${svcList.map(_.path.toString).mkString(", ")}]")
            Behaviors.same
          case CallRemote(cmd) =>
            svcList.randomEle match {
              case Some(actor) => actor ! cmd
              case None        =>
                // type unsafe, huh
                val reply = cmd.productElement(cmd.productArity - 1).asInstanceOf[ActorRef[Any]]
                reply ! MayBe(Left(RpcServerNotFound(svcId)))
            }
            Behaviors.same
        }
      }
      .receiveSignal {
        case (ctx, PostStop) =>
          ctx.log.info(s"Rpc client actor for [$svcId] stopped")
          Behaviors.same
        case (ctx, PreRestart) =>
          ctx.log.info(s"Rpc client actor for [$svcId] restarted")
          Behaviors.same
      }
      .onFailure[Exception](restart)
  }

  /**
   * Binding actor receiving message behavior and ZIO behavior.
   */
  protected def bind[E, A](f: ActorRef[MayBe[E, A]] => Proto)(
      implicit actor: ActorRef[Request],
      sc: Scheduler,
      timeout: Timeout,
      actorErrAdapter: ActorInteropException => E): IO[E, A] = {
    actor
      .askZIO[MayBe[E, A]](ref => CallRemote(f(ref)))
      .mapError(actorErrAdapter)
      .flatMap {
        case MayBe(Left(e)) =>
          e match {
            case err: RpcServerNotFound => fail(actorErrAdapter(ActorInteropException(err)))
            case err                     => fail(err)
          }
        case MayBe(Right(a)) => succeed(a)
      }
  }

}
