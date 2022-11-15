package potamoi.cluster

import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, PostStop, PreRestart}
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.cluster.Rpc.MayBe
import potamoi.common.ActorExtension._
import potamoi.common.ActorInteropException
import potamoi.common.ZIOExtension._
import zio.{IO, ZIO}

import scala.reflect.ClassTag

abstract class RpcServerActor[Proto: ClassTag] {

  /**
   * Provide rpc client actor.
   */
  protected def provideActor(svcId: String)(bindShapes: Proto => Behavior[Proto]): ZIO[ActorGuardian, ActorInteropException, ActorRef[Proto]] = {
    for {
      system <- ZIO.service[ActorGuardian]
      actor  <- system.spawn[Proto](provide(svcId)(bindShapes), s"${svcId}RpcServer")
    } yield actor
  }

  /**
   * Provide rpc server actor behavior.
   */
  protected def provide(svcId: String)(bindShapes: Proto => Behavior[Proto]): Behavior[Proto] = Behaviors.setup { ctx =>
    ctx.log.info(s"Rpc server actor for [$svcId] started")
    val svcKey = ServiceKey[Proto](svcId)
    ctx.system.receptionist ! Receptionist.Register(svcKey, ctx.self)
    Behaviors
      .receiveMessage[Proto] { req =>
        bindShapes(req)
        Behaviors.same
      }
      .receiveSignal {
        case (ctx, PostStop) =>
          ctx.log.info(s"Rpc server actor for [$svcId] stopped")
          ctx.system.receptionist ! Receptionist.Deregister(svcKey, ctx.self)
          Behaviors.same
        case (ctx, PreRestart) =>
          ctx.log.info(s"Rpc server actor for [$svcId] restarted")
          ctx.system.receptionist ! Receptionist.Register(svcKey, ctx.self)
          Behaviors.same
      }
      .onFailure[Exception](restart)
  }

  /**
   * Binding actor receiving message behavior and ZIO behavior.
   * TODO resolve logger layers
   */
  protected def bind[E, A](reply: ActorRef[MayBe[E, A]], f: => IO[E, A]): Behavior[Proto] = {
    f.either.map(reply ! MayBe(_)).runToFuture
    Behaviors.same
  }
}
