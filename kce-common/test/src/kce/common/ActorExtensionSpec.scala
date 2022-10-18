package kce.common

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import testkit.STActorSpec

class ActorExtensionSpec extends STActorSpec {

  "ActorExtension" should {

    "ActorRefWrapper ZIO interop behavior" in {
      val actor = actorKit.spawn(Actor1())
      val ef1   = actor !> Actor1.Touch
      val ef2   = actor ?> (Actor1.CountLen("hello world", _))
      ef1.run.toEither shouldBe Right(())
      ef2.provide(actorSysLayer).run.toEither shouldBe Right(11)
    }

    "findActor from Receptionist" in {
      val key = ServiceKey[Actor1.Cmd]("actorService")
      actorKit.system.receptionist ! Receptionist.Register(key, actorKit.spawn(Actor1()))
      val ef = for {
        actor <- findActor(key)
        count <- actor ?> (Actor1.CountLen("hello world", _))
      } yield count
      ef.provide(actorSysLayer).run.toEither shouldBe Right(11)
    }
  }

  object Actor1 {
    sealed trait Cmd
    case object Touch                                       extends Cmd
    case class CountLen(word: String, reply: ActorRef[Int]) extends Cmd
    def apply(): Behavior[Cmd] = Behaviors.receive { (ctx, msg) =>
      msg match {
        case Touch =>
          ctx.log.info("touch")
          Behaviors.same
        case CountLen(word, reply) =>
          reply ! word.length
          Behaviors.same
      }
    }
  }
}
