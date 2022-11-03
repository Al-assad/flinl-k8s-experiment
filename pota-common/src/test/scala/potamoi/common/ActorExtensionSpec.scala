package potamoi.common

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import potamoi.testkit.STActorSpec
import zio.ZIO.attempt

class ActorExtensionSpec extends STActorSpec {

  "ActorExtension" should {

    "ActorRefWrapper ZIO interop behavior" in {
      val actor = actorKit.spawn(TestActor())
      val ef1   = actor !> TestActor.Touch
      val ef2   = actor ?> (TestActor.CountLen("hello world", _))
      ef1.runActorSpec shouldBe ()
      ef2.runActorSpec shouldBe 11
    }

    "findActor from Receptionist" in {
      val key = ServiceKey[TestActor.Cmd]("actorService")
      val ef = for {
        _     <- attempt(actorKit.system.receptionist ! Receptionist.Register(key, actorKit.spawn(TestActor())))
        actor <- findActor(key)
        count <- actor ?> (TestActor.CountLen("hello world", _))
      } yield count
      ef.runActorSpec shouldBe 11
    }
  }

}

object TestActor {
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
