package potamoi.common

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import potamoi.testkit.STActorSpec
import potamoi.timex._

class ActorExtensionSpec extends STActorSpec {

  "ActorExtension" should {
    implicit val timeout: Timeout = 10.seconds
    "ActorRefWrapper ZIO interop behavior" in {
      val actor = actorKit.spawn(TestActor())
      val ef1   = actor !> TestActor.Touch
      val ef2   = actor ?> (TestActor.CountLen("hello world", _))
      ef1.runActorSpec shouldBe ()
      ef2.runActorSpec shouldBe 11
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
