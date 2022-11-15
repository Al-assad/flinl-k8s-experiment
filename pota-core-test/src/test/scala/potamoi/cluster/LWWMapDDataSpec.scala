package potamoi.cluster

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.util.Timeout
import potamoi.config.DDataConf
import potamoi.testkit.STActorClusterSpec

import scala.concurrent.duration.DurationInt

class LWWMapDDataSpec extends STActorClusterSpec {

  "LWWMapDData" should {
    implicit val askTimeout: Timeout = 5.seconds

    "default implementation" in {
      import SimpleLWWMapActor._

      val spec = for {
        cache <- spawn(SimpleLWWMapActor())
        _     <- cache !> Put("k1", "1")
        r1    <- cache ?> (Get("k1", _))
        _     <- pure(r1 shouldBe Some("1"))
        _     <- cache !> Remove("k1")
        r2    <- cache ?> (Get("k1", _))
        _     <- pure(r2 shouldBe None)

        _  <- cache !> Put("k1", "1")
        _  <- cache !> Put("k2", "2")
        _  <- cache !> Put("k3", "3")
        r3 <- cache ?> Size
        _  <- pure(r3 shouldBe 3)
        r4 <- cache ?> ListKeys
        _  <- pure(r4 shouldBe Set("k1", "k2", "k3"))
        r5 <- cache ?> ListAll
        _ <- pure(
          r5 shouldBe Map(
            "k1" -> "1",
            "k2" -> "2",
            "k3" -> "3"
          ))
        r6 <- cache ?> Size
        _  <- pure(r6 shouldBe 3)
        r7 <- cache ?> (Contains("k2", _))
        _  <- pure(r7 shouldBe true)
      } yield ()
      spec.runActorSpec
    }

    "additional get/update implementation" in {
      import ExtLWWMapActor._

      val spec = for {
        cache <- spawn(ExtLWWMapActor())
        _     <- cache !> Put("k1", 1)
        _     <- cache !> Put("k2", 2)
        _     <- cache !> Put("k3", 3)
        _     <- cache !> Put("k4", 4)

        r1 <- cache ?> (BiggerThan(2, _))
        _  <- pure(r1 shouldBe Set("k3", "k4"))
        r2 <- cache ?> (LessThan(3, _))
        _  <- pure(r2 shouldBe Set("k1", "k2"))
        _  <- cache !> Incr("k1", 10)
        r3 <- cache ?> (Get("k1", _))
        _  <- pure(r3 shouldBe Some(11))
      } yield ()
      spec.runActorSpec
    }
  }

  object SimpleLWWMapActor extends LWWMapDData[String, String] {

    val cacheId = "simple"

    def apply(): Behavior[Cmd] = Behaviors.setup { implicit ctx =>
      implicit val node             = DistributedData(ctx.system).selfUniqueAddress
      implicit val timeout: Timeout = 2.seconds
      action(DDataConf())()
    }
  }

  object ExtLWWMapActor extends LWWMapDData[String, Int] {

    final case class BiggerThan(n: Int, reply: ActorRef[Set[String]]) extends GetCmd
    final case class LessThan(n: Int, reply: ActorRef[Set[String]])   extends GetCmd
    final case class Incr(key: String, inc: Int)                      extends UpdateCmd

    val cacheId = "ext"

    def apply(): Behavior[Cmd] = Behaviors.setup { implicit ctx =>
      implicit val node = DistributedData(ctx.system).selfUniqueAddress
      action(DDataConf())(
        get = {
          case (BiggerThan(n, reply), map) => reply ! map.entries.filter(_._2 > n).keySet
          case (LessThan(n, reply), map)   => reply ! map.entries.filter(_._2 < n).keySet
        },
        notYetInit = {
          case BiggerThan(_, reply) => reply ! Set.empty
          case LessThan(_, reply)   => reply ! Set.empty
        },
        update = { case (Incr(key, inc), map) =>
          map.get(key) match {
            case Some(value) => map :+ (key, value + inc)
            case None        => map
          }
        }
      )
    }
  }

}
