package potamoi.cluster

import akka.actor.typed.Behavior
import potamoi.config.DDataConf
import potamoi.testkit.STActorClusterSpec

class ORSetDDataSpec extends STActorClusterSpec {

  "ORSetDData" should {

    "simple implementation" in {
      import SimpleOrSetActor._

      val spec = for {
        cache <- spawn(SimpleOrSetActor())
        _     <- (cache ?> Size).map(_ shouldBe 0)
        _     <- cache !> Put(1)
        _     <- cache !> Put(2)
        _     <- cache !> Put(3)
        _     <- (cache ?> List).map(_ shouldBe Set(1, 2, 3))
        _     <- (cache ?> (Contains(2, _))).map(_ shouldBe true)
        _     <- cache !> Remove(1)
        _     <- (cache ?> List).map(_ shouldBe Set(2, 3))
        _     <- cache !> Clear
        _     <- (cache ?> Size).map(_ shouldBe 0)
      } yield ()
      spec.runSpec
    }
  }

  object SimpleOrSetActor extends ORSetDData[Int] {
    val cacheId                = "simple"
    def apply(): Behavior[Cmd] = start(DDataConf())
  }

}
