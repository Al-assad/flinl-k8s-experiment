package potamoi.flink.observer

import akka.util.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import potamoi.config.DDataConf
import potamoi.flink.observer.RestEptCache._
import potamoi.flink.share.FlinkRestSvcEndpoint
import potamoi.testkit.STActorClusterSpec

class RestEptCacheSpec extends STActorClusterSpec {

  "ClusterRestEndpointDDataSpec" should {
    implicit val askTimeout: Timeout = 2.seconds

    "behavior normally" in {
      val endpoint = FlinkRestSvcEndpoint("app1", "n1", 8081, "10.2.1.0")
      val ef = for {
        actor <- spawn(RestEptCache(DDataConf()))
        _     <- actor !> Put("c1" -> "n1", endpoint)
        r     <- actor ?> (Get("c1" -> "n1", _))
        r1    <- actor ?> (Get("c1" -> "n1", _))
        _ = r1 shouldBe Some(endpoint)

        _  <- actor !> Put("c1" -> "n1", endpoint.copy(svcName = "app2"))
        r2 <- actor ?> (Get("c1" -> "n1", _))
        _ = r2 shouldBe Some(endpoint.copy(svcName = "app2"))

        _  <- actor !> Remove("c1" -> "n1")
        r3 <- actor ?> (Get("c1" -> "n1", _))
        _ = r3 shouldBe None
      } yield ()
      ef.runActorSpec
    }
  }

}
